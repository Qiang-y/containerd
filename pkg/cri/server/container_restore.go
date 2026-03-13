/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/containerd"
	containerdio "github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/nri"
	v1 "github.com/containerd/nri/types/v1"
	"golang.org/x/net/context"

	cio "github.com/containerd/containerd/pkg/cri/io"
	containerstore "github.com/containerd/containerd/pkg/cri/store/container"
	sandboxstore "github.com/containerd/containerd/pkg/cri/store/sandbox"
	ctrdutil "github.com/containerd/containerd/pkg/cri/util"
)

// RestoreContainer restores a previously checkpointed container from its CRIU checkpoint.
// This is NOT a standard CRI RPC — it's a custom extension for the CRIU checkpoint/restore PoC.
// It is called internally by kubelet's restore handler.
//
// The restore process:
// 1. Verify the container exists and was previously checkpointed
// 2. Delete the old (exited) task if it exists
// 3. Create a new task from the CRIU checkpoint using WithRestoreImagePath
// 4. Start the restored task
func (c *criService) RestoreContainer(ctx context.Context, containerID string, checkpointPath string) error {
	start := time.Now()

	// Get the container from store
	cntr, err := c.containerStore.Get(containerID)
	if err != nil {
		return fmt.Errorf("failed to find container %q: %w", containerID, err)
	}

	id := cntr.ID
	meta := cntr.Metadata
	container := cntr.Container
	config := meta.Config

	log.G(ctx).Infof("RestoreContainer: starting restore for container %q from %s", id, checkpointPath)

	// Verify checkpoint files exist
	if _, err := os.Stat(checkpointPath); os.IsNotExist(err) {
		return fmt.Errorf("checkpoint path %q does not exist", checkpointPath)
	}
	// Check for core CRIU dump files
	if _, err := os.Stat(filepath.Join(checkpointPath, "core-1.img")); os.IsNotExist(err) {
		return fmt.Errorf("checkpoint at %q appears invalid (no core-1.img)", checkpointPath)
	}

	// Get sandbox config from sandbox store
	sandbox, err := c.sandboxStore.Get(meta.SandboxID)
	if err != nil {
		return fmt.Errorf("sandbox %q not found: %w", meta.SandboxID, err)
	}
	sandboxID := meta.SandboxID
	if sandbox.Status.Get().State != sandboxstore.StateReady {
		return fmt.Errorf("sandbox container %q is not running", sandboxID)
	}

	// Delete old task if it exists (checkpoint leaves the task in exited state)
	oldTask, err := container.Task(ctx, nil)
	if err == nil {
		log.G(ctx).Infof("RestoreContainer: deleting old task for container %q (pid=%d)", id, oldTask.Pid())
		if _, err := oldTask.Delete(ctx, containerd.WithProcessKill); err != nil && !errdefs.IsNotFound(err) {
			return fmt.Errorf("failed to delete old task for container %q: %w", id, err)
		}
	} else if !errdefs.IsNotFound(err) {
		return fmt.Errorf("failed to check task for container %q: %w", id, err)
	}

	// Close old I/O (the old container's FIFO pipes are already closed/EOF)
	if cntr.IO != nil {
		cntr.IO.Close()
	}

	// Create brand new I/O for the restored container.
	// We cannot reuse the old cntr.IO because its FIFO pipes are closed after checkpoint.
	volatileContainerRootDir := c.getVolatileContainerRootDir(id)
	newContainerIO, err := cio.NewContainerIO(id,
		cio.WithNewFIFOs(volatileContainerRootDir, config.GetTty(), config.GetStdin()),
	)
	if err != nil {
		return fmt.Errorf("failed to create new container IO for restore: %w", err)
	}
	cntr.IO = newContainerIO

	ioCreation := func(id string) (_ containerdio.IO, err error) {
		stdoutWC, stderrWC, err := c.createContainerLoggers(meta.LogPath, config.GetTty())
		if err != nil {
			return nil, fmt.Errorf("failed to create container loggers: %w", err)
		}
		cntr.IO.AddOutput("log", stdoutWC, stderrWC)
		cntr.IO.Pipe()
		return cntr.IO, nil
	}

	ctrInfo, err := container.Info(ctx)
	if err != nil {
		return fmt.Errorf("failed to get container info: %w", err)
	}

	ociRuntime, err := c.getSandboxRuntime(sandbox.Config, sandbox.Metadata.RuntimeHandler)
	if err != nil {
		return fmt.Errorf("failed to get sandbox runtime: %w", err)
	}

	// Build task options with checkpoint restore path
	taskOpts := c.taskOpts(ctrInfo.Runtime.Name)
	if ociRuntime.Path != "" {
		taskOpts = append(taskOpts, containerd.WithRuntimePath(ociRuntime.Path))
	}
	// Key: use WithRestoreImagePath to restore from CRIU checkpoint
	taskOpts = append(taskOpts, containerd.WithRestoreImagePath(checkpointPath))

	log.G(ctx).Infof("RestoreContainer: creating new task for container %q with checkpoint=%s", id, checkpointPath)

	// Create new task from checkpoint
	task, err := container.NewTask(ctx, ioCreation, taskOpts...)
	if err != nil {
		return fmt.Errorf("failed to create restored task for container %q: %w", id, err)
	}

	// Set up exit monitoring
	exitCh, err := task.Wait(ctrdutil.NamespacedContext())
	if err != nil {
		// Clean up task on failure
		task.Delete(ctx, containerd.WithProcessKill)
		return fmt.Errorf("failed to wait for restored task: %w", err)
	}

	// NRI notification
	nric, err := nri.New()
	if err != nil {
		log.G(ctx).WithError(err).Error("unable to create nri client")
	}
	if nric != nil {
		nriSB := &nri.Sandbox{
			ID:     sandboxID,
			Labels: sandbox.Config.Labels,
		}
		if _, err := nric.InvokeWithSandbox(ctx, task, v1.Create, nriSB); err != nil {
			task.Delete(ctx, containerd.WithProcessKill)
			return fmt.Errorf("nri invoke: %w", err)
		}
	}

	// Start the restored task (this triggers runc restore → CRIU restore)
	if err := task.Start(ctx); err != nil {
		task.Delete(ctx, containerd.WithProcessKill)
		return fmt.Errorf("failed to start restored task %q: %w", id, err)
	}

	// Update container state
	if err := cntr.Status.UpdateSync(func(status containerstore.Status) (containerstore.Status, error) {
		status.Pid = task.Pid()
		status.StartedAt = time.Now().UnixNano()
		status.FinishedAt = 0
		status.ExitCode = 0
		status.Reason = ""
		status.Message = ""
		return status, nil
	}); err != nil {
		return fmt.Errorf("failed to update container %q state: %w", id, err)
	}

	// Start exit monitor
	c.eventMonitor.startContainerExitMonitor(context.Background(), id, task.Pid(), exitCh)

	elapsed := time.Since(start)
	log.G(ctx).Infof("RestoreContainer: restore completed for container %q in %v, new pid=%d", id, elapsed, task.Pid())

	return nil
}

// GetCheckpointPath returns the checkpoint path for a container, or empty string if not checkpointed.
func (c *criService) GetCheckpointPath(containerID string) string {
	return filepath.Join(c.getContainerRootDir(containerID), "checkpoint")
}
