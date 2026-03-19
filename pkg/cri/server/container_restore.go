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
	"strings"
	"time"

	"github.com/containerd/containerd"
	containerdio "github.com/containerd/containerd/cio"
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/nri"
	v1 "github.com/containerd/nri/types/v1"
	runtimespec "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/containerd/typeurl"
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
// 3. (v2) If sandboxNetnsPath + oldNetnsInode are provided:
//    a. Generate a temporary CRIU config file with `tcp-close` + `external net[<old>]:netns[<new>]`
//    b. Update the container's OCI spec annotations with `org.criu.config=<config_file>`
//    c. runc reads the annotation, passes the config file to CRIU via RPC ConfigFile
// 4. Create a new task from the CRIU checkpoint using WithRestoreImagePath
// 5. Start the restored task
// 6. Clean up: remove temporary config file, restore original annotations
//
// Parameters:
//   - containerID: the container to restore
//   - checkpointPath: path to the CRIU checkpoint directory
//   - sandboxNetnsPath: (v2) path to the new sandbox's network namespace (e.g. /proc/<sandbox_pid>/ns/net).
//     Empty string means no netns mapping (v1 mode: sandbox was preserved).
//   - oldNetnsInode: (v2) the inode number of the old netns recorded during checkpoint.
//     Zero means no netns mapping.
func (c *criService) RestoreContainer(ctx context.Context, containerID string, checkpointPath string,
	sandboxNetnsPath string, oldNetnsInode uint64) error {
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

	log.G(ctx).Infof("RestoreContainer: starting restore for container %q from %s (sandboxNetnsPath=%s, oldNetnsInode=%d)",
		id, checkpointPath, sandboxNetnsPath, oldNetnsInode)

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
	sandboxAvailable := (err == nil)
	if err != nil {
		// v2: If sandboxNetnsPath is provided, the old sandbox may have been destroyed.
		// The caller (kubelet) has already created a new sandbox and confirmed it's ready.
		if sandboxNetnsPath != "" {
			log.G(ctx).Infof("RestoreContainer: old sandbox %q not found (expected in v2 mode), proceeding with netns mapping", meta.SandboxID)
		} else {
			return fmt.Errorf("sandbox %q not found: %w", meta.SandboxID, err)
		}
	}
	sandboxID := meta.SandboxID
	// v2: Skip sandbox state check when sandboxNetnsPath is provided.
	if sandboxNetnsPath == "" {
		// v1 mode: sandbox must be running
		if sandboxAvailable && sandbox.Status.Get().State != sandboxstore.StateReady {
			return fmt.Errorf("sandbox container %q is not running", sandboxID)
		}
	} else {
		log.G(ctx).Infof("RestoreContainer: v2 mode — skipping sandbox state check (old sandbox %q may be NotReady, new netns=%s)",
			sandboxID, sandboxNetnsPath)
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

	// ===== v2: Generate temporary CRIU config file and set org.criu.config annotation =====
	var criuConfigPath string
	if sandboxNetnsPath != "" && oldNetnsInode > 0 {
		criuConfigPath, err = c.setupCriuRestoreConfig(ctx, container, id, sandboxNetnsPath, oldNetnsInode)
		if err != nil {
			return fmt.Errorf("failed to setup CRIU restore config for container %q: %w", id, err)
		}
		// Ensure cleanup of the temporary config file and annotation restoration
		defer c.cleanupCriuRestoreConfig(ctx, container, id, criuConfigPath)
	}
	// ===== end v2 =====

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

	// Build task options with checkpoint restore path
	taskOpts := c.taskOpts(ctrInfo.Runtime.Name)
	// Get OCI runtime path from sandbox config (if available)
	if sandboxAvailable {
		ociRuntime, err := c.getSandboxRuntime(sandbox.Config, sandbox.Metadata.RuntimeHandler)
		if err != nil {
			return fmt.Errorf("failed to get sandbox runtime: %w", err)
		}
		if ociRuntime.Path != "" {
			taskOpts = append(taskOpts, containerd.WithRuntimePath(ociRuntime.Path))
		}
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
		var sandboxLabels map[string]string
		if sandboxAvailable {
			sandboxLabels = sandbox.Config.Labels
		}
		nriSB := &nri.Sandbox{
			ID:     sandboxID,
			Labels: sandboxLabels,
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

// setupCriuRestoreConfig generates a temporary CRIU config file and updates the container's
// OCI spec annotations with org.criu.config pointing to the config file.
//
// This implements "方案 A" for passing CRIU --external net[...] parameter:
//   1. Write a temp config file with `tcp-close` and `external net[<old_inode>]:netns[<new_path>]`
//   2. Update the container's OCI spec to add `org.criu.config=<config_file_path>` annotation
//   3. When runc reads the OCI config.json during restore, it finds the annotation
//   4. runc's handleCriuConfigurationFile() sets rpcOpts.ConfigFile to our config file
//   5. CRIU reads the config file and gets the --external and --tcp-close options
//
// The config file has highest priority (RPC config_file overrides /etc/criu/runc.conf).
func (c *criService) setupCriuRestoreConfig(ctx context.Context, container containerd.Container,
	containerID string, sandboxNetnsPath string, oldNetnsInode uint64) (string, error) {

	// 1. Generate the temporary CRIU config file
	criuConfigPath := filepath.Join(os.TempDir(), fmt.Sprintf("criu-restore-%s.conf", containerID))
	configContent := fmt.Sprintf(
		"# CRIU restore config for container %s (auto-generated, will be cleaned up)\n"+
			"tcp-close\n"+
			"external net[%d]:netns[%s]\n",
		containerID, oldNetnsInode, sandboxNetnsPath,
	)

	if err := os.WriteFile(criuConfigPath, []byte(configContent), 0644); err != nil {
		return "", fmt.Errorf("failed to write CRIU config file %s: %w", criuConfigPath, err)
	}
	log.G(ctx).Infof("RestoreContainer: wrote CRIU config file %s: tcp-close + external net[%d]:netns[%s]",
		criuConfigPath, oldNetnsInode, sandboxNetnsPath)

	// 2. Update the container's OCI spec annotations with org.criu.config
	//    This follows the same pattern as sandbox_run.go which updates container spec after network setup.
	ctrInfo, err := container.Info(ctx)
	if err != nil {
		os.Remove(criuConfigPath)
		return "", fmt.Errorf("failed to get container info: %w", err)
	}

	spec := &runtimespec.Spec{}
	if err := typeurl.UnmarshalTo(ctrInfo.Spec, spec); err != nil {
		os.Remove(criuConfigPath)
		return "", fmt.Errorf("failed to unmarshal container spec: %w", err)
	}

	// Add org.criu.config annotation
	if spec.Annotations == nil {
		spec.Annotations = make(map[string]string)
	}
	spec.Annotations["org.criu.config"] = criuConfigPath

	// v2: Update ALL namespace paths in OCI spec that point to the old (destroyed) sandbox's /proc/<old_pid>/ns/.
	// The old sandbox process is gone, so /proc/<old_pid>/ns/{net,ipc,uts} no longer exist.
	// We need to rewrite them to use the new sandbox's PID. The sandboxNetnsPath is like /proc/<new_pid>/ns/net,
	// so we extract the new sandbox PID from it and replace old PID in all namespace paths.
	if spec.Linux != nil {
		// Extract new sandbox PID from sandboxNetnsPath (format: /proc/<pid>/ns/net)
		newSandboxPid := ""
		parts := strings.Split(sandboxNetnsPath, "/")
		if len(parts) >= 4 && parts[1] == "proc" && parts[3] == "ns" {
			newSandboxPid = parts[2]
		}

		if newSandboxPid != "" {
			for i, ns := range spec.Linux.Namespaces {
				if ns.Path == "" {
					continue
				}
				// Check if this namespace path is a /proc/<pid>/ns/<type> path
				nsParts := strings.Split(ns.Path, "/")
				if len(nsParts) >= 4 && nsParts[1] == "proc" && nsParts[3] == "ns" {
					oldPid := nsParts[2]
					if oldPid != newSandboxPid {
						// Replace old PID with new sandbox PID
						nsParts[2] = newSandboxPid
						newPath := strings.Join(nsParts, "/")
						log.G(ctx).Infof("RestoreContainer: updated OCI spec %s namespace path: %s -> %s",
							ns.Type, ns.Path, newPath)
						spec.Linux.Namespaces[i].Path = newPath
					}
				}
			}
		} else {
			log.G(ctx).Warnf("RestoreContainer: could not extract sandbox PID from sandboxNetnsPath %q, only updating network ns", sandboxNetnsPath)
			for i, ns := range spec.Linux.Namespaces {
				if ns.Type == runtimespec.NetworkNamespace {
					oldPath := ns.Path
					spec.Linux.Namespaces[i].Path = sandboxNetnsPath
					log.G(ctx).Infof("RestoreContainer: updated OCI spec network namespace path: %s -> %s", oldPath, sandboxNetnsPath)
					break
				}
			}
		}
	}

	// Update the container with the modified spec
	if err := container.Update(ctx,
		containerd.UpdateContainerOpts(containerd.WithSpec(spec)),
	); err != nil {
		os.Remove(criuConfigPath)
		return "", fmt.Errorf("failed to update container spec with org.criu.config annotation: %w", err)
	}
	log.G(ctx).Infof("RestoreContainer: set org.criu.config=%s on container %q OCI spec", criuConfigPath, containerID)

	return criuConfigPath, nil
}

// cleanupCriuRestoreConfig removes the temporary CRIU config file and restores the container's
// OCI spec by removing the org.criu.config annotation.
func (c *criService) cleanupCriuRestoreConfig(ctx context.Context, container containerd.Container,
	containerID string, criuConfigPath string) {

	// 1. Remove the temporary config file
	if err := os.Remove(criuConfigPath); err != nil && !os.IsNotExist(err) {
		log.G(ctx).WithError(err).Warnf("RestoreContainer: failed to remove CRIU config file %s", criuConfigPath)
	} else {
		log.G(ctx).Infof("RestoreContainer: cleaned up CRIU config file %s", criuConfigPath)
	}

	// 2. Remove org.criu.config annotation from container spec
	//    This ensures the annotation doesn't persist and affect future operations
	//    (e.g., if the container is checkpointed again and restored without netns mapping)
	ctrInfo, err := container.Info(ctx)
	if err != nil {
		log.G(ctx).WithError(err).Warn("RestoreContainer: failed to get container info for annotation cleanup")
		return
	}

	spec := &runtimespec.Spec{}
	if err := typeurl.UnmarshalTo(ctrInfo.Spec, spec); err != nil {
		log.G(ctx).WithError(err).Warn("RestoreContainer: failed to unmarshal spec for annotation cleanup")
		return
	}

	if _, ok := spec.Annotations["org.criu.config"]; ok {
		delete(spec.Annotations, "org.criu.config")
		if err := container.Update(ctx,
			containerd.UpdateContainerOpts(containerd.WithSpec(spec)),
		); err != nil {
			log.G(ctx).WithError(err).Warn("RestoreContainer: failed to remove org.criu.config annotation")
		} else {
			log.G(ctx).Infof("RestoreContainer: removed org.criu.config annotation from container %q", containerID)
		}
	}
}

// GetCheckpointPath returns the checkpoint path for a container, or empty string if not checkpointed.
func (c *criService) GetCheckpointPath(containerID string) string {
	return filepath.Join(c.getContainerRootDir(containerID), "checkpoint")
}
