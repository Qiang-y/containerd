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
	"github.com/containerd/containerd/errdefs"
	"github.com/containerd/containerd/log"
	"github.com/containerd/containerd/runtime/v2/runc/options"
	"golang.org/x/net/context"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"

	containerstore "github.com/containerd/containerd/pkg/cri/store/container"
)

// CheckpointContainer checkpoints a running container using CRIU.
// The container process is frozen and its state is saved to disk.
// After checkpoint, the container process exits (CRIU dump behavior).
func (c *criService) CheckpointContainer(ctx context.Context, r *runtime.CheckpointContainerRequest) (*runtime.CheckpointContainerResponse, error) {
	start := time.Now()
	containerID := r.GetContainerId()

	// Get the container from store
	cntr, err := c.containerStore.Get(containerID)
	if err != nil {
		return nil, fmt.Errorf("failed to find container %q: %w", containerID, err)
	}

	// Verify container is running
	state := cntr.Status.Get().State()
	if state != runtime.ContainerState_CONTAINER_RUNNING {
		return nil, fmt.Errorf("container %q is not running (state: %s), cannot checkpoint",
			containerID, criContainerStateToString(state))
	}

	id := cntr.ID
	log.G(ctx).Infof("CheckpointContainer: starting checkpoint for container %q", id)

	// Get the containerd task
	task, err := cntr.Container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("no task found for container %q: %w", id, err)
		}
		return nil, fmt.Errorf("failed to get task for container %q: %w", id, err)
	}

	// Determine checkpoint path
	checkpointDir := r.GetLocation()
	if checkpointDir == "" {
		// Default: store checkpoint under container root dir
		checkpointDir = filepath.Join(c.getContainerRootDir(id), "checkpoint")
	}

	// Create checkpoint directory
	if err := os.MkdirAll(checkpointDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory %q: %w", checkpointDir, err)
	}

	log.G(ctx).Infof("CheckpointContainer: checkpoint dir=%s, container=%s", checkpointDir, id)

	// Call containerd task.Checkpoint with CRIU options:
	// - Exit=true: container process exits after checkpoint (CRIU dump, no --leave-running)
	// - OpenTcp=true: allow checkpointing open TCP connections (--tcp-established)
	// - ImagePath: where to store the CRIU checkpoint image files
	_, err = task.Checkpoint(ctx, func(info *containerd.CheckpointTaskInfo) error {
		info.Options = &options.CheckpointOptions{
			Exit:                true,
			OpenTcp:             true,
			ExternalUnixSockets: true,
			FileLocks:           true,
			ImagePath:           checkpointDir,
			WorkPath:            filepath.Join(checkpointDir, "criu-work"),
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to checkpoint container %q: %w", id, err)
	}

	elapsed := time.Since(start)
	log.G(ctx).Infof("CheckpointContainer: checkpoint completed for container %q in %v, path=%s",
		id, elapsed, checkpointDir)

	// Update container status to mark it as checkpointed
	if err := cntr.Status.UpdateSync(func(status containerstore.Status) (containerstore.Status, error) {
		// Keep the container in a state where kubelet knows it was checkpointed
		status.Reason = "Checkpointed"
		status.Message = fmt.Sprintf("Container checkpointed at %s", checkpointDir)
		return status, nil
	}); err != nil {
		log.G(ctx).WithError(err).Warnf("CheckpointContainer: failed to update status for container %q", id)
		// Non-fatal: checkpoint already succeeded
	}

	return &runtime.CheckpointContainerResponse{}, nil
}
