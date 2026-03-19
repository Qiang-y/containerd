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
	"context"
	"fmt"
	"strconv"

	"github.com/containerd/containerd/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RestoreContainerRequest is the request message for RestoreContainer RPC.
// It mirrors CheckpointContainerRequest from CRI v1 proto for simplicity,
// with v2 extensions for sandbox netns mapping.
type RestoreContainerRequest struct {
	ContainerId     string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	Location        string `protobuf:"bytes,2,opt,name=location,proto3" json:"location,omitempty"`
	Timeout         int64  `protobuf:"varint,3,opt,name=timeout,proto3" json:"timeout,omitempty"`
	// v2: Path to the new sandbox's network namespace (e.g. /proc/<sandbox_pid>/ns/net).
	// Empty means no netns mapping (v1 mode: sandbox preserved).
	SandboxNetnsPath string `protobuf:"bytes,4,opt,name=sandbox_netns_path,json=sandboxNetnsPath,proto3" json:"sandbox_netns_path,omitempty"`
	// v2: Inode number of the old netns recorded during checkpoint.
	// Zero means no netns mapping.
	OldNetnsInode    uint64 `protobuf:"varint,5,opt,name=old_netns_inode,json=oldNetnsInode,proto3" json:"old_netns_inode,omitempty"`
}

func (m *RestoreContainerRequest) Reset()         { *m = RestoreContainerRequest{} }
func (m *RestoreContainerRequest) String() string  { return fmt.Sprintf("%+v", *m) }
func (m *RestoreContainerRequest) ProtoMessage()   {}

// RestoreContainerResponse is the response message for RestoreContainer RPC.
type RestoreContainerResponse struct{}

func (m *RestoreContainerResponse) Reset()         { *m = RestoreContainerResponse{} }
func (m *RestoreContainerResponse) String() string { return "RestoreContainerResponse{}" }
func (m *RestoreContainerResponse) ProtoMessage()  {}

// restoreContainerHandler wraps the criService to handle RestoreContainer gRPC calls.
type restoreContainerHandler struct {
	cri *criService
}

// RestoreContainer handles the RestoreContainer gRPC call.
func (h *restoreContainerHandler) RestoreContainer(ctx context.Context, req *RestoreContainerRequest) (*RestoreContainerResponse, error) {
	// v2: Read sandboxNetnsPath and oldNetnsInode from gRPC metadata headers.
	// These cannot be reliably passed via proto struct tags across different proto libraries,
	// so we use gRPC metadata as a workaround.
	sandboxNetnsPath := req.SandboxNetnsPath
	oldNetnsInode := req.OldNetnsInode
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-criu-sandbox-netns-path"); len(vals) > 0 && vals[0] != "" {
			sandboxNetnsPath = vals[0]
		}
		if vals := md.Get("x-criu-old-netns-inode"); len(vals) > 0 {
			if inode, err := strconv.ParseUint(vals[0], 10, 64); err == nil {
				oldNetnsInode = inode
			}
		}
	}

	log.G(ctx).Infof("RestoreContainer gRPC handler called: containerID=%s, location=%s, sandboxNetnsPath=%s, oldNetnsInode=%d",
		req.ContainerId, req.Location, sandboxNetnsPath, oldNetnsInode)

	if err := h.cri.RestoreContainer(ctx, req.ContainerId, req.Location, sandboxNetnsPath, oldNetnsInode); err != nil {
		log.G(ctx).WithError(err).Errorf("RestoreContainer failed for container %s", req.ContainerId)
		return nil, err
	}

	return &RestoreContainerResponse{}, nil
}

// registerRestoreContainerService registers the custom RestoreContainer gRPC method
// on the same service path as CRI RuntimeService.
// This allows kubelet to call /runtime.v1.RuntimeService/RestoreContainer via grpc.Invoke.
func registerRestoreContainerService(s *grpc.Server, cri *criService) {
	handler := &restoreContainerHandler{cri: cri}

	// Register a custom gRPC service descriptor that adds RestoreContainer
	// to the runtime.v1.RuntimeService service path.
	sd := &grpc.ServiceDesc{
		ServiceName: "runtime.v1.RuntimeService",
		HandlerType: (*interface{})(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "RestoreContainer",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					req := new(RestoreContainerRequest)
					if err := dec(req); err != nil {
						return nil, err
					}
					if interceptor == nil {
						return handler.RestoreContainer(ctx, req)
					}
					info := &grpc.UnaryServerInfo{
						Server:     srv,
						FullMethod: "/runtime.v1.RuntimeService/RestoreContainer",
					}
					return interceptor(ctx, req, info, func(ctx context.Context, req interface{}) (interface{}, error) {
						return handler.RestoreContainer(ctx, req.(*RestoreContainerRequest))
					})
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}

	// We can't register the same service name twice, so we use a slightly modified name
	// and rely on the gRPC unknown service handler or register it differently.
	// Actually, since gRPC server won't allow two services with the same name,
	// we register with a custom service name and have kubelet call that instead.
	sd.ServiceName = "runtime.v1.ContainerRestoreService"
	s.RegisterService(sd, handler)

	log.L.Info("Custom RestoreContainer gRPC service registered as runtime.v1.ContainerRestoreService")
}
