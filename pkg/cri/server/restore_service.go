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

	"github.com/containerd/containerd/log"
	"google.golang.org/grpc"
)

// RestoreContainerRequest is the request message for RestoreContainer RPC.
// It mirrors CheckpointContainerRequest from CRI v1 proto for simplicity.
type RestoreContainerRequest struct {
	ContainerId string `protobuf:"bytes,1,opt,name=container_id,json=containerId,proto3" json:"container_id,omitempty"`
	Location    string `protobuf:"bytes,2,opt,name=location,proto3" json:"location,omitempty"`
	Timeout     int64  `protobuf:"varint,3,opt,name=timeout,proto3" json:"timeout,omitempty"`
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
	log.G(ctx).Infof("RestoreContainer gRPC handler called: containerID=%s, location=%s", req.ContainerId, req.Location)

	if err := h.cri.RestoreContainer(ctx, req.ContainerId, req.Location); err != nil {
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
