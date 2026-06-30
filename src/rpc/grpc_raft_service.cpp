#include "rpc/grpc_raft_service.h"

#include "rpc/proto_conversion.h"

namespace raftkv::rpc {

grpc::Status GrpcRaftService::RequestVote(
    grpc::ServerContext*, const proto::RequestVoteRequest* request,
    proto::RequestVoteResponse* response) {
  *response = toProto(node_.handleRequestVote(fromProto(*request)));
  return grpc::Status::OK;
}

grpc::Status GrpcRaftService::AppendEntries(
    grpc::ServerContext*, const proto::AppendEntriesRequest* request,
    proto::AppendEntriesResponse* response) {
  *response = toProto(node_.handleAppendEntries(fromProto(*request)));
  return grpc::Status::OK;
}

grpc::Status GrpcRaftService::InstallSnapshot(
    grpc::ServerContext*, const proto::InstallSnapshotRequest* request,
    proto::InstallSnapshotResponse* response) {
  *response = toProto(node_.handleInstallSnapshot(fromProto(*request)));
  return grpc::Status::OK;
}

grpc::Status GrpcRaftService::ClientCommand(
    grpc::ServerContext*, const proto::ClientCommandRequest* request,
    proto::ClientCommandResponse* response) {
  *response = toProto(node_.submit(fromProto(request->command())));
  return grpc::Status::OK;
}

}  // namespace raftkv::rpc
