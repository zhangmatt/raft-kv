#pragma once

#include "raft/raft_node.h"
#include "raft.grpc.pb.h"

namespace raftkv::rpc {

class GrpcRaftService final : public proto::Raft::Service {
 public:
  explicit GrpcRaftService(RaftNode& node) : node_(node) {}

  grpc::Status RequestVote(grpc::ServerContext* context,
                           const proto::RequestVoteRequest* request,
                           proto::RequestVoteResponse* response) override;

  grpc::Status AppendEntries(grpc::ServerContext* context,
                             const proto::AppendEntriesRequest* request,
                             proto::AppendEntriesResponse* response) override;

  grpc::Status InstallSnapshot(
      grpc::ServerContext* context,
      const proto::InstallSnapshotRequest* request,
      proto::InstallSnapshotResponse* response) override;

  grpc::Status ClientCommand(grpc::ServerContext* context,
                             const proto::ClientCommandRequest* request,
                             proto::ClientCommandResponse* response) override;

 private:
  RaftNode& node_;
};

}  // namespace raftkv::rpc
