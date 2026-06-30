#pragma once

#include "raft/transport.h"
#include "raft.grpc.pb.h"

#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <string>

namespace raftkv::rpc {

class GrpcPeerClient final : public IRaftTransport {
 public:
  explicit GrpcPeerClient(std::map<NodeId, std::string> peer_addresses,
                          std::chrono::milliseconds rpc_timeout =
                              std::chrono::milliseconds(300));

  std::optional<RequestVoteResponse> sendRequestVote(
      NodeId from, NodeId to, const RequestVoteRequest& request) override;

  std::optional<AppendEntriesResponse> sendAppendEntries(
      NodeId from, NodeId to, const AppendEntriesRequest& request) override;

  std::optional<InstallSnapshotResponse> sendInstallSnapshot(
      NodeId from, NodeId to, const InstallSnapshotRequest& request) override;

 private:
  proto::Raft::Stub* stubFor(NodeId node_id);

  std::map<NodeId, std::string> peer_addresses_;
  std::map<NodeId, std::unique_ptr<proto::Raft::Stub>> stubs_;
  std::chrono::milliseconds rpc_timeout_;
};

}  // namespace raftkv::rpc
