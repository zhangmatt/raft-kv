#include "rpc/grpc_peer_client.h"

#include "rpc/proto_conversion.h"

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

namespace raftkv::rpc {

GrpcPeerClient::GrpcPeerClient(std::map<NodeId, std::string> peer_addresses,
                               std::chrono::milliseconds rpc_timeout)
    : peer_addresses_(std::move(peer_addresses)), rpc_timeout_(rpc_timeout) {
  for (const auto& [node_id, address] : peer_addresses_) {
    stubs_[node_id] = proto::Raft::NewStub(
        grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  }
}

proto::Raft::Stub* GrpcPeerClient::stubFor(NodeId node_id) {
  const auto it = stubs_.find(node_id);
  if (it == stubs_.end()) {
    return nullptr;
  }
  return it->second.get();
}

std::optional<RequestVoteResponse> GrpcPeerClient::sendRequestVote(
    NodeId, NodeId to, const RequestVoteRequest& request) {
  auto* stub = stubFor(to);
  if (stub == nullptr) {
    return std::nullopt;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);
  proto::RequestVoteResponse response;
  const auto status = stub->RequestVote(&context, toProto(request), &response);
  if (!status.ok()) {
    return std::nullopt;
  }
  return fromProto(response);
}

std::optional<AppendEntriesResponse> GrpcPeerClient::sendAppendEntries(
    NodeId, NodeId to, const AppendEntriesRequest& request) {
  auto* stub = stubFor(to);
  if (stub == nullptr) {
    return std::nullopt;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);
  proto::AppendEntriesResponse response;
  const auto status = stub->AppendEntries(&context, toProto(request), &response);
  if (!status.ok()) {
    return std::nullopt;
  }
  return fromProto(response);
}

std::optional<InstallSnapshotResponse> GrpcPeerClient::sendInstallSnapshot(
    NodeId, NodeId to, const InstallSnapshotRequest& request) {
  auto* stub = stubFor(to);
  if (stub == nullptr) {
    return std::nullopt;
  }

  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);
  proto::InstallSnapshotResponse response;
  const auto status =
      stub->InstallSnapshot(&context, toProto(request), &response);
  if (!status.ok()) {
    return std::nullopt;
  }
  return fromProto(response);
}

}  // namespace raftkv::rpc
