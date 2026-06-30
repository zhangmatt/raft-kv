#pragma once

#include "raft/types.h"
#include "raft.pb.h"

namespace raftkv::rpc {

proto::Command toProto(const ClientCommand& command);
ClientCommand fromProto(const proto::Command& command);

proto::CommandResult toProto(const CommandResult& result);
CommandResult fromProto(const proto::CommandResult& result);

proto::LogEntry toProto(const LogEntry& entry);
LogEntry fromProto(const proto::LogEntry& entry);

proto::RequestVoteRequest toProto(const RequestVoteRequest& request);
RequestVoteRequest fromProto(const proto::RequestVoteRequest& request);

proto::RequestVoteResponse toProto(const RequestVoteResponse& response);
RequestVoteResponse fromProto(const proto::RequestVoteResponse& response);

proto::AppendEntriesRequest toProto(const AppendEntriesRequest& request);
AppendEntriesRequest fromProto(const proto::AppendEntriesRequest& request);

proto::AppendEntriesResponse toProto(const AppendEntriesResponse& response);
AppendEntriesResponse fromProto(const proto::AppendEntriesResponse& response);

proto::Snapshot toProto(const SnapshotData& snapshot);
SnapshotData fromProto(const proto::Snapshot& snapshot);

proto::InstallSnapshotRequest toProto(
    const InstallSnapshotRequest& request);
InstallSnapshotRequest fromProto(
    const proto::InstallSnapshotRequest& request);

proto::InstallSnapshotResponse toProto(
    const InstallSnapshotResponse& response);
InstallSnapshotResponse fromProto(
    const proto::InstallSnapshotResponse& response);

proto::ClientCommandResponse toProto(const ClientResponse& response);
ClientResponse fromProto(const proto::ClientCommandResponse& response);

}  // namespace raftkv::rpc
