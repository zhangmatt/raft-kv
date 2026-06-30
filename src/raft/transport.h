#pragma once

#include "raft/types.h"

#include <optional>

namespace raftkv {

class IRaftTransport {
 public:
  virtual ~IRaftTransport() = default;

  virtual std::optional<RequestVoteResponse> sendRequestVote(
      NodeId from, NodeId to, const RequestVoteRequest& request) = 0;

  virtual std::optional<AppendEntriesResponse> sendAppendEntries(
      NodeId from, NodeId to, const AppendEntriesRequest& request) = 0;

  virtual std::optional<InstallSnapshotResponse> sendInstallSnapshot(
      NodeId from, NodeId to, const InstallSnapshotRequest& request) = 0;
};

}  // namespace raftkv
