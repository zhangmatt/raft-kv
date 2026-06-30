#pragma once

#include "raft/raft_node.h"

namespace raftkv {

class RaftService {
 public:
  explicit RaftService(RaftNode& node) : node_(node) {}

  RequestVoteResponse requestVote(const RequestVoteRequest& request);
  AppendEntriesResponse appendEntries(const AppendEntriesRequest& request);
  InstallSnapshotResponse installSnapshot(
      const InstallSnapshotRequest& request);

 private:
  RaftNode& node_;
};

}  // namespace raftkv
