#pragma once

#include "raft/transport.h"

#include <chrono>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <vector>

namespace raftkv {

class RaftNode;

class SimulatedTransport final : public IRaftTransport {
 public:
  void registerNode(NodeId id, RaftNode* node);
  void unregisterNode(NodeId id);

  void isolate(NodeId id);
  void heal(NodeId id);
  void clearPartitions();
  void partition(const std::vector<std::set<NodeId>>& groups);
  void setFixedDelay(std::chrono::milliseconds delay);
  void dropNextMessages(int count);

  std::optional<RequestVoteResponse> sendRequestVote(
      NodeId from, NodeId to, const RequestVoteRequest& request) override;

  std::optional<AppendEntriesResponse> sendAppendEntries(
      NodeId from, NodeId to, const AppendEntriesRequest& request) override;

  std::optional<InstallSnapshotResponse> sendInstallSnapshot(
      NodeId from, NodeId to, const InstallSnapshotRequest& request) override;

 private:
  bool allowedLocked(NodeId from, NodeId to);
  RaftNode* targetFor(NodeId from, NodeId to);
  void maybeDelay() const;

  mutable std::mutex mutex_;
  std::map<NodeId, RaftNode*> nodes_;
  std::set<NodeId> isolated_;
  std::map<NodeId, int> partition_group_;
  std::chrono::milliseconds fixed_delay_{0};
  int drops_remaining_{0};
};

}  // namespace raftkv
