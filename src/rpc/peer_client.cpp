#include "rpc/peer_client.h"

#include "raft/raft_node.h"

#include <thread>

namespace raftkv {

void SimulatedTransport::registerNode(NodeId id, RaftNode* node) {
  std::lock_guard lock(mutex_);
  nodes_[id] = node;
}

void SimulatedTransport::unregisterNode(NodeId id) {
  std::lock_guard lock(mutex_);
  nodes_.erase(id);
  isolated_.erase(id);
  partition_group_.erase(id);
}

void SimulatedTransport::isolate(NodeId id) {
  std::lock_guard lock(mutex_);
  isolated_.insert(id);
}

void SimulatedTransport::heal(NodeId id) {
  std::lock_guard lock(mutex_);
  isolated_.erase(id);
}

void SimulatedTransport::clearPartitions() {
  std::lock_guard lock(mutex_);
  isolated_.clear();
  partition_group_.clear();
  drops_remaining_ = 0;
}

void SimulatedTransport::partition(
    const std::vector<std::set<NodeId>>& groups) {
  std::lock_guard lock(mutex_);
  isolated_.clear();
  partition_group_.clear();
  for (std::size_t group_index = 0; group_index < groups.size();
       ++group_index) {
    for (const auto node_id : groups[group_index]) {
      partition_group_[node_id] = static_cast<int>(group_index);
    }
  }
}

void SimulatedTransport::setFixedDelay(std::chrono::milliseconds delay) {
  std::lock_guard lock(mutex_);
  fixed_delay_ = delay;
}

void SimulatedTransport::dropNextMessages(int count) {
  std::lock_guard lock(mutex_);
  drops_remaining_ = count;
}

bool SimulatedTransport::allowedLocked(NodeId from, NodeId to) {
  if (drops_remaining_ > 0) {
    --drops_remaining_;
    return false;
  }
  if (isolated_.contains(from) || isolated_.contains(to)) {
    return false;
  }
  if (!partition_group_.empty()) {
    const auto from_group = partition_group_.find(from);
    const auto to_group = partition_group_.find(to);
    if (from_group == partition_group_.end() ||
        to_group == partition_group_.end() ||
        from_group->second != to_group->second) {
      return false;
    }
  }
  return true;
}

RaftNode* SimulatedTransport::targetFor(NodeId from, NodeId to) {
  std::lock_guard lock(mutex_);
  if (!allowedLocked(from, to)) {
    return nullptr;
  }
  const auto it = nodes_.find(to);
  if (it == nodes_.end() || it->second == nullptr || !it->second->running()) {
    return nullptr;
  }
  return it->second;
}

void SimulatedTransport::maybeDelay() const {
  std::chrono::milliseconds delay{0};
  {
    std::lock_guard lock(mutex_);
    delay = fixed_delay_;
  }
  if (delay.count() > 0) {
    std::this_thread::sleep_for(delay);
  }
}

std::optional<RequestVoteResponse> SimulatedTransport::sendRequestVote(
    NodeId from, NodeId to, const RequestVoteRequest& request) {
  RaftNode* node = targetFor(from, to);
  if (node == nullptr) {
    return std::nullopt;
  }
  maybeDelay();
  return node->handleRequestVote(request);
}

std::optional<AppendEntriesResponse> SimulatedTransport::sendAppendEntries(
    NodeId from, NodeId to, const AppendEntriesRequest& request) {
  RaftNode* node = targetFor(from, to);
  if (node == nullptr) {
    return std::nullopt;
  }
  maybeDelay();
  return node->handleAppendEntries(request);
}

std::optional<InstallSnapshotResponse> SimulatedTransport::sendInstallSnapshot(
    NodeId from, NodeId to, const InstallSnapshotRequest& request) {
  RaftNode* node = targetFor(from, to);
  if (node == nullptr) {
    return std::nullopt;
  }
  maybeDelay();
  return node->handleInstallSnapshot(request);
}

}  // namespace raftkv
