#pragma once

#include "raft/log.h"
#include "raft/state_machine.h"
#include "raft/timer.h"
#include "raft/transport.h"
#include "raft/types.h"

#include <atomic>
#include <condition_variable>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace raftkv {

class RaftNode {
 public:
  explicit RaftNode(NodeConfig config);
  ~RaftNode();

  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  void setTransport(IRaftTransport* transport);

  void start();
  void stop();
  bool running() const { return running_.load(); }

  NodeId id() const { return config_.id; }
  Role role() const;
  Term currentTerm() const;
  Index commitIndex() const;
  Index lastApplied() const;
  Index lastLogIndex() const;
  std::optional<NodeId> leaderId() const;
  std::optional<std::string> localGet(const std::string& key) const;

  RequestVoteResponse handleRequestVote(const RequestVoteRequest& request);
  AppendEntriesResponse handleAppendEntries(
      const AppendEntriesRequest& request);
  InstallSnapshotResponse handleInstallSnapshot(
      const InstallSnapshotRequest& request);

  ClientResponse submit(const ClientCommand& command,
                        std::chrono::milliseconds timeout =
                            std::chrono::milliseconds(1500));
  ClientResponse addServer(NodeId node_id,
                           std::chrono::milliseconds timeout =
                               std::chrono::milliseconds(1500));
  ClientResponse removeServer(NodeId node_id,
                              std::chrono::milliseconds timeout =
                                  std::chrono::milliseconds(1500));

  bool createSnapshot();

 private:
  void runLoop();
  void resetElectionDeadlineLocked();
  void startElection();
  void becomeFollowerLocked(Term term, std::optional<NodeId> leader_id);
  void becomeLeaderLocked();
  void replicateAll();
  void replicateToPeer(NodeId peer_id);
  void advanceCommitLocked();
  void applyCommittedLocked();
  void applyMembershipChangeLocked(const ClientCommand& command);
  Index findLastIndexOfTermLocked(Term term) const;
  int clusterSize() const;

  NodeConfig config_;
  IRaftTransport* transport_{nullptr};

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  PersistentLog log_;
  KeyValueStateMachine state_machine_;
  Role role_{Role::Follower};
  std::optional<NodeId> leader_id_;
  Index commit_index_{0};
  Index last_applied_{0};
  std::map<NodeId, Index> next_index_;
  std::map<NodeId, Index> match_index_;
  std::chrono::steady_clock::time_point election_deadline_;
  std::chrono::steady_clock::time_point last_heartbeat_sent_;
  ElectionTimeoutGenerator election_timer_;
  std::atomic<bool> running_{false};
  std::thread worker_;
};

}  // namespace raftkv
