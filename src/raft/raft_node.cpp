#include "raft/raft_node.h"

#include <algorithm>
#include <chrono>
#include <set>
#include <stdexcept>
#include <thread>
#include <variant>

namespace raftkv {
namespace {

constexpr auto kLoopSleep = std::chrono::milliseconds(10);

}  // namespace

RaftNode::RaftNode(NodeConfig config)
    : config_(std::move(config)),
      log_(config_.storage_dir, config_.id),
      election_timer_(config_.election_timeout_min,
                      config_.election_timeout_max,
                      static_cast<std::uint64_t>(config_.id) * 7919ULL + 11ULL) {
  log_.load();
  state_machine_.restore(log_.snapshot());
  commit_index_ = log_.snapshotLastIndex();
  last_applied_ = log_.snapshotLastIndex();
  resetElectionDeadlineLocked();
}

RaftNode::~RaftNode() { stop(); }

void RaftNode::setTransport(IRaftTransport* transport) {
  std::lock_guard lock(mutex_);
  transport_ = transport;
}

void RaftNode::start() {
  bool expected = false;
  if (!running_.compare_exchange_strong(expected, true)) {
    return;
  }

  {
    std::lock_guard lock(mutex_);
    role_ = Role::Follower;
    leader_id_.reset();
    resetElectionDeadlineLocked();
  }
  worker_ = std::thread([this] { runLoop(); });
}

void RaftNode::stop() {
  if (!running_.exchange(false)) {
    return;
  }
  cv_.notify_all();
  if (worker_.joinable()) {
    worker_.join();
  }
}

Role RaftNode::role() const {
  std::lock_guard lock(mutex_);
  return role_;
}

Term RaftNode::currentTerm() const {
  std::lock_guard lock(mutex_);
  return log_.currentTerm();
}

Index RaftNode::commitIndex() const {
  std::lock_guard lock(mutex_);
  return commit_index_;
}

Index RaftNode::lastApplied() const {
  std::lock_guard lock(mutex_);
  return last_applied_;
}

Index RaftNode::lastLogIndex() const {
  std::lock_guard lock(mutex_);
  return log_.lastIndex();
}

std::optional<NodeId> RaftNode::leaderId() const {
  std::lock_guard lock(mutex_);
  return leader_id_;
}

std::optional<std::string> RaftNode::localGet(const std::string& key) const {
  std::lock_guard lock(mutex_);
  return state_machine_.localGet(key);
}

void RaftNode::runLoop() {
  while (running()) {
    bool should_start_election = false;
    bool should_replicate = false;

    {
      std::lock_guard lock(mutex_);
      const auto now = std::chrono::steady_clock::now();
      if (role_ == Role::Leader) {
        if (now - last_heartbeat_sent_ >= config_.heartbeat_interval) {
          should_replicate = true;
          last_heartbeat_sent_ = now;
        }
      } else if (now >= election_deadline_) {
        should_start_election = true;
      }
    }

    if (should_start_election) {
      startElection();
    }
    if (should_replicate) {
      replicateAll();
    }

    std::this_thread::sleep_for(kLoopSleep);
  }
}

void RaftNode::resetElectionDeadlineLocked() {
  election_deadline_ = std::chrono::steady_clock::now() + election_timer_.next();
}

int RaftNode::clusterSize() const {
  return static_cast<int>(config_.peers.size()) + 1;
}

void RaftNode::startElection() {
  RequestVoteRequest request;
  int votes = 1;

  {
    std::lock_guard lock(mutex_);
    if (!running() || role_ == Role::Leader) {
      return;
    }

    role_ = Role::Candidate;
    leader_id_.reset();
    log_.setCurrentTerm(log_.currentTerm() + 1);
    log_.setVotedFor(config_.id);
    log_.persist();
    resetElectionDeadlineLocked();

    request.term = log_.currentTerm();
    request.candidate_id = config_.id;
    request.last_log_index = log_.lastIndex();
    request.last_log_term = log_.lastTerm();
  }

  if (votes >= majority_count(clusterSize())) {
    {
      std::lock_guard lock(mutex_);
      if (running() && role_ == Role::Candidate &&
          log_.currentTerm() == request.term) {
        becomeLeaderLocked();
      }
    }
    replicateAll();
    return;
  }

  for (const auto peer_id : config_.peers) {
    IRaftTransport* transport = nullptr;
    {
      std::lock_guard lock(mutex_);
      transport = transport_;
    }
    if (transport == nullptr) {
      continue;
    }

    const auto response =
        transport->sendRequestVote(config_.id, peer_id, request);
    if (!response) {
      continue;
    }

    bool became_leader = false;
    {
      std::lock_guard lock(mutex_);
      if (!running() || role_ != Role::Candidate ||
          log_.currentTerm() != request.term) {
        continue;
      }

      if (response->term > log_.currentTerm()) {
        becomeFollowerLocked(response->term, std::nullopt);
        return;
      }

      if (response->vote_granted) {
        ++votes;
        if (votes >= majority_count(clusterSize())) {
          becomeLeaderLocked();
          became_leader = true;
        }
      }
    }

    if (became_leader) {
      replicateAll();
      return;
    }
  }
}

void RaftNode::becomeFollowerLocked(Term term, std::optional<NodeId> leader_id) {
  bool must_persist = false;
  if (term > log_.currentTerm()) {
    log_.setCurrentTerm(term);
    log_.setVotedFor(std::nullopt);
    must_persist = true;
  }

  role_ = Role::Follower;
  leader_id_ = leader_id;
  resetElectionDeadlineLocked();
  if (must_persist) {
    log_.persist();
  }
}

void RaftNode::becomeLeaderLocked() {
  if (role_ == Role::Leader) {
    return;
  }

  role_ = Role::Leader;
  leader_id_ = config_.id;

  const auto follower_next_index = log_.lastIndex() + 1;
  next_index_.clear();
  match_index_.clear();
  for (const auto peer_id : config_.peers) {
    next_index_[peer_id] = follower_next_index;
    match_index_[peer_id] = 0;
  }

  const LogEntry noop{log_.lastIndex() + 1, log_.currentTerm(),
                      ClientCommand::noop()};
  log_.append(noop);
  log_.persist();
  match_index_[config_.id] = log_.lastIndex();
  next_index_[config_.id] = log_.lastIndex() + 1;
  last_heartbeat_sent_ =
      std::chrono::steady_clock::now() - config_.heartbeat_interval;
  advanceCommitLocked();
}

RequestVoteResponse RaftNode::handleRequestVote(
    const RequestVoteRequest& request) {
  std::lock_guard lock(mutex_);
  RequestVoteResponse response{log_.currentTerm(), false};

  if (!running()) {
    return response;
  }

  if (request.term < log_.currentTerm()) {
    return response;
  }

  if (request.term > log_.currentTerm()) {
    becomeFollowerLocked(request.term, std::nullopt);
  }

  const bool log_is_up_to_date =
      request.last_log_term > log_.lastTerm() ||
      (request.last_log_term == log_.lastTerm() &&
       request.last_log_index >= log_.lastIndex());

  const auto voted_for = log_.votedFor();
  if ((!voted_for || *voted_for == request.candidate_id) && log_is_up_to_date) {
    log_.setVotedFor(request.candidate_id);
    log_.persist();
    role_ = Role::Follower;
    leader_id_.reset();
    resetElectionDeadlineLocked();
    response.vote_granted = true;
  }

  response.term = log_.currentTerm();
  return response;
}

AppendEntriesResponse RaftNode::handleAppendEntries(
    const AppendEntriesRequest& request) {
  std::lock_guard lock(mutex_);
  AppendEntriesResponse response;
  response.term = log_.currentTerm();
  response.conflict_index = log_.lastIndex() + 1;

  if (!running()) {
    return response;
  }

  bool must_persist = false;
  if (request.term < log_.currentTerm()) {
    return response;
  }

  if (request.term > log_.currentTerm()) {
    log_.setCurrentTerm(request.term);
    log_.setVotedFor(std::nullopt);
    must_persist = true;
  }

  role_ = Role::Follower;
  leader_id_ = request.leader_id;
  resetElectionDeadlineLocked();

  const auto local_prev_term = log_.termAt(request.prev_log_index);
  if (!local_prev_term || *local_prev_term != request.prev_log_term) {
    if (must_persist) {
      log_.persist();
    }

    response.term = log_.currentTerm();
    response.success = false;
    if (local_prev_term) {
      response.conflict_term = *local_prev_term;
      Index first_index = request.prev_log_index;
      while (first_index > log_.snapshotLastIndex() + 1) {
        const auto previous_term = log_.termAt(first_index - 1);
        if (!previous_term || *previous_term != response.conflict_term) {
          break;
        }
        --first_index;
      }
      response.conflict_index = first_index;
    } else {
      response.conflict_index = log_.snapshotLastIndex() + 1;
    }
    return response;
  }

  std::size_t first_new = 0;
  while (first_new < request.entries.size()) {
    const auto& incoming = request.entries[first_new];
    if (incoming.index <= log_.snapshotLastIndex()) {
      ++first_new;
      continue;
    }

    const auto local_term = log_.termAt(incoming.index);
    if (local_term && *local_term == incoming.term) {
      ++first_new;
      continue;
    }

    if (local_term && *local_term != incoming.term) {
      log_.truncateSuffix(incoming.index);
      must_persist = true;
    }
    break;
  }

  for (std::size_t i = first_new; i < request.entries.size(); ++i) {
    if (request.entries[i].index <= log_.snapshotLastIndex()) {
      continue;
    }
    log_.append(request.entries[i]);
    must_persist = true;
  }

  if (must_persist) {
    log_.persist();
  }

  if (request.leader_commit > commit_index_) {
    commit_index_ = std::min(request.leader_commit, log_.lastIndex());
    applyCommittedLocked();
  }

  response.term = log_.currentTerm();
  response.success = true;
  response.match_index =
      request.entries.empty()
          ? request.prev_log_index
          : std::min(request.entries.back().index, log_.lastIndex());
  response.conflict_index = response.match_index + 1;
  return response;
}

InstallSnapshotResponse RaftNode::handleInstallSnapshot(
    const InstallSnapshotRequest& request) {
  std::lock_guard lock(mutex_);
  InstallSnapshotResponse response;
  response.term = log_.currentTerm();
  response.last_included_index = log_.snapshotLastIndex();

  if (!running()) {
    return response;
  }

  bool must_persist = false;
  if (request.term < log_.currentTerm()) {
    return response;
  }

  if (request.term > log_.currentTerm()) {
    log_.setCurrentTerm(request.term);
    log_.setVotedFor(std::nullopt);
    must_persist = true;
  }

  role_ = Role::Follower;
  leader_id_ = request.leader_id;
  resetElectionDeadlineLocked();

  if (request.snapshot.last_included_index >= log_.snapshotLastIndex()) {
    log_.installSnapshot(request.snapshot);
    state_machine_.restore(request.snapshot);
    commit_index_ = std::max(commit_index_, request.snapshot.last_included_index);
    last_applied_ =
        std::max(last_applied_, request.snapshot.last_included_index);
    must_persist = true;
  }

  if (must_persist) {
    log_.persist();
  }

  response.term = log_.currentTerm();
  response.success = true;
  response.last_included_index = log_.snapshotLastIndex();
  cv_.notify_all();
  return response;
}

ClientResponse RaftNode::submit(const ClientCommand& command,
                                std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  Index entry_index = 0;
  Term entry_term = 0;

  {
    std::lock_guard lock(mutex_);
    if (!running() || role_ != Role::Leader) {
      return ClientResponse{ClientResponse::Status::NotLeader, leader_id_, {},
                            "not leader"};
    }

    if (auto cached = state_machine_.cachedResult(command)) {
      return ClientResponse{ClientResponse::Status::Ok, config_.id, *cached, {}};
    }

    entry_index = log_.lastIndex() + 1;
    entry_term = log_.currentTerm();
    log_.append(LogEntry{entry_index, entry_term, command});
    log_.persist();
    match_index_[config_.id] = entry_index;
    next_index_[config_.id] = entry_index + 1;
  }

  replicateAll();

  std::unique_lock lock(mutex_);
  while (std::chrono::steady_clock::now() < deadline) {
    if (commit_index_ >= entry_index) {
      if (auto cached = state_machine_.cachedResult(command)) {
        return ClientResponse{ClientResponse::Status::Ok, config_.id, *cached,
                              {}};
      }
      CommandResult result;
      result.ok = true;
      result.index = entry_index;
      return ClientResponse{ClientResponse::Status::Ok, config_.id, result, {}};
    }

    if (!running() || role_ != Role::Leader || log_.currentTerm() != entry_term) {
      return ClientResponse{ClientResponse::Status::NotLeader, leader_id_, {},
                            "leadership changed before commit"};
    }

    lock.unlock();
    replicateAll();
    lock.lock();
    cv_.wait_until(lock, std::min(deadline,
                                  std::chrono::steady_clock::now() +
                                      std::chrono::milliseconds(20)));
  }

  return ClientResponse{ClientResponse::Status::Timeout, leader_id_, {},
                        "timed out waiting for majority commit"};
}

ClientResponse RaftNode::addServer(NodeId node_id,
                                   std::chrono::milliseconds timeout) {
  {
    std::lock_guard lock(mutex_);
    if (!running() || role_ != Role::Leader) {
      return ClientResponse{ClientResponse::Status::NotLeader, leader_id_, {},
                            "not leader"};
    }
    if (node_id == config_.id) {
      CommandResult result;
      result.ok = true;
      return ClientResponse{ClientResponse::Status::Ok, config_.id, result, {}};
    }
    if (std::find(config_.peers.begin(), config_.peers.end(), node_id) !=
        config_.peers.end()) {
      CommandResult result;
      result.ok = true;
      return ClientResponse{ClientResponse::Status::Ok, config_.id, result, {}};
    }
  }
  return submit(ClientCommand::addServer(node_id), timeout);
}

ClientResponse RaftNode::removeServer(NodeId node_id,
                                      std::chrono::milliseconds timeout) {
  {
    std::lock_guard lock(mutex_);
    if (!running() || role_ != Role::Leader) {
      return ClientResponse{ClientResponse::Status::NotLeader, leader_id_, {},
                            "not leader"};
    }
    if (node_id != config_.id &&
        std::find(config_.peers.begin(), config_.peers.end(), node_id) ==
            config_.peers.end()) {
      CommandResult result;
      result.ok = true;
      return ClientResponse{ClientResponse::Status::Ok, config_.id, result, {}};
    }
  }
  return submit(ClientCommand::removeServer(node_id), timeout);
}

void RaftNode::replicateAll() {
  std::vector<NodeId> peers;
  {
    std::lock_guard lock(mutex_);
    if (!running() || role_ != Role::Leader) {
      return;
    }
    peers = config_.peers;
  }

  for (const auto peer_id : peers) {
    replicateToPeer(peer_id);
  }
}

void RaftNode::replicateToPeer(NodeId peer_id) {
  struct SnapshotRpc {
    InstallSnapshotRequest request;
  };
  struct AppendRpc {
    AppendEntriesRequest request;
  };

  std::variant<SnapshotRpc, AppendRpc> rpc;
  IRaftTransport* transport = nullptr;
  Term rpc_term = 0;

  {
    std::lock_guard lock(mutex_);
    if (!running() || role_ != Role::Leader) {
      return;
    }

    transport = transport_;
    if (transport == nullptr) {
      return;
    }

    auto next_index = next_index_[peer_id];
    if (next_index == 0) {
      next_index = 1;
    }

    rpc_term = log_.currentTerm();
    if (next_index <= log_.snapshotLastIndex()) {
      rpc = SnapshotRpc{InstallSnapshotRequest{log_.currentTerm(), config_.id,
                                               log_.snapshot()}};
    } else {
      const auto prev_log_index = next_index - 1;
      const auto prev_log_term = log_.termAt(prev_log_index).value_or(0);
      rpc = AppendRpc{AppendEntriesRequest{
          log_.currentTerm(), config_.id, prev_log_index, prev_log_term,
          log_.entriesFrom(next_index), commit_index_}};
    }
  }

  if (std::holds_alternative<SnapshotRpc>(rpc)) {
    const auto response = transport->sendInstallSnapshot(
        config_.id, peer_id, std::get<SnapshotRpc>(rpc).request);
    if (!response) {
      return;
    }

    std::lock_guard lock(mutex_);
    if (!running() || role_ != Role::Leader || log_.currentTerm() != rpc_term) {
      return;
    }
    if (response->term > log_.currentTerm()) {
      becomeFollowerLocked(response->term, std::nullopt);
      return;
    }
    if (response->success) {
      match_index_[peer_id] =
          std::max(match_index_[peer_id], response->last_included_index);
      next_index_[peer_id] = response->last_included_index + 1;
      advanceCommitLocked();
    }
    return;
  }

  const auto response = transport->sendAppendEntries(
      config_.id, peer_id, std::get<AppendRpc>(rpc).request);
  if (!response) {
    return;
  }

  std::lock_guard lock(mutex_);
  if (!running() || role_ != Role::Leader || log_.currentTerm() != rpc_term) {
    return;
  }
  if (response->term > log_.currentTerm()) {
    becomeFollowerLocked(response->term, std::nullopt);
    return;
  }

  if (response->success) {
    match_index_[peer_id] =
        std::max(match_index_[peer_id], response->match_index);
    next_index_[peer_id] = match_index_[peer_id] + 1;
    advanceCommitLocked();
    return;
  }

  Index next_index = response->conflict_index;
  if (response->conflict_term != 0) {
    const auto last_index_with_term =
        findLastIndexOfTermLocked(response->conflict_term);
    if (last_index_with_term != 0) {
      next_index = last_index_with_term + 1;
    }
  }
  if (next_index == 0) {
    next_index = 1;
  }
  next_index_[peer_id] = next_index;
}

void RaftNode::advanceCommitLocked() {
  for (Index index = log_.lastIndex(); index > commit_index_; --index) {
    const auto term = log_.termAt(index);
    if (!term || *term != log_.currentTerm()) {
      continue;
    }

    int replicated = 1;
    for (const auto peer_id : config_.peers) {
      if (match_index_[peer_id] >= index) {
        ++replicated;
      }
    }

    if (replicated >= majority_count(clusterSize())) {
      commit_index_ = index;
      applyCommittedLocked();
      return;
    }
  }
}

void RaftNode::applyCommittedLocked() {
  while (last_applied_ < commit_index_) {
    ++last_applied_;
    if (last_applied_ <= log_.snapshotLastIndex()) {
      continue;
    }
    const auto entry = log_.entryAt(last_applied_);
    if (!entry) {
      throw std::logic_error("committed log entry is missing");
    }
    state_machine_.apply(entry->command, entry->index);
    applyMembershipChangeLocked(entry->command);
  }
  cv_.notify_all();
}

void RaftNode::applyMembershipChangeLocked(const ClientCommand& command) {
  if (command.type == CommandType::AddServer) {
    if (command.node_id == config_.id) {
      return;
    }
    if (std::find(config_.peers.begin(), config_.peers.end(),
                  command.node_id) == config_.peers.end()) {
      config_.peers.push_back(command.node_id);
      std::sort(config_.peers.begin(), config_.peers.end());
    }
    if (role_ == Role::Leader && !next_index_.contains(command.node_id)) {
      next_index_[command.node_id] =
          log_.snapshotLastIndex() == 0 ? 1 : log_.snapshotLastIndex();
      match_index_[command.node_id] = 0;
    }
    return;
  }

  if (command.type == CommandType::RemoveServer) {
    if (command.node_id == config_.id) {
      role_ = Role::Follower;
      leader_id_.reset();
      return;
    }
    config_.peers.erase(
        std::remove(config_.peers.begin(), config_.peers.end(),
                    command.node_id),
        config_.peers.end());
    next_index_.erase(command.node_id);
    match_index_.erase(command.node_id);
  }
}

Index RaftNode::findLastIndexOfTermLocked(Term term) const {
  for (Index index = log_.lastIndex(); index > log_.snapshotLastIndex(); --index) {
    const auto entry_term = log_.termAt(index);
    if (entry_term && *entry_term == term) {
      return index;
    }
  }
  if (log_.snapshotLastTerm() == term) {
    return log_.snapshotLastIndex();
  }
  return 0;
}

bool RaftNode::createSnapshot() {
  std::lock_guard lock(mutex_);
  if (commit_index_ <= log_.snapshotLastIndex()) {
    return false;
  }

  const auto last_included_term = log_.termAt(commit_index_);
  if (!last_included_term) {
    return false;
  }
  auto snapshot = state_machine_.snapshot(commit_index_, *last_included_term);
  log_.compactThrough(commit_index_, *last_included_term, snapshot);
  log_.persist();
  return true;
}

}  // namespace raftkv
