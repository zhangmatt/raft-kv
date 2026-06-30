#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace raftkv {

using NodeId = int;
using Index = std::uint64_t;
using Term = std::uint64_t;

enum class Role {
  Follower,
  Candidate,
  Leader,
};

enum class CommandType : std::uint8_t {
  Noop = 0,
  Put = 1,
  Delete = 2,
  Get = 3,
  AddServer = 4,
  RemoveServer = 5,
};

struct CommandResult {
  bool ok{true};
  std::optional<std::string> value;
  std::string error;
  Index index{0};
};

struct ClientCommand {
  CommandType type{CommandType::Noop};
  std::string key;
  std::string value;
  std::string client_id;
  std::uint64_t sequence{0};
  NodeId node_id{0};

  static ClientCommand noop() { return {}; }

  static ClientCommand put(std::string key, std::string value,
                           std::string client_id, std::uint64_t sequence) {
    ClientCommand command;
    command.type = CommandType::Put;
    command.key = std::move(key);
    command.value = std::move(value);
    command.client_id = std::move(client_id);
    command.sequence = sequence;
    return command;
  }

  static ClientCommand erase(std::string key, std::string client_id,
                             std::uint64_t sequence) {
    ClientCommand command;
    command.type = CommandType::Delete;
    command.key = std::move(key);
    command.client_id = std::move(client_id);
    command.sequence = sequence;
    return command;
  }

  static ClientCommand get(std::string key, std::string client_id,
                           std::uint64_t sequence) {
    ClientCommand command;
    command.type = CommandType::Get;
    command.key = std::move(key);
    command.client_id = std::move(client_id);
    command.sequence = sequence;
    return command;
  }

  static ClientCommand addServer(NodeId node_id) {
    ClientCommand command;
    command.type = CommandType::AddServer;
    command.node_id = node_id;
    return command;
  }

  static ClientCommand removeServer(NodeId node_id) {
    ClientCommand command;
    command.type = CommandType::RemoveServer;
    command.node_id = node_id;
    return command;
  }
};

struct DedupRecord {
  std::uint64_t sequence{0};
  CommandResult result;
};

struct SnapshotData {
  Index last_included_index{0};
  Term last_included_term{0};
  std::map<std::string, std::string> kv;
  std::map<std::string, DedupRecord> dedup;
};

struct LogEntry {
  Index index{0};
  Term term{0};
  ClientCommand command;
};

struct RequestVoteRequest {
  Term term{0};
  NodeId candidate_id{0};
  Index last_log_index{0};
  Term last_log_term{0};
};

struct RequestVoteResponse {
  Term term{0};
  bool vote_granted{false};
};

struct AppendEntriesRequest {
  Term term{0};
  NodeId leader_id{0};
  Index prev_log_index{0};
  Term prev_log_term{0};
  std::vector<LogEntry> entries;
  Index leader_commit{0};
};

struct AppendEntriesResponse {
  Term term{0};
  bool success{false};
  Index match_index{0};
  Index conflict_index{1};
  Term conflict_term{0};
};

struct InstallSnapshotRequest {
  Term term{0};
  NodeId leader_id{0};
  SnapshotData snapshot;
};

struct InstallSnapshotResponse {
  Term term{0};
  bool success{false};
  Index last_included_index{0};
};

struct ClientResponse {
  enum class Status {
    Ok,
    NotLeader,
    Timeout,
    Failed,
  };

  Status status{Status::Failed};
  std::optional<NodeId> leader_id;
  CommandResult result;
  std::string message;
};

struct NodeConfig {
  NodeId id{0};
  std::vector<NodeId> peers;
  std::string storage_dir{"data"};
  std::chrono::milliseconds election_timeout_min{150};
  std::chrono::milliseconds election_timeout_max{300};
  std::chrono::milliseconds heartbeat_interval{50};
};

inline int majority_count(int cluster_size) {
  return (cluster_size / 2) + 1;
}

}  // namespace raftkv
