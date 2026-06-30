#pragma once

#include "raft/raft_node.h"
#include "rpc/peer_client.h"

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace raftkv::test {

using namespace std::chrono_literals;

inline bool waitUntil(const std::function<bool()>& predicate,
                      std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return predicate();
}

inline std::filesystem::path uniqueTempDir(const std::string& prefix) {
  const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  auto path = std::filesystem::temp_directory_path() /
              (prefix + "-" + std::to_string(now));
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  return path;
}

class Cluster {
 public:
  explicit Cluster(int size)
      : data_dir_(uniqueTempDir("raft-kv-test")), configs_(size + 1) {
    for (int id = 1; id <= size; ++id) {
      NodeConfig config;
      config.id = id;
      config.storage_dir = data_dir_.string();
      config.election_timeout_min = 80ms + std::chrono::milliseconds(id * 15);
      config.election_timeout_max = 160ms + std::chrono::milliseconds(id * 15);
      config.heartbeat_interval = 25ms;
      for (int peer = 1; peer <= size; ++peer) {
        if (peer != id) {
          config.peers.push_back(peer);
        }
      }
      configs_[static_cast<std::size_t>(id)] = config;
    }

    nodes_.resize(static_cast<std::size_t>(size + 1));
    for (int id = 1; id <= size; ++id) {
      createNode(id);
    }
  }

  ~Cluster() {
    stop();
    std::filesystem::remove_all(data_dir_);
  }

  Cluster(const Cluster&) = delete;
  Cluster& operator=(const Cluster&) = delete;

  void start() {
    for (std::size_t id = 1; id < nodes_.size(); ++id) {
      nodes_[id]->start();
    }
  }

  void stop() {
    for (std::size_t id = 1; id < nodes_.size(); ++id) {
      if (nodes_[id]) {
        nodes_[id]->stop();
      }
    }
  }

  RaftNode& node(NodeId id) { return *nodes_[static_cast<std::size_t>(id)]; }
  SimulatedTransport& transport() { return transport_; }

  void crash(NodeId id) { nodes_[static_cast<std::size_t>(id)]->stop(); }

  void restart(NodeId id) {
    nodes_[static_cast<std::size_t>(id)]->stop();
    createNode(id);
    nodes_[static_cast<std::size_t>(id)]->start();
  }

  std::optional<NodeId> leaderWithin(std::set<NodeId> ids,
                                     std::chrono::milliseconds timeout) {
    std::optional<NodeId> leader;
    waitUntil(
        [&] {
          int count = 0;
          leader.reset();
          for (const auto id : ids) {
            if (nodes_[static_cast<std::size_t>(id)]->running() &&
                nodes_[static_cast<std::size_t>(id)]->role() == Role::Leader) {
              ++count;
              leader = id;
            }
          }
          return count == 1;
        },
        timeout);
    return leader;
  }

  std::optional<NodeId> leader(std::chrono::milliseconds timeout = 3s) {
    std::set<NodeId> ids;
    for (std::size_t id = 1; id < nodes_.size(); ++id) {
      ids.insert(static_cast<NodeId>(id));
    }
    return leaderWithin(std::move(ids), timeout);
  }

  ClientResponse submitToLeader(const ClientCommand& command,
                                std::chrono::milliseconds timeout = 2s) {
    const auto current_leader = leader(timeout);
    if (!current_leader) {
      return ClientResponse{ClientResponse::Status::Timeout, std::nullopt, {},
                            "no leader"};
    }
    return node(*current_leader).submit(command, timeout);
  }

  bool allHave(const std::string& key, const std::string& value,
               std::chrono::milliseconds timeout = 2s) {
    return waitUntil(
        [&] {
          for (std::size_t id = 1; id < nodes_.size(); ++id) {
            if (!nodes_[id]->running()) {
              continue;
            }
            const auto local = nodes_[id]->localGet(key);
            if (!local || *local != value) {
              return false;
            }
          }
          return true;
        },
        timeout);
  }

 private:
  void createNode(NodeId id) {
    nodes_[static_cast<std::size_t>(id)] =
        std::make_unique<RaftNode>(configs_[static_cast<std::size_t>(id)]);
    nodes_[static_cast<std::size_t>(id)]->setTransport(&transport_);
    transport_.registerNode(id, nodes_[static_cast<std::size_t>(id)].get());
  }

  std::filesystem::path data_dir_;
  std::vector<NodeConfig> configs_;
  SimulatedTransport transport_;
  std::vector<std::unique_ptr<RaftNode>> nodes_;
};

}  // namespace raftkv::test
