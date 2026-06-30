#include "raft/raft_node.h"
#include "rpc/peer_client.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

std::optional<raftkv::NodeId> findLeader(
    const std::vector<std::unique_ptr<raftkv::RaftNode>>& nodes) {
  for (const auto& node : nodes) {
    if (node->running() && node->role() == raftkv::Role::Leader) {
      return node->id();
    }
  }
  return std::nullopt;
}

}  // namespace

int main(int argc, char** argv) {
  int cluster_size = 3;
  if (argc > 1) {
    cluster_size = std::stoi(argv[1]);
  }
  if (cluster_size <= 0 || cluster_size % 2 == 0) {
    std::cerr << "cluster size must be a positive odd number\n";
    return 2;
  }

  const std::filesystem::path data_dir =
      argc > 2 ? std::filesystem::path(argv[2])
               : std::filesystem::temp_directory_path() / "raft-kv-demo";
  std::filesystem::create_directories(data_dir);

  raftkv::SimulatedTransport transport;
  std::vector<std::unique_ptr<raftkv::RaftNode>> nodes;
  nodes.reserve(static_cast<std::size_t>(cluster_size));

  for (int id = 1; id <= cluster_size; ++id) {
    std::vector<raftkv::NodeId> peers;
    for (int peer = 1; peer <= cluster_size; ++peer) {
      if (peer != id) {
        peers.push_back(peer);
      }
    }
    raftkv::NodeConfig config;
    config.id = id;
    config.peers = peers;
    config.storage_dir = data_dir.string();
    nodes.push_back(std::make_unique<raftkv::RaftNode>(config));
    nodes.back()->setTransport(&transport);
    transport.registerNode(id, nodes.back().get());
  }

  for (auto& node : nodes) {
    node->start();
  }

  std::cout << "Started " << cluster_size
            << "-node in-process Raft KV cluster using " << data_dir << "\n";
  for (int i = 0; i < 50; ++i) {
    if (auto leader = findLeader(nodes)) {
      std::cout << "Leader: node " << *leader << "\n";
      auto& leader_node = *nodes[static_cast<std::size_t>(*leader - 1)];
      const auto response = leader_node.submit(
          raftkv::ClientCommand::put("hello", "raft", "demo-client", 1), 2s);
      std::cout << "put hello=raft status="
                << (response.status == raftkv::ClientResponse::Status::Ok
                        ? "ok"
                        : "failed")
                << "\n";
      std::this_thread::sleep_for(200ms);
      for (const auto& node : nodes) {
        const auto value = node->localGet("hello");
        std::cout << "node " << node->id() << " local hello="
                  << (value ? *value : "<missing>") << "\n";
      }
      return response.status == raftkv::ClientResponse::Status::Ok ? 0 : 1;
    }
    std::this_thread::sleep_for(100ms);
  }

  std::cerr << "no leader elected\n";
  return 1;
}
