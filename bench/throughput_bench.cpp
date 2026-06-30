#include "raft/raft_node.h"
#include "rpc/peer_client.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <vector>

using namespace std::chrono_literals;

namespace {

std::unique_ptr<raftkv::RaftNode> makeNode(
    int id, int size, const std::filesystem::path& dir,
    raftkv::SimulatedTransport& transport) {
  raftkv::NodeConfig config;
  config.id = id;
  config.storage_dir = dir.string();
  config.election_timeout_min = 80ms + std::chrono::milliseconds(id * 15);
  config.election_timeout_max = 160ms + std::chrono::milliseconds(id * 15);
  config.heartbeat_interval = 20ms;
  for (int peer = 1; peer <= size; ++peer) {
    if (peer != id) {
      config.peers.push_back(peer);
    }
  }

  auto node = std::make_unique<raftkv::RaftNode>(config);
  node->setTransport(&transport);
  transport.registerNode(id, node.get());
  return node;
}

raftkv::RaftNode* waitForLeader(
    std::vector<std::unique_ptr<raftkv::RaftNode>>& nodes) {
  const auto deadline = std::chrono::steady_clock::now() + 3s;
  while (std::chrono::steady_clock::now() < deadline) {
    for (auto& node : nodes) {
      if (node->role() == raftkv::Role::Leader) {
        return node.get();
      }
    }
    std::this_thread::sleep_for(10ms);
  }
  return nullptr;
}

}  // namespace

int main(int argc, char** argv) {
  const int operations = argc > 1 ? std::stoi(argv[1]) : 1000;
  constexpr int kClusterSize = 3;

  const auto dir = std::filesystem::temp_directory_path() / "raft-kv-bench";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  raftkv::SimulatedTransport transport;
  std::vector<std::unique_ptr<raftkv::RaftNode>> nodes;
  for (int id = 1; id <= kClusterSize; ++id) {
    nodes.push_back(makeNode(id, kClusterSize, dir, transport));
  }
  for (auto& node : nodes) {
    node->start();
  }

  auto* leader = waitForLeader(nodes);
  if (leader == nullptr) {
    std::cerr << "no leader elected\n";
    return 1;
  }

  const auto start = std::chrono::steady_clock::now();
  for (int i = 1; i <= operations; ++i) {
    auto response = leader->submit(
        raftkv::ClientCommand::put("k" + std::to_string(i),
                                   "v" + std::to_string(i), "bench",
                                   static_cast<std::uint64_t>(i)),
        2s);
    if (response.status == raftkv::ClientResponse::Status::NotLeader) {
      leader = waitForLeader(nodes);
      if (leader == nullptr) {
        std::cerr << "lost leader\n";
        return 1;
      }
      --i;
      continue;
    }
    if (response.status != raftkv::ClientResponse::Status::Ok) {
      std::cerr << "operation failed at " << i << ": " << response.message
                << '\n';
      return 1;
    }
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  const auto seconds =
      std::chrono::duration_cast<std::chrono::duration<double>>(elapsed)
          .count();

  std::cout << operations << " committed writes in " << seconds << "s ("
            << static_cast<double>(operations) / seconds << " ops/s)\n";

  for (auto& node : nodes) {
    node->stop();
  }
  std::filesystem::remove_all(dir);
  return 0;
}
