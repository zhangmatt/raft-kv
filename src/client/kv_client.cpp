#include "raft/raft_node.h"
#include "rpc/peer_client.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

struct Options {
  int nodes{3};
  std::filesystem::path data_dir{
      std::filesystem::temp_directory_path() / "raft-kv-client"};
  std::string command;
  std::string key;
  std::string value;
};

void usage() {
  std::cerr
      << "usage: kv_client [--nodes N] [--data DIR] put KEY VALUE\n"
      << "       kv_client [--nodes N] [--data DIR] get KEY\n"
      << "       kv_client [--nodes N] [--data DIR] delete KEY\n";
}

std::optional<Options> parse(int argc, char** argv) {
  Options options;
  int i = 1;
  while (i < argc) {
    const std::string arg = argv[i];
    if (arg == "--nodes" && i + 1 < argc) {
      options.nodes = std::stoi(argv[i + 1]);
      i += 2;
    } else if (arg == "--data" && i + 1 < argc) {
      options.data_dir = argv[i + 1];
      i += 2;
    } else {
      break;
    }
  }

  if (i >= argc) {
    return std::nullopt;
  }
  options.command = argv[i++];
  if (i >= argc) {
    return std::nullopt;
  }
  options.key = argv[i++];

  if (options.command == "put") {
    if (i >= argc) {
      return std::nullopt;
    }
    options.value = argv[i++];
  }
  if (i != argc) {
    return std::nullopt;
  }
  if (options.nodes <= 0 || options.nodes % 2 == 0) {
    return std::nullopt;
  }
  if (options.command != "put" && options.command != "get" &&
      options.command != "delete") {
    return std::nullopt;
  }
  return options;
}

std::unique_ptr<raftkv::RaftNode> makeNode(
    int id, const Options& options, raftkv::SimulatedTransport& transport) {
  raftkv::NodeConfig config;
  config.id = id;
  config.storage_dir = options.data_dir.string();
  config.election_timeout_min = 80ms + std::chrono::milliseconds(id * 15);
  config.election_timeout_max = 160ms + std::chrono::milliseconds(id * 15);
  config.heartbeat_interval = 20ms;

  for (int peer = 1; peer <= options.nodes; ++peer) {
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

raftkv::ClientCommand toCommand(const Options& options,
                                const std::string& client_id) {
  constexpr std::uint64_t kSequence = 1;
  if (options.command == "put") {
    return raftkv::ClientCommand::put(options.key, options.value, client_id,
                                      kSequence);
  }
  if (options.command == "delete") {
    return raftkv::ClientCommand::erase(options.key, client_id, kSequence);
  }
  return raftkv::ClientCommand::get(options.key, client_id, kSequence);
}

}  // namespace

int main(int argc, char** argv) {
  const auto options = parse(argc, argv);
  if (!options) {
    usage();
    return 2;
  }

  std::filesystem::create_directories(options->data_dir);

  raftkv::SimulatedTransport transport;
  std::vector<std::unique_ptr<raftkv::RaftNode>> nodes;
  for (int id = 1; id <= options->nodes; ++id) {
    nodes.push_back(makeNode(id, *options, transport));
  }
  for (auto& node : nodes) {
    node->start();
  }

  raftkv::RaftNode* leader = waitForLeader(nodes);
  if (leader == nullptr) {
    std::cerr << "no leader elected\n";
    return 1;
  }

  const auto client_id =
      "kv-client-" +
      std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count());
  auto response = leader->submit(toCommand(*options, client_id), 3s);
  if (response.status == raftkv::ClientResponse::Status::NotLeader) {
    leader = waitForLeader(nodes);
    if (leader != nullptr) {
      response = leader->submit(toCommand(*options, client_id), 3s);
    }
  }

  for (auto& node : nodes) {
    node->stop();
  }

  if (response.status != raftkv::ClientResponse::Status::Ok) {
    std::cerr << response.message << '\n';
    return 1;
  }

  if (options->command == "get") {
    if (!response.result.ok) {
      std::cout << "not found\n";
      return 1;
    }
    std::cout << *response.result.value << '\n';
  } else {
    std::cout << "ok\n";
  }
  return 0;
}
