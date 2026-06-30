#include "raft/raft_node.h"
#include "rpc/grpc_peer_client.h"
#include "rpc/grpc_raft_service.h"

#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/security/server_credentials.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <vector>

using namespace std::chrono_literals;

namespace {

struct PeerSpec {
  raftkv::NodeId id{0};
  std::string address;
};

struct Options {
  raftkv::NodeId id{0};
  std::string listen_address;
  std::string advertise_address;
  std::filesystem::path data_dir{"data"};
  std::vector<PeerSpec> peers;
};

void usage() {
  std::cerr
      << "usage: raft_grpc_node --id ID --listen HOST:PORT "
      << "--advertise HOST:PORT --data DIR --peer ID=HOST:PORT ...\n";
}

std::optional<PeerSpec> parsePeer(const std::string& value) {
  const auto equals = value.find('=');
  if (equals == std::string::npos || equals == 0 || equals + 1 >= value.size()) {
    return std::nullopt;
  }
  return PeerSpec{std::stoi(value.substr(0, equals)),
                  value.substr(equals + 1)};
}

std::optional<Options> parse(int argc, char** argv) {
  Options options;
  for (int i = 1; i < argc;) {
    const std::string arg = argv[i];
    if (arg == "--id" && i + 1 < argc) {
      options.id = std::stoi(argv[i + 1]);
      i += 2;
    } else if (arg == "--listen" && i + 1 < argc) {
      options.listen_address = argv[i + 1];
      i += 2;
    } else if (arg == "--advertise" && i + 1 < argc) {
      options.advertise_address = argv[i + 1];
      i += 2;
    } else if (arg == "--data" && i + 1 < argc) {
      options.data_dir = argv[i + 1];
      i += 2;
    } else if (arg == "--peer" && i + 1 < argc) {
      auto peer = parsePeer(argv[i + 1]);
      if (!peer) {
        return std::nullopt;
      }
      options.peers.push_back(*peer);
      i += 2;
    } else {
      return std::nullopt;
    }
  }

  if (options.id <= 0 || options.listen_address.empty()) {
    return std::nullopt;
  }
  if (options.advertise_address.empty()) {
    options.advertise_address = options.listen_address;
  }
  return options;
}

}  // namespace

int main(int argc, char** argv) {
  const auto options = parse(argc, argv);
  if (!options) {
    usage();
    return 2;
  }

  std::filesystem::create_directories(options->data_dir);

  raftkv::NodeConfig config;
  config.id = options->id;
  config.storage_dir = options->data_dir.string();
  config.election_timeout_min = 150ms + std::chrono::milliseconds(config.id * 25);
  config.election_timeout_max = 300ms + std::chrono::milliseconds(config.id * 25);
  config.heartbeat_interval = 50ms;

  std::map<raftkv::NodeId, std::string> peer_addresses;
  for (const auto& peer : options->peers) {
    if (peer.id == options->id) {
      continue;
    }
    config.peers.push_back(peer.id);
    peer_addresses.emplace(peer.id, peer.address);
  }

  raftkv::rpc::GrpcPeerClient transport(peer_addresses);
  raftkv::RaftNode node(config);
  node.setTransport(&transport);

  raftkv::rpc::GrpcRaftService service(node);
  grpc::ServerBuilder builder;
  builder.AddListeningPort(options->listen_address,
                           grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  auto server = builder.BuildAndStart();
  if (!server) {
    std::cerr << "failed to listen on " << options->listen_address << '\n';
    return 1;
  }

  node.start();
  std::cout << "raft node " << options->id << " listening on "
            << options->listen_address << " advertising "
            << options->advertise_address << " with " << config.peers.size()
            << " peers\n";
  server->Wait();
  node.stop();
  return 0;
}
