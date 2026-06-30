#include "raft/types.h"
#include "raft.grpc.pb.h"
#include "rpc/proto_conversion.h"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <chrono>
#include <iostream>
#include <map>
#include <optional>
#include <string>

using namespace std::chrono_literals;

namespace {

struct Options {
  std::string target;
  std::map<raftkv::NodeId, std::string> peers;
  std::string command;
  std::string key;
  std::string value;
};

void usage() {
  std::cerr
      << "usage: kv_grpc_client --target HOST:PORT [--peer ID=HOST:PORT ...] "
      << "put KEY VALUE\n"
      << "       kv_grpc_client --target HOST:PORT [--peer ID=HOST:PORT ...] "
      << "get KEY\n"
      << "       kv_grpc_client --target HOST:PORT [--peer ID=HOST:PORT ...] "
      << "delete KEY\n";
}

std::optional<std::pair<raftkv::NodeId, std::string>> parsePeer(
    const std::string& value) {
  const auto equals = value.find('=');
  if (equals == std::string::npos || equals == 0 || equals + 1 >= value.size()) {
    return std::nullopt;
  }
  return std::pair<raftkv::NodeId, std::string>{
      std::stoi(value.substr(0, equals)), value.substr(equals + 1)};
}

std::optional<Options> parse(int argc, char** argv) {
  Options options;
  int i = 1;
  while (i < argc) {
    const std::string arg = argv[i];
    if (arg == "--target" && i + 1 < argc) {
      options.target = argv[i + 1];
      i += 2;
    } else if (arg == "--peer" && i + 1 < argc) {
      auto peer = parsePeer(argv[i + 1]);
      if (!peer) {
        return std::nullopt;
      }
      options.peers.emplace(peer->first, peer->second);
      i += 2;
    } else {
      break;
    }
  }

  if (options.target.empty() || i >= argc) {
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
  if (options.command != "put" && options.command != "get" &&
      options.command != "delete") {
    return std::nullopt;
  }
  return options;
}

raftkv::ClientCommand makeCommand(const Options& options,
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

raftkv::ClientResponse send(const std::string& target,
                            const raftkv::ClientCommand& command) {
  auto channel =
      grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
  auto stub = raftkv::proto::Raft::NewStub(channel);

  raftkv::proto::ClientCommandRequest request;
  *request.mutable_command() = raftkv::rpc::toProto(command);

  raftkv::proto::ClientCommandResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + 3s);
  const auto status = stub->ClientCommand(&context, request, &response);
  if (!status.ok()) {
    return raftkv::ClientResponse{raftkv::ClientResponse::Status::Failed,
                                  std::nullopt, {}, status.error_message()};
  }
  return raftkv::rpc::fromProto(response);
}

}  // namespace

int main(int argc, char** argv) {
  const auto options = parse(argc, argv);
  if (!options) {
    usage();
    return 2;
  }

  const auto client_id =
      "grpc-client-" +
      std::to_string(
          std::chrono::steady_clock::now().time_since_epoch().count());
  const auto command = makeCommand(*options, client_id);

  auto response = send(options->target, command);
  if (response.status == raftkv::ClientResponse::Status::NotLeader &&
      response.leader_id) {
    const auto leader = options->peers.find(*response.leader_id);
    if (leader != options->peers.end()) {
      response = send(leader->second, command);
    }
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
