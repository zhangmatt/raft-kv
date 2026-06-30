#include "raft/types.h"
#include "raft.grpc.pb.h"
#include "rpc/proto_conversion.h"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace {

struct Options {
  std::string mode{"steady"};
  std::map<raftkv::NodeId, std::string> peers;
  int operations{2000};
  int warmup{200};
  int value_bytes{32};
  int rpc_timeout_ms{1000};
  int overall_timeout_ms{10000};
  int retry_sleep_ms{5};
  std::string client_id{"grpc-bench"};
  std::string kill_command_template;
  bool trace{false};
};

struct SubmitResult {
  raftkv::ClientResponse response;
  raftkv::NodeId contacted_id{0};
};

void usage() {
  std::cerr
      << "usage: grpc_cluster_bench --mode steady --peer ID=HOST:PORT ... "
      << "[--operations N] [--warmup N] [--value-bytes N] [--trace]\n"
      << "       grpc_cluster_bench --mode failover --peer ID=HOST:PORT ... "
      << "--kill-command 'docker compose kill raft{leader_id}' [--trace]\n";
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
  for (int i = 1; i < argc;) {
    const std::string arg = argv[i];
    if (arg == "--mode" && i + 1 < argc) {
      options.mode = argv[i + 1];
      i += 2;
    } else if (arg == "--peer" && i + 1 < argc) {
      auto peer = parsePeer(argv[i + 1]);
      if (!peer) {
        return std::nullopt;
      }
      options.peers.emplace(peer->first, peer->second);
      i += 2;
    } else if (arg == "--operations" && i + 1 < argc) {
      options.operations = std::stoi(argv[i + 1]);
      i += 2;
    } else if (arg == "--warmup" && i + 1 < argc) {
      options.warmup = std::stoi(argv[i + 1]);
      i += 2;
    } else if (arg == "--value-bytes" && i + 1 < argc) {
      options.value_bytes = std::stoi(argv[i + 1]);
      i += 2;
    } else if (arg == "--rpc-timeout-ms" && i + 1 < argc) {
      options.rpc_timeout_ms = std::stoi(argv[i + 1]);
      i += 2;
    } else if (arg == "--overall-timeout-ms" && i + 1 < argc) {
      options.overall_timeout_ms = std::stoi(argv[i + 1]);
      i += 2;
    } else if (arg == "--retry-sleep-ms" && i + 1 < argc) {
      options.retry_sleep_ms = std::stoi(argv[i + 1]);
      i += 2;
    } else if (arg == "--client-id" && i + 1 < argc) {
      options.client_id = argv[i + 1];
      i += 2;
    } else if (arg == "--kill-command" && i + 1 < argc) {
      options.kill_command_template = argv[i + 1];
      i += 2;
    } else if (arg == "--trace") {
      options.trace = true;
      i += 1;
    } else {
      return std::nullopt;
    }
  }

  if (options.peers.empty() || options.operations <= 0 || options.warmup < 0 ||
      options.value_bytes < 0 ||
      (options.mode != "steady" && options.mode != "failover")) {
    return std::nullopt;
  }
  if (options.mode == "failover" && options.kill_command_template.empty()) {
    return std::nullopt;
  }
  return options;
}

std::string replaceAll(std::string value, const std::string& needle,
                       const std::string& replacement) {
  std::size_t pos = 0;
  while ((pos = value.find(needle, pos)) != std::string::npos) {
    value.replace(pos, needle.size(), replacement);
    pos += replacement.size();
  }
  return value;
}

double nsToMs(std::chrono::nanoseconds value) {
  return static_cast<double>(value.count()) / 1'000'000.0;
}

double percentileMs(std::vector<std::chrono::nanoseconds> values,
                    double percentile) {
  std::sort(values.begin(), values.end());
  const auto index = static_cast<std::size_t>(
      percentile * static_cast<double>(values.size() - 1));
  return nsToMs(values[index]);
}

std::string statusName(raftkv::ClientResponse::Status status) {
  switch (status) {
    case raftkv::ClientResponse::Status::Ok:
      return "ok";
    case raftkv::ClientResponse::Status::NotLeader:
      return "not_leader";
    case raftkv::ClientResponse::Status::Timeout:
      return "timeout";
    case raftkv::ClientResponse::Status::Failed:
      return "failed";
  }
  return "unknown";
}

class GrpcBenchClient {
 public:
  explicit GrpcBenchClient(const Options& options) : options_(options) {
    for (const auto& [id, address] : options_.peers) {
      stubs_[id] = raftkv::proto::Raft::NewStub(grpc::CreateChannel(
          address, grpc::InsecureChannelCredentials()));
    }
  }

  SubmitResult submitTo(raftkv::NodeId id,
                        const raftkv::ClientCommand& command) {
    raftkv::proto::ClientCommandRequest request;
    *request.mutable_command() = raftkv::rpc::toProto(command);

    raftkv::proto::ClientCommandResponse response;
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() +
                         std::chrono::milliseconds(options_.rpc_timeout_ms));

    const auto status = stubs_.at(id)->ClientCommand(&context, request, &response);
    if (!status.ok()) {
      if (options_.trace) {
        std::cerr << "rpc node=" << id << " grpc_error="
                  << status.error_message() << '\n';
      }
      return SubmitResult{
          raftkv::ClientResponse{raftkv::ClientResponse::Status::Failed,
                                 std::nullopt, {}, status.error_message()},
          id};
    }
    auto parsed = raftkv::rpc::fromProto(response);
    if (options_.trace) {
      std::cerr << "rpc node=" << id
                << " status=" << statusName(parsed.status)
                << " leader="
                << (parsed.leader_id ? std::to_string(*parsed.leader_id)
                                     : "none")
                << " index=" << parsed.result.index
                << " message=" << parsed.message << '\n';
    }
    return SubmitResult{parsed, id};
  }

  SubmitResult submitUntilCommitted(const raftkv::ClientCommand& command,
                                    std::optional<raftkv::NodeId> leader_hint,
                                    std::optional<raftkv::NodeId> excluded) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(options_.overall_timeout_ms);
    std::optional<raftkv::NodeId> next = leader_hint;

    while (std::chrono::steady_clock::now() < deadline) {
      if (next && (!excluded || *next != *excluded) &&
          options_.peers.contains(*next)) {
        auto result = submitTo(*next, command);
        if (result.response.status == raftkv::ClientResponse::Status::Ok) {
          return result;
        }
        if (result.response.status == raftkv::ClientResponse::Status::NotLeader &&
            result.response.leader_id) {
          next = result.response.leader_id;
          continue;
        }
      }

      for (const auto& [id, _] : options_.peers) {
        if (excluded && id == *excluded) {
          continue;
        }
        auto result = submitTo(id, command);
        if (result.response.status == raftkv::ClientResponse::Status::Ok) {
          return result;
        }
        if (result.response.status == raftkv::ClientResponse::Status::NotLeader &&
            result.response.leader_id) {
          next = result.response.leader_id;
          break;
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(options_.retry_sleep_ms));
    }

    return SubmitResult{
        raftkv::ClientResponse{raftkv::ClientResponse::Status::Timeout,
                               std::nullopt, {},
                               "timed out waiting for committed write"},
        0};
  }

 private:
  const Options& options_;
  std::map<raftkv::NodeId, std::unique_ptr<raftkv::proto::Raft::Stub>> stubs_;
};

raftkv::ClientCommand makePut(const Options& options, int i,
                              std::uint64_t sequence) {
  return raftkv::ClientCommand::put(
      "bench-key-" + std::to_string(i),
      std::string(static_cast<std::size_t>(options.value_bytes), 'x'),
      options.client_id, sequence);
}

int runSteady(const Options& options) {
  GrpcBenchClient client(options);
  std::uint64_t sequence = 1;
  std::optional<raftkv::NodeId> leader;

  for (int i = 0; i < options.warmup; ++i) {
    auto result = client.submitUntilCommitted(makePut(options, -i - 1, sequence++),
                                             leader, std::nullopt);
    if (result.response.status != raftkv::ClientResponse::Status::Ok) {
      std::cerr << "warmup write failed: " << result.response.message << '\n';
      return 1;
    }
    leader = result.contacted_id;
  }

  std::vector<std::chrono::nanoseconds> latencies;
  latencies.reserve(static_cast<std::size_t>(options.operations));
  const auto total_start = std::chrono::steady_clock::now();
  for (int i = 0; i < options.operations; ++i) {
    const auto op_start = std::chrono::steady_clock::now();
    auto result =
        client.submitUntilCommitted(makePut(options, i, sequence++), leader,
                                    std::nullopt);
    const auto op_end = std::chrono::steady_clock::now();
    if (result.response.status != raftkv::ClientResponse::Status::Ok) {
      std::cerr << "write failed: " << result.response.message << '\n';
      return 1;
    }
    leader = result.contacted_id;
    latencies.push_back(
        std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start));
  }
  const auto total_end = std::chrono::steady_clock::now();
  const auto total_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(total_end - total_start);
  const auto total_seconds = static_cast<double>(total_ns.count()) / 1'000'000'000.0;

  const auto [min_it, max_it] =
      std::minmax_element(latencies.begin(), latencies.end());

  std::cout << "mode=steady\n";
  std::cout << "cluster_size=" << options.peers.size() << '\n';
  std::cout << "workload=single_client_sequential_committed_put\n";
  std::cout << "transport=grpc_docker_published_ports\n";
  std::cout << "operations=" << options.operations << '\n';
  std::cout << "warmup_operations=" << options.warmup << '\n';
  std::cout << "value_bytes=" << options.value_bytes << '\n';
  std::cout << "leader_id=" << *leader << '\n';
  std::cout << "total_seconds=" << total_seconds << '\n';
  std::cout << "throughput_ops_per_sec="
            << static_cast<double>(options.operations) / total_seconds << '\n';
  std::cout << "latency_min_ms=" << nsToMs(*min_it) << '\n';
  std::cout << "latency_p50_ms=" << percentileMs(latencies, 0.50) << '\n';
  std::cout << "latency_p99_ms=" << percentileMs(latencies, 0.99) << '\n';
  std::cout << "latency_max_ms=" << nsToMs(*max_it) << '\n';
  return 0;
}

int runFailover(const Options& options) {
  GrpcBenchClient client(options);
  std::uint64_t sequence = 1;
  std::optional<raftkv::NodeId> leader;

  auto prekill = client.submitUntilCommitted(
      makePut(options, -1, sequence++), leader, std::nullopt);
  if (prekill.response.status != raftkv::ClientResponse::Status::Ok) {
    std::cerr << "pre-kill write failed: " << prekill.response.message << '\n';
    return 1;
  }
  leader = prekill.contacted_id;

  const auto kill_command =
      replaceAll(options.kill_command_template, "{leader_id}",
                 std::to_string(*leader));
  const auto kill_start = std::chrono::steady_clock::now();
  const int kill_status = std::system(kill_command.c_str());
  const auto kill_return = std::chrono::steady_clock::now();
  if (kill_status != 0) {
    std::cerr << "kill command failed with status " << kill_status << ": "
              << kill_command << '\n';
    return 1;
  }

  int attempts = 0;
  SubmitResult recovery;
  do {
    ++attempts;
    recovery = client.submitUntilCommitted(makePut(options, attempts, sequence++),
                                           std::nullopt, *leader);
  } while (recovery.response.status != raftkv::ClientResponse::Status::Ok &&
           attempts < options.operations);

  const auto success_time = std::chrono::steady_clock::now();
  if (recovery.response.status != raftkv::ClientResponse::Status::Ok) {
    std::cerr << "recovery write failed: " << recovery.response.message << '\n';
    return 1;
  }

  std::cout << "mode=failover\n";
  std::cout << "cluster_size=" << options.peers.size() << '\n';
  std::cout << "workload=prekill_put_then_kill_leader_then_first_committed_put\n";
  std::cout << "transport=grpc_docker_published_ports\n";
  std::cout << "killed_leader_id=" << *leader << '\n';
  std::cout << "new_leader_id=" << recovery.contacted_id << '\n';
  std::cout << "kill_command=" << kill_command << '\n';
  std::cout << "kill_command_status=" << kill_status << '\n';
  std::cout << "kill_command_elapsed_ms="
            << nsToMs(std::chrono::duration_cast<std::chrono::nanoseconds>(
                   kill_return - kill_start))
            << '\n';
  std::cout << "resume_write_after_kill_start_ms="
            << nsToMs(std::chrono::duration_cast<std::chrono::nanoseconds>(
                   success_time - kill_start))
            << '\n';
  std::cout << "resume_write_after_kill_return_ms="
            << nsToMs(std::chrono::duration_cast<std::chrono::nanoseconds>(
                   success_time - kill_return))
            << '\n';
  std::cout << "recovery_attempts=" << attempts << '\n';
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  const auto options = parse(argc, argv);
  if (!options) {
    usage();
    return 2;
  }

  std::cout.setf(std::ios::fixed);
  std::cout.precision(6);

  if (options->mode == "steady") {
    return runSteady(*options);
  }
  return runFailover(*options);
}
