# Raft KV

Raft KV is a C++20 distributed-systems project: a replicated key-value store
that implements Raft consensus, exposes a gRPC/protobuf API, persists log and
snapshot state, and tests behavior under crashes and network partitions.

The point of the project is correctness under failure, not CRUD features. It is
meant to show the core infra skills behind systems like replicated control
planes, metadata stores, schedulers, and storage services: leader election, log
replication, quorum commit, recovery, linearizability, and fault injection.

## Highlights

- **C++20 systems programming:** threads, mutexes, durable state, binary
  serialization, CMake, Docker.
- **Distributed consensus:** Raft leader election, AppendEntries replication,
  log consistency checks, conflict backtracking, snapshots, and membership
  changes.
- **Production-shaped RPC boundary:** gRPC/protobuf service, client, and peer
  transport; the Raft core is transport-agnostic behind `IRaftTransport`.
- **Failure testing:** deterministic simulator for partitions, crashes, message
  loss, restarts, duplicate client retries, and snapshot catch-up.
- **Engineering hygiene:** GoogleTest, CTest discovery, GitHub Actions CI,
  `vcpkg.json`, Docker Compose, and a documented architecture.

## Current Status

Implemented and tested:

- Single-leader election with randomized timeouts.
- Strongly consistent log replication with majority commit.
- Durable `currentTerm`, `votedFor`, log entries, snapshots, and de-dup state.
- Crash/restart recovery and follower catch-up.
- Snapshot compaction and `InstallSnapshot`.
- Linearizable reads and writes through the Raft log.
- Exactly-once state-machine application via `(clientId, sequence)`.
- Single-server membership changes committed through Raft.
- 3-node and 5-node gRPC clusters via Docker Compose.

## Build

Install dependencies:

```sh
# macOS
brew install grpc googletest protobuf

# Ubuntu 24.04
sudo apt-get install -y build-essential cmake ninja-build \
  libgrpc++-dev libgtest-dev libprotobuf-dev \
  protobuf-compiler protobuf-compiler-grpc
```

Build and test:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build
ctest --test-dir build --output-on-failure
```

The repo also includes `vcpkg.json`:

```sh
cmake -S . -B build \
  -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
```

## Run

Start a 3-node gRPC cluster:

```sh
docker compose up --build raft1 raft2 raft3
```

Write and read through the gRPC client:

```sh
./build/kv_grpc_client --target 127.0.0.1:5001 \
  --peer 1=127.0.0.1:5001 --peer 2=127.0.0.1:5002 --peer 3=127.0.0.1:5003 \
  put hello raft

./build/kv_grpc_client --target 127.0.0.1:5001 \
  --peer 1=127.0.0.1:5001 --peer 2=127.0.0.1:5002 --peer 3=127.0.0.1:5003 \
  get hello
```

Run a 5-node cluster:

```sh
docker compose --profile five-node up --build \
  raft5-1 raft5-2 raft5-3 raft5-4 raft5-5
```

Run the benchmark:

```sh
./build/throughput_bench 1000
```

## Architecture

```text
kv_grpc_client
    |
    v
GrpcRaftService  <---->  GrpcPeerClient
    |                       |
    v                       v
RaftNode  <----------  IRaftTransport
    |
    +--> PersistentLog          term, vote, log, snapshot
    +--> KeyValueStateMachine   KV data + client de-dup
    +--> Commit logic           majority matchIndex, current-term rule
```

The simulator transport implements the same `IRaftTransport` interface as the
gRPC peer client, so tests exercise the real Raft state machine without needing
slow or flaky socket orchestration.

## Design Choices

- **Raft over Paxos:** Raft separates leader election, replication, and safety,
  making the implementation easier to audit and explain.
- **One mutex per node:** correctness first. Raft state transitions touch term,
  vote, log, role, and commit index together.
- **Persist before responding:** votes and accepted log entries are durable
  before the node acknowledges RPC success.
- **Reads through the log:** slower than ReadIndex, but straightforwardly
  linearizable under partitions.
- **At-least-once transport, exactly-once apply:** clients can retry safely
  because the state machine caches the latest result per client sequence.

## Test Coverage

GoogleTest/CTest covers:

- leader election and stale-vote rejection
- leader crash and replacement election
- minority partition unable to commit, majority partition continues
- restart replay and catch-up
- duplicate client retry across leader change
- snapshot catch-up for lagging followers
- dynamic add-server membership path
- protobuf conversion round trips

CI runs the full build and test suite on Ubuntu.
