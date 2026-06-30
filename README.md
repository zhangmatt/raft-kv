# Raft KV

A C++20 replicated key-value store built around the Raft consensus protocol.
The project has two transports:

- gRPC/protobuf for real node-to-node and client-to-node communication.
- An in-process simulator for deterministic crash, partition, delay, and
  restart tests.

The consensus implementation is transport-agnostic behind `IRaftTransport`, so
the same `RaftNode` core is exercised by fast simulator tests and by live gRPC
nodes.

## What Works

- Leader election with randomized timeouts and RequestVote log freshness checks.
- Log replication with AppendEntries heartbeats, conflict truncation, and leader
  commit advancement only for entries from the current term.
- Durable `currentTerm`, `votedFor`, log entries, snapshots, and client de-dup
  records, persisted before acknowledging Raft RPC success.
- Crash/restart recovery and follower catch-up.
- Snapshot creation, log compaction, and InstallSnapshot catch-up.
- Linearizable writes and reads; reads are submitted as Raft log commands.
- Exactly-once state machine application for retried client requests using
  `(clientId, sequence)`.
- Single-server dynamic membership changes committed through the Raft log.
- GoogleTest coverage for elections, leader crashes, partitions, restarts,
  duplicate client retries, snapshots, and membership changes.
- Live gRPC binaries and Docker Compose clusters.

## Dependencies

The repository includes `vcpkg.json` for reproducible dependency resolution:

- gRPC
- protobuf
- GoogleTest

On macOS with Homebrew:

```sh
brew install grpc googletest protobuf
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
```

With vcpkg:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
```

On Ubuntu 24.04:

```sh
sudo apt-get install -y build-essential cmake ninja-build \
  libgrpc++-dev libgtest-dev libprotobuf-dev \
  protobuf-compiler protobuf-compiler-grpc
cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
```

## Build And Test

```sh
cmake --build build
ctest --test-dir build --output-on-failure
```

Run the benchmark:

```sh
./build/throughput_bench 1000
```

## Run A gRPC Cluster

Start three nodes in separate terminals:

```sh
./build/raft_grpc_node --id 1 --listen 127.0.0.1:5001 \
  --advertise 127.0.0.1:5001 --data /tmp/raft-kv/node1 \
  --peer 2=127.0.0.1:5002 --peer 3=127.0.0.1:5003

./build/raft_grpc_node --id 2 --listen 127.0.0.1:5002 \
  --advertise 127.0.0.1:5002 --data /tmp/raft-kv/node2 \
  --peer 1=127.0.0.1:5001 --peer 3=127.0.0.1:5003

./build/raft_grpc_node --id 3 --listen 127.0.0.1:5003 \
  --advertise 127.0.0.1:5003 --data /tmp/raft-kv/node3 \
  --peer 1=127.0.0.1:5001 --peer 2=127.0.0.1:5002
```

Submit client commands:

```sh
./build/kv_grpc_client --target 127.0.0.1:5001 \
  --peer 1=127.0.0.1:5001 --peer 2=127.0.0.1:5002 --peer 3=127.0.0.1:5003 \
  put hello raft

./build/kv_grpc_client --target 127.0.0.1:5001 \
  --peer 1=127.0.0.1:5001 --peer 2=127.0.0.1:5002 --peer 3=127.0.0.1:5003 \
  get hello
```

The client retries once on `NotLeader` when a leader hint maps to a supplied
`--peer` address.

## Docker

Build and run the default 3-node gRPC cluster:

```sh
docker compose up --build raft1 raft2 raft3
```

Run a 5-node cluster:

```sh
docker compose --profile five-node up --build \
  raft5-1 raft5-2 raft5-3 raft5-4 raft5-5
```

## In-Process Demo

The simulator-backed demo is useful for a quick local sanity check without
opening sockets:

```sh
./build/raft_server 3 /tmp/raft-kv-demo
./build/kv_client --data /tmp/raft-kv-demo put local-key local-value
./build/kv_client --data /tmp/raft-kv-demo get local-key
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
    +--> PersistentLog          currentTerm, votedFor, log, snapshot
    +--> KeyValueStateMachine   put/delete/get + client request de-dup
    +--> commit advancement     majority matchIndex, current-term rule
```

Each node uses one mutex around Raft state. RPC handlers persist term, vote, and
log changes before acknowledging success. Reads go through the log; this is
simpler and linearizable, while ReadIndex would reduce read latency at the cost
of another leadership-confirmation path.

## Design Rationale

- Raft over Paxos: Raft separates leader election, log replication, and safety,
  which makes the implementation easier to audit and explain.
- Single mutex per node: Raft safety depends on atomic transitions across term,
  vote, log, commit index, and role. Fine-grained locking is a later
  optimization.
- Persist before responding: a vote or accepted log entry that is acknowledged
  but lost on crash can violate election safety or log matching after restart.
- Linearizable reads: logged reads are slower than ReadIndex but straightforward
  and correct under partitions.
- Partitions: a minority cannot elect or commit because it lacks a quorum. The
  majority continues; after healing, higher terms and log matching overwrite
  uncommitted minority entries.
- Exactly-once application: the system assumes at-least-once delivery, then
  makes state machine application idempotent with `(clientId, sequence)`.
- Membership: this uses the single-server change approach. Joint consensus is
  the natural extension for arbitrary batched reconfiguration.

## Known Tradeoffs

- Persistence uses an atomic whole-state file per node. This preserves the Raft
  durability rule for this implementation, but a production storage layer should
  use an append-only WAL plus checkpoint snapshots.
- The gRPC transport uses insecure local credentials. TLS/authentication are
  deployment concerns outside the consensus core.
