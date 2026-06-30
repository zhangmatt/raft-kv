FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      build-essential \
      ca-certificates \
      cmake \
      libgrpc++-dev \
      libgtest-dev \
      libprotobuf-dev \
      ninja-build \
      protobuf-compiler \
      protobuf-compiler-grpc && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release && \
    cmake --build build && \
    ctest --test-dir build --output-on-failure

CMD ["./build/raft_grpc_node", "--id", "1", "--listen", "0.0.0.0:5001", "--advertise", "raft1:5001", "--data", "/data/node1", "--peer", "2=raft2:5002", "--peer", "3=raft3:5003"]
