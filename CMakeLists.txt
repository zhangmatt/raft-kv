cmake_minimum_required(VERSION 3.20)

project(raft_kv LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(RAFT_KV_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)

if(APPLE)
  foreach(homebrew_prefix "/opt/homebrew" "/usr/local")
    if(EXISTS "${homebrew_prefix}")
      list(APPEND CMAKE_PREFIX_PATH "${homebrew_prefix}")
    endif()
  endforeach()
endif()

find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(GTest CONFIG REQUIRED)

if(TARGET protobuf::protoc)
  set(RAFT_PROTOC_EXECUTABLE "$<TARGET_FILE:protobuf::protoc>")
else()
  find_program(RAFT_PROTOC_EXECUTABLE protoc REQUIRED)
endif()

if(TARGET gRPC::grpc_cpp_plugin)
  set(RAFT_GRPC_CPP_PLUGIN "$<TARGET_FILE:gRPC::grpc_cpp_plugin>")
else()
  find_program(RAFT_GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)
endif()

set(RAFT_PROTO_FILE "${CMAKE_CURRENT_SOURCE_DIR}/proto/raft.proto")
set(RAFT_PROTO_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${RAFT_PROTO_GEN_DIR}")

add_custom_command(
  OUTPUT
    "${RAFT_PROTO_GEN_DIR}/raft.pb.cc"
    "${RAFT_PROTO_GEN_DIR}/raft.pb.h"
    "${RAFT_PROTO_GEN_DIR}/raft.grpc.pb.cc"
    "${RAFT_PROTO_GEN_DIR}/raft.grpc.pb.h"
  COMMAND ${RAFT_PROTOC_EXECUTABLE}
  ARGS
    --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
    --cpp_out=${RAFT_PROTO_GEN_DIR}
    --grpc_out=${RAFT_PROTO_GEN_DIR}
    --plugin=protoc-gen-grpc=${RAFT_GRPC_CPP_PLUGIN}
    ${RAFT_PROTO_FILE}
  DEPENDS "${RAFT_PROTO_FILE}"
  VERBATIM
)

add_library(raft_kv_proto
  "${RAFT_PROTO_GEN_DIR}/raft.pb.cc"
  "${RAFT_PROTO_GEN_DIR}/raft.grpc.pb.cc"
)

target_include_directories(raft_kv_proto PUBLIC "${RAFT_PROTO_GEN_DIR}")
target_link_libraries(raft_kv_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

add_library(raft_kv_core
  src/raft/log.cpp
  src/raft/raft_node.cpp
  src/raft/state_machine.cpp
  src/raft/timer.cpp
  src/rpc/peer_client.cpp
  src/rpc/raft_service.cpp
)

target_include_directories(raft_kv_core PUBLIC src)

if(MSVC)
  target_compile_options(raft_kv_core PRIVATE /W4)
  if(RAFT_KV_WARNINGS_AS_ERRORS)
    target_compile_options(raft_kv_core PRIVATE /WX)
  endif()
else()
  target_compile_options(raft_kv_core PRIVATE -Wall -Wextra -Wpedantic)
  if(RAFT_KV_WARNINGS_AS_ERRORS)
    target_compile_options(raft_kv_core PRIVATE -Werror)
  endif()
endif()

add_library(raft_kv_grpc
  src/rpc/grpc_peer_client.cpp
  src/rpc/grpc_raft_service.cpp
  src/rpc/proto_conversion.cpp
)

target_link_libraries(raft_kv_grpc PUBLIC raft_kv_core raft_kv_proto)

add_executable(raft_server src/server/main.cpp)
target_link_libraries(raft_server PRIVATE raft_kv_core)

add_executable(kv_client src/client/kv_client.cpp)
target_link_libraries(kv_client PRIVATE raft_kv_core)

add_executable(raft_grpc_node src/server/grpc_main.cpp)
target_link_libraries(raft_grpc_node PRIVATE raft_kv_grpc)

add_executable(kv_grpc_client src/client/grpc_kv_client.cpp)
target_link_libraries(kv_grpc_client PRIVATE raft_kv_grpc)

add_executable(throughput_bench bench/throughput_bench.cpp)
target_link_libraries(throughput_bench PRIVATE raft_kv_core)

add_executable(raft_unit_tests test/raft_unit_test.cpp)
target_link_libraries(raft_unit_tests PRIVATE raft_kv_core GTest::gtest_main)

add_executable(network_sim_tests test/network_sim_test.cpp)
target_link_libraries(network_sim_tests PRIVATE raft_kv_core GTest::gtest_main)

add_executable(grpc_proto_tests test/grpc_proto_test.cpp)
target_link_libraries(grpc_proto_tests PRIVATE raft_kv_grpc GTest::gtest_main)

enable_testing()
include(GoogleTest)
gtest_discover_tests(raft_unit_tests)
gtest_discover_tests(network_sim_tests)
gtest_discover_tests(grpc_proto_tests)
