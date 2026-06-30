#include "rpc/proto_conversion.h"

#include <gtest/gtest.h>

namespace {

using namespace raftkv;

TEST(ProtoConversionTest, CommandRoundTripPreservesMembershipFields) {
  const auto command = ClientCommand::addServer(4);
  const auto decoded = rpc::fromProto(rpc::toProto(command));

  EXPECT_EQ(decoded.type, CommandType::AddServer);
  EXPECT_EQ(decoded.node_id, 4);
}

TEST(ProtoConversionTest, AppendEntriesRoundTripPreservesEntries) {
  AppendEntriesRequest request;
  request.term = 3;
  request.leader_id = 1;
  request.prev_log_index = 7;
  request.prev_log_term = 2;
  request.leader_commit = 9;
  request.entries.push_back(
      LogEntry{8, 3, ClientCommand::put("key", "value", "client", 1)});

  const auto decoded = rpc::fromProto(rpc::toProto(request));

  EXPECT_EQ(decoded.term, request.term);
  EXPECT_EQ(decoded.leader_id, request.leader_id);
  EXPECT_EQ(decoded.prev_log_index, request.prev_log_index);
  EXPECT_EQ(decoded.prev_log_term, request.prev_log_term);
  EXPECT_EQ(decoded.leader_commit, request.leader_commit);
  ASSERT_EQ(decoded.entries.size(), 1U);
  EXPECT_EQ(decoded.entries[0].command.key, "key");
  EXPECT_EQ(decoded.entries[0].command.value, "value");
}

TEST(ProtoConversionTest, SnapshotRoundTripPreservesKvAndDedup) {
  SnapshotData snapshot;
  snapshot.last_included_index = 12;
  snapshot.last_included_term = 4;
  snapshot.kv.emplace("a", "b");
  CommandResult result;
  result.ok = true;
  result.value = "b";
  result.index = 12;
  snapshot.dedup.emplace("client", DedupRecord{7, result});

  const auto decoded = rpc::fromProto(rpc::toProto(snapshot));

  EXPECT_EQ(decoded.last_included_index, snapshot.last_included_index);
  EXPECT_EQ(decoded.last_included_term, snapshot.last_included_term);
  ASSERT_TRUE(decoded.kv.contains("a"));
  EXPECT_EQ(decoded.kv.at("a"), "b");
  ASSERT_TRUE(decoded.dedup.contains("client"));
  EXPECT_EQ(decoded.dedup.at("client").sequence, 7U);
  ASSERT_TRUE(decoded.dedup.at("client").result.value.has_value());
  EXPECT_EQ(*decoded.dedup.at("client").result.value, "b");
}

}  // namespace
