#include "test_util.h"

#include "raft/log.h"
#include "raft/state_machine.h"

#include <gtest/gtest.h>

#include <filesystem>

namespace {

using namespace raftkv;
using namespace raftkv::test;

TEST(StateMachineTest, DeduplicatesClientRetries) {
  KeyValueStateMachine machine;
  const auto first =
      machine.apply(ClientCommand::put("answer", "42", "client-a", 1), 1);
  const auto duplicate =
      machine.apply(ClientCommand::put("answer", "wrong", "client-a", 1), 2);

  EXPECT_TRUE(first.ok);
  EXPECT_TRUE(duplicate.ok);
  ASSERT_TRUE(machine.localGet("answer").has_value());
  EXPECT_EQ(*machine.localGet("answer"), "42");
  ASSERT_TRUE(duplicate.value.has_value());
  EXPECT_EQ(*duplicate.value, "42");
  EXPECT_EQ(duplicate.index, first.index);
}

TEST(StateMachineTest, SnapshotRoundTripRestoresKvAndDedup) {
  KeyValueStateMachine machine;
  machine.apply(ClientCommand::put("k", "v", "client-a", 1), 1);
  const auto snapshot = machine.snapshot(1, 7);

  KeyValueStateMachine restored;
  restored.restore(snapshot);
  ASSERT_TRUE(restored.localGet("k").has_value());
  EXPECT_EQ(*restored.localGet("k"), "v");

  const auto duplicate =
      restored.apply(ClientCommand::put("k", "other", "client-a", 1), 2);
  ASSERT_TRUE(duplicate.value.has_value());
  EXPECT_EQ(*duplicate.value, "v");
  ASSERT_TRUE(restored.localGet("k").has_value());
  EXPECT_EQ(*restored.localGet("k"), "v");
}

TEST(PersistentLogTest, SurvivesRestart) {
  const auto dir = uniqueTempDir("raft-kv-log");
  {
    PersistentLog log(dir, 1);
    log.setCurrentTerm(3);
    log.setVotedFor(2);
    log.append(LogEntry{1, 1, ClientCommand::noop()});
    log.append(LogEntry{2, 3,
                        ClientCommand::put("x", "1", "client-a", 1)});
    log.persist();
  }

  PersistentLog loaded(dir, 1);
  loaded.load();
  EXPECT_EQ(loaded.currentTerm(), Term{3});
  ASSERT_TRUE(loaded.votedFor().has_value());
  EXPECT_EQ(*loaded.votedFor(), 2);
  EXPECT_EQ(loaded.lastIndex(), Index{2});
  EXPECT_EQ(loaded.lastTerm(), Term{3});
  ASSERT_TRUE(loaded.entryAt(2).has_value());
  EXPECT_EQ(loaded.entryAt(2)->command.key, "x");
  std::filesystem::remove_all(dir);
}

TEST(RaftNodeTest, RequestVoteRejectsStaleCandidateLog) {
  Cluster cluster(3);
  auto& follower = cluster.node(1);
  follower.start();

  AppendEntriesRequest append;
  append.term = 2;
  append.leader_id = 2;
  append.prev_log_index = 0;
  append.prev_log_term = 0;
  append.entries.push_back(
      LogEntry{1, 2, ClientCommand::put("x", "1", "seed", 1)});
  append.leader_commit = 0;
  const auto append_response = follower.handleAppendEntries(append);
  EXPECT_TRUE(append_response.success);

  RequestVoteRequest stale;
  stale.term = 3;
  stale.candidate_id = 3;
  stale.last_log_index = 0;
  stale.last_log_term = 0;
  const auto vote = follower.handleRequestVote(stale);
  EXPECT_FALSE(vote.vote_granted);
  follower.stop();
}

TEST(RaftNodeTest, ThreeNodesElectOneLeader) {
  Cluster cluster(3);
  cluster.start();
  const auto leader = cluster.leader();
  ASSERT_TRUE(leader.has_value());

  int leaders = 0;
  for (int id = 1; id <= 3; ++id) {
    if (cluster.node(id).role() == Role::Leader) {
      ++leaders;
    }
  }
  EXPECT_EQ(leaders, 1);
}

}  // namespace
