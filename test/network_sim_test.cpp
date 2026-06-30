#include "test_util.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <set>
#include <string>
#include <vector>

namespace {

using namespace raftkv;
using namespace raftkv::test;

TEST(NetworkSimTest, LeaderCrashElectsReplacement) {
  Cluster cluster(3);
  cluster.start();
  const auto first_leader = cluster.leader();
  ASSERT_TRUE(first_leader.has_value());

  cluster.crash(*first_leader);
  std::set<NodeId> survivors;
  for (int id = 1; id <= 3; ++id) {
    if (id != *first_leader) {
      survivors.insert(id);
    }
  }

  const auto replacement = cluster.leaderWithin(survivors, 3s);
  ASSERT_TRUE(replacement.has_value());
  EXPECT_NE(*replacement, *first_leader);
}

TEST(NetworkSimTest, CommittedPutReplicatesToFollowers) {
  Cluster cluster(3);
  cluster.start();

  const auto response =
      cluster.submitToLeader(ClientCommand::put("color", "blue", "client-a", 1));
  EXPECT_EQ(response.status, ClientResponse::Status::Ok);
  EXPECT_TRUE(cluster.allHave("color", "blue", 3s));
}

TEST(NetworkSimTest, MinorityPartitionCannotCommitButMajorityCan) {
  Cluster cluster(3);
  cluster.start();
  const auto old_leader = cluster.leader();
  ASSERT_TRUE(old_leader.has_value());

  std::set<NodeId> majority;
  for (int id = 1; id <= 3; ++id) {
    if (id != *old_leader) {
      majority.insert(id);
    }
  }
  cluster.transport().partition({{*old_leader}, majority});

  const auto minority_response = cluster.node(*old_leader).submit(
      ClientCommand::put("blocked", "minority", "client-a", 1), 300ms);
  EXPECT_TRUE(minority_response.status == ClientResponse::Status::Timeout ||
              minority_response.status == ClientResponse::Status::NotLeader);

  const auto majority_leader = cluster.leaderWithin(majority, 3s);
  ASSERT_TRUE(majority_leader.has_value());
  const auto majority_response = cluster.node(*majority_leader)
                                     .submit(ClientCommand::put(
                                                 "side", "majority",
                                                 "client-b", 1),
                                             2s);
  EXPECT_EQ(majority_response.status, ClientResponse::Status::Ok);

  cluster.transport().clearPartitions();
  EXPECT_TRUE(cluster.allHave("side", "majority", 3s));
}

TEST(NetworkSimTest, RestartedNodeReplaysAndCatchesUp) {
  Cluster cluster(3);
  cluster.start();

  auto response =
      cluster.submitToLeader(ClientCommand::put("before", "crash", "client-a", 1));
  EXPECT_EQ(response.status, ClientResponse::Status::Ok);
  EXPECT_TRUE(cluster.allHave("before", "crash", 3s));

  cluster.crash(3);
  response =
      cluster.submitToLeader(ClientCommand::put("after", "restart", "client-a", 2));
  EXPECT_EQ(response.status, ClientResponse::Status::Ok);

  cluster.restart(3);
  EXPECT_TRUE(waitUntil(
      [&] {
        const auto before = cluster.node(3).localGet("before");
        const auto after = cluster.node(3).localGet("after");
        return before && *before == "crash" && after && *after == "restart";
      },
      4s));
}

TEST(NetworkSimTest, DuplicateClientRetryAppliesOnceAcrossLeaderChange) {
  Cluster cluster(3);
  cluster.start();

  auto response =
      cluster.submitToLeader(ClientCommand::put("dedup", "first", "client-a", 1));
  EXPECT_EQ(response.status, ClientResponse::Status::Ok);

  const auto current_leader = cluster.leader();
  ASSERT_TRUE(current_leader.has_value());
  cluster.crash(*current_leader);

  std::set<NodeId> survivors;
  for (int id = 1; id <= 3; ++id) {
    if (id != *current_leader) {
      survivors.insert(id);
    }
  }
  const auto new_leader = cluster.leaderWithin(survivors, 3s);
  ASSERT_TRUE(new_leader.has_value());

  response = cluster.node(*new_leader)
                 .submit(ClientCommand::put("dedup", "second", "client-a", 1),
                         2s);
  EXPECT_EQ(response.status, ClientResponse::Status::Ok);
  ASSERT_TRUE(response.result.value.has_value());
  EXPECT_EQ(*response.result.value, "first");
  ASSERT_TRUE(cluster.node(*new_leader).localGet("dedup").has_value());
  EXPECT_EQ(*cluster.node(*new_leader).localGet("dedup"), "first");
}

TEST(NetworkSimTest, SnapshotCatchesUpIsolatedFollower) {
  Cluster cluster(3);
  cluster.start();
  const auto leader = cluster.leader();
  ASSERT_TRUE(leader.has_value());

  const NodeId lagging = *leader == 3 ? 2 : 3;
  cluster.transport().isolate(lagging);

  for (int i = 1; i <= 8; ++i) {
    const auto response = cluster.node(*leader).submit(
        ClientCommand::put("k" + std::to_string(i), "v" + std::to_string(i),
                           "snapshot-client", static_cast<std::uint64_t>(i)),
        2s);
    ASSERT_EQ(response.status, ClientResponse::Status::Ok);
  }

  EXPECT_TRUE(cluster.node(*leader).createSnapshot());
  cluster.transport().heal(lagging);

  EXPECT_TRUE(waitUntil(
      [&] {
        const auto value = cluster.node(lagging).localGet("k8");
        return value && *value == "v8";
      },
      5s));
}

std::unique_ptr<RaftNode> makeMembershipNode(
    NodeId id, const std::filesystem::path& dir, SimulatedTransport& transport,
    bool fast_timeout) {
  NodeConfig config;
  config.id = id;
  config.storage_dir = dir.string();
  config.heartbeat_interval = 25ms;
  config.election_timeout_min =
      fast_timeout ? 70ms : 160ms + std::chrono::milliseconds(id * 20);
  config.election_timeout_max =
      fast_timeout ? 95ms : 240ms + std::chrono::milliseconds(id * 20);

  const int initial_size = id == 4 ? 4 : 3;
  for (int peer = 1; peer <= initial_size; ++peer) {
    if (peer != id) {
      config.peers.push_back(peer);
    }
  }

  auto node = std::make_unique<RaftNode>(config);
  node->setTransport(&transport);
  transport.registerNode(id, node.get());
  return node;
}

std::optional<NodeId> currentLeader(
    const std::vector<std::unique_ptr<RaftNode>>& nodes,
    const std::set<NodeId>& ids) {
  std::optional<NodeId> leader;
  int count = 0;
  for (const auto id : ids) {
    if (nodes[static_cast<std::size_t>(id)]->running() &&
        nodes[static_cast<std::size_t>(id)]->role() == Role::Leader) {
      leader = id;
      ++count;
    }
  }
  if (count == 1) {
    return leader;
  }
  return std::nullopt;
}

TEST(NetworkSimTest, AddFourthNodeReplicatesAndCanBeElected) {
  const auto dir = uniqueTempDir("raft-kv-membership");
  SimulatedTransport transport;
  std::vector<std::unique_ptr<RaftNode>> nodes(5);

  for (int id = 1; id <= 3; ++id) {
    nodes[static_cast<std::size_t>(id)] =
        makeMembershipNode(id, dir, transport, false);
    nodes[static_cast<std::size_t>(id)]->start();
  }
  nodes[4] = makeMembershipNode(4, dir, transport, true);

  std::optional<NodeId> leader;
  ASSERT_TRUE(waitUntil(
      [&] {
        leader = currentLeader(nodes, {1, 2, 3});
        return leader.has_value();
      },
      3s));

  auto response = nodes[static_cast<std::size_t>(*leader)]->addServer(4, 3s);
  ASSERT_EQ(response.status, ClientResponse::Status::Ok);

  nodes[4]->start();
  response = nodes[static_cast<std::size_t>(*leader)]->submit(
      ClientCommand::put("member", "node4", "membership-client", 1), 3s);
  ASSERT_EQ(response.status, ClientResponse::Status::Ok);

  EXPECT_TRUE(waitUntil(
      [&] {
        const auto value = nodes[4]->localGet("member");
        return value && *value == "node4";
      },
      4s));

  nodes[static_cast<std::size_t>(*leader)]->stop();
  EXPECT_TRUE(waitUntil([&] { return nodes[4]->role() == Role::Leader; }, 4s));

  for (std::size_t id = 1; id < nodes.size(); ++id) {
    if (nodes[id]) {
      nodes[id]->stop();
    }
  }
  std::filesystem::remove_all(dir);
}

}  // namespace
