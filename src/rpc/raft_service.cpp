#include "rpc/raft_service.h"

namespace raftkv {

RequestVoteResponse RaftService::requestVote(
    const RequestVoteRequest& request) {
  return node_.handleRequestVote(request);
}

AppendEntriesResponse RaftService::appendEntries(
    const AppendEntriesRequest& request) {
  return node_.handleAppendEntries(request);
}

InstallSnapshotResponse RaftService::installSnapshot(
    const InstallSnapshotRequest& request) {
  return node_.handleInstallSnapshot(request);
}

}  // namespace raftkv
