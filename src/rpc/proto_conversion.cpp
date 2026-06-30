#include "rpc/proto_conversion.h"

#include <stdexcept>

namespace raftkv::rpc {
namespace {

proto::CommandType toProtoCommandType(CommandType type) {
  switch (type) {
    case CommandType::Noop:
      return proto::COMMAND_TYPE_NOOP;
    case CommandType::Put:
      return proto::COMMAND_TYPE_PUT;
    case CommandType::Delete:
      return proto::COMMAND_TYPE_DELETE;
    case CommandType::Get:
      return proto::COMMAND_TYPE_GET;
    case CommandType::AddServer:
      return proto::COMMAND_TYPE_ADD_SERVER;
    case CommandType::RemoveServer:
      return proto::COMMAND_TYPE_REMOVE_SERVER;
  }
  throw std::invalid_argument("unknown command type");
}

CommandType fromProtoCommandType(proto::CommandType type) {
  switch (type) {
    case proto::COMMAND_TYPE_NOOP:
      return CommandType::Noop;
    case proto::COMMAND_TYPE_PUT:
      return CommandType::Put;
    case proto::COMMAND_TYPE_DELETE:
      return CommandType::Delete;
    case proto::COMMAND_TYPE_GET:
      return CommandType::Get;
    case proto::COMMAND_TYPE_ADD_SERVER:
      return CommandType::AddServer;
    case proto::COMMAND_TYPE_REMOVE_SERVER:
      return CommandType::RemoveServer;
    default:
      throw std::invalid_argument("unknown protobuf command type");
  }
}

}  // namespace

proto::Command toProto(const ClientCommand& command) {
  proto::Command out;
  out.set_type(toProtoCommandType(command.type));
  out.set_key(command.key);
  out.set_value(command.value);
  out.set_client_id(command.client_id);
  out.set_sequence(command.sequence);
  out.set_node_id(command.node_id);
  return out;
}

ClientCommand fromProto(const proto::Command& command) {
  ClientCommand out;
  out.type = fromProtoCommandType(command.type());
  out.key = command.key();
  out.value = command.value();
  out.client_id = command.client_id();
  out.sequence = command.sequence();
  out.node_id = command.node_id();
  return out;
}

proto::CommandResult toProto(const CommandResult& result) {
  proto::CommandResult out;
  out.set_ok(result.ok);
  if (result.value) {
    out.set_value(*result.value);
  }
  out.set_error(result.error);
  out.set_index(result.index);
  return out;
}

CommandResult fromProto(const proto::CommandResult& result) {
  CommandResult out;
  out.ok = result.ok();
  if (result.has_value()) {
    out.value = result.value();
  }
  out.error = result.error();
  out.index = result.index();
  return out;
}

proto::LogEntry toProto(const LogEntry& entry) {
  proto::LogEntry out;
  out.set_index(entry.index);
  out.set_term(entry.term);
  *out.mutable_command() = toProto(entry.command);
  return out;
}

LogEntry fromProto(const proto::LogEntry& entry) {
  LogEntry out;
  out.index = entry.index();
  out.term = entry.term();
  out.command = fromProto(entry.command());
  return out;
}

proto::RequestVoteRequest toProto(const RequestVoteRequest& request) {
  proto::RequestVoteRequest out;
  out.set_term(request.term);
  out.set_candidate_id(request.candidate_id);
  out.set_last_log_index(request.last_log_index);
  out.set_last_log_term(request.last_log_term);
  return out;
}

RequestVoteRequest fromProto(const proto::RequestVoteRequest& request) {
  return RequestVoteRequest{request.term(), request.candidate_id(),
                            request.last_log_index(),
                            request.last_log_term()};
}

proto::RequestVoteResponse toProto(const RequestVoteResponse& response) {
  proto::RequestVoteResponse out;
  out.set_term(response.term);
  out.set_vote_granted(response.vote_granted);
  return out;
}

RequestVoteResponse fromProto(const proto::RequestVoteResponse& response) {
  return RequestVoteResponse{response.term(), response.vote_granted()};
}

proto::AppendEntriesRequest toProto(const AppendEntriesRequest& request) {
  proto::AppendEntriesRequest out;
  out.set_term(request.term);
  out.set_leader_id(request.leader_id);
  out.set_prev_log_index(request.prev_log_index);
  out.set_prev_log_term(request.prev_log_term);
  out.set_leader_commit(request.leader_commit);
  for (const auto& entry : request.entries) {
    *out.add_entries() = toProto(entry);
  }
  return out;
}

AppendEntriesRequest fromProto(const proto::AppendEntriesRequest& request) {
  AppendEntriesRequest out;
  out.term = request.term();
  out.leader_id = request.leader_id();
  out.prev_log_index = request.prev_log_index();
  out.prev_log_term = request.prev_log_term();
  out.leader_commit = request.leader_commit();
  out.entries.reserve(static_cast<std::size_t>(request.entries_size()));
  for (const auto& entry : request.entries()) {
    out.entries.push_back(fromProto(entry));
  }
  return out;
}

proto::AppendEntriesResponse toProto(const AppendEntriesResponse& response) {
  proto::AppendEntriesResponse out;
  out.set_term(response.term);
  out.set_success(response.success);
  out.set_match_index(response.match_index);
  out.set_conflict_index(response.conflict_index);
  out.set_conflict_term(response.conflict_term);
  return out;
}

AppendEntriesResponse fromProto(const proto::AppendEntriesResponse& response) {
  return AppendEntriesResponse{response.term(), response.success(),
                               response.match_index(),
                               response.conflict_index(),
                               response.conflict_term()};
}

proto::Snapshot toProto(const SnapshotData& snapshot) {
  proto::Snapshot out;
  out.set_last_included_index(snapshot.last_included_index);
  out.set_last_included_term(snapshot.last_included_term);
  for (const auto& [key, value] : snapshot.kv) {
    auto* entry = out.add_kv();
    entry->set_key(key);
    entry->set_value(value);
  }
  for (const auto& [client_id, record] : snapshot.dedup) {
    auto* entry = out.add_dedup();
    entry->set_client_id(client_id);
    entry->mutable_record()->set_sequence(record.sequence);
    *entry->mutable_record()->mutable_result() = toProto(record.result);
  }
  return out;
}

SnapshotData fromProto(const proto::Snapshot& snapshot) {
  SnapshotData out;
  out.last_included_index = snapshot.last_included_index();
  out.last_included_term = snapshot.last_included_term();
  for (const auto& entry : snapshot.kv()) {
    out.kv.emplace(entry.key(), entry.value());
  }
  for (const auto& entry : snapshot.dedup()) {
    DedupRecord record;
    record.sequence = entry.record().sequence();
    record.result = fromProto(entry.record().result());
    out.dedup.emplace(entry.client_id(), std::move(record));
  }
  return out;
}

proto::InstallSnapshotRequest toProto(
    const InstallSnapshotRequest& request) {
  proto::InstallSnapshotRequest out;
  out.set_term(request.term);
  out.set_leader_id(request.leader_id);
  *out.mutable_snapshot() = toProto(request.snapshot);
  return out;
}

InstallSnapshotRequest fromProto(
    const proto::InstallSnapshotRequest& request) {
  InstallSnapshotRequest out;
  out.term = request.term();
  out.leader_id = request.leader_id();
  out.snapshot = fromProto(request.snapshot());
  return out;
}

proto::InstallSnapshotResponse toProto(
    const InstallSnapshotResponse& response) {
  proto::InstallSnapshotResponse out;
  out.set_term(response.term);
  out.set_success(response.success);
  out.set_last_included_index(response.last_included_index);
  return out;
}

InstallSnapshotResponse fromProto(
    const proto::InstallSnapshotResponse& response) {
  return InstallSnapshotResponse{response.term(), response.success(),
                                 response.last_included_index()};
}

proto::ClientCommandResponse toProto(const ClientResponse& response) {
  proto::ClientCommandResponse out;
  switch (response.status) {
    case ClientResponse::Status::Ok:
      out.set_status(proto::ClientCommandResponse::STATUS_OK);
      break;
    case ClientResponse::Status::NotLeader:
      out.set_status(proto::ClientCommandResponse::STATUS_NOT_LEADER);
      break;
    case ClientResponse::Status::Timeout:
      out.set_status(proto::ClientCommandResponse::STATUS_TIMEOUT);
      break;
    case ClientResponse::Status::Failed:
      out.set_status(proto::ClientCommandResponse::STATUS_FAILED);
      break;
  }
  if (response.leader_id) {
    out.set_leader_id(*response.leader_id);
  }
  *out.mutable_result() = toProto(response.result);
  out.set_message(response.message);
  return out;
}

ClientResponse fromProto(const proto::ClientCommandResponse& response) {
  ClientResponse out;
  switch (response.status()) {
    case proto::ClientCommandResponse::STATUS_OK:
      out.status = ClientResponse::Status::Ok;
      break;
    case proto::ClientCommandResponse::STATUS_NOT_LEADER:
      out.status = ClientResponse::Status::NotLeader;
      break;
    case proto::ClientCommandResponse::STATUS_TIMEOUT:
      out.status = ClientResponse::Status::Timeout;
      break;
    case proto::ClientCommandResponse::STATUS_FAILED:
      out.status = ClientResponse::Status::Failed;
      break;
    default:
      out.status = ClientResponse::Status::Failed;
      break;
  }
  if (response.has_leader_id()) {
    out.leader_id = response.leader_id();
  }
  out.result = fromProto(response.result());
  out.message = response.message();
  return out;
}

}  // namespace raftkv::rpc
