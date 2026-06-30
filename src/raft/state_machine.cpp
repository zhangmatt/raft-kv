#include "raft/state_machine.h"

namespace raftkv {

std::optional<CommandResult> KeyValueStateMachine::cachedResult(
    const ClientCommand& command) const {
  if (command.client_id.empty() || command.sequence == 0) {
    return std::nullopt;
  }

  const auto it = dedup_.find(command.client_id);
  if (it == dedup_.end() || it->second.sequence < command.sequence) {
    return std::nullopt;
  }
  return it->second.result;
}

CommandResult KeyValueStateMachine::apply(const ClientCommand& command,
                                          Index index) {
  if (auto cached = cachedResult(command)) {
    return *cached;
  }

  CommandResult result;
  result.index = index;

  switch (command.type) {
    case CommandType::Noop:
      result.ok = true;
      break;
    case CommandType::Put:
      kv_[command.key] = command.value;
      result.ok = true;
      result.value = command.value;
      break;
    case CommandType::Delete:
      kv_.erase(command.key);
      result.ok = true;
      break;
    case CommandType::Get: {
      const auto it = kv_.find(command.key);
      if (it == kv_.end()) {
        result.ok = false;
        result.error = "not found";
      } else {
        result.ok = true;
        result.value = it->second;
      }
      break;
    }
    case CommandType::AddServer:
    case CommandType::RemoveServer:
      result.ok = true;
      break;
  }

  if (!command.client_id.empty() && command.sequence != 0) {
    dedup_[command.client_id] = DedupRecord{command.sequence, result};
  }
  return result;
}

std::optional<std::string> KeyValueStateMachine::localGet(
    const std::string& key) const {
  const auto it = kv_.find(key);
  if (it == kv_.end()) {
    return std::nullopt;
  }
  return it->second;
}

SnapshotData KeyValueStateMachine::snapshot(Index last_included_index,
                                            Term last_included_term) const {
  SnapshotData data;
  data.last_included_index = last_included_index;
  data.last_included_term = last_included_term;
  data.kv = kv_;
  data.dedup = dedup_;
  return data;
}

void KeyValueStateMachine::restore(const SnapshotData& snapshot) {
  kv_ = snapshot.kv;
  dedup_ = snapshot.dedup;
}

}  // namespace raftkv
