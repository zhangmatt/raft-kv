#pragma once

#include "raft/types.h"

#include <map>
#include <optional>
#include <string>

namespace raftkv {

class KeyValueStateMachine {
 public:
  CommandResult apply(const ClientCommand& command, Index index);

  std::optional<CommandResult> cachedResult(const ClientCommand& command) const;
  std::optional<std::string> localGet(const std::string& key) const;

  SnapshotData snapshot(Index last_included_index,
                        Term last_included_term) const;
  void restore(const SnapshotData& snapshot);

  std::size_t size() const { return kv_.size(); }

 private:
  std::map<std::string, std::string> kv_;
  std::map<std::string, DedupRecord> dedup_;
};

}  // namespace raftkv
