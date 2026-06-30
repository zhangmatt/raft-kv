#pragma once

#include "raft/types.h"

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace raftkv {

class PersistentLog {
 public:
  explicit PersistentLog(std::filesystem::path storage_dir, NodeId node_id);

  void load();
  void persist() const;

  Term currentTerm() const { return current_term_; }
  void setCurrentTerm(Term term) { current_term_ = term; }

  std::optional<NodeId> votedFor() const { return voted_for_; }
  void setVotedFor(std::optional<NodeId> voted_for) { voted_for_ = voted_for; }

  Index snapshotLastIndex() const { return snapshot_.last_included_index; }
  Term snapshotLastTerm() const { return snapshot_.last_included_term; }
  const SnapshotData& snapshot() const { return snapshot_; }

  Index lastIndex() const;
  Term lastTerm() const;
  std::optional<Term> termAt(Index index) const;
  std::optional<LogEntry> entryAt(Index index) const;
  std::vector<LogEntry> entriesFrom(Index index) const;

  const std::vector<LogEntry>& entries() const { return entries_; }

  void append(const LogEntry& entry);
  void appendAll(const std::vector<LogEntry>& entries);
  void truncateSuffix(Index first_index_to_remove);
  void installSnapshot(const SnapshotData& snapshot);
  void compactThrough(Index last_included_index, Term last_included_term,
                      const SnapshotData& snapshot);

 private:
  std::filesystem::path filePath() const;

  std::filesystem::path storage_dir_;
  NodeId node_id_;
  Term current_term_{0};
  std::optional<NodeId> voted_for_;
  SnapshotData snapshot_;
  std::vector<LogEntry> entries_;
};

}  // namespace raftkv
