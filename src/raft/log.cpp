#include "raft/log.h"

#include <algorithm>
#include <array>
#include <cstdio>
#include <fstream>
#include <stdexcept>

namespace raftkv {
namespace {

constexpr std::array<char, 8> kMagic{'R', 'K', 'V', 'S', 'T', '0', '0', '2'};

template <typename T>
void writePod(std::ostream& out, const T& value) {
  out.write(reinterpret_cast<const char*>(&value), sizeof(T));
  if (!out) {
    throw std::runtime_error("failed writing persistent state");
  }
}

template <typename T>
T readPod(std::istream& in) {
  T value{};
  in.read(reinterpret_cast<char*>(&value), sizeof(T));
  if (!in) {
    throw std::runtime_error("failed reading persistent state");
  }
  return value;
}

void writeString(std::ostream& out, const std::string& value) {
  const auto size = static_cast<std::uint64_t>(value.size());
  writePod(out, size);
  out.write(value.data(), static_cast<std::streamsize>(value.size()));
  if (!out) {
    throw std::runtime_error("failed writing string");
  }
}

std::string readString(std::istream& in) {
  const auto size = readPod<std::uint64_t>(in);
  if (size > 64ULL * 1024ULL * 1024ULL) {
    throw std::runtime_error("refusing to read oversized persistent string");
  }
  std::string value(size, '\0');
  in.read(value.data(), static_cast<std::streamsize>(size));
  if (!in) {
    throw std::runtime_error("failed reading string");
  }
  return value;
}

void writeResult(std::ostream& out, const CommandResult& result) {
  writePod(out, static_cast<std::uint8_t>(result.ok ? 1 : 0));
  writePod(out, static_cast<std::uint8_t>(result.value.has_value() ? 1 : 0));
  if (result.value) {
    writeString(out, *result.value);
  }
  writeString(out, result.error);
  writePod(out, result.index);
}

CommandResult readResult(std::istream& in) {
  CommandResult result;
  result.ok = readPod<std::uint8_t>(in) != 0;
  const bool has_value = readPod<std::uint8_t>(in) != 0;
  if (has_value) {
    result.value = readString(in);
  }
  result.error = readString(in);
  result.index = readPod<Index>(in);
  return result;
}

void writeCommand(std::ostream& out, const ClientCommand& command) {
  writePod(out, static_cast<std::uint8_t>(command.type));
  writeString(out, command.key);
  writeString(out, command.value);
  writeString(out, command.client_id);
  writePod(out, command.sequence);
  writePod(out, command.node_id);
}

ClientCommand readCommand(std::istream& in) {
  ClientCommand command;
  command.type = static_cast<CommandType>(readPod<std::uint8_t>(in));
  command.key = readString(in);
  command.value = readString(in);
  command.client_id = readString(in);
  command.sequence = readPod<std::uint64_t>(in);
  command.node_id = readPod<NodeId>(in);
  return command;
}

void writeSnapshot(std::ostream& out, const SnapshotData& snapshot) {
  writePod(out, snapshot.last_included_index);
  writePod(out, snapshot.last_included_term);

  writePod(out, static_cast<std::uint64_t>(snapshot.kv.size()));
  for (const auto& [key, value] : snapshot.kv) {
    writeString(out, key);
    writeString(out, value);
  }

  writePod(out, static_cast<std::uint64_t>(snapshot.dedup.size()));
  for (const auto& [client_id, record] : snapshot.dedup) {
    writeString(out, client_id);
    writePod(out, record.sequence);
    writeResult(out, record.result);
  }
}

SnapshotData readSnapshot(std::istream& in) {
  SnapshotData snapshot;
  snapshot.last_included_index = readPod<Index>(in);
  snapshot.last_included_term = readPod<Term>(in);

  const auto kv_size = readPod<std::uint64_t>(in);
  for (std::uint64_t i = 0; i < kv_size; ++i) {
    snapshot.kv.emplace(readString(in), readString(in));
  }

  const auto dedup_size = readPod<std::uint64_t>(in);
  for (std::uint64_t i = 0; i < dedup_size; ++i) {
    const auto client_id = readString(in);
    DedupRecord record;
    record.sequence = readPod<std::uint64_t>(in);
    record.result = readResult(in);
    snapshot.dedup.emplace(client_id, std::move(record));
  }
  return snapshot;
}

}  // namespace

PersistentLog::PersistentLog(std::filesystem::path storage_dir, NodeId node_id)
    : storage_dir_(std::move(storage_dir)), node_id_(node_id) {}

std::filesystem::path PersistentLog::filePath() const {
  return storage_dir_ / ("node_" + std::to_string(node_id_) + ".state");
}

void PersistentLog::load() {
  current_term_ = 0;
  voted_for_.reset();
  snapshot_ = SnapshotData{};
  entries_.clear();

  const auto path = filePath();
  if (!std::filesystem::exists(path)) {
    return;
  }

  std::ifstream in(path, std::ios::binary);
  if (!in) {
    throw std::runtime_error("failed opening persistent state: " +
                             path.string());
  }

  std::array<char, 8> magic{};
  in.read(magic.data(), static_cast<std::streamsize>(magic.size()));
  if (magic != kMagic) {
    throw std::runtime_error("bad persistent state magic: " + path.string());
  }

  current_term_ = readPod<Term>(in);
  const auto voted_for = readPod<std::int32_t>(in);
  voted_for_ = voted_for < 0 ? std::optional<NodeId>{}
                             : std::optional<NodeId>{voted_for};
  snapshot_ = readSnapshot(in);

  const auto entry_count = readPod<std::uint64_t>(in);
  entries_.reserve(static_cast<std::size_t>(entry_count));
  Index expected_index = snapshot_.last_included_index + 1;
  for (std::uint64_t i = 0; i < entry_count; ++i) {
    LogEntry entry;
    entry.index = readPod<Index>(in);
    entry.term = readPod<Term>(in);
    entry.command = readCommand(in);
    if (entry.index != expected_index) {
      throw std::runtime_error("persistent log contains non-contiguous index");
    }
    entries_.push_back(std::move(entry));
    ++expected_index;
  }
}

void PersistentLog::persist() const {
  std::filesystem::create_directories(storage_dir_);
  const auto path = filePath();
  const auto tmp_path = path.string() + ".tmp";

  {
    std::ofstream out(tmp_path, std::ios::binary | std::ios::trunc);
    if (!out) {
      throw std::runtime_error("failed opening temp persistent state: " +
                               tmp_path);
    }

    out.write(kMagic.data(), static_cast<std::streamsize>(kMagic.size()));
    writePod(out, current_term_);
    writePod(out, static_cast<std::int32_t>(voted_for_.value_or(-1)));
    writeSnapshot(out, snapshot_);
    writePod(out, static_cast<std::uint64_t>(entries_.size()));
    for (const auto& entry : entries_) {
      writePod(out, entry.index);
      writePod(out, entry.term);
      writeCommand(out, entry.command);
    }
  }

  std::filesystem::rename(tmp_path, path);
}

Index PersistentLog::lastIndex() const {
  if (entries_.empty()) {
    return snapshot_.last_included_index;
  }
  return entries_.back().index;
}

Term PersistentLog::lastTerm() const {
  if (entries_.empty()) {
    return snapshot_.last_included_term;
  }
  return entries_.back().term;
}

std::optional<Term> PersistentLog::termAt(Index index) const {
  if (index == 0) {
    return Term{0};
  }
  if (index == snapshot_.last_included_index) {
    return snapshot_.last_included_term;
  }
  if (index < snapshot_.last_included_index || index > lastIndex()) {
    return std::nullopt;
  }
  return entries_.at(static_cast<std::size_t>(
                         index - snapshot_.last_included_index - 1))
      .term;
}

std::optional<LogEntry> PersistentLog::entryAt(Index index) const {
  if (index <= snapshot_.last_included_index || index > lastIndex()) {
    return std::nullopt;
  }
  return entries_.at(static_cast<std::size_t>(
      index - snapshot_.last_included_index - 1));
}

std::vector<LogEntry> PersistentLog::entriesFrom(Index index) const {
  std::vector<LogEntry> result;
  if (index <= snapshot_.last_included_index) {
    index = snapshot_.last_included_index + 1;
  }
  if (index > lastIndex()) {
    return result;
  }

  const auto offset =
      static_cast<std::size_t>(index - snapshot_.last_included_index - 1);
  result.insert(result.end(), entries_.begin() + static_cast<std::ptrdiff_t>(offset),
                entries_.end());
  return result;
}

void PersistentLog::append(const LogEntry& entry) {
  if (entry.index != lastIndex() + 1) {
    throw std::logic_error("append would make log non-contiguous");
  }
  entries_.push_back(entry);
}

void PersistentLog::appendAll(const std::vector<LogEntry>& entries) {
  for (const auto& entry : entries) {
    append(entry);
  }
}

void PersistentLog::truncateSuffix(Index first_index_to_remove) {
  if (first_index_to_remove <= snapshot_.last_included_index + 1) {
    entries_.clear();
    return;
  }
  if (first_index_to_remove > lastIndex()) {
    return;
  }
  const auto new_size =
      static_cast<std::size_t>(first_index_to_remove -
                               snapshot_.last_included_index - 1);
  entries_.resize(new_size);
}

void PersistentLog::installSnapshot(const SnapshotData& snapshot) {
  if (snapshot.last_included_index < snapshot_.last_included_index) {
    return;
  }

  std::vector<LogEntry> suffix;
  if (snapshot.last_included_index < lastIndex()) {
    for (const auto& entry : entries_) {
      if (entry.index > snapshot.last_included_index) {
        suffix.push_back(entry);
      }
    }
  }

  snapshot_ = snapshot;
  entries_ = std::move(suffix);
}

void PersistentLog::compactThrough(Index last_included_index,
                                   Term last_included_term,
                                   const SnapshotData& snapshot) {
  if (last_included_index <= snapshot_.last_included_index) {
    return;
  }
  if (last_included_index > lastIndex()) {
    throw std::logic_error("cannot compact beyond last log index");
  }

  snapshot_ = snapshot;
  snapshot_.last_included_index = last_included_index;
  snapshot_.last_included_term = last_included_term;

  entries_.erase(
      std::remove_if(entries_.begin(), entries_.end(),
                     [last_included_index](const LogEntry& entry) {
                       return entry.index <= last_included_index;
                     }),
      entries_.end());
}

}  // namespace raftkv
