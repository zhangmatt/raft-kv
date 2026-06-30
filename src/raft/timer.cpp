#include "raft/timer.h"

#include <algorithm>

namespace raftkv {

ElectionTimeoutGenerator::ElectionTimeoutGenerator(
    std::chrono::milliseconds min_timeout,
    std::chrono::milliseconds max_timeout,
    std::uint64_t seed)
    : min_timeout_(min_timeout),
      max_timeout_(std::max(min_timeout, max_timeout)),
      rng_(seed) {}

std::chrono::milliseconds ElectionTimeoutGenerator::next() {
  const auto min_count = min_timeout_.count();
  const auto max_count = max_timeout_.count();
  std::uniform_int_distribution<long long> dist(min_count, max_count);
  return std::chrono::milliseconds(dist(rng_));
}

}  // namespace raftkv
