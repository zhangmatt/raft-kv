#pragma once

#include <chrono>
#include <cstdint>
#include <random>

namespace raftkv {

class ElectionTimeoutGenerator {
 public:
  ElectionTimeoutGenerator(std::chrono::milliseconds min_timeout,
                           std::chrono::milliseconds max_timeout,
                           std::uint64_t seed);

  std::chrono::milliseconds next();

 private:
  std::chrono::milliseconds min_timeout_;
  std::chrono::milliseconds max_timeout_;
  std::mt19937_64 rng_;
};

}  // namespace raftkv
