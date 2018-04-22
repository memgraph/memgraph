#pragma once

#include <chrono>

namespace utils {

// This class is threadsafe.
class Timer {
 public:
  /** Time elapsed since creation. */
  std::chrono::duration<double> Elapsed() const {
    return std::chrono::steady_clock::now() - start_time_;
  }

 private:
  std::chrono::time_point<std::chrono::steady_clock> start_time_ =
      std::chrono::steady_clock::now();
};

}  // namespace utils
