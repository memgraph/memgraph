#pragma once

#include <chrono>

namespace utils {

class Timer {
 public:
  /** Time elapsed since creation. */
  std::chrono::duration<double> Elapsed() {
    return std::chrono::steady_clock::now() - start_time_;
  }

 private:
  std::chrono::time_point<std::chrono::steady_clock> start_time_ =
      std::chrono::steady_clock::now();
};
};
