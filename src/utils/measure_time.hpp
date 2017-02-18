#pragma once

#include <chrono>
#include <ratio>
#include <utility>

namespace utils {

using ms = std::chrono::milliseconds;

/**
 * Casts the time difference into DurationUnit (default values is
 * std::chrono::nanoseconds).
 *
 * @tparam DurationUnit type of return value
 * @param delta time difference
 * @return time difference in DurationUnit
 */
template <typename DurationUnit = std::chrono::nanoseconds>
auto to_duration(const std::chrono::duration<long, std::nano> &delta) {
  return std::chrono::duration_cast<DurationUnit>(delta).count();
}

/**
 * Measures time for the function call.
 *
 * @tparam DurationUnit unit of returned value
 * @param func function to execute
 * @param args function arguments
 * @return duration of a function call
 */
template <typename DurationUnit, typename F, typename... Args>
auto measure_time(F func, Args &&... args) {
  auto start_time = std::chrono::high_resolution_clock::now();
  func(std::forward<Args>(args)...);
  return to_duration<DurationUnit>(std::chrono::high_resolution_clock::now() -
                                   start_time);
}

/**
 * @brief Stopwatch
 *
 * idea from Optimized C++ Kurt Guntheroth 2016
 *
 * The instance of this class can return delta time from a single start point.
 * The start point in time is stored within the object.
 */
class Stopwatch {
 public:
  /**
   * Initializes start point to system clock min.
   */
  Stopwatch() : start_(std::chrono::system_clock::time_point::min()) {}

  /**
   * Set start point to system clock min.
   */
  void Clear() { start_ = std::chrono::system_clock::time_point::min(); }

  /**
   * If start isn't system clock min than the Stopwatch has been started.
   *
   * @return bool is the Stopwatch started.
   */
  auto IsStarted() const {
    return (start_ != std::chrono::system_clock::time_point::min());
  }

  /**
   * Set start point to the current time.
   */
  void Start() { start_ = std::chrono::system_clock::now(); }

  /**
   * @return elapsed time in milliseconds
   *    if the Stopwatch isn't active returns 0
   */
  auto GetMs() {
    if (IsStarted()) {
      std::chrono::system_clock::duration diff;
      diff = std::chrono::system_clock::now() - start_;
      return std::chrono::duration_cast<std::chrono::milliseconds>(diff);
    }
    return std::chrono::milliseconds(0);
  }

  /**
   * Empty destructor.
   */
  ~Stopwatch() {}

 private:
  std::chrono::system_clock::time_point start_;
};
}
