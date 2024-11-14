// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <thread>

#include "utils/logging.hpp"
#include "utils/synchronized.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::utils {

struct SchedulerInterval {
  SchedulerInterval() = default;

  template <typename TRep, typename TPeriod>
  SchedulerInterval(const std::chrono::duration<TRep, TPeriod> &period)
      : period_or_cron{std::chrono::duration_cast<std::chrono::seconds>(period)} {}
  explicit SchedulerInterval(const std::string &str);

  friend bool operator==(const SchedulerInterval &lrh, const SchedulerInterval &rhs) = default;

  std::variant<std::chrono::seconds, std::string> period_or_cron{};

  explicit operator bool() const {
    return std::visit(utils::Overloaded{[](std::chrono::seconds s) { return s != std::chrono::seconds(0); },
                                        [](const std::string &cron) { return !cron.empty(); }},
                      period_or_cron);
  }

  void Execute(auto &&overloaded) const { std::visit(overloaded, period_or_cron); }
};

/**
 * Class used to run scheduled function execution.
 */
class Scheduler {
 public:
  Scheduler() = default;
  void Run(const std::string &service_name, const std::function<void()> &f);

  template <typename TRep, typename TPeriod>
  void Setup(const std::chrono::duration<TRep, TPeriod> &pause,
             std::optional<std::chrono::system_clock::time_point> start_time = {}) {
    DMG_ASSERT(pause > std::chrono::seconds(0), "Pause is invalid. Expected > 0, got {}.", pause.count());

    // Setup
    std::chrono::system_clock::time_point next_execution;
    if (start_time) {                        // Custom start time; execute as soon as possible
      next_execution = *start_time - pause;  // -= simplifies the logic later on
    } else {
      next_execution = std::chrono::system_clock::now();
    }

    // Function to calculate next
    *find_next_.Lock() = [=](const auto &now) mutable {
      next_execution += pause;
      if (next_execution > now) return next_execution;
      if (start_time) {                                  // Custom start time
        while (*start_time < now) *start_time += pause;  // Find first start in the future
        *start_time -= pause;                            // -= simplifies the logic later on
        return *start_time;
      }
      return now;
    };
  }

  void Setup(std::string_view cron_expr);

  void Setup(const SchedulerInterval &setup) {
    if (!setup) {  // Un-setup; let the scheduler wait till infinity
      *find_next_.Lock() = [](auto && /* unused */) { return time_point::max(); };
      return;
    }
    setup.Execute(utils::Overloaded{[this](auto &in) { Setup(in); }});
  }

  void Resume();

  void Pause();

  void Stop();

  bool IsRunning();

  void SpinOne();

  ~Scheduler() { Stop(); }

 private:
  void ThreadRun(std::string service_name, std::function<void()> f, std::stop_token token);

  using time_point = std::chrono::system_clock::time_point;
  Synchronized<std::function<time_point(const time_point &)>> find_next_{
      [](auto && /* unused */) { return time_point::max(); }};  // default to infinity

  /**
   * Variable is true for a single cycle when we spin without executing anything.
   */
  std::atomic_bool spin_ = false;

  /**
   * Variable is true when thread is paused.
   */
  bool is_paused_ = false;

  /**
   * Mutex used to synchronize threads using condition variable.
   */
  std::mutex mutex_;

  /**
   * Condition variable is used to stop waiting until the end of the
   * time interval if destructor is called.
   */
  std::condition_variable_any condition_variable_;

  /**
   * Thread which runs function.
   */
  std::jthread thread_;
};

}  // namespace memgraph::utils
