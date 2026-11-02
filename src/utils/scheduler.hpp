// Copyright 2026 Memgraph Ltd.
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

#include <chrono>
#include <condition_variable>
#include <functional>
#include <thread>
#include <variant>

#include "utils/synchronized.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::utils {

struct SchedulerInterval {
  SchedulerInterval() = default;

  template <typename TRep, typename TPeriod>
  explicit SchedulerInterval(std::chrono::duration<TRep, TPeriod> period,
                             std::optional<std::chrono::system_clock::time_point> start_time = {})
      : period_or_cron{PeriodStartTime{period, start_time}} {}

  explicit SchedulerInterval(std::string str);

  friend bool operator==(const SchedulerInterval &lrh, const SchedulerInterval &rhs) = default;

  struct PeriodStartTime {
    PeriodStartTime() {}

    template <typename TRep, typename TPeriod>
    PeriodStartTime(std::chrono::duration<TRep, TPeriod> period,
                    std::optional<std::chrono::system_clock::time_point> start_time)
        : period{std::chrono::duration_cast<std::chrono::milliseconds>(period)}, start_time{start_time} {}

    std::chrono::milliseconds period{0};
    std::optional<std::chrono::system_clock::time_point> start_time{};

    friend bool operator==(const PeriodStartTime &lrh, const PeriodStartTime &rhs) = default;
  };

  std::variant<PeriodStartTime, std::string> period_or_cron{};

  explicit operator bool() const;

  void Execute(auto &&overloaded) const {
    std::visit(utils::Overloaded([&overloaded](PeriodStartTime pst) { overloaded(pst.period, pst.start_time); },
                                 [&overloaded](std::string_view cron) { overloaded(cron); }),
               period_or_cron);
  }
};

/**
 * Class used to run scheduled function execution.
 */
class Scheduler {
 public:
  Scheduler() = default;
  void Run(const std::string &service_name, const std::function<void()> &f);

  void SetInterval(const SchedulerInterval &setup);

  template <typename TRep, typename TPeriod>
  void SetInterval(const std::chrono::duration<TRep, TPeriod> &period,
                   std::optional<std::chrono::system_clock::time_point> start_time = {}) {
    SetInterval(SchedulerInterval{period, start_time});
  }

  void SetInterval(std::string cron_expr) { SetInterval(SchedulerInterval{cron_expr}); }

  // Wakes a running worker so the new interval applies immediately — plain SetInterval leaves a parked
  // worker on its old wait until timeout (contract verified by Scheduler.SetupAndSpinOnce).
  void SetIntervalAndWake(const SchedulerInterval &setup) {
    SetInterval(setup);
    SpinOnce();
  }

  template <typename TRep, typename TPeriod>
  void SetIntervalAndWake(const std::chrono::duration<TRep, TPeriod> &period,
                          std::optional<std::chrono::system_clock::time_point> start_time = {}) {
    SetIntervalAndWake(SchedulerInterval{period, start_time});
  }

  void Resume();

  void Pause();

  void Stop();

  bool IsRunning();

  bool IsPaused() const { return is_paused_; }

  void SpinOnce();

  using time_point = std::chrono::system_clock::time_point;

  // Taking the current instant as a parameter is what makes a schedule's behaviour around a
  // timezone shift assertable, since a test cannot move the clock to one.
  std::optional<time_point> NextExecution(time_point now) {
    if (is_paused_) return {};
    const auto next = find_next_.WithLock([now](auto &f) { return f(now, false); });
    return next != time_point::max() ? std::make_optional(next) : std::nullopt;
  }

  std::optional<time_point> NextExecution() { return NextExecution(std::chrono::system_clock::now()); }

  ~Scheduler() { Stop(); }

 private:
  void SetInterval_(std::chrono::milliseconds pause,
                    std::optional<std::chrono::system_clock::time_point> start_time = {});

  void SetInterval_(std::string_view cron_expr);

  void ThreadRun(std::string service_name, std::function<void()> f, std::stop_token token);

  Synchronized<std::function<time_point(const time_point &, bool)>> find_next_{
      [](auto && /* unused */, bool /* unused */) { return time_point::max(); }};  // default to infinity

  /**
   * Variable is true for a single cycle when we spin without executing anything.
   */
  bool spin_once_ = false;

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
