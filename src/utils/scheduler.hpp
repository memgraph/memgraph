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
#include <ctime>
#include <functional>
#include <thread>

#include "utils/logging.hpp"
#include "utils/temporal.hpp"
#include "utils/thread.hpp"

namespace memgraph::utils {

/**
 * Class used to run scheduled function execution.
 */
class Scheduler {
 public:
  Scheduler() = default;
  /**
   * @param pause - Duration between two function executions. If function is
   * still running when it should be ran again, it will run right after it
   * finishes its previous run.
   * @param f - Function
   * @Tparam TRep underlying arithmetic type in duration
   * @Tparam TPeriod duration in seconds between two ticks
   * @throw std::system_error if thread could not be started.
   * @throw std::bad_alloc
   */
  template <typename TRep, typename TPeriod>
  void Run(const std::string &service_name, const std::chrono::duration<TRep, TPeriod> &pause,
           const std::function<void()> &f, std::optional<std::chrono::system_clock::time_point> start_time = {}) {
    DMG_ASSERT(!IsRunning(), "Thread already running.");
    DMG_ASSERT(pause > std::chrono::seconds(0), "Pause is invalid. Expected > 0, got {}.", pause.count());

    // Setup
    std::chrono::system_clock::time_point next_execution;
    if (start_time) {                        // Custom start time; execute as soon as possible
      next_execution = *start_time - pause;  // -= simplifies the logic later on
    } else {
      next_execution = std::chrono::system_clock::now();
    }

    // Function to calculate next
    auto find_next_execution = [=](const auto &now) mutable {
      next_execution += pause;
      if (next_execution > now) return next_execution;
      if (start_time) {                                  // Custom start time
        while (*start_time < now) *start_time += pause;  // Find first start in the future
        *start_time -= pause;                            // -= simplifies the logic later on
        return *start_time;
      }
      return now;
    };

    thread_ = std::jthread([this, f = f, service_name = service_name,
                            find_next_execution = std::move(find_next_execution)](std::stop_token token) mutable {
      ThreadRun(
          std::move(service_name), std::move(f),
          [find_next_execution = std::move(find_next_execution)](const auto &now) mutable {
            return find_next_execution(now);
          },
          token);
    });
  };

  // Can throw if cron expression is incorrect
  void Run(const std::string &service_name, const std::function<void()> &f, std::string_view cron_expr);

  void Resume();

  void Pause();

  void Stop();

  bool IsRunning();

  ~Scheduler() { Stop(); }

 private:
  template <typename F>
  void ThreadRun(std::string service_name, std::function<void()> f, F &&get_next, std::stop_token token) {
    utils::ThreadSetName(service_name);

    while (true) {
      // First wait then execute the function. We do that in that order
      // because most of the schedulers are started at the beginning of the
      // program and there is probably no work to do in scheduled function at
      // the start of the program. Since Server will log some messages on
      // the program start we let him log first and we make sure by first
      // waiting that function f will not log before it.
      // Check for pause also.
      const auto now = std::chrono::system_clock::now();
      const auto next = get_next(now);
      if (next > now) {
        auto lk = std::unique_lock{mutex_};
        condition_variable_.wait_until(lk, next, [&] { return token.stop_requested(); });
      }

      if (is_paused_) {
        auto lk = std::unique_lock{mutex_};
        pause_cv_.wait(lk, [&] { return !is_paused_.load(std::memory_order_acquire) || token.stop_requested(); });
      }

      if (token.stop_requested()) break;

      f();
    }
  }

  /**
   * Variable is true when thread is paused.
   */
  std::atomic<bool> is_paused_{false};

  /*
   * Wait until the thread is resumed.
   */
  std::condition_variable pause_cv_;

  /**
   * Mutex used to synchronize threads using condition variable.
   */
  std::mutex mutex_;

  /**
   * Condition variable is used to stop waiting until the end of the
   * time interval if destructor is called.
   */
  std::condition_variable condition_variable_;

  /**
   * Thread which runs function.
   */
  std::jthread thread_;
};

}  // namespace memgraph::utils
