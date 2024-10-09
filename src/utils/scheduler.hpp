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
    DMG_ASSERT(!thread_.joinable(), "Thread already running.");
    DMG_ASSERT(pause > std::chrono::seconds(0), "Pause is invalid. Expected > 0, got {}.", pause.count());

    thread_ = std::jthread([this, pause, f, service_name, start_time](std::stop_token token) mutable {
      auto find_first_execution = [&]() {
        if (start_time) {              // Custom start time; execute as soon as possible
          return *start_time - pause;  // -= simplifies the logic later on
        }
        return std::chrono::system_clock::now();
      };

      auto find_next_execution = [&](auto now) {
        if (start_time) {                                  // Custom start time
          while (*start_time < now) *start_time += pause;  // Find first start in the future
          *start_time -= pause;                            // -= simplifies the logic later on
          return *start_time;
        }
        return now;
      };

      auto next_execution = find_first_execution();

      utils::ThreadSetName(service_name);

      while (!token.stop_requested()) {
        // First wait then execute the function. We do that in that order
        // because most of the schedulers are started at the beginning of the
        // program and there is probably no work to do in scheduled function at
        // the start of the program. Since Server will log some messages on
        // the program start we let him log first and we make sure by first
        // waiting that function f will not log before it.
        // Check for pause also.
        auto lk = std::unique_lock{mutex_};
        next_execution += pause;
        auto now = std::chrono::system_clock::now();
        if (next_execution > now) {
          condition_variable_.wait_until(lk, next_execution, [&] { return token.stop_requested(); });
        } else {
          next_execution = find_next_execution(now);  // Compensate for time drift when using a start time
        }
        // This check must come also before checking if scheduler is paused.
        // Otherwise deadlock could happen, e.g
        // t1 calls Stop on scheduler thread t3
        // t2 calls Pause on scheduler thread t3 right after wait_until finished
        // scheduler then waits until is_paused is false but this possibly won't ever happen
        if (token.stop_requested()) break;
        pause_cv_.wait(lk, [&] { return !is_paused_.load(std::memory_order_acquire); });

        // This check is to allow the stopping thread to stop paused scheduler before executing function one more time.
        if (token.stop_requested()) break;

        f();
      }
    });
  }

  void Resume() {
    is_paused_.store(false, std::memory_order_release);
    pause_cv_.notify_one();
  }

  void Pause() { is_paused_.store(true, std::memory_order_release); }

  void Stop() {
    is_paused_.store(false, std::memory_order_release);
    thread_.request_stop();
    pause_cv_.notify_one();
    condition_variable_.notify_one();
    if (thread_.joinable()) thread_.join();
  }

  bool IsRunning() { return thread_.joinable(); }

  ~Scheduler() { Stop(); }

 private:
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
