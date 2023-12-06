// Copyright 2023 Memgraph Ltd.
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
           const std::function<void()> &f) {
    DMG_ASSERT(is_working_ == false, "Thread already running.");
    DMG_ASSERT(pause > std::chrono::seconds(0), "Pause is invalid.");

    is_working_ = true;
    thread_ = std::thread([this, pause, f, service_name]() {
      auto start_time = std::chrono::system_clock::now();

      utils::ThreadSetName(service_name);

      while (true) {
        // First wait then execute the function. We do that in that order
        // because most of the schedulers are started at the beginning of the
        // program and there is probably no work to do in scheduled function at
        // the start of the program. Since Server will log some messages on
        // the program start we let him log first and we make sure by first
        // waiting that funcion f will not log before it.
        std::unique_lock<std::mutex> lk(mutex_);
        auto now = std::chrono::system_clock::now();
        start_time += pause;
        if (start_time > now) {
          condition_variable_.wait_until(lk, start_time, [&] { return is_working_.load() == false; });
        } else {
          start_time = now;
        }

        if (!is_working_) break;
        f();
      }
    });
  }

  /**
   * @brief Stops the thread execution. This is a blocking call and may take as
   * much time as one call to the function given previously to Run takes.
   * @throw std::system_error
   */
  void Stop() {
    is_working_.store(false);
    condition_variable_.notify_one();
    if (thread_.joinable()) thread_.join();
  }

  /**
   * Returns whether the scheduler is running.
   */
  bool IsRunning() { return is_working_; }

  ~Scheduler() { Stop(); }

 private:
  /**
   * Variable is true when thread is running.
   */
  std::atomic<bool> is_working_{false};

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
  std::thread thread_;
};

}  // namespace memgraph::utils
