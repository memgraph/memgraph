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

#include "utils/scheduler.hpp"

#include <ctime>

#include "croncpp.h"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"
#include "utils/temporal.hpp"
#include "utils/thread.hpp"

namespace memgraph::utils {

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
void Scheduler::Run(const std::string &service_name, const std::function<void()> &f) {
  DMG_ASSERT(!IsRunning(), "Thread already running.");
  // Thread setup
  thread_ = std::jthread([this, f = f, service_name = service_name](std::stop_token token) mutable {
    ThreadRun(std::move(service_name), std::move(f), token);
  });
}

void Scheduler::Setup(std::string_view cron_expr) {
  *find_next_.Lock() = [cron = cron::make_cron(cron_expr)](const auto &now, bool /* unused */) {
    auto tm_now = LocalDateTime{now}.tm();
    // Hack to force the mktime to reinterpret the time as is in the system tz
    tm_now.tm_gmtoff = 0;
    tm_now.tm_isdst = -1;
    tm_now.tm_zone = nullptr;
    const auto tm_next = cron::cron_next(cron, tm_now);
    // LocalDateTime reads only the date and time values, ignoring the system tz
    return LocalDateTime{tm_next}.us_since_epoch_;
  };
}

SchedulerInterval::SchedulerInterval(const std::string &str) : period_or_cron{str} {
  if (str.empty()) return;
  try {
    // Try period
    const auto period = std::chrono::seconds(std::stol(str));
    period_or_cron = period;
  } catch (std::invalid_argument /* unused */) {
    // Try cron
    try {
      cron::make_cron(str);
    } catch (cron::bad_cronexpr & /* unused */) {
      LOG_FATAL("Scheduler setup not an interval or cron expression");
    }
  }
}

// Checking stop_possible() is necessary because otherwise calling IsRunning
// on a non-started Scheduler would return true.
bool Scheduler::IsRunning() {
  std::stop_token token = thread_.get_stop_token();
  return token.stop_possible() && !token.stop_requested();
}

void Scheduler::SpinOne() {
  {
    auto lk = std::unique_lock{mutex_};
    spin_ = true;
  }
  condition_variable_.notify_one();
}

void Scheduler::ThreadRun(std::string service_name, std::function<void()> f, std::stop_token token) {
  utils::ThreadSetName(service_name);

  while (true) {
    // First wait then execute the function. We do that in that order
    // because most of the schedulers are started at the beginning of the
    // program and there is probably no work to do in scheduled function at
    // the start of the program. Since Server will log some messages on
    // the program start we let him log first and we make sure by first
    // waiting that function f will not log before it.
    // Check for pause also.
    auto lk = std::unique_lock{mutex_};
    const auto now = std::chrono::system_clock::now();
    time_point next{};
    {
      auto find_locked = find_next_.Lock();
      DMG_ASSERT(*find_locked, "Scheduler not setup properly");
      next = find_locked->operator()(now, true);
    }
    if (next > now) {
      condition_variable_.wait_until(lk, token, next, [&] { return spin_.load(); });
    }
    if (is_paused_) {
      condition_variable_.wait(lk, token, [&] { return !is_paused_ || spin_.load(); });
    }

    if (token.stop_requested()) break;
    if (spin_) {
      spin_ = false;
      continue;
    }

    f();
  }
}

// Concurrent threads may request stopping the scheduler. In that case only one of them will
// actually stop the scheduler, the other one won't. We need to know which one is the successful
// one so that we don't try to join thread concurrently since this could cause undefined behavior.
void Scheduler::Stop() {
  if (thread_.request_stop()) {
    {
      // Lock needs to be held when modifying cv even if atomic
      auto lk = std::unique_lock{mutex_};
      is_paused_ = false;
    }
    condition_variable_.notify_one();
    if (thread_.joinable()) thread_.join();
  }
}

// Sets atomic is_paused_ to true.
void Scheduler::Pause() {
  // Lock needs to be held when modifying cv even if atomic
  auto lk = std::unique_lock{mutex_};
  is_paused_ = true;
}

// Sets atomic is_paused_ to false and notifies thread
void Scheduler::Resume() {
  {
    // Lock needs to be held when modifying cv even if atomic
    auto lk = std::unique_lock{mutex_};
    if (!is_paused_) return;
    is_paused_ = false;
  }
  condition_variable_.notify_one();
}

}  // namespace memgraph::utils