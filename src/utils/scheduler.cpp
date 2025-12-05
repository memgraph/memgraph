// Copyright 2025 Memgraph Ltd.
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

#include "flags/run_time_configurable.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/synchronized.hpp"
#include "utils/temporal.hpp"
#include "utils/thread.hpp"
#include "utils/variant_helpers.hpp"

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
  // stop any running thread
  thread_.request_stop();

  // Thread setup
  thread_ = std::jthread([this, f = f, service_name = service_name](std::stop_token token) mutable {
    ThreadRun(std::move(service_name), std::move(f), token);
  });
}

void Scheduler::SetInterval(const SchedulerInterval &setup) {
  if (!setup) {  // Un-setup; let the scheduler wait till infinity
    *find_next_.Lock() = [](auto && /* unused */, bool /* unused */) { return time_point::max(); };
    return;
  }
  setup.Execute(utils::Overloaded{
      [this](std::chrono::milliseconds pause, std::optional<std::chrono::system_clock::time_point> start_time) {
        SetInterval_(pause, start_time);
      },
      [this](std::string_view cron) { SetInterval_(cron); }});
}

void Scheduler::SetInterval_(std::chrono::milliseconds pause,
                             std::optional<std::chrono::system_clock::time_point> start_time) {
  DMG_ASSERT(pause > std::chrono::milliseconds(0), "Pause is invalid. Expected > 0, got {}.", pause.count());

  // Setup
  std::chrono::system_clock::time_point next_execution;
  const auto now = std::chrono::system_clock::now();
  if (start_time) {
    while (*start_time < now) *start_time += pause;
    next_execution = *start_time;
  } else {
    next_execution = now;
  }

  // Function to calculate next
  *find_next_.Lock() = [=](const auto &now, const bool incr) mutable {
    if (!incr) return std::max(now, next_execution);
    if (now > next_execution) {
      next_execution += pause;
      // If multiple periods are missed, execute as soon as possible once
      const auto delta = now - next_execution;
      const int n_periods = delta / pause;
      next_execution += n_periods * pause;  // Closest previous execution
    }
    return next_execution;
  };
}

void Scheduler::SetInterval_(std::string_view cron_expr) {
  *find_next_.Lock() = [cron = cron::make_cron(cron_expr)](const auto &now, bool /* unused */) {
    auto tm_now = LocalDateTime{now}.tm();
    // Hack to force the mktime to reinterpret the time as is in the system tz
    tm_now.tm_gmtoff = 0;
    tm_now.tm_isdst = -1;
    tm_now.tm_zone = nullptr;
    const auto tm_next = cron::cron_next(cron, tm_now);
    // LocalDateTime reads only the date and time values, ignoring the system tz
    return LocalDateTime{tm_next, flags::run_time::GetTimezone()}.us_since_epoch_;
  };
}

SchedulerInterval::SchedulerInterval(std::string str) {
  str = utils::Trim(str);
  if (str.empty()) return;  // Default period_or_cron -> evaluates to false
  bool failure = false;
#ifdef MG_ENTERPRISE
  // Try cron
  try {
    (void)cron::make_cron(str);
    period_or_cron = str;
    return;
  } catch (cron::bad_cronexpr & /* unused */) {
    // Handled later on
    failure = true;
  }
#endif
  try {
    // Try period
    size_t n_processed = 0;
    const auto period = std::chrono::seconds(std::stol(str, &n_processed));
    if (n_processed != str.size()) {
      throw std::invalid_argument{"String contains non numerical characteres."};
    }
    period_or_cron = PeriodStartTime{period, std::nullopt};
    return;
  } catch (const std::invalid_argument & /* unused */) {
    // Handled later on
    failure = true;
  }
  MG_ASSERT(failure, "Failure not handled correctly.");
  LOG_FATAL("Scheduler setup not an interval or cron expression");
}

SchedulerInterval::operator bool() const {
  return std::visit(
      utils::Overloaded{[](const PeriodStartTime &pst) { return pst.period != std::chrono::milliseconds(0); },
                        [](const std::string &cron) { return !cron.empty(); }},
      period_or_cron);
}

// Checking stop_possible() is necessary because otherwise calling IsRunning
// on a non-started Scheduler would return true.
bool Scheduler::IsRunning() {
  const auto token = thread_.get_stop_token();
  return token.stop_possible() && !token.stop_requested();
}

void Scheduler::SpinOnce() {
  {
    auto lk = std::unique_lock{mutex_};
    spin_once_ = true;
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
    const auto now = std::chrono::system_clock::now();
    time_point next{};
    {
      auto find_locked = find_next_.Lock();
      DMG_ASSERT(*find_locked, "Scheduler not setup properly");
      next = find_locked->operator()(now, true);
    }

    {
      auto lk = std::unique_lock{mutex_};
      if (next > now) {
        condition_variable_.wait_until(lk, token, next, [&] { return spin_once_; });
      }
      if (is_paused_) {
        condition_variable_.wait(lk, token, [&] { return !is_paused_ || spin_once_; });
      }

      if (token.stop_requested()) break;
      if (spin_once_) {
        spin_once_ = false;
        continue;
      }
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
