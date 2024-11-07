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
#include <memory>

#include "croncpp.h"
#include "flags/run_time_configurable.hpp"
#include "utils/observer.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/temporal.hpp"

namespace memgraph::utils {

namespace {
class CronObserver : public Observer<std::optional<std::string>> {
 public:
  CronObserver(Scheduler *scheduler, cron::cronexpr cron_expr)
      : scheduler_{scheduler}, cron_expr_{std::move(cron_expr)} {}

  // String HAS to be a valid cron expr
  void Update(const std::optional<std::string> &in) override {
    if (!in) {
      scheduler_->Stop();
    }
    *cron_expr_.Lock() = cron::make_cron(*in);
    scheduler_->SpinOne();
  }

  auto get() const { return *cron_expr_.ReadLock(); }

  auto next(const auto &now) {
    const auto cron_locked = cron_expr_.ReadLock();
    return cron::cron_next(*cron_locked, now);
  }

 private:
  Scheduler *scheduler_;
  Synchronized<cron::cronexpr, RWSpinLock> cron_expr_;
};
}  // namespace

void Scheduler::Run(const std::string &service_name, const std::function<void()> &f, std::string_view cron_expr) {
  DMG_ASSERT(!IsRunning(), "Thread already running.");

  thread_ = std::jthread(
      [this, f = f, service_name = service_name, cron = cron::make_cron(cron_expr)](std::stop_token token) mutable {
        auto cron_observer = std::make_shared<CronObserver>(this, std::move(cron));
        memgraph::flags::run_time::SnapshotCronAttach(cron_observer);
        ThreadRun(
            std::move(service_name), std::move(f),
            [cron_observer = std::move(cron_observer)](const auto &now) {
              auto tm_now = LocalDateTime{now}.tm();
              // Hack to force the mktime to reinterpret the time as is in the system tz
              tm_now.tm_gmtoff = 0;
              tm_now.tm_isdst = -1;
              tm_now.tm_zone = nullptr;
              const auto tm_next = cron_observer->next(tm_now);
              // LocalDateTime reads only the date and time values, ignoring the system tz
              return LocalDateTime{tm_next}.us_since_epoch_;
            },
            token);
      });
}
}  // namespace memgraph::utils

// Checking stop_possible() is necessary because otherwise calling IsRunning
// on a non-started Scheduler would return true.
bool memgraph::utils::Scheduler::IsRunning() {
  std::stop_token token = thread_.get_stop_token();
  return token.stop_possible() && !token.stop_requested();
}

// Concurrent threads may request stopping the scheduler. In that case only one of them will
// actually stop the scheduler, the other one won't. We need to know which one is the successful
// one so that we don't try to join thread concurrently since this could cause undefined behavior.
void memgraph::utils::Scheduler::Stop() {
  if (thread_.request_stop()) {
    {
      // Lock needs to be held when modifying cv even if atomic
      auto lk = std::unique_lock{mutex_};
      is_paused_.store(false, std::memory_order_release);
    }
    pause_cv_.notify_one();
    condition_variable_.notify_one();
    if (thread_.joinable()) thread_.join();
  }
}

// Sets atomic is_paused_ to true.
void memgraph::utils::Scheduler::Pause() {
  // Lock needs to be held when modifying cv even if atomic
  auto lk = std::unique_lock{mutex_};
  is_paused_.store(true, std::memory_order_release);
}

// Sets atomic is_paused_ to false and notifies thread
void memgraph::utils::Scheduler::Resume() {
  {
    // Lock needs to be held when modifying cv even if atomic
    auto lk = std::unique_lock{mutex_};
    is_paused_.store(false, std::memory_order_release);
  }
  pause_cv_.notify_one();
}
