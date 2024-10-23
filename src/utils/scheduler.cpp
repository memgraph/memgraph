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

#include "croncpp.h"

namespace memgraph::utils {

void Scheduler::Run(const std::string &service_name, const std::function<void()> &f, std::string_view cron_expr) {
  DMG_ASSERT(!IsRunning(), "Thread already running.");

  thread_ = std::jthread(
      [this, f = f, service_name = service_name, cron = cron::make_cron(cron_expr)](std::stop_token token) mutable {
        ThreadRun(
            std::move(service_name), std::move(f),
            [cron = std::move(cron)](const auto &now) { return cron::cron_next(cron, now); }, token);
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
