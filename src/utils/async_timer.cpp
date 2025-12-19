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

#include "utils/async_timer.hpp"

#include "utils/logging.hpp"

namespace memgraph::utils {

AsyncTimer::AsyncTimer(double seconds) : expiration_flag_{std::make_shared<std::atomic<bool>>(false)} {
  MG_ASSERT(seconds >= 0.0, "AsyncTimer cannot handle negative time values: {:f}", seconds);

  // Convert seconds to milliseconds
  auto ms = static_cast<int64_t>(seconds * 1000.0);
  if (ms == 0 && seconds > 0) {
    ms = 1;  // Minimum 1ms for non-zero timers
  }

  auto flag = expiration_flag_;  // Capture by value for callback
  handle_ = ConsolidatedScheduler::Global().ScheduleAfter(
      "query-timeout", std::chrono::milliseconds(ms), [flag]() { flag->store(true, std::memory_order_release); },
      SchedulerPriority::CRITICAL);
}

AsyncTimer::~AsyncTimer() = default;  // TaskHandle RAII handles cleanup

AsyncTimer::AsyncTimer(AsyncTimer &&other) noexcept
    : expiration_flag_{std::move(other.expiration_flag_)}, handle_{std::move(other.handle_)} {}

AsyncTimer &AsyncTimer::operator=(AsyncTimer &&other) noexcept {
  if (this != &other) {
    expiration_flag_ = std::move(other.expiration_flag_);
    handle_ = std::move(other.handle_);
  }
  return *this;
}

bool AsyncTimer::IsExpired() const noexcept {
  return expiration_flag_ && expiration_flag_->load(std::memory_order_acquire);
}

void AsyncTimer::GCRun() {
  // No-op: ConsolidatedScheduler handles cleanup automatically
}

}  // namespace memgraph::utils
