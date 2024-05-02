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

#include <algorithm>
#include <chrono>
#include <random>
#include <thread>

class ExponentialBackoffInternals {
 public:
  ExponentialBackoffInternals(std::chrono::milliseconds initial_delay, std::chrono::milliseconds max_delay)
      : initial_delay_(initial_delay), max_delay_(max_delay), cached_delay_(initial_delay) {}

  std::chrono::milliseconds calculate_delay() {
    if (cached_delay_ < max_delay_) {
      ++retry_count_;
      auto base_delay = std::chrono::milliseconds{initial_delay_.count() * (1 << (retry_count_ - 1))};
      cached_delay_ = base_delay < max_delay_ ? base_delay : max_delay_;
    }
    return cached_delay_;
  }

 private:
  std::chrono::milliseconds initial_delay_;
  std::chrono::milliseconds max_delay_;
  std::chrono::milliseconds cached_delay_;
  int64_t retry_count_{0};
};

class ExponentialBackoff {
 public:
  explicit ExponentialBackoff(std::chrono::milliseconds initial_delay, std::chrono::milliseconds max_delay)
      : exponential_backoff_internals_(initial_delay, max_delay) {}

  void wait() {
    std::chrono::milliseconds delay = exponential_backoff_internals_.calculate_delay();
    std::this_thread::sleep_for(delay);
  }

 private:
  ExponentialBackoffInternals exponential_backoff_internals_;
};
