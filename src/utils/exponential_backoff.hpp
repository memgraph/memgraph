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

#include <chrono>
#include <cstdint>

namespace memgraph::utils {

class ExponentialBackoffInternals {
 public:
  ExponentialBackoffInternals(std::chrono::milliseconds initial_delay, std::chrono::milliseconds max_delay);
  auto calculate_delay() -> std::chrono::milliseconds;

 private:
  std::chrono::milliseconds initial_delay_;
  std::chrono::milliseconds max_delay_;
  std::chrono::milliseconds cached_delay_;
  int64_t retry_count_{0};
};

class ExponentialBackoff {
 public:
  explicit ExponentialBackoff(std::chrono::milliseconds initial_delay, std::chrono::milliseconds max_delay);
  void wait();

 private:
  ExponentialBackoffInternals exponential_backoff_internals_;
};

}  // namespace memgraph::utils
