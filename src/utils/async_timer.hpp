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

#pragma once

#include <atomic>
#include <memory>

#include "utils/consolidated_scheduler.hpp"

namespace memgraph::utils {

class AsyncTimer {
 public:
  AsyncTimer() = default;
  explicit AsyncTimer(double seconds);
  ~AsyncTimer();
  AsyncTimer(AsyncTimer &&other) noexcept;
  AsyncTimer &operator=(AsyncTimer &&other) noexcept;

  AsyncTimer(const AsyncTimer &) = delete;
  AsyncTimer &operator=(const AsyncTimer &) = delete;

  // Returns false if the object isn't associated with any timer.
  bool IsExpired() const noexcept;

  // No-op: ConsolidatedScheduler handles cleanup automatically
  static void GCRun();

 private:
  std::shared_ptr<std::atomic<bool>> expiration_flag_;
  TaskHandle handle_;
};

}  // namespace memgraph::utils
