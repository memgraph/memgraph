// Copyright 2022 Memgraph Ltd.
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
#include <time.h>

#include <memory>

#include "utils/logging.hpp"

namespace memgraph::utils {

#define SIGTIMER (SIGRTMAX - 2)

class AsyncTimer {
 public:
  AsyncTimer();
  explicit AsyncTimer(double seconds);
  ~AsyncTimer();
  AsyncTimer(AsyncTimer &&other) noexcept;
  // NOLINTNEXTLINE (hicpp-noexcept-move)
  AsyncTimer &operator=(AsyncTimer &&other);

  AsyncTimer(const AsyncTimer &) = delete;
  AsyncTimer &operator=(const AsyncTimer &) = delete;

  // Returns false if the object isn't associated with any timer.
  bool IsExpired() const noexcept;

 private:
  void ReleaseResources();

  // If the expiration_flag_ is nullptr, then the object is not associated with any timer, therefore no clean up
  // is necessary. Furthermore, the POSIX API doesn't specify any value as "invalid" for timer_t, so the timer_id_
  // cannot be used to determine whether the object is associated with any timer or not.
  std::shared_ptr<std::atomic<bool>> expiration_flag_;
  uint64_t flag_id_;
  timer_t timer_id_;
};
}  // namespace memgraph::utils
