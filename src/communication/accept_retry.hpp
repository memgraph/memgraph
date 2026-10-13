// Copyright 2026 Memgraph Ltd.
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

#include <cerrno>
#include <chrono>
#include <string_view>

#include <spdlog/spdlog.h>
#include <boost/system/error_code.hpp>

namespace memgraph::communication {

// Fixed delay before re-arming accept() after a transient resource-exhaustion error. Short enough to recover quickly
// once descriptors are freed, long enough that a persistent condition costs ~2 wakeups/s instead of a busy loop.
inline constexpr std::chrono::milliseconds kAcceptRetryDelay{500};

// Minimum spacing between "accept failed, retrying" log lines.
inline constexpr std::chrono::seconds kAcceptRetryLogInterval{10};

inline bool IsTransientAcceptErrno(int err) {
  return err == EMFILE || err == ENFILE || err == ENOBUFS || err == ENOMEM;
}

inline bool IsTransientAcceptError(const boost::system::error_code &ec) {
  return ec == boost::system::errc::too_many_files_open || ec == boost::system::errc::too_many_files_open_in_system ||
         ec == boost::system::errc::no_buffer_space || ec == boost::system::errc::not_enough_memory;
}

// Not thread-safe; accept handlers for one acceptor never run concurrently.
class AcceptRetryLogThrottle {
 public:
  void Warn(std::string_view service, std::string_view reason) {
    auto const now = std::chrono::steady_clock::now();
    if (logged_ && now - last_log_ < kAcceptRetryLogInterval) return;
    logged_ = true;
    last_log_ = now;
    spdlog::warn("{} accept failed ({}); retrying every {}ms, further occurrences logged at most every {}s",
                 service,
                 reason,
                 kAcceptRetryDelay.count(),
                 std::chrono::duration_cast<std::chrono::seconds>(kAcceptRetryLogInterval).count());
  }

 private:
  std::chrono::steady_clock::time_point last_log_{};
  bool logged_{false};
};

}  // namespace memgraph::communication
