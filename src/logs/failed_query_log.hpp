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

#include <filesystem>
#include <memory>
#include <mutex>
#include <string_view>

namespace spdlog {
class logger;
}  // namespace spdlog

namespace memgraph::logs {

/// Process-global, thread-safe log for failed queries. Records query failures
/// with their exception messages to a daily-rotating log file.
///
/// Reconfigure() swaps the underlying logger atomically, so live changes to
/// --failed-query-log-directory propagate to all sessions on the next Record()
/// call. Empty path disables logging without removing the instance.
class FailedQueryLog {
 public:
  FailedQueryLog() = default;
  ~FailedQueryLog();

  FailedQueryLog(const FailedQueryLog &) = delete;
  FailedQueryLog &operator=(const FailedQueryLog &) = delete;
  FailedQueryLog(FailedQueryLog &&) = delete;
  FailedQueryLog &operator=(FailedQueryLog &&) = delete;

  /// Replace the underlying sink. Empty path produces a no-op logger.
  /// Safe to call concurrently with Record().
  void Reconfigure(const std::filesystem::path &log_dir);

  /// Records a failed query. No-op if currently disabled.
  void Record(std::string_view session_uuid, std::string_view username, std::string_view db, std::string_view query,
              std::string_view error);

 private:
  mutable std::mutex mtx_;
  std::shared_ptr<spdlog::logger> logger_;  // guarded by mtx_
};

/// Returns the process-global FailedQueryLog instance.
FailedQueryLog &GlobalFailedQueryLog();

}  // namespace memgraph::logs
