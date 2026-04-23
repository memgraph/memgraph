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
#include <string>
#include <string_view>

#include <spdlog/spdlog.h>

namespace memgraph::query {

/// Thread-safe log for failed queries. Records query failures with their
/// exception messages to a daily-rotating log file. Activated by setting the
/// --query-log-directory flag; disabled if the directory is empty.
class FailedQueryLog {
 public:
  explicit FailedQueryLog(const std::filesystem::path &log_dir);
  ~FailedQueryLog();

  FailedQueryLog(const FailedQueryLog &) = delete;
  FailedQueryLog &operator=(const FailedQueryLog &) = delete;
  FailedQueryLog(FailedQueryLog &&) = default;
  FailedQueryLog &operator=(FailedQueryLog &&) = default;

  /// Records a failed query. Thread-safe.
  void Record(std::string_view session_uuid, std::string_view username, std::string_view db, std::string_view query,
              std::string_view error);

 private:
  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace memgraph::query
