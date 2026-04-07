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
#include <string_view>

#include <spdlog/spdlog.h>

namespace memgraph::query {

/// Thread-safe log for slow queries. Records queries whose execution time
/// exceeds the configured threshold. Activated by setting --slow-query-log-dir;
/// the threshold is controlled by --slow-query-log-threshold-ms (runtime-configurable).
class SlowQueryLog {
 public:
  explicit SlowQueryLog(const std::filesystem::path &log_dir);
  ~SlowQueryLog();

  SlowQueryLog(const SlowQueryLog &) = delete;
  SlowQueryLog &operator=(const SlowQueryLog &) = delete;
  SlowQueryLog(SlowQueryLog &&) = default;
  SlowQueryLog &operator=(SlowQueryLog &&) = default;

  /// Records a slow query. Thread-safe.
  void Record(std::string_view session_uuid, std::string_view username, std::string_view db, std::string_view query,
              double duration_ms);

 private:
  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace memgraph::query
