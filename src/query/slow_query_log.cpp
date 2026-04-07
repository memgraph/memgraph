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

#include "query/slow_query_log.hpp"

#include <ctime>

#include <fmt/format.h>
#include <spdlog/sinks/daily_file_sink.h>

namespace {
constexpr int kLogRetentionDays = 35;
}  // namespace

namespace memgraph::query {

SlowQueryLog::SlowQueryLog(const std::filesystem::path &log_dir) {
  auto log_file = (log_dir / "slow_queries.log").string();

  time_t current_time{0};
  struct tm *local_time{nullptr};
  time(&current_time);  // NOLINT
  local_time = localtime(&current_time);

  auto sink = std::make_shared<spdlog::sinks::daily_file_sink_mt>(
      log_file, local_time->tm_hour, local_time->tm_min, false, kLogRetentionDays);
  logger_ = std::make_shared<spdlog::logger>("SlowQueryLog", std::move(sink));
  logger_->set_level(spdlog::level::info);
  logger_->flush_on(spdlog::level::info);
}

SlowQueryLog::~SlowQueryLog() {
  if (logger_) {
    logger_->flush();
  }
}

void SlowQueryLog::Record(std::string_view session_uuid, std::string_view username, std::string_view db,
                          std::string_view query, double duration_ms) {
  logger_->info("[{}] [{}] [{}] duration: {:.3f} ms | query: {}", session_uuid, username, db, duration_ms, query);
}

}  // namespace memgraph::query
