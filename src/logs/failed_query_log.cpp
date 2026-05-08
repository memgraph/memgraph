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

#include "logs/failed_query_log.hpp"

#include <spdlog/logger.h>
#include <spdlog/sinks/daily_file_sink.h>

namespace memgraph::logs {

namespace {
// 5 weeks of daily files. Matches QueryLogger.
constexpr int kLogRetentionDays = 35;

std::shared_ptr<spdlog::logger> MakeLogger(const std::filesystem::path &log_dir) {
  if (log_dir.empty()) {
    // No-op logger: no sinks attached. info() calls are silently dropped.
    return std::make_shared<spdlog::logger>("FailedQueryLog");
  }
  auto sink = std::make_shared<spdlog::sinks::daily_file_sink_mt>((log_dir / "failed_queries.log").string(),
                                                                  /*hour=*/0,
                                                                  /*minute=*/0,
                                                                  /*truncate=*/false,
                                                                  kLogRetentionDays);
  auto logger = std::make_shared<spdlog::logger>("FailedQueryLog", std::move(sink));
  logger->set_level(spdlog::level::info);
  logger->flush_on(spdlog::level::info);
  return logger;
}
}  // namespace

FailedQueryLog::~FailedQueryLog() {
  std::lock_guard g{mtx_};
  if (logger_) logger_->flush();
}

void FailedQueryLog::Reconfigure(const std::filesystem::path &log_dir) {
  auto next = MakeLogger(log_dir);
  std::shared_ptr<spdlog::logger> prev;
  {
    std::lock_guard g{mtx_};
    prev = std::exchange(logger_, std::move(next));
  }
  if (prev) prev->flush();
}

void FailedQueryLog::Record(std::string_view session_uuid, std::string_view username, std::string_view db,
                            std::string_view query, std::string_view error) {
  std::shared_ptr<spdlog::logger> snapshot;
  {
    std::lock_guard g{mtx_};
    snapshot = logger_;
  }
  if (!snapshot) return;
  snapshot->info("[{}] [{}] [{}] query: {} | error: {}", session_uuid, username, db, query, error);
}

FailedQueryLog &GlobalFailedQueryLog() {
  static FailedQueryLog instance;
  return instance;
}

}  // namespace memgraph::logs
