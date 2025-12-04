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

module;

#include <string>
#include <vector>

#include <spdlog/sinks/daily_file_sink.h>

module memgraph.coordination.logger;

#ifdef MG_ENTERPRISE

namespace {
constexpr int log_retention_count = 35;

}  // namespace

namespace memgraph::coordination {

Logger::Logger(std::string log_file) {
  std::vector<spdlog::sink_ptr> sinks;
  if (!log_file.empty()) {
    time_t current_time{0};
    struct tm *local_time{nullptr};

    time(&current_time);  // NOLINT
    local_time = localtime(&current_time);

    sinks.emplace_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>(
        std::move(log_file), local_time->tm_hour, local_time->tm_min, false, log_retention_count));
  }
  // If log file is empty, logger can be used but without sinks = no logging.
  logger_ = std::make_shared<spdlog::logger>("NuRaft", sinks.begin(), sinks.end());
  logger_->set_level(spdlog::level::trace);
  logger_->flush_on(spdlog::level::trace);
  set_level(static_cast<int>(nuraft_log_level::TRACE));
}

Logger::~Logger() {
  logger_->flush();
  logger_.reset();
  logger_ = nullptr;
}

void Logger::debug(const std::string &log_line) { logger_->log(spdlog::level::debug, log_line); }
void Logger::info(const std::string &log_line) { logger_->log(spdlog::level::info, log_line); }
void Logger::warn(const std::string &log_line) { logger_->log(spdlog::level::warn, log_line); }
void Logger::err(const std::string &log_line) { logger_->log(spdlog::level::err, log_line); }

void Logger::put_details(int level, const char *source_file, const char *func_name, size_t line_number,
                         const std::string &log_line) {
  logger_->log(spdlog::source_loc{source_file, static_cast<int>(line_number), func_name}, GetSpdlogLevel(level),
               log_line);
}

void Logger::set_level(int l) { logger_->set_level(GetSpdlogLevel(l)); }

int Logger::get_level() {
  auto const nuraft_log_level = GetNuRaftLevel(logger_->level());
  return static_cast<int>(nuraft_log_level);
}

spdlog::level::level_enum Logger::GetSpdlogLevel(int nuraft_log_level) {
  auto const nuraft_level = static_cast<enum nuraft_log_level>(nuraft_log_level);

  switch (nuraft_level) {
    case nuraft_log_level::TRACE:
      return spdlog::level::trace;
    case nuraft_log_level::DEBUG:
      return spdlog::level::debug;
    case nuraft_log_level::INFO:
      return spdlog::level::info;
    case nuraft_log_level::WARNING:
      return spdlog::level::warn;
    case nuraft_log_level::ERROR:
      return spdlog::level::err;
    case nuraft_log_level::FATAL:
      return spdlog::level::critical;  // critical=fatal
    default:
      return spdlog::level::trace;
  }
}

nuraft_log_level Logger::GetNuRaftLevel(spdlog::level::level_enum spdlog_level) {
  switch (spdlog_level) {
    case spdlog::level::trace:
      return nuraft_log_level::TRACE;
    case spdlog::level::debug:
      return nuraft_log_level::DEBUG;
    case spdlog::level::info:
      return nuraft_log_level::INFO;
    case spdlog::level::warn:
      return nuraft_log_level::WARNING;
    case spdlog::level::err:
      return nuraft_log_level::ERROR;
    case spdlog::level::critical:  // critical=fatal
      return nuraft_log_level::FATAL;
    default:
      return nuraft_log_level::TRACE;
  }
}

}  // namespace memgraph::coordination

#endif
