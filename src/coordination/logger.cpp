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

#pragma once

#ifdef MG_ENTERPRISE

#include "nuraft/logger.hpp"

namespace {
constexpr int log_retention_count = 35;

}  // namespace

namespace memgraph::coordination {

Logger::Logger(std::string log_file) {
  time_t current_time{0};
  struct tm *local_time{nullptr};

  // TODO: (andi) I think not needed
  time(&current_time);  // NOLINT
  local_time = localtime(&current_time);

  auto const sinks = {std::make_shared<spdlog::sinks::daily_file_sink_mt>(
      std::move(log_file), local_time->tm_hour, local_time->tm_min, false, log_retention_count)};
  logger_ = std::make_shared<spdlog::logger>("NuRaft", sinks.begin(), sinks.end());
  logger_->set_level(spdlog::level::trace);
  logger_->flush_on(spdlog::level::trace);
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
  auto const level = logger_->level();
  if (level == spdlog::level::trace) {
    return 6;
  }
  if (level == spdlog::level::debug) {
    return 5;
  }
  if (level == spdlog::level::info) {
    return 4;
  }
  if (level == spdlog::level::warn) {
    return 3;
  }
  if (level == spdlog::level::err) {
    return 2;
  }
  if (level == spdlog::level::critical) {  // critical=fatal
    return 1;
  }

  return 0;
}

spdlog::level::level_enum Logger::GetSpdlogLevel(int nuraft_log_level) {
  switch (nuraft_log_level) {
    case 6:
      return spdlog::level::trace;
    case 5:
      return spdlog::level::debug;
    case 4:
      return spdlog::level::info;
    case 3:
      return spdlog::level::warn;
    case 2:
      return spdlog::level::err;
    case 1:
      return spdlog::level::critical;  // critical=fatal
    default:
      return spdlog::level::trace;
  }
}

}  // namespace memgraph::coordination

#endif
