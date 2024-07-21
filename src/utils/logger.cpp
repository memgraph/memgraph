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

#include <string>
#include <vector>

#include "utils/logger.hpp"

namespace {
constexpr int log_retention_count = 35;

}  // namespace

namespace memgraph::utils {

Logger::Logger(std::string name, std::string log_file) {
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
  logger_ = std::make_shared<spdlog::logger>(name, sinks.begin(), sinks.end());
  logger_->set_level(spdlog::level::trace);
  logger_->flush_on(spdlog::level::trace);
}

Logger::~Logger() {
  logger_->flush();
  logger_.reset();
  logger_ = nullptr;
}

void Logger::trace(const std::string &log_line) { logger_->log(spdlog::level::trace, log_line); }
void Logger::debug(const std::string &log_line) { logger_->log(spdlog::level::debug, log_line); }
void Logger::info(const std::string &log_line) { logger_->log(spdlog::level::info, log_line); }
void Logger::warn(const std::string &log_line) { logger_->log(spdlog::level::warn, log_line); }
void Logger::err(const std::string &log_line) { logger_->log(spdlog::level::err, log_line); }

void Logger::set_level(spdlog::level::level_enum l) { logger_->set_level(l); }

int Logger::get_level() { return logger_->level(); }

}  // namespace memgraph::utils
