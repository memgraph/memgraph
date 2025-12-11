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

#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>

#include <string>
#include <vector>
// clang-format off
// First import nuraft.hxx then logger.hxx to avoid problem with __interface_body__
#include <libnuraft/nuraft.hxx>
#include <libnuraft/logger.hxx>
// clang-format on

export module memgraph.coordination.logger;

#ifdef MG_ENTERPRISE

import memgraph.coordination.log_level;

namespace {
constexpr int log_retention_count = 35;

}  // namespace

namespace memgraph::coordination {
using nuraft::logger;
}  // namespace memgraph::coordination

export namespace memgraph::coordination {

// TODO: (andi) Experiment with module private fragment
/**
 * Logger class that wraps the spdlog logger. NuRaft uses directly this object. However, devs should use @LoggerWrapper
 * to log messages.
 */
class Logger final : public logger {
 public:
  explicit Logger(std::string log_file) {
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

  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  Logger(Logger &&) = delete;
  Logger &operator=(Logger &&) = delete;

  ~Logger() override {
    logger_->flush();
    logger_.reset();
    logger_ = nullptr;
  }

  // Deprecated
  void debug(const std::string &log_line) override { logger_->log(spdlog::level::debug, log_line); }

  // Deprecated
  void info(const std::string &log_line) override { logger_->log(spdlog::level::info, log_line); }

  // Deprecated
  void warn(const std::string &log_line) override { logger_->log(spdlog::level::warn, log_line); }

  // Deprecated
  void err(const std::string &log_line) override { logger_->log(spdlog::level::err, log_line); }

  void put_details(int level, const char *source_file, const char *func_name, size_t line_number,
                   const std::string &log_line) override {
    logger_->log(spdlog::source_loc{source_file, static_cast<int>(line_number), func_name}, GetSpdlogLevel(level),
                 log_line);
  }

  // Map from NuRaft log level to Spdlog log level.
  // Not intended to be used, implementation provided if necessary
  void set_level(int l) override { logger_->set_level(GetSpdlogLevel(l)); }

  // Map from spdlog log level to NuRaft log level.
  // Not intended to be used, implementation provided if necessary
  int get_level() override {
    auto const nuraft_log_level = GetNuRaftLevel(logger_->level());
    return static_cast<int>(nuraft_log_level);
  }

 private:
  static spdlog::level::level_enum GetSpdlogLevel(int nuraft_log_level) {
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
  static nuraft_log_level GetNuRaftLevel(spdlog::level::level_enum spdlog_level) {
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

  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace memgraph::coordination

#endif
