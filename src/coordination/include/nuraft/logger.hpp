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

#include <string>

#include <spdlog/common.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>
// clang-format off
// First import nuraft.hxx then logger.hxx to avoid problem with __interface_body__
#include <libnuraft/nuraft.hxx>
#include <libnuraft/logger.hxx>
// clang-format on

namespace memgraph::coordination {

using nuraft::logger;

class Logger final : public logger {
 public:
  explicit Logger(std::string log_file);

  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  Logger(Logger &&) = delete;
  Logger &operator=(Logger &&) = delete;

  ~Logger() override;

  // Deprecated
  void debug(const std::string &log_line) override;

  // Deprecated
  void info(const std::string &log_line) override;

  // Deprecated
  void warn(const std::string &log_line) override;

  // Deprecated
  void err(const std::string &log_line) override;

  void put_details(int level, const char *source_file, const char *func_name, size_t line_number,
                   const std::string &log_line) override;

  // Map from NuRaft log level to Spdlog log level.
  void set_level(int l) override;

  // Map from spdlog log level to NuRaft log level.
  int get_level() override;

 private:
  /**
   NuRaft log level:
   *    Trace:    6
   *    Debug:    5
   *    Info:     4
   *    Warning:  3
   *    Error:    2
   *    Fatal:    1
   */
  static spdlog::level::level_enum GetSpdlogLevel(int nuraft_log_level);

  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace memgraph::coordination

#endif
