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

#include <spdlog/common.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>
#include <string>

namespace memgraph::utils {

class Logger {
 public:
  explicit Logger(std::string name, std::string log_file);

  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  Logger(Logger &&) = delete;
  Logger &operator=(Logger &&) = delete;

  virtual ~Logger();

  virtual void trace(const std::string &log_line);

  virtual void debug(const std::string &log_line);

  virtual void info(const std::string &log_line);

  virtual void warn(const std::string &log_line);

  virtual void err(const std::string &log_line);

  void set_level(spdlog::level::level_enum l);

  int get_level();

 protected:
  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace memgraph::utils

#endif
