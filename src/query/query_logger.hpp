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

namespace memgraph::query {

class QueryLogger {
 public:
  explicit QueryLogger();
  explicit QueryLogger(std::string log_file);

  QueryLogger(const QueryLogger &) = delete;
  QueryLogger &operator=(const QueryLogger &) = delete;
  QueryLogger(QueryLogger &&) = delete;
  QueryLogger &operator=(QueryLogger &&) = delete;

  ~QueryLogger();

  bool IsActive();

  void trace(const std::string &log_line);

  void debug(const std::string &log_line);

  void info(const std::string &log_line);

  void warn(const std::string &log_line);

  void err(const std::string &log_line);

  void set_level(spdlog::level::level_enum l);

  int get_level();

 private:
  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace memgraph::query

#endif
