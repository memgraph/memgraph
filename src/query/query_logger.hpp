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

#include "utils/logger.hpp"

namespace memgraph::query {

class QueryLogger : public utils::Logger {
 public:
  explicit QueryLogger(std::string log_file);

  QueryLogger(const QueryLogger &) = delete;
  QueryLogger &operator=(const QueryLogger &) = delete;
  QueryLogger(QueryLogger &&) = delete;
  QueryLogger &operator=(QueryLogger &&) = delete;

  void trace(const std::string &log_line) override;

  void debug(const std::string &log_line) override;

  void info(const std::string &log_line) override;

  void warn(const std::string &log_line) override;

  void err(const std::string &log_line) override;

  void SetTransactionId(std::string t_id);
  void SetSessionId(std::string s_id);
  void SetUser(std::string u);
  void ResetUser();
  void ResetTransactionId();

 private:
  std::string session_id{""};
  std::string user_or_role{""};
  std::string transaction_id{""};

  std::string GetMessage(const std::string &log_line);
};

}  // namespace memgraph::query

#endif
