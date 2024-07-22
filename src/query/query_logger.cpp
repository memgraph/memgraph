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

#include <fmt/core.h>

#include "query/query_logger.hpp"

namespace {
constexpr int log_retention_count = 35;

}  // namespace

namespace memgraph::query {

QueryLogger::QueryLogger(std::string log_file) : Logger("QueryLogger", log_file) {}

void QueryLogger::trace(const std::string &log_line) { logger_->log(spdlog::level::trace, GetMessage(log_line)); }
void QueryLogger::debug(const std::string &log_line) { logger_->log(spdlog::level::debug, GetMessage(log_line)); }
void QueryLogger::info(const std::string &log_line) { logger_->log(spdlog::level::info, GetMessage(log_line)); }
void QueryLogger::warn(const std::string &log_line) { logger_->log(spdlog::level::warn, GetMessage(log_line)); }
void QueryLogger::err(const std::string &log_line) { logger_->log(spdlog::level::err, GetMessage(log_line)); }

void QueryLogger::SetTransactionId(std::string t_id) { transaction_id = t_id; }
void QueryLogger::SetSessionId(std::string s_id) { session_id = s_id; }
void QueryLogger::SetUser(std::string u) { user_or_role = u; }
void QueryLogger::ResetUser() { user_or_role = ""; }
void QueryLogger::ResetTransactionId() { transaction_id = ""; }

std::string QueryLogger::GetMessage(const std::string &log_line) {
  std::string message;
  if (session_id != "") {
    message += fmt::format("[{}]", session_id);
  }
  if (user_or_role != "") {
    if (!message.empty()) {
      message += " ";
    }
    message += fmt::format("[{}]", user_or_role);
  }
  if (transaction_id != "") {
    if (!message.empty()) {
      message += " ";
    }
    message += fmt::format("[{}]", transaction_id);
  }

  if (!message.empty()) {
    return fmt::format("{} {}", message, log_line);
  }

  return log_line;
}
}  // namespace memgraph::query
