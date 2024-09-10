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
// Helper function to append a formatted tag if it's not empty
void AppendTag(std::stringstream &ss, const std::string &tag) {
  if (!tag.empty()) {
    if (ss.tellp() > 0) {  // tellp returns the current position in the stream
      ss << " ";
    }
    ss << fmt::format("[{}]", tag);
  }
}
}  // namespace

namespace memgraph::query {

QueryLogger::QueryLogger(std::string log_file, std::string session_uuid, std::string username)
    : Logger("QueryLogger", std::move(log_file)),
      session_id_(std::move(session_uuid)),
      user_or_role_(std::move(username)) {}

void QueryLogger::trace(const std::string &log_line) { logger_->log(spdlog::level::trace, GetMessage(log_line)); }
void QueryLogger::debug(const std::string &log_line) { logger_->log(spdlog::level::debug, GetMessage(log_line)); }
void QueryLogger::info(const std::string &log_line) { logger_->log(spdlog::level::info, GetMessage(log_line)); }
void QueryLogger::warn(const std::string &log_line) { logger_->log(spdlog::level::warn, GetMessage(log_line)); }
void QueryLogger::err(const std::string &log_line) { logger_->log(spdlog::level::err, GetMessage(log_line)); }

void QueryLogger::SetTransactionId(const std::string &t_id) { transaction_id_ = t_id; }
void QueryLogger::SetSessionId(const std::string &s_id) { session_id_ = s_id; }
void QueryLogger::SetUser(const std::string &u) { user_or_role_ = u; }
void QueryLogger::ResetUser() { user_or_role_ = ""; }
void QueryLogger::ResetTransactionId() { transaction_id_ = ""; }

std::string QueryLogger::GetMessage(const std::string &log_line) {
  std::stringstream ss;

  AppendTag(ss, session_id_);
  AppendTag(ss, user_or_role_);
  AppendTag(ss, transaction_id_);

  if (ss.tellp() > 0) {  // If something was appended
    ss << " " << log_line;
    return ss.str();
  }

  return log_line;
}

}  // namespace memgraph::query
