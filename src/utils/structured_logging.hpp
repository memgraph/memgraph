// Copyright 2026 Memgraph Ltd.
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

#include <memory>
#include <string_view>

#include <spdlog/common.h>
#include <nlohmann/json_fwd.hpp>

namespace spdlog {
class logger;

namespace details {
struct log_msg;
}
}  // namespace spdlog

namespace memgraph::logging {

// Configure before publishing the logger to other threads.
std::shared_ptr<spdlog::logger> EnableStructuredEvents(std::shared_ptr<spdlog::logger> logger);
bool IsStructuredLoggingEnabled();
bool AddStructuredLogSink(spdlog::logger &logger, spdlog::sink_ptr sink);

void LogEvent(spdlog::level::level_enum level, std::string_view name, std::string_view message,
              const nlohmann::json &attributes);
void AddStructuredEventFields(const spdlog::details::log_msg &message, nlohmann::json &record);

// Previously silent authentication rejections stay silent unless JSON output is enabled.
void EmitAuthenticationFailure(std::string_view session_id, std::string_view reason);

}  // namespace memgraph::logging
