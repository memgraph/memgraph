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

#include "spdlog/sinks/sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"

#include <optional>

namespace memgraph::flags {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
inline std::shared_ptr<spdlog::sinks::sink> stderr_sink = std::make_shared<spdlog::sinks::stderr_color_sink_st>();

const std::string &GetAllowedLogLevels();
constexpr const char *GetLogLevelHelpString() {
  return "Minimum log level. Allowed values: TRACE, DEBUG, INFO, WARNING, ERROR, CRITICAL";
}

bool ValidLogLevel(std::string_view value);
std::optional<spdlog::level::level_enum> LogLevelToEnum(std::string_view value);

void InitializeLogger();
void AddLoggerSink(spdlog::sink_ptr new_sink);
// Sets stderr log level to off
void TurnOffStdErr();
// Sets logging level to the global logging level
void TurnOnStdErr();
}  // namespace memgraph::flags
