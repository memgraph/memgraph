// Copyright 2023 Memgraph Ltd.
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

#include <spdlog/sinks/sink.h>
#include <optional>
#include "gflags/gflags.h"

DECLARE_string(log_level);
DECLARE_bool(also_log_to_stderr);

namespace memgraph::flags {

bool ValidLogLevel(std::string_view value);
std::optional<spdlog::level::level_enum> LogLevelToEnum(std::string_view value);

void InitializeLogger();
void AddLoggerSink(spdlog::sink_ptr new_sink);
void LogToStderr(spdlog::level::level_enum log_level);

}  // namespace memgraph::flags
