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
#include "flags/log_level.hpp"

#include "utils/enum.hpp"
#include "utils/flag_validation.hpp"
#include "utils/logging.hpp"

#include "gflags/gflags.h"
#include "spdlog/common.h"
#include "spdlog/sinks/daily_file_sink.h"

#include <array>
#include <string_view>
#include <utility>

using namespace std::string_view_literals;

// Logging flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_bool(also_log_to_stderr, false, "Log messages go to stderr in addition to logfiles");
DEFINE_string(log_file, "", "Path to where the log should be stored.");

inline constexpr std::array log_level_mappings{
    std::pair{"TRACE"sv, spdlog::level::trace}, std::pair{"DEBUG"sv, spdlog::level::debug},
    std::pair{"INFO"sv, spdlog::level::info},   std::pair{"WARNING"sv, spdlog::level::warn},
    std::pair{"ERROR"sv, spdlog::level::err},   std::pair{"CRITICAL"sv, spdlog::level::critical}};

const std::string log_level_help_string = fmt::format("Minimum log level. Allowed values: {}",
                                                      memgraph::utils::GetAllowedEnumValuesString(log_level_mappings));

DEFINE_VALIDATED_string(log_level, "WARNING", log_level_help_string.c_str(),
                        { return memgraph::flags::ValidLogLevel(value); });

bool memgraph::flags::ValidLogLevel(std::string_view value) {
  if (const auto result = memgraph::utils::IsValidEnumValueString(value, log_level_mappings); result.HasError()) {
    const auto error = result.GetError();
    switch (error) {
      case memgraph::utils::ValidationError::EmptyValue: {
        std::cout << "Log level cannot be empty." << std::endl;
        break;
      }
      case memgraph::utils::ValidationError::InvalidValue: {
        std::cout << "Invalid value for log level. Allowed values: "
                  << memgraph::utils::GetAllowedEnumValuesString(log_level_mappings) << std::endl;
        break;
      }
    }
    return false;
  }

  return true;
}

std::optional<spdlog::level::level_enum> memgraph::flags::LogLevelToEnum(std::string_view value) {
  return memgraph::utils::StringToEnum<spdlog::level::level_enum>(value, log_level_mappings);
}

spdlog::level::level_enum ParseLogLevel() {
  std::string ll;
  gflags::GetCommandLineOption("log_level", &ll);
  const auto log_level = memgraph::flags::LogLevelToEnum(ll);
  MG_ASSERT(log_level, "Invalid log level");
  return *log_level;
}

// 5 weeks * 7 days
inline constexpr auto log_retention_count = 35;
void CreateLoggerFromSink(const auto &sinks, const auto log_level) {
  auto logger = std::make_shared<spdlog::logger>("memgraph_log", sinks.begin(), sinks.end());
  logger->set_level(log_level);
  logger->flush_on(spdlog::level::trace);
  spdlog::set_default_logger(std::move(logger));
  // Enable stderr sink
  if (FLAGS_also_log_to_stderr) {
    memgraph::flags::LogToStderr(log_level);
  }
}

void memgraph::flags::InitializeLogger() {
  std::vector<spdlog::sink_ptr> sinks;

  // Force the stderr logger to be at the front of the sinks vector
  // Will be used to disable/enable it at run-time by settings its log level
  sinks.emplace_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
  sinks.back()->set_level(spdlog::level::off);

  if (!FLAGS_log_file.empty()) {
    // get local time
    time_t current_time{0};
    struct tm *local_time{nullptr};

    time(&current_time);
    local_time = localtime(&current_time);

    sinks.emplace_back(std::make_shared<spdlog::sinks::daily_file_sink_mt>(
        FLAGS_log_file, local_time->tm_hour, local_time->tm_min, false, log_retention_count));
  }
  CreateLoggerFromSink(sinks, ParseLogLevel());
}

// TODO: Make sure this is used in a safe way
void memgraph::flags::AddLoggerSink(spdlog::sink_ptr new_sink) {
  auto default_logger = spdlog::default_logger();
  auto sinks = default_logger->sinks();
  sinks.push_back(new_sink);
  CreateLoggerFromSink(sinks, default_logger->level());
}

// Thread-safe because the level enum is an atomic
// NOTE: default_logger is not thread-safe and shouldn't be changed during application lifetime
void memgraph::flags::LogToStderr(spdlog::level::level_enum log_level) {
  auto default_logger = spdlog::default_logger();
  auto sink = default_logger->sinks().front();
  sink->set_level(log_level);
}
