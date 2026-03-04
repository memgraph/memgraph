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
#include "flags/logging.hpp"

#include "flags/run_time_configurable.hpp"
#include "utils/enum.hpp"
#include "utils/flag_validation.hpp"
#include "utils/logging.hpp"

#include "gflags/gflags.h"
#include "spdlog/async.h"
#include "spdlog/common.h"
#include "spdlog/sinks/daily_file_sink.h"
#include "spdlog/sinks/dist_sink.h"

#include <array>
#include <iostream>
#include <string_view>
#include <utility>

using namespace std::string_view_literals;

namespace {

constexpr auto kSync = "sync";
constexpr auto kAsync = "async";

spdlog::level::level_enum ParseLogLevel() {
  std::string ll;
  gflags::GetCommandLineOption("log_level", &ll);
  const auto log_level = memgraph::flags::LogLevelToEnum(ll);
  MG_ASSERT(log_level, "Invalid log level");
  return *log_level;
}
}  // namespace

// Logging flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(log_file, "", "Path to where the log should be stored.");

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_string(logger_type, "sync",
                        "Controls whether synchronous or asynchronous logger will be used. Options: sync, async", {
                          auto const logger_lower = memgraph::utils::ToLowerCase(value);
                          if (logger_lower != kSync && logger_lower != kAsync) {
                            std::cout << "Expected --" << flagname << " to be 'sync' or 'async' string\n";
                            return false;
                          }
                          return true;
                        });

// default set to 35 days
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(log_retention_days, 35, "Controls for how many days will daily log files be preserved.",
                        FLAG_IN_RANGE(1, std::numeric_limits<uint64_t>::max()));

inline constexpr std::array log_level_mappings{std::pair{"TRACE"sv, spdlog::level::trace},
                                               std::pair{"DEBUG"sv, spdlog::level::debug},
                                               std::pair{"INFO"sv, spdlog::level::info},
                                               std::pair{"WARNING"sv, spdlog::level::warn},
                                               std::pair{"ERROR"sv, spdlog::level::err},
                                               std::pair{"CRITICAL"sv, spdlog::level::critical}};

namespace memgraph::flags {
const std::string &GetAllowedLogLevels() {
  static const std::string allowed_levels = memgraph::utils::GetAllowedEnumValuesString(log_level_mappings);
  return allowed_levels;
}

bool ValidLogLevel(std::string_view value) {
  if (const auto result = memgraph::utils::IsValidEnumValueString(value, log_level_mappings); !result.has_value()) {
    const auto error = result.error();
    switch (error) {
      case memgraph::utils::ValidationError::EmptyValue: {
        std::cout << "Log level cannot be empty." << '\n';
        break;
      }
      case memgraph::utils::ValidationError::InvalidValue: {
        std::cout << "Invalid value for log level. Allowed values: " << GetAllowedLogLevels() << '\n';
        break;
      }
    }
    return false;
  }

  return true;
}

std::optional<spdlog::level::level_enum> LogLevelToEnum(std::string_view value) {
  return memgraph::utils::StringToEnum<spdlog::level::level_enum>(value, log_level_mappings);
}

// We use dist_sink which is MT safe together with _st subsinks
// This allows us MT safe
void InitializeLogger() {
  // stderr subsink
  stderr_sink()->set_level(run_time::GetAlsoLogToStderr() ? spdlog::level::trace : spdlog::level::off);

  std::vector<spdlog::sink_ptr> sub_sinks;
  sub_sinks.emplace_back(stderr_sink());

  if (!FLAGS_log_file.empty()) {
    // get local time
    time_t current_time{0};
    struct tm *local_time{nullptr};

    // Silent the error
    (void)time(&current_time);
    local_time = localtime(&current_time);

    sub_sinks.emplace_back(std::make_shared<spdlog::sinks::daily_file_sink_st>(
        FLAGS_log_file, local_time->tm_hour, local_time->tm_min, false, FLAGS_log_retention_days));
  }

  auto dist_sink = std::make_shared<spdlog::sinks::dist_sink_mt>(std::move(sub_sinks));

  auto logger = std::invoke([dist_sink_local = std::move(dist_sink)]() mutable -> std::shared_ptr<spdlog::logger> {
    if (FLAGS_logger_type == kAsync) {
      // 8k size of the buffer
      spdlog::init_thread_pool(8192, 1);
      return std::make_shared<spdlog::async_logger>("memgraph_log",
                                                    std::move(dist_sink_local),
                                                    spdlog::thread_pool(),
                                                    spdlog::async_overflow_policy::overrun_oldest);
    } else {
      return std::make_shared<spdlog::logger>("memgraph_log", std::move(dist_sink_local));
    }
  });

  logger->set_level(ParseLogLevel());
  logger->flush_on(spdlog::level::trace);
  spdlog::set_default_logger(std::move(logger));
}

// This is thread-safe now because add_sink takes a lock from base_sink before adding subsink
// Main thread can execute this while the other thread is logging
void AddLoggerSink(spdlog::sink_ptr new_sink) {
  auto default_logger = spdlog::default_logger();
  auto dist_sink = std::dynamic_pointer_cast<spdlog::sinks::dist_sink_mt>(*(default_logger->sinks().begin()));
  dist_sink->add_sink(std::move(new_sink));
}

// Thread-safe because the level enum is an atomic
void TurnOffStdErr() { stderr_sink()->set_level(spdlog::level::off); }

// Thread-safe because the level enum is an atomic
// Sets log-level to trace
// Filtering done on logger's level
void TurnOnStdErr() {
  // stderr level allows everything, will be filtered on logger's elvel
  stderr_sink()->set_level(spdlog::level::trace);
}

}  // namespace memgraph::flags
