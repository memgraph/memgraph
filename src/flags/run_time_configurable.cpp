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

#include "flags/run_time_configurable.hpp"
#include <string>
#include <tuple>
#include "flags/bolt.hpp"
#include "flags/general.hpp"
#include "flags/log_level.hpp"
#include "spdlog/cfg/helpers-inl.h"
#include "spdlog/spdlog.h"
#include "utils/exceptions.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"

namespace {
// Bolt server name
constexpr auto kServerNameSettingKey = "server.name";
// Query timeout
constexpr auto kQueryTxSettingKey = "query.timeout";
// Log level
// No default value because it is not persistent
constexpr auto kLogLevelSettingKey = "log.level";
// Log to stderr
// No default value because it is not persistent
constexpr auto kLogToStderrSettingKey = "log.to_stderr";
}  // namespace

namespace memgraph::flags::run_time {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<double> execution_timeout_sec_;

std::string GetServerName() {
  std::string s;
  // Thread safe read of gflag
  gflags::GetCommandLineOption("bolt_server_name_for_init", &s);
  return s;
}

void Initialize() {
  gflags::CommandLineFlagInfo info;

  /*
   * Register bolt server name settings
   */
  gflags::GetCommandLineFlagInfo("bolt_server_name_for_init", &info);
  auto update_server_name = [&]() {
    const auto &name = memgraph::utils::global_settings.GetValue(kServerNameSettingKey);
    MG_ASSERT(name, "Failed to read server name from settings.");
    gflags::SetCommandLineOption("bolt_server_name_for_init", name->c_str());
  };
  memgraph::utils::global_settings.RegisterSetting(kServerNameSettingKey, info.default_value,
                                                   [&] { update_server_name(); });
  // Override server name if passed via command line argument
  if (!info.is_default) {
    memgraph::utils::global_settings.SetValue(kServerNameSettingKey, info.current_value);
  } else {
    // Force read from settings
    update_server_name();
  }

  /*
   * Register query timeout
   */
  gflags::GetCommandLineFlagInfo("query_execution_timeout_sec", &info);
  auto update_exe_timeout = [&]() {
    const auto query_tx = memgraph::utils::global_settings.GetValue(kQueryTxSettingKey);
    MG_ASSERT(query_tx, "Query timeout is missing from the settings");
    gflags::SetCommandLineOption("query_execution_timeout_sec", query_tx->c_str());
    execution_timeout_sec_ = std::stod(*query_tx);  // For faster reads
  };
  memgraph::utils::global_settings.RegisterSetting(kQueryTxSettingKey, info.default_value,
                                                   [&] { update_exe_timeout(); });
  // Override query timeout via command line argument
  memgraph::utils::global_settings.SetValue(kQueryTxSettingKey, info.current_value);

  /*
   * Register log level
   */
  auto get_global_log_level = []() {
    const auto log_level = memgraph::utils::global_settings.GetValue(kLogLevelSettingKey);
    MG_ASSERT(log_level, "Log level is missing from the settings");
    const auto ll_enum = memgraph::flags::LogLevelToEnum(*log_level);
    if (!ll_enum) {
      throw utils::BasicException("Unsupported log level {}", *log_level);
    }
    return std::make_tuple(*log_level, *ll_enum);
  };
  gflags::GetCommandLineFlagInfo("log_level", &info);
  memgraph::utils::global_settings.RegisterSetting(
      kLogLevelSettingKey, info.default_value,
      [&] {
        const auto [str, e] = get_global_log_level();
        spdlog::set_level(e);
        gflags::SetCommandLineOption("log_level", str.c_str());
      },
      memgraph::flags::ValidLogLevel);
  // Override with the flag's value
  memgraph::utils::global_settings.SetValue(kLogLevelSettingKey, info.current_value);

  /*
   * Register logging to stderr
   */
  gflags::GetCommandLineFlagInfo("also_log_to_stderr", &info);
  memgraph::utils::global_settings.RegisterSetting(
      kLogToStderrSettingKey, info.default_value,
      [&] {
        const auto enable = memgraph::utils::global_settings.GetValue(kLogToStderrSettingKey);
        if (enable == "true") {
          LogToStderr(std::get<1>(get_global_log_level()));
        } else {
          LogToStderr(spdlog::level::off);
        }
        gflags::SetCommandLineOption("also_log_to_stderr", enable->c_str());
      },
      [](std::string_view in) {
        const auto lc = memgraph::utils::ToLowerCase(in);
        return lc == "false" || lc == "true";
      });
  // Override log to stderr with command line argument
  memgraph::utils::global_settings.SetValue(kLogToStderrSettingKey, info.current_value);
}
}  // namespace memgraph::flags::run_time
