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

#include "flags/run_time_configurable.hpp"

#include <string>
#include <tuple>

#include "gflags/gflags.h"

#include "flags/bolt.hpp"
#include "flags/general.hpp"
#include "flags/log_level.hpp"
#include "flags/query.hpp"
#include "spdlog/cfg/helpers-inl.h"
#include "spdlog/spdlog.h"
#include "utils/exceptions.hpp"
#include "utils/flag_validation.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"

/*
 * Setup GFlags
 */

// Bolt server flags.
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_string(bolt_server_name_for_init, "Neo4j/v5.11.0 compatible graph database server - Memgraph",
              "Server name which the database should send to the client in the "
              "Bolt INIT message.");

// Logging flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_HIDDEN_bool(also_log_to_stderr, false, "Log messages go to stderr in addition to logfiles");
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, misc-unused-parameters)
DEFINE_VALIDATED_string(log_level, "WARNING", memgraph::flags::log_level_help_string.c_str(),
                        { return memgraph::flags::ValidLogLevel(value); });

// Query flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_double(query_execution_timeout_sec, 600,
              "Maximum allowed query execution time. Queries exceeding this "
              "limit will be aborted. Value of 0 means no limit.");

// Query plan flags
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_bool(cartesian_product_enabled, true, "Enable cartesian product expansion.");

namespace {
// Bolt server name
constexpr auto kServerNameSettingKey = "server.name";
constexpr auto kServerNameGFlagsKey = "bolt_server_name_for_init";
// Query timeout
constexpr auto kQueryTxSettingKey = "query.timeout";
constexpr auto kQueryTxGFlagsKey = "query_execution_timeout_sec";
// Log level
// No default value because it is not persistent
constexpr auto kLogLevelSettingKey = "log.level";
constexpr auto kLogLevelGFlagsKey = "log_level";
// Log to stderr
// No default value because it is not persistent
constexpr auto kLogToStderrSettingKey = "log.to_stderr";
constexpr auto kLogToStderrGFlagsKey = "also_log_to_stderr";

constexpr auto kCartesianProductEnabledSettingKey = "cartesian-product-enabled";
constexpr auto kCartesianProductEnabledGFlagsKey = "cartesian-product-enabled";

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<double> execution_timeout_sec_;  // Local cache-like thing

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> cartesian_product_enabled_{true};  // Local cache-like thing

auto ToLLEnum(std::string_view val) {
  const auto ll_enum = memgraph::flags::LogLevelToEnum(val);
  if (!ll_enum) {
    throw memgraph::utils::BasicException("Unsupported log level {}", val);
  }
  return *ll_enum;
}

bool ValidBoolStr(std::string_view in) {
  const auto lc = memgraph::utils::ToLowerCase(in);
  return lc == "false" || lc == "true";
}

auto GenHandler(std::string flag, std::string key) {
  return [key = std::move(key), flag = std::move(flag)]() -> std::string {
    const auto &val = memgraph::utils::global_settings.GetValue(key);
    MG_ASSERT(val, "Failed to read value at '{}' from settings.", key);
    gflags::SetCommandLineOption(flag.c_str(), val->c_str());
    return *val;
  };
}

}  // namespace

namespace memgraph::flags::run_time {

void Initialize() {
  constexpr bool kRestore = true;  //!< run-time flag is persistent between Memgraph restarts

  /**
   * @brief Helper function that registers a run-time flag
   *
   * @param flag - GFlag name
   * @param key - Settings key used to store the flag
   * @param restore - true if the flag is persistent between restarts
   * @param post_update - user defined callback executed post flag update
   * @param validator - user defined value correctness checker
   */
  auto register_flag = [&](
                           const std::string &flag, const std::string &key, bool restore,
                           std::function<void(const std::string &)> post_update = [](auto) {},
                           std::function<bool(std::string_view)> validator = [](std::string_view) { return true; }) {
    // Get flag info
    gflags::CommandLineFlagInfo info;
    gflags::GetCommandLineFlagInfo(flag.c_str(), &info);
    // Register setting
    auto update = GenHandler(flag, key);
    memgraph::utils::global_settings.RegisterSetting(
        key, info.default_value,
        [update, post_update = std::move(post_update)] {
          const auto &val = update();
          post_update(val);
        },
        validator);

    if (restore && info.is_default) {
      // No input from the user, restore persistent value from settings
      update();
    } else {
      // Override with current value - user defined a new value or the run-time flag is not persistent between starts
      memgraph::utils::global_settings.SetValue(key, info.current_value);
    }
  };

  /*
   * Register bolt server name settings
   */
  register_flag(kServerNameGFlagsKey, kServerNameSettingKey, kRestore);

  /*
   * Register query timeout
   */
  register_flag(kQueryTxGFlagsKey, kQueryTxSettingKey, !kRestore, [&](const std::string &val) {
    execution_timeout_sec_ = std::stod(val);  // Cache for faster reads
  });

  /*
   * Register log level
   */
  register_flag(
      kLogLevelGFlagsKey, kLogLevelSettingKey, !kRestore,
      [](const std::string &val) {
        const auto ll_enum = ToLLEnum(val);
        spdlog::set_level(ll_enum);
        UpdateStderr(ll_enum);  // Updates level if active
      },
      memgraph::flags::ValidLogLevel);

  /*
   * Register logging to stderr
   */
  register_flag(
      kLogToStderrGFlagsKey, kLogToStderrSettingKey, !kRestore,
      [](const std::string &val) {
        if (val == "true") {
          // No need to check if ll_val exists, we got here, so the log_level must exist already
          const auto &ll_val = memgraph::utils::global_settings.GetValue(kLogLevelSettingKey);
          LogToStderr(ToLLEnum(*ll_val));
        } else {
          LogToStderr(spdlog::level::off);
        }
      },
      ValidBoolStr);

  register_flag(
      kCartesianProductEnabledGFlagsKey, kCartesianProductEnabledSettingKey, !kRestore,
      [](const std::string &val) { cartesian_product_enabled_ = val == "true"; }, ValidBoolStr);
}

std::string GetServerName() {
  std::string s;
  // Thread safe read of gflag
  gflags::GetCommandLineOption(kServerNameGFlagsKey, &s);
  return s;
}

double GetExecutionTimeout() { return execution_timeout_sec_; }

bool GetCartesianProductEnabled() { return cartesian_product_enabled_; }

}  // namespace memgraph::flags::run_time
