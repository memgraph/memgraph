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

#include "flags/bolt.hpp"
#include "flags/general.hpp"
#include "flags/log_level.hpp"
#include "flags/query.hpp"
#include "spdlog/cfg/helpers-inl.h"
#include "spdlog/spdlog.h"
#include "utils/exceptions.hpp"
#include "utils/settings.hpp"
#include "utils/string.hpp"

namespace {
// Bolt server name
constexpr auto kServerNameSettingKey = "server.name";
constexpr auto kDefaultServerName = "Neo4j/v5.11.0 compatible graph database server - Memgraph";
// Query timeout
constexpr auto kQueryTxSettingKey = "query.timeout";
constexpr auto kDefaultQueryTx = "600";  // seconds
// Log level
// No default value because it is not persistent
constexpr auto kLogLevelSettingKey = "log.level";
// Log to stderr
// No default value because it is not persistent
constexpr auto kLogToStderrSettingKey = "log.to_stderr";

constexpr auto kCartesianProductEnabled = "cartesian-product-enabled";
}  // namespace

namespace memgraph::flags::run_time {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
memgraph::utils::Synchronized<std::string, memgraph::utils::SpinLock> bolt_server_name_;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<double> execution_timeout_sec_;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<bool> cartesian_product_enabled_;

void Initialize() {
  // Register bolt server name settings
  memgraph::utils::global_settings.RegisterSetting(kServerNameSettingKey, kDefaultServerName, [&] {
    const auto server_name = memgraph::utils::global_settings.GetValue(kServerNameSettingKey);
    MG_ASSERT(server_name, "Bolt server name is missing from the settings");
    *(bolt_server_name_.Lock()) = *server_name;
  });
  // Update value from read settings
  const auto &name = memgraph::utils::global_settings.GetValue(kServerNameSettingKey);
  MG_ASSERT(name, "Failed to read server name from settings.");
  *(bolt_server_name_.Lock()) = *name;
  // Override server name if passed via command line argument
  if (!FLAGS_bolt_server_name_for_init.empty()) {
    memgraph::utils::global_settings.SetValue(kServerNameSettingKey, FLAGS_bolt_server_name_for_init);
  }

  // Register query timeout
  memgraph::utils::global_settings.RegisterSetting(kQueryTxSettingKey, kDefaultQueryTx, [&] {
    const auto query_tx = memgraph::utils::global_settings.GetValue(kQueryTxSettingKey);
    MG_ASSERT(query_tx, "Query timeout is missing from the settings");
    execution_timeout_sec_ = std::stod(*query_tx);
  });
  // Update value from read settings
  const auto &tx = memgraph::utils::global_settings.GetValue(kQueryTxSettingKey);
  MG_ASSERT(tx, "Failed to read query timeout from settings.");
  execution_timeout_sec_ = std::stod(*tx);
  // Override query timeout if passed via command line argument
  if (FLAGS_query_execution_timeout_sec != -1) {
    memgraph::utils::global_settings.SetValue(kQueryTxSettingKey, std::to_string(FLAGS_query_execution_timeout_sec));
  }

  // Register log level
  auto get_global_log_level = []() {
    const auto log_level = memgraph::utils::global_settings.GetValue(kLogLevelSettingKey);
    MG_ASSERT(log_level, "Log level is missing from the settings");
    const auto ll_enum = memgraph::flags::LogLevelToEnum(*log_level);
    if (!ll_enum) {
      throw utils::BasicException("Unsupported log level {}", *log_level);
    }
    return *ll_enum;
  };
  memgraph::utils::global_settings.RegisterSetting(
      kLogLevelSettingKey, FLAGS_log_level, [&] { spdlog::set_level(get_global_log_level()); },
      memgraph::flags::ValidLogLevel);
  // Always override log level with command line argument
  memgraph::utils::global_settings.SetValue(kLogLevelSettingKey, FLAGS_log_level);

  // Register logging to stderr
  auto bool_to_str = [](bool in) { return in ? "true" : "false"; };
  const std::string log_to_stderr_s = bool_to_str(FLAGS_also_log_to_stderr);
  memgraph::utils::global_settings.RegisterSetting(
      kLogToStderrSettingKey, log_to_stderr_s,
      [&] {
        const auto enable = memgraph::utils::global_settings.GetValue(kLogToStderrSettingKey);
        if (enable == "true") {
          LogToStderr(get_global_log_level());
        } else {
          LogToStderr(spdlog::level::off);
        }
      },
      [](std::string_view in) {
        const auto lc = memgraph::utils::ToLowerCase(in);
        return lc == "false" || lc == "true";
      });
  // Always override log to stderr with command line argument
  memgraph::utils::global_settings.SetValue(kLogToStderrSettingKey, log_to_stderr_s);

  // Register cartesian product enabled
  auto string_to_bool = [](auto &item) {
    bool myBool = false;
    std::istringstream(item) >> std::boolalpha >> myBool;
    return myBool;
  };

  const std::string cartesian_product_enabled_s = bool_to_str(FLAGS_cartesian_product_enabled);
  memgraph::utils::global_settings.RegisterSetting(kCartesianProductEnabled, cartesian_product_enabled_s, [&] {
    const auto cartesian_product_enabled = memgraph::utils::global_settings.GetValue(kCartesianProductEnabled);
    MG_ASSERT(cartesian_product_enabled, "Cartesian product enabled is missing from the settings");

    cartesian_product_enabled_ = string_to_bool(*cartesian_product_enabled);
  });

  // Always override log to stderr with command line argument
  memgraph::utils::global_settings.SetValue(kCartesianProductEnabled, cartesian_product_enabled_s);
}
}  // namespace memgraph::flags::run_time
