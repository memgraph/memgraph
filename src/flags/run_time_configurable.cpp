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

namespace {
// Bolt server name
constexpr const char *kServerNameSettingKey = "server.name";
constexpr auto kDefaultServerName = "Neo4j/v5.11.0 compatible graph database server - memgraph";
// Query timeout
constexpr const char *kQueryTxSettingKey = "query.timeout";
constexpr auto kDefaultQueryTx = "600";  // seconds
}  // namespace

namespace memgraph::flags::run_time {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
memgraph::utils::Synchronized<std::string, memgraph::utils::SpinLock> bolt_server_name_;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<double> query_tx_sec_;

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
    query_tx_sec_ = std::stod(*query_tx);
  });
  // Update value from read settings
  const auto &tx = memgraph::utils::global_settings.GetValue(kQueryTxSettingKey);
  MG_ASSERT(tx, "Failed to read query timeout from settings.");
  query_tx_sec_ = std::stod(*tx);
  // Override query timeout if passed via command line argument
  if (FLAGS_query_execution_timeout_sec != -1) {
    memgraph::utils::global_settings.SetValue(kQueryTxSettingKey, std::to_string(FLAGS_query_execution_timeout_sec));
  }
}
}  // namespace memgraph::flags::run_time
