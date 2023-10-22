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

#include "metrics.hpp"

namespace {
constexpr auto kName = "name";
constexpr auto kSupportedBoltVersions = "supported_bolt_versions";
constexpr auto kBoltVersion = "bolt_version";
constexpr auto kConnectionTypes = "connection_types";
constexpr auto kSessions = "sessions";
constexpr auto kQueries = "queries";
}  // namespace
namespace memgraph::communication {

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
BoltMetrics bolt_metrics;

nlohmann::json BoltMetrics::Info::ToJson() const {
  nlohmann::json res;

  res[kName] = name;
  res[kSupportedBoltVersions] = nlohmann::json::array();
  for (const auto &sbv : supported_bolt_v) {
    res[kSupportedBoltVersions].push_back(sbv);
  }
  res[kBoltVersion] = bolt_v;
  res[kConnectionTypes] = {{ConnectionTypeStr((ConnectionType)0), connection_types[0].load()},
                           {ConnectionTypeStr((ConnectionType)1), connection_types[1].load()}};
  res[kSessions] = sessions.load();
  res[kQueries] = queries.load();
  return res;
}
}  // namespace memgraph::communication
