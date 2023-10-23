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

#include "communication/bolt/v1/value.hpp"
#include "communication/metrics.hpp"

namespace memgraph::communication::bolt {

template <typename TSession>
inline void RegisterNewSession(TSession &session, Value &metadata) {
  auto &data = metadata.ValueMap();
  session.metrics_ = bolt_metrics.Add(data.contains("user_agent") ? data["user_agent"].ValueString() : "unknown",
                                      fmt::format("{}.{}", session.version_.major, session.version_.minor),
                                      session.client_supported_bolt_versions_);
  ++session.metrics_.value()->sessions;
  auto conn_type = (!data.contains("scheme") || data["scheme"].ValueString() == "none")
                       ? BoltMetrics::ConnectionType::kAnonymous
                       : BoltMetrics::ConnectionType::kBasic;
  ++session.metrics_.value()->connection_types[(int)conn_type];
}

template <typename TSession>
inline void TouchNewSession(TSession &session, Value &metadata) {
  auto &data = metadata.ValueMap();
  session.metrics_ = bolt_metrics.Add(data.contains("user_agent") ? data["user_agent"].ValueString() : "unknown");
}

template <typename TSession>
inline void UpdateNewSession(TSession &session, Value &metadata) {
  auto &data = metadata.ValueMap();
  session.metrics_.value()->bolt_v = fmt::format("{}.{}", session.version_.major, session.version_.minor);
  session.metrics_.value()->supported_bolt_v = session.client_supported_bolt_versions_;
  ++session.metrics_.value()->sessions;
  auto conn_type = (!data.contains("scheme") || data["scheme"].ValueString() == "none")
                       ? BoltMetrics::ConnectionType::kAnonymous
                       : BoltMetrics::ConnectionType::kBasic;
  ++session.metrics_.value()->connection_types[(int)conn_type];
}

template <typename TSession>
inline void IncrementQueryMetrics(TSession &session) {
  ++session.metrics_.value()->queries;
}

}  // namespace memgraph::communication::bolt
