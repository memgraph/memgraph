// Copyright 2022 Memgraph Ltd.
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

#include <chrono>
#include <compare>
#include <optional>
#include <string>

#include <json/json.hpp>

#include "storage/v2/replication/config.hpp"
#include "storage/v2/replication/enums.hpp"

namespace memgraph::storage::replication {

struct ReplicaStatus {
  std::string name;
  std::string ip_address;
  uint16_t port;
  ReplicationMode sync_mode;
  std::chrono::seconds replica_check_frequency;
  std::optional<ReplicationClientConfig::SSL> ssl;

  friend bool operator==(const ReplicaStatus &, const ReplicaStatus &) = default;
};

nlohmann::json ReplicaStatusToJSON(ReplicaStatus &&status);

std::optional<ReplicaStatus> JSONToReplicaStatus(nlohmann::json &&data);
}  // namespace memgraph::storage::replication
