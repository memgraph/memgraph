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

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>

#include "json/json.hpp"

#include "replication/config.hpp"
#include "replication/role.hpp"

namespace memgraph::replication {

namespace durability {

// Keys
constexpr auto *kReplicationRoleName{"__replication_role"};
constexpr auto *kReplicationReplicaPrefix{"__replication_replica:"};  // introduced in V2
constexpr auto *kReplicationEpoch{"__replication_epoch"};             // introduced in V2

enum class DurabilityVersion : uint8_t {
  V1,  // no distinct key for replicas
  V2,  // this version, epoch, replica prefix introduced
};

struct MainRole {
  //  ReplicationEpoch epoch;
};

struct ReplicaRole {
  ReplicationServerConfig config;
};

struct ReplicationRoleEntry {
  DurabilityVersion version =
      DurabilityVersion::V2;  // if not latest then migration required for kReplicationReplicaPrefix
  std::variant<MainRole, ReplicaRole> role;
};

struct ReplicationReplicaEntry {
  ReplicationClientConfig config;
};

void to_json(nlohmann::json &j, const ReplicationRoleEntry &p);
void from_json(const nlohmann::json &j, ReplicationRoleEntry &p);

void to_json(nlohmann::json &j, const ReplicationReplicaEntry &p);
void from_json(const nlohmann::json &j, ReplicationReplicaEntry &p);

}  // namespace durability

struct ReplicationStatus {
  std::string name;
  std::string ip_address;
  uint16_t port;
  ReplicationMode sync_mode;
  std::chrono::seconds replica_check_frequency;
  std::optional<ReplicationClientConfig::SSL> ssl;
  std::optional<ReplicationRole> role;

  friend bool operator==(const ReplicationStatus &, const ReplicationStatus &) = default;
};

nlohmann::json ReplicationStatusToJSON(ReplicationStatus &&status);
std::optional<ReplicationStatus> JSONToReplicationStatus(nlohmann::json &&data);

}  // namespace memgraph::replication
