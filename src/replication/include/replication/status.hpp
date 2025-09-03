// Copyright 2025 Memgraph Ltd.
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

#include <optional>
#include <variant>

#include <nlohmann/json_fwd.hpp>

#include "replication/config.hpp"
#include "replication/epoch.hpp"

namespace memgraph::replication::durability {

// Keys
constexpr auto *kReplicationRoleName{"__replication_role"};
constexpr auto *kReplicationReplicaPrefix{"__replication_replica:"};  // introduced in V2

enum class DurabilityVersion : uint8_t {
  V1,  // no distinct key for replicas
  V2,  // epoch, replica prefix introduced
  V3,  // version where main uuid was introduced
  V4,  // addresses as provided by users are saved to disk instead of eager evaluation
  V5   // epoch from main role is removed
};

// fragment of key: "__replication_role"
struct MainRole {
  std::optional<utils::UUID> main_uuid{};
  friend bool operator==(MainRole const &, MainRole const &) = default;
};

// fragment of key: "__replication_role"
struct ReplicaRole {
  ReplicationServerConfig config;
  utils::UUID main_uuid;
  friend bool operator==(ReplicaRole const &, ReplicaRole const &) = default;
};

// from key: "__replication_role"
struct ReplicationRoleEntry {
  DurabilityVersion version =
      DurabilityVersion::V5;  // if not latest has been read then migration required to the latest
  std::variant<MainRole, ReplicaRole> role;

  friend bool operator==(ReplicationRoleEntry const &, ReplicationRoleEntry const &) = default;
};

// from key: "__replication_replica:"
struct ReplicationReplicaEntry {
  // NOTE: There are multiple versions of ReplicationReplicaEntry, but we distinguish them in terms of
  // ReplicationRoleEntry.
  ReplicationClientConfig config;
  friend bool operator==(ReplicationReplicaEntry const &, ReplicationReplicaEntry const &) = default;
};

void to_json(nlohmann::json &j, const ReplicationRoleEntry &p);
void from_json(const nlohmann::json &j, ReplicationRoleEntry &p);

void to_json(nlohmann::json &j, const ReplicationReplicaEntry &p);
void from_json(const nlohmann::json &j, ReplicationReplicaEntry &p);

}  // namespace memgraph::replication::durability
