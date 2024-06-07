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

#pragma once

#ifdef MG_ENTERPRISE

#include "io/network/endpoint.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/uuid.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include "kvstore/kvstore.hpp"

#include <fmt/format.h>
#include "json/json.hpp"

namespace memgraph::coordination {

inline constexpr auto *kDefaultReplicationServerIp = "0.0.0.0";

struct ReplicationInstanceInitConfig {
  int management_port{0};
};

struct CoordinatorInstanceInitConfig {
  uint32_t coordinator_id{0};
  int coordinator_port{0};
  int bolt_port{0};
  std::filesystem::path durability_dir;
  std::string nuraft_log_file;
  bool use_durability;

  // If nuraft_log_file isn't provided, spdlog::logger for NuRaft will still get created but without sinks effectively
  // then being a no-op logger.
  explicit CoordinatorInstanceInitConfig(uint32_t coordinator_id, int coordinator_port, int bolt_port,
                                         std::filesystem::path durability_dir, std::string nuraft_log_file = "",
                                         bool use_durability = true)
      : coordinator_id(coordinator_id),
        coordinator_port(coordinator_port),
        bolt_port(bolt_port),
        durability_dir(std::move(durability_dir)),
        nuraft_log_file(std::move(nuraft_log_file)),
        use_durability(use_durability) {
    MG_ASSERT(!this->durability_dir.empty(), "Path empty");
  }
};

struct CoordinatorStateManagerConfig {
  uint32_t coordinator_id_{0};
  int coordinator_port_{0};
  int bolt_port_{0};
  std::filesystem::path state_manager_durability_dir_;
  std::shared_ptr<kvstore::KVStore> durability_store_;

  CoordinatorStateManagerConfig(uint32_t coordinator_id, int coordinator_port, int bolt_port,
                                std::filesystem::path state_manager_durability_dir,
                                std::shared_ptr<kvstore::KVStore> durability_store = nullptr)
      : coordinator_id_(coordinator_id),
        coordinator_port_(coordinator_port),
        bolt_port_(bolt_port),
        state_manager_durability_dir_(std::move(state_manager_durability_dir)),
        durability_store_(std::move(durability_store)) {
    MG_ASSERT(!this->state_manager_durability_dir_.empty(), "State manager durability dir path is empty");
  }
};

struct CoordinatorStateMachineConfig {
  uint32_t coordinator_id_{0};
  std::shared_ptr<kvstore::KVStore> durability_store_;

  explicit CoordinatorStateMachineConfig(uint32_t coordinator_id,
                                         std::shared_ptr<kvstore::KVStore> durability_store = nullptr)
      : coordinator_id_(coordinator_id), durability_store_(std::move(durability_store)) {}
};

// NOTE: We need to be careful about durability versioning when changing the config which is persisted on disk.

struct ReplicationClientInfo {
  std::string instance_name{};
  replication_coordination_glue::ReplicationMode replication_mode{};
  io::network::Endpoint replication_server;

  friend bool operator==(ReplicationClientInfo const &, ReplicationClientInfo const &) = default;
};

struct CoordinatorToReplicaConfig {
  auto BoltSocketAddress() const -> std::string { return bolt_server.SocketAddress(); }
  auto ManagementSocketAddress() const -> std::string { return mgt_server.SocketAddress(); }
  auto ReplicationSocketAddress() const -> std::string {
    return replication_client_info.replication_server.SocketAddress();
  }

  std::string instance_name{};
  io::network::Endpoint mgt_server;
  io::network::Endpoint bolt_server;
  ReplicationClientInfo replication_client_info;

  std::chrono::seconds instance_health_check_frequency_sec{1};
  std::chrono::seconds instance_down_timeout_sec{5};
  std::chrono::seconds instance_get_uuid_frequency_sec{10};

  struct SSL {
    std::string key_file;
    std::string cert_file;
    friend bool operator==(const SSL &, const SSL &) = default;
  };

  std::optional<SSL> ssl;

  friend bool operator==(CoordinatorToReplicaConfig const &, CoordinatorToReplicaConfig const &) = default;
};

struct CoordinatorToCoordinatorConfig {
  uint32_t coordinator_id{0};
  io::network::Endpoint bolt_server;
  io::network::Endpoint coordinator_server;
  std::chrono::seconds instance_down_timeout_sec{5};

  friend bool operator==(CoordinatorToCoordinatorConfig const &, CoordinatorToCoordinatorConfig const &) = default;
};

struct ManagementServerConfig {
  io::network::Endpoint endpoint;
  struct SSL {
    std::string key_file;
    std::string cert_file;
    std::string ca_file;
    bool verify_peer{};
    friend bool operator==(SSL const &, SSL const &) = default;
  };

  std::optional<SSL> ssl;

  friend bool operator==(ManagementServerConfig const &, ManagementServerConfig const &) = default;
};

struct InstanceUUIDUpdate {
  std::string instance_name;
  memgraph::utils::UUID uuid;
};

void to_json(nlohmann::json &j, CoordinatorToReplicaConfig const &config);
void from_json(nlohmann::json const &j, CoordinatorToReplicaConfig &config);

void to_json(nlohmann::json &j, CoordinatorToCoordinatorConfig const &config);
void from_json(nlohmann::json const &j, CoordinatorToCoordinatorConfig &config);

void to_json(nlohmann::json &j, ReplicationClientInfo const &config);
void from_json(nlohmann::json const &j, ReplicationClientInfo &config);

void to_json(nlohmann::json &j, InstanceUUIDUpdate const &instance_uuid_update);
void from_json(nlohmann::json const &j, InstanceUUIDUpdate &instance_uuid_update);

}  // namespace memgraph::coordination
#endif
