// Copyright 2026 Memgraph Ltd.
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

#include <cstdint>
#include <filesystem>
#include <memory>
#include <nlohmann/json_fwd.hpp>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "coordination/constants.hpp"
#include "io/network/endpoint.hpp"
#include "kvstore/kvstore.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "utils/tls.hpp"
#include "utils/uuid.hpp"

namespace memgraph::coordination {

inline constexpr auto *kDefaultManagementServerIp = "0.0.0.0";

// Used for updating configuration of both coordinators and data instances. V1 is the coordinator-to-coordinator
// forward format before data instances had regions.
struct UpdateInstanceConfigV1 {
  std::variant<int32_t, std::string> data;
  io::network::Endpoint bolt_endpoint;
};

struct UpdateInstanceConfig {
  std::variant<int32_t, std::string> data;
  std::optional<io::network::Endpoint> bolt_endpoint;
  // nullopt leaves the region unchanged; an engaged empty optional removes it.
  std::optional<std::optional<std::string>> region;

  static auto Upgrade(UpdateInstanceConfigV1 const &prev) -> UpdateInstanceConfig {
    return UpdateInstanceConfig{.data = prev.data, .bolt_endpoint = prev.bolt_endpoint, .region = std::nullopt};
  }
};

struct ReplicationInstanceInitConfig {
  int management_port{0};
  // No need to persist TlsConfig, this is just cached value from gflags
  std::optional<utils::TlsConfig> tls_config;
};

// If nuraft_log_file isn't provided, spdlog::logger for NuRaft will still get created but without sinks effectively
// then being a no-op logger.
struct CoordinatorInstanceInitConfig {
  int32_t coordinator_id{0};
  int coordinator_port{0};
  int bolt_port{0};
  int management_port{0};
  std::filesystem::path durability_dir;
  std::string coordinator_hostname;
  std::string nuraft_log_file;
  // No need to persist TlsConfig, this is just cached value from gflags
  std::optional<utils::TlsConfig> tls_config;
};

struct LogStoreDurability {
  std::shared_ptr<kvstore::KVStore> durability_store_{nullptr};
  LogStoreVersion stored_log_store_version_{kActiveVersion};
};

struct CoordinatorStateManagerConfig {
  int32_t coordinator_id_{0};
  int coordinator_port_{0};
  int bolt_port_{0};
  int management_port_{0};
  std::string coordinator_hostname;
  std::filesystem::path state_manager_durability_dir_;
  LogStoreDurability log_store_durability_;
};

// NOTE: We need to be careful about durability versioning when changing the config which is persisted on disk.

// The wire format of ReplicationClientInfo before the replication mode became optional.
struct ReplicationClientInfoV1 {
  std::string instance_name;
  replication_coordination_glue::ReplicationMode replication_mode;
  io::network::Endpoint replication_server;

  friend bool operator==(ReplicationClientInfoV1 const &, ReplicationClientInfoV1 const &) = default;
};

struct ReplicationClientInfo {
  std::string instance_name;
  // Absent once the coordinator resolves the mode from the instance's region instead of storing it.
  std::optional<replication_coordination_glue::ReplicationMode> replication_mode;
  io::network::Endpoint replication_server;

  static auto Upgrade(ReplicationClientInfoV1 const &prev) -> ReplicationClientInfo {
    return ReplicationClientInfo{.instance_name = prev.instance_name,
                                 .replication_mode = prev.replication_mode,
                                 .replication_server = prev.replication_server};
  }

  // An absent mode downgrades to ASYNC.
  auto Downgrade() const -> ReplicationClientInfoV1 {
    return ReplicationClientInfoV1{
        .instance_name = instance_name,
        .replication_mode = replication_mode.value_or(replication_coordination_glue::ReplicationMode::ASYNC),
        .replication_server = replication_server};
  }

  friend bool operator==(ReplicationClientInfo const &, ReplicationClientInfo const &) = default;
};

inline auto UpgradeReplicationClientsInfo(std::vector<ReplicationClientInfoV1> const &prev)
    -> std::vector<ReplicationClientInfo> {
  return prev | std::views::transform(&ReplicationClientInfo::Upgrade) | std::ranges::to<std::vector>();
}

inline auto DowngradeReplicationClientsInfo(std::vector<ReplicationClientInfo> const &infos)
    -> std::vector<ReplicationClientInfoV1> {
  return infos | std::views::transform(&ReplicationClientInfo::Downgrade) | std::ranges::to<std::vector>();
}

// The coordinator-to-coordinator forward format of DataInstanceConfig before data instances had regions.
struct DataInstanceConfigV1 {
  std::string instance_name;
  io::network::Endpoint mgt_server;
  io::network::Endpoint bolt_server;
  ReplicationClientInfoV1 replication_client_info;
};

struct DataInstanceConfig {
  auto BoltSocketAddress() const -> std::string { return bolt_server.SocketAddress(); }

  auto ManagementSocketAddress() const -> std::string { return mgt_server.SocketAddress(); }

  auto ReplicationSocketAddress() const -> std::string {
    return replication_client_info.replication_server.SocketAddress();
  }

  std::string instance_name;
  io::network::Endpoint mgt_server;
  io::network::Endpoint bolt_server;
  ReplicationClientInfo replication_client_info;
  std::optional<std::string> region;

  static auto Upgrade(DataInstanceConfigV1 const &prev) -> DataInstanceConfig {
    return DataInstanceConfig{.instance_name = prev.instance_name,
                              .mgt_server = prev.mgt_server,
                              .bolt_server = prev.bolt_server,
                              .replication_client_info = ReplicationClientInfo::Upgrade(prev.replication_client_info),
                              .region = std::nullopt};
  }

  friend bool operator==(DataInstanceConfig const &, DataInstanceConfig const &) = default;
};

struct LeaderCoordinatorData {
  int id{0};
  std::string bolt_server;
};

struct CoordinatorInstanceConfig {
  int32_t coordinator_id{0};
  io::network::Endpoint bolt_server;
  io::network::Endpoint coordinator_server;
  io::network::Endpoint management_server;
  // Currently, this is needed additionally to the coordinator_server but maybe we could put hostname into bolt_server
  // and coordinator_server.
  std::string coordinator_hostname;

  friend bool operator==(CoordinatorInstanceConfig const &, CoordinatorInstanceConfig const &) = default;
};

struct ManagementServerConfig {
  io::network::Endpoint endpoint;

  explicit ManagementServerConfig(io::network::Endpoint endpoint) : endpoint(std::move(endpoint)) {}

  friend bool operator==(ManagementServerConfig const &, ManagementServerConfig const &) = default;
};

struct InstanceUUIDUpdate {
  std::string instance_name;
  utils::UUID uuid;
};

void to_json(nlohmann::json &j, DataInstanceConfig const &config);
void from_json(nlohmann::json const &j, DataInstanceConfig &config);

void to_json(nlohmann::json &j, CoordinatorInstanceConfig const &config);
void from_json(nlohmann::json const &j, CoordinatorInstanceConfig &config);

void to_json(nlohmann::json &j, ReplicationClientInfo const &config);
void from_json(nlohmann::json const &j, ReplicationClientInfo &config);

void to_json(nlohmann::json &j, InstanceUUIDUpdate const &instance_uuid_update);
void from_json(nlohmann::json const &j, InstanceUUIDUpdate &instance_uuid_update);

}  // namespace memgraph::coordination
#endif
