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

#include "replication/status.hpp"
#include "io/network/endpoint.hpp"
#include "replication_coordination_glue/role.hpp"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

#include "fmt/format.h"

#include <nlohmann/json.hpp>

namespace memgraph::replication::durability {

constexpr auto *kReplicaName = "replica_name";
constexpr auto *kIpAddress = "replica_ip_address";
constexpr auto *kReplicaServer = "replica_server";
constexpr auto *kPort = "replica_port";
constexpr auto *kSyncMode = "replica_sync_mode";
constexpr auto *kCheckFrequency = "replica_check_frequency";
constexpr auto *kSSLKeyFile = "replica_ssl_key_file";
constexpr auto *kSSLCertFile = "replica_ssl_cert_file";
constexpr auto *kReplicationRole = "replication_role";
constexpr auto *kVersion = "durability_version";
constexpr auto *kMainUUID = "main_uuid";

void to_json(nlohmann::json &j, const ReplicationRoleEntry &p) {
  auto processMAIN = [&](MainRole const &main) {
    auto common =
        nlohmann::json{{kVersion, p.version}, {kReplicationRole, replication_coordination_glue::ReplicationRole::MAIN}};
    MG_ASSERT(main.main_uuid.has_value(), "Main should have id ready on version >= V3");
    common[kMainUUID] = main.main_uuid.value();
    j = std::move(common);
  };
  auto processREPLICA = [&](ReplicaRole const &replica) {
    auto common = nlohmann::json{{kVersion, p.version},
                                 {kReplicationRole, replication_coordination_glue::ReplicationRole::REPLICA}};

    common[kReplicaServer] = replica.config.repl_server;  // non-resolved
    common[kMainUUID] = replica.main_uuid;
    j = std::move(common);
  };
  std::visit(utils::Overloaded{processMAIN, processREPLICA}, p.role);
}

void from_json(const nlohmann::json &j, ReplicationRoleEntry &p) {
  // This value did not exist in V1, hence default DurabilityVersion::V1
  auto const version = j.value(kVersion, DurabilityVersion::V1);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  replication_coordination_glue::ReplicationRole role;
  j.at(kReplicationRole).get_to(role);
  switch (role) {
    case replication_coordination_glue::ReplicationRole::MAIN: {
      auto main_role = MainRole{};
      if (j.contains(kMainUUID)) {
        main_role.main_uuid = j.at(kMainUUID);
      }
      p = ReplicationRoleEntry{.version = version, .role = std::move(main_role)};
      break;
    }
    case replication_coordination_glue::ReplicationRole::REPLICA: {
      using io::network::Endpoint;
      // In V4 we renamed key `replica_ip_address` to `replica_server`
      auto repl_server = [j]() -> Endpoint {
        if (j.contains(kReplicaServer)) {
          return j.at(kReplicaServer).get<Endpoint>();
        }
        return {j.at(kIpAddress).get<std::string>(), j.at(kPort).get<uint16_t>()};
      }();

      auto config = ReplicationServerConfig{.repl_server = std::move(repl_server)};
      // main_uuid was introduced in V3
      auto replica_role = ReplicaRole{.config = std::move(config)};
      if (j.contains(kMainUUID)) {
        replica_role.main_uuid = j.at(kMainUUID);
      }
      p = ReplicationRoleEntry{.version = version, .role = std::move(replica_role)};
      break;
    }
  }
}

void to_json(nlohmann::json &j, const ReplicationReplicaEntry &p) {
  auto common = nlohmann::json{{kReplicaName, p.config.name},
                               {kSyncMode, p.config.mode},
                               {kCheckFrequency, p.config.replica_check_frequency.count()}};

  common[kReplicaServer] = p.config.repl_server_endpoint;  // non-resolved

  if (p.config.ssl.has_value()) {
    common[kSSLKeyFile] = p.config.ssl->key_file;
    common[kSSLCertFile] = p.config.ssl->cert_file;
  } else {
    common[kSSLKeyFile] = nullptr;
    common[kSSLCertFile] = nullptr;
  }
  j = std::move(common);
}
void from_json(const nlohmann::json &j, ReplicationReplicaEntry &p) {
  using io::network::Endpoint;

  const auto &key_file = j.at(kSSLKeyFile);
  const auto &cert_file = j.at(kSSLCertFile);

  MG_ASSERT(key_file.is_null() == cert_file.is_null());

  auto const seconds = j.at(kCheckFrequency).get<std::chrono::seconds::rep>();
  auto config = ReplicationClientConfig{
      .name = j.at(kReplicaName).get<std::string>(),
      .mode = j.at(kSyncMode).get<replication_coordination_glue::ReplicationMode>(),
      .replica_check_frequency = std::chrono::seconds{seconds},
  };

  auto repl_server_endpoint = [j]() -> Endpoint {
    if (j.contains(kReplicaServer)) {
      return j.at(kReplicaServer).get<Endpoint>();
    }
    return {j.at(kIpAddress).get<std::string>(), j.at(kPort).get<uint16_t>()};
  }();

  config.repl_server_endpoint = std::move(repl_server_endpoint);

  if (!key_file.is_null()) {
    config.ssl = ReplicationClientConfig::SSL{};
    key_file.get_to(config.ssl->key_file);
    cert_file.get_to(config.ssl->cert_file);
  }
  p = ReplicationReplicaEntry{.config = std::move(config)};
}
}  // namespace memgraph::replication::durability
