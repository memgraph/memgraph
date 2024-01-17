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
#include "replication/status.hpp"

#include "fmt/format.h"
#include "utils/logging.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::replication::durability {

constexpr auto *kReplicaName = "replica_name";
constexpr auto *kIpAddress = "replica_ip_address";
constexpr auto *kPort = "replica_port";
constexpr auto *kSyncMode = "replica_sync_mode";
constexpr auto *kCheckFrequency = "replica_check_frequency";
constexpr auto *kSSLKeyFile = "replica_ssl_key_file";
constexpr auto *kSSLCertFile = "replica_ssl_cert_file";
constexpr auto *kReplicationRole = "replication_role";
constexpr auto *kEpoch = "epoch";
constexpr auto *kVersion = "durability_version";

void to_json(nlohmann::json &j, const ReplicationRoleEntry &p) {
  auto processMAIN = [&](MainRole const &main) {
    j = nlohmann::json{{kVersion, p.version}, {kReplicationRole, ReplicationRole::MAIN}, {kEpoch, main.epoch.id()}};
  };
  auto processREPLICA = [&](ReplicaRole const &replica) {
    j = nlohmann::json{
        {kVersion, p.version},
        {kReplicationRole, ReplicationRole::REPLICA},
        {kIpAddress, replica.config.ip_address},
        {kPort, replica.config.port}
        // TODO: SSL
    };
  };
  std::visit(utils::Overloaded{processMAIN, processREPLICA}, p.role);
}

void from_json(const nlohmann::json &j, ReplicationRoleEntry &p) {
  // This value did not exist in V1, hence default DurabilityVersion::V1
  DurabilityVersion version = j.value(kVersion, DurabilityVersion::V1);
  // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
  ReplicationRole role;
  j.at(kReplicationRole).get_to(role);
  switch (role) {
    case ReplicationRole::MAIN: {
      auto json_epoch = j.value(kEpoch, std::string{});
      auto epoch = ReplicationEpoch{};
      if (!json_epoch.empty()) epoch.SetEpoch(json_epoch);
      p = ReplicationRoleEntry{.version = version, .role = MainRole{.epoch = std::move(epoch)}};
      break;
    }
    case ReplicationRole::REPLICA: {
      std::string ip_address;
      // NOLINTNEXTLINE(cppcoreguidelines-init-variables)
      uint16_t port;
      j.at(kIpAddress).get_to(ip_address);
      j.at(kPort).get_to(port);
      auto config = ReplicationServerConfig{.ip_address = std::move(ip_address), .port = port};
      p = ReplicationRoleEntry{.version = version, .role = ReplicaRole{.config = std::move(config)}};
      break;
    }
  }
}

void to_json(nlohmann::json &j, const ReplicationReplicaEntry &p) {
  auto common = nlohmann::json{{kReplicaName, p.config.name},
                               {kIpAddress, p.config.ip_address},
                               {kPort, p.config.port},
                               {kSyncMode, p.config.mode},
                               {kCheckFrequency, p.config.replica_check_frequency.count()}};

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
  const auto &key_file = j.at(kSSLKeyFile);
  const auto &cert_file = j.at(kSSLCertFile);

  MG_ASSERT(key_file.is_null() == cert_file.is_null());

  auto seconds = j.at(kCheckFrequency).get<std::chrono::seconds::rep>();
  auto config = ReplicationClientConfig{
      .name = j.at(kReplicaName).get<std::string>(),
      .mode = j.at(kSyncMode).get<replication_coordination_glue::ReplicationMode>(),
      .ip_address = j.at(kIpAddress).get<std::string>(),
      .port = j.at(kPort).get<uint16_t>(),
      .replica_check_frequency = std::chrono::seconds{seconds},
  };
  if (!key_file.is_null()) {
    config.ssl = ReplicationClientConfig::SSL{};
    key_file.get_to(config.ssl->key_file);
    cert_file.get_to(config.ssl->cert_file);
  }
  p = ReplicationReplicaEntry{.config = std::move(config)};
}
}  // namespace memgraph::replication::durability
