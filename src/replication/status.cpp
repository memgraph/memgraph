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
#include "replication/status.hpp"

#include "fmt/format.h"
#include "utils/logging.hpp"

constexpr auto *kReplicaName = "replica_name";
constexpr auto *kIpAddress = "replica_ip_address";
constexpr auto *kPort = "replica_port";
constexpr auto *kSyncMode = "replica_sync_mode";
constexpr auto *kCheckFrequency = "replica_check_frequency";
constexpr auto *kSSLKeyFile = "replica_ssl_key_file";
constexpr auto *kSSLCertFile = "replica_ssl_cert_file";
constexpr auto *kReplicationRole = "replication_role";
constexpr auto *kVersion = "durability_version";

// TODO: use utils::...
template <class... Ts>
struct overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
overloaded(Ts...) -> overloaded<Ts...>;

namespace memgraph::replication {

namespace durability {

void to_json(nlohmann::json &j, const ReplicationRoleEntry &p) {
  auto processMAIN = [&](MainRole const &main) {
    j = nlohmann::json{{kVersion, p.version}, {kReplicationRole, ReplicationRole::MAIN}};
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
  std::visit(overloaded{processMAIN, processREPLICA}, p.role);
}

void from_json(const nlohmann::json &j, ReplicationRoleEntry &p) {
  // This value did not exist in V1, hence default DurabilityVersion::V1
  DurabilityVersion version = j.value(kVersion, DurabilityVersion::V1);
  ReplicationRole role;
  j.at(kReplicationRole).get_to(role);
  switch (role) {
    case ReplicationRole::MAIN: {
      p = ReplicationRoleEntry{.version = version, .role = MainRole{}};
      break;
    }
    case ReplicationRole::REPLICA: {
      std::string ip_address;
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
      .ip_address = j.at(kIpAddress).get<std::string>(),
      .port = j.at(kPort).get<uint16_t>(),
      .mode = j.at(kSyncMode).get<ReplicationMode>(),
      .replica_check_frequency = std::chrono::seconds{seconds},
  };
  if (!key_file.is_null()) {
    config.ssl = ReplicationClientConfig::SSL{};
    key_file.get_to(config.ssl->key_file);
    cert_file.get_to(config.ssl->cert_file);
  }
  p = ReplicationReplicaEntry{.config = std::move(config)};
}
}  // namespace durability

nlohmann::json ReplicationStatusToJSON(ReplicationStatus &&status) {
  auto data = nlohmann::json::object();

  data[kReplicaName] = std::move(status.name);
  data[kIpAddress] = std::move(status.ip_address);
  data[kPort] = status.port;
  data[kSyncMode] = status.sync_mode;

  data[kCheckFrequency] = status.replica_check_frequency.count();

  if (status.ssl.has_value()) {
    data[kSSLKeyFile] = std::move(status.ssl->key_file);
    data[kSSLCertFile] = std::move(status.ssl->cert_file);
  } else {
    data[kSSLKeyFile] = nullptr;
    data[kSSLCertFile] = nullptr;
  }

  if (status.role.has_value()) {
    data[kReplicationRole] = *status.role;
  }

  return data;
}
std::optional<ReplicationStatus> JSONToReplicationStatus(nlohmann::json &&data) {
  ReplicationStatus replica_status;

  const auto get_failed_message = [](const std::string_view message, const std::string_view nested_message) {
    return fmt::format("Failed to deserialize replica's configuration: {} : {}", message, nested_message);
  };

  try {
    data.at(kReplicaName).get_to(replica_status.name);
    data.at(kIpAddress).get_to(replica_status.ip_address);
    data.at(kPort).get_to(replica_status.port);
    data.at(kSyncMode).get_to(replica_status.sync_mode);

    replica_status.replica_check_frequency = std::chrono::seconds(data.at(kCheckFrequency));

    const auto &key_file = data.at(kSSLKeyFile);
    const auto &cert_file = data.at(kSSLCertFile);

    MG_ASSERT(key_file.is_null() == cert_file.is_null());

    if (!key_file.is_null()) {
      replica_status.ssl = ReplicationClientConfig::SSL{};
      data.at(kSSLKeyFile).get_to(replica_status.ssl->key_file);
      data.at(kSSLCertFile).get_to(replica_status.ssl->cert_file);
    }

    if (data.find(kReplicationRole) != data.end()) {
      replica_status.role = ReplicationRole::MAIN;
      data.at(kReplicationRole).get_to(replica_status.role.value());
    }
  } catch (const nlohmann::json::type_error &exception) {
    spdlog::error(get_failed_message("Invalid type conversion", exception.what()));
    return std::nullopt;
  } catch (const nlohmann::json::out_of_range &exception) {
    spdlog::error(get_failed_message("Non existing field", exception.what()));
    return std::nullopt;
  }

  return replica_status;
}
}  // namespace memgraph::replication
