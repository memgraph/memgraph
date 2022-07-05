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

#include "storage/v2/replication/replication_persistence_helper.hpp"
#include "utils/logging.hpp"

namespace {
const std::string kReplicaName = "replica_name";
const std::string kIpAddress = "replica_ip_address";
const std::string kPort = "replica_port";
const std::string kSyncMode = "replica_sync_mode";
const std::string kCheckFrequency = "replica_check_frequency";
const std::string kSSLKeyFile = "replica_ssl_key_file";
const std::string kSSLCertFile = "replica_ssl_cert_file";
}  // namespace

namespace memgraph::storage::replication {

nlohmann::json ReplicaStatusToJSON(ReplicaStatus &&status) {
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

  return data;
}

std::optional<ReplicaStatus> JSONToReplicaStatus(nlohmann::json &&data) {
  ReplicaStatus replica_status;

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
      replica_status.ssl = replication::ReplicationClientConfig::SSL{};
      data.at(kSSLKeyFile).get_to(replica_status.ssl->key_file);
      data.at(kSSLCertFile).get_to(replica_status.ssl->cert_file);
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
}  // namespace memgraph::storage::replication
