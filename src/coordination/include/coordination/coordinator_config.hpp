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

#include "replication_coordination_glue/mode.hpp"
#include "utils/string.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

#include <fmt/format.h>

namespace memgraph::coordination {

inline constexpr auto *kDefaultReplicationServerIp = "0.0.0.0";

struct CoordinatorClientConfig {
  std::string instance_name;
  std::string ip_address;
  uint16_t port{};
  std::chrono::seconds instance_health_check_frequency_sec{1};
  std::chrono::seconds instance_down_timeout_sec{5};
  std::chrono::seconds instance_get_uuid_frequency_sec{10};

  auto CoordinatorSocketAddress() const -> std::string { return fmt::format("{}:{}", ip_address, port); }
  auto ReplicationSocketAddress() const -> std::string {
    return fmt::format("{}:{}", replication_client_info.replication_ip_address,
                       replication_client_info.replication_port);
  }

  struct ReplicationClientInfo {
    // TODO: (andi) Do we even need here instance_name for this struct?
    std::string instance_name;
    replication_coordination_glue::ReplicationMode replication_mode{};
    std::string replication_ip_address;
    uint16_t replication_port{};

    auto ToString() const -> std::string {
      return fmt::format("{}#{}#{}#{}", instance_name, replication_ip_address, replication_port,
                         replication_coordination_glue::ReplicationModeToString(replication_mode));
    }

    // TODO: (andi) How can I make use of monadic parsers here?
    static auto FromString(std::string_view log) -> ReplicationClientInfo {
      ReplicationClientInfo replication_client_info;
      auto splitted = utils::Split(log, "#");
      replication_client_info.instance_name = splitted[0];
      replication_client_info.replication_ip_address = splitted[1];
      replication_client_info.replication_port = std::stoi(splitted[2]);
      replication_client_info.replication_mode = replication_coordination_glue::ReplicationModeFromString(splitted[3]);
      return replication_client_info;
    }

    friend bool operator==(ReplicationClientInfo const &, ReplicationClientInfo const &) = default;
  };

  ReplicationClientInfo replication_client_info;

  struct SSL {
    std::string key_file;
    std::string cert_file;

    friend bool operator==(const SSL &, const SSL &) = default;
  };

  std::optional<SSL> ssl;

  auto ToString() const -> std::string {
    return fmt::format("{}|{}|{}|{}|{}|{}|{}", instance_name, ip_address, port,
                       instance_health_check_frequency_sec.count(), instance_down_timeout_sec.count(),
                       instance_get_uuid_frequency_sec.count(), replication_client_info.ToString());
  }

  static auto FromString(std::string_view log) -> CoordinatorClientConfig {
    CoordinatorClientConfig config;
    auto splitted = utils::Split(log, "|");
    config.instance_name = splitted[0];
    config.ip_address = splitted[1];
    config.port = std::stoi(splitted[2]);
    config.instance_health_check_frequency_sec = std::chrono::seconds(std::stoi(splitted[3]));
    config.instance_down_timeout_sec = std::chrono::seconds(std::stoi(splitted[4]));
    config.instance_get_uuid_frequency_sec = std::chrono::seconds(std::stoi(splitted[5]));
    config.replication_client_info = ReplicationClientInfo::FromString(splitted[6]);
    return config;
  }

  friend bool operator==(CoordinatorClientConfig const &, CoordinatorClientConfig const &) = default;
};

using ReplClientInfo = CoordinatorClientConfig::ReplicationClientInfo;

struct CoordinatorServerConfig {
  std::string ip_address;
  uint16_t port{};
  struct SSL {
    std::string key_file;
    std::string cert_file;
    std::string ca_file;
    bool verify_peer{};
    friend bool operator==(SSL const &, SSL const &) = default;
  };

  std::optional<SSL> ssl;

  friend bool operator==(CoordinatorServerConfig const &, CoordinatorServerConfig const &) = default;
};

}  // namespace memgraph::coordination
#endif
