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

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace memgraph::coordination {

inline constexpr auto *kDefaultReplicationServerIp = "0.0.0.0";

struct CoordinatorClientConfig {
  std::string instance_name;
  std::string ip_address;
  uint16_t port{};
  std::chrono::seconds health_check_frequency_sec{1};

  auto SocketAddress() const -> std::string { return ip_address + ":" + std::to_string(port); }

  struct ReplicationClientInfo {
    std::string instance_name;
    replication_coordination_glue::ReplicationMode replication_mode{};
    std::string replication_ip_address;
    uint16_t replication_port{};

    friend bool operator==(ReplicationClientInfo const &, ReplicationClientInfo const &) = default;
  };

  ReplicationClientInfo replication_client_info;

  struct SSL {
    std::string key_file;
    std::string cert_file;

    friend bool operator==(const SSL &, const SSL &) = default;
  };

  std::optional<SSL> ssl;

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
