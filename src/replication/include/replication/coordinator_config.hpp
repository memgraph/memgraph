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

#include "replication/mode.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>

namespace memgraph::replication {

struct CoordinatorClientConfig {
  const std::string instance_name;
  const std::string ip_address;
  const uint16_t port{};

  // Frequency with which coordinator pings main/replicas about it status
  const std::chrono::seconds health_check_frequency{1};

  const std::chrono::seconds last_response_alive_threshold{1};

  // Info which coordinator will send to new main when performing failover
  struct ReplicationClientInfo {
    // Should be the same as CoordinatorClientConfig's instance_name
    std::string instance_name;
    ReplicationMode replication_mode{};
    std::string replication_ip_address;
    uint16_t replication_port{};

    friend bool operator==(ReplicationClientInfo const &, ReplicationClientInfo const &) = default;
  };

  std::optional<ReplicationClientInfo> replication_client_info;

  struct SSL {
    const std::string key_file;
    const std::string cert_file;

    friend bool operator==(const SSL &, const SSL &) = default;
  };

  const std::optional<SSL> ssl;

  friend bool operator==(CoordinatorClientConfig const &, CoordinatorClientConfig const &) = default;
};

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

}  // namespace memgraph::replication
#endif
