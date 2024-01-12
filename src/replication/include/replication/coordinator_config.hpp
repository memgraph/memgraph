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

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include "replication/mode.hpp"

namespace memgraph::replication {

#ifdef MG_ENTERPRISE
struct CoordinatorClientConfig {
  std::string name;
  std::string ip_address;
  uint16_t port{};

  // The default delay between main checking/pinging replicas is 1s because
  // that seems like a reasonable timeframe in which main should notice a
  // replica is down.
  std::chrono::seconds replica_check_frequency{1};

  struct SSL {
    std::string key_file;
    std::string cert_file;

    friend bool operator==(const SSL &, const SSL &) = default;
  };

  std::optional<SSL> ssl;

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
#endif

}  // namespace memgraph::replication
