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

#include "coordination/coordinator_cluster_config.hpp"
#include "io/network/endpoint.hpp"

#include <atomic>
#include <chrono>

namespace memgraph::coordination {

struct CoordinatorClientInfo {
  CoordinatorClientInfo(std::string_view instance_name, const io::network::Endpoint *endpoint)
      : last_response_time_(std::chrono::system_clock::now()),
        is_alive_(true),
        instance_name_(instance_name),
        endpoint(endpoint) {}

  ~CoordinatorClientInfo() = default;

  CoordinatorClientInfo(const CoordinatorClientInfo &other)
      : last_response_time_(other.last_response_time_.load()),
        is_alive_(other.is_alive_),
        instance_name_(other.instance_name_),
        endpoint(other.endpoint) {}

  CoordinatorClientInfo &operator=(const CoordinatorClientInfo &other) {
    if (this != &other) {
      last_response_time_.store(other.last_response_time_.load());
      is_alive_ = other.is_alive_;
      instance_name_ = other.instance_name_;
      endpoint = other.endpoint;
    }
    return *this;
  }

  CoordinatorClientInfo(CoordinatorClientInfo &&other) noexcept
      : last_response_time_(other.last_response_time_.load()),
        is_alive_(other.is_alive_),
        instance_name_(other.instance_name_),
        endpoint(other.endpoint) {}

  CoordinatorClientInfo &operator=(CoordinatorClientInfo &&other) noexcept {
    if (this != &other) {
      last_response_time_.store(other.last_response_time_.load());
      is_alive_ = other.is_alive_;
      instance_name_ = other.instance_name_;
      endpoint = other.endpoint;
    }
    return *this;
  }

  auto UpdateInstanceStatus() -> bool {
    is_alive_ =
        std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() - last_response_time_.load())
            .count() < CoordinatorClusterConfig::alive_response_time_difference_sec_;
    return is_alive_;
  }

  auto UpdateLastResponseTime() -> void { last_response_time_.store(std::chrono::system_clock::now()); }

  // TODO: (andi) Wrap in private to forbid modification
  std::atomic<std::chrono::system_clock::time_point> last_response_time_{};
  bool is_alive_{false};
  std::string_view instance_name_;
  const io::network::Endpoint *endpoint;
};

}  // namespace memgraph::coordination

#endif
