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

// TODO: (andi) Fix ownerships with std::string_view
class CoordinatorClientInfo {
 public:
  CoordinatorClientInfo(std::string instance_name, std::string socket_address)
      : last_response_time_(std::chrono::system_clock::now()),
        is_alive_(true),  // TODO: (andi) Maybe it should be false until the first ping
        instance_name_(instance_name),
        socket_address_(socket_address) {}

  ~CoordinatorClientInfo() = default;

  CoordinatorClientInfo(const CoordinatorClientInfo &other)
      : last_response_time_(other.last_response_time_.load()),
        is_alive_(other.is_alive_.load()),
        instance_name_(other.instance_name_),
        socket_address_(other.socket_address_) {}

  CoordinatorClientInfo &operator=(const CoordinatorClientInfo &other) {
    if (this != &other) {
      last_response_time_ = other.last_response_time_.load();
      is_alive_ = other.is_alive_.load();
      instance_name_ = other.instance_name_;
      socket_address_ = other.socket_address_;
    }
    return *this;
  }

  CoordinatorClientInfo(CoordinatorClientInfo &&other) noexcept
      : last_response_time_(other.last_response_time_.load()),
        is_alive_(other.is_alive_.load()),
        instance_name_(std::move(other.instance_name_)),
        socket_address_(std::move(other.socket_address_)) {}

  CoordinatorClientInfo &operator=(CoordinatorClientInfo &&other) noexcept {
    if (this != &other) {
      last_response_time_.store(other.last_response_time_.load());
      is_alive_ = other.is_alive_.load();
      instance_name_ = std::move(other.instance_name_);
      socket_address_ = std::move(other.socket_address_);
    }
    return *this;
  }

  auto UpdateInstanceStatus() -> bool {
    is_alive_ = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now() -
                                                                 last_response_time_.load(std::memory_order_acquire))
                    .count() < CoordinatorClusterConfig::alive_response_time_difference_sec_;
    return is_alive_;
  }

  auto UpdateLastResponseTime() -> void { last_response_time_ = std::chrono::system_clock::now(); }
  auto InstanceName() const -> std::string_view { return instance_name_; }
  auto IsAlive() const -> bool { return is_alive_; }
  auto SocketAddress() const -> std::string_view { return socket_address_; }

 private:
  std::atomic<std::chrono::system_clock::time_point> last_response_time_{};
  std::atomic<bool> is_alive_{false};
  // TODO: (andi) Who owns this info?
  std::string instance_name_;
  std::string socket_address_;
};

}  // namespace memgraph::coordination

#endif
