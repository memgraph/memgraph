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

#pragma once

#ifdef MG_ENTERPRISE

#include "coordination/replication_instance_client.hpp"

#include "utils/uuid.hpp"

namespace memgraph::coordination {

class TimedFailureDetector {
 public:
  explicit TimedFailureDetector(std::chrono::seconds instance_down_timeout_sec);

  auto IsAlive() const -> bool;
  auto LastSuccRespMs() const -> std::chrono::milliseconds;

  // Notifies that a process p is suspected to have crashed
  auto Suspect() -> bool;
  // Notifies that a process p is not suspected anymore
  auto Restore() -> void;

  friend bool operator==(TimedFailureDetector const &first, TimedFailureDetector const &second) = default;

 private:
  std::chrono::seconds instance_down_timeout_sec_{5};
  std::chrono::system_clock::time_point last_response_time_;
  bool is_alive_{true};
};

// Class used for managing the connection from coordinator to the data instance.
// Contains embedded timed failure detector
class ReplicationInstanceConnector {
 public:
  ReplicationInstanceConnector(DataInstanceConfig const &config, CoordinatorInstance *coord_instance,
                               std::chrono::seconds instance_down_timeout_sec,
                               std::chrono::seconds instance_health_check_frequency_sec);

  ReplicationInstanceConnector(ReplicationInstanceConnector const &other) = delete;
  ReplicationInstanceConnector &operator=(ReplicationInstanceConnector const &other) = delete;
  ReplicationInstanceConnector(ReplicationInstanceConnector &&other) noexcept = delete;
  ReplicationInstanceConnector &operator=(ReplicationInstanceConnector &&other) noexcept = delete;
  ~ReplicationInstanceConnector() = default;

  auto OnFailPing() const -> bool;
  auto OnSuccessPing() const -> void;

  auto IsAlive() const -> bool;
  auto LastSuccRespMs() const -> std::chrono::milliseconds;

  auto InstanceName() const -> std::string;

  auto SendSwapAndUpdateUUID(utils::UUID const &new_main_uuid) const -> bool;

  template <rpc::IsRpc T, typename... Args>
  auto SendRpc(Args &&...args) const -> bool {
    return client_.SendRpc<T>(std::forward<Args>(args)...);
  }

  auto StartStateCheck() -> void;
  auto StopStateCheck() -> void;
  auto PauseStateCheck() -> void;
  auto ResumeStateCheck() -> void;

  auto GetClient() const -> ReplicationInstanceClient const &;

 private:
  ReplicationInstanceClient client_;
  mutable TimedFailureDetector timed_failure_detector_;

  friend bool operator==(ReplicationInstanceConnector const &first,
                         ReplicationInstanceConnector const &second) = default;
};

}  // namespace memgraph::coordination
#endif
