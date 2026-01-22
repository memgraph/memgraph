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

#ifdef MG_ENTERPRISE

#include "coordination/replication_instance_connector.hpp"

#include "replication_coordination_glue/handler.hpp"

#include <chrono>
#include <string>

namespace memgraph::coordination {

TimedFailureDetector::TimedFailureDetector(std::chrono::seconds const instance_down_timeout_sec)
    : instance_down_timeout_sec_(instance_down_timeout_sec) {}

auto TimedFailureDetector::IsAlive() const -> bool { return is_alive_; }

auto TimedFailureDetector::LastSuccRespMs() const -> std::chrono::milliseconds {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;
  using std::chrono::system_clock;

  return duration_cast<milliseconds>(system_clock::now() - last_response_time_);
}

auto TimedFailureDetector::Suspect() -> bool {
  const auto elapsed_time = std::chrono::system_clock::now() - last_response_time_;
  is_alive_ = elapsed_time < instance_down_timeout_sec_;
  return is_alive_;
}

auto TimedFailureDetector::Restore() -> void {
  last_response_time_ = std::chrono::system_clock::now();
  is_alive_ = true;
}

ReplicationInstanceConnector::ReplicationInstanceConnector(
    DataInstanceConfig const &config, CoordinatorInstance *coord_instance,
    const std::chrono::seconds instance_down_timeout_sec,
    const std::chrono::seconds instance_health_check_frequency_sec)
    : client_(ReplicationInstanceClient(config.instance_name, config.mgt_server, coord_instance,
                                        instance_health_check_frequency_sec)),
      timed_failure_detector_(instance_down_timeout_sec) {}

// Intentional logical constness
void ReplicationInstanceConnector::OnSuccessPing() const { timed_failure_detector_.Restore(); }

// Intentional logical constness
auto ReplicationInstanceConnector::OnFailPing() const -> bool { return timed_failure_detector_.Suspect(); }

auto ReplicationInstanceConnector::IsAlive() const -> bool { return timed_failure_detector_.IsAlive(); }

auto ReplicationInstanceConnector::LastSuccRespMs() const -> std::chrono::milliseconds {
  return timed_failure_detector_.LastSuccRespMs();
}

auto ReplicationInstanceConnector::InstanceName() const -> std::string const & { return client_.InstanceName(); }

auto ReplicationInstanceConnector::SendSwapAndUpdateUUID(utils::UUID const &new_main_uuid) const -> bool {
  return replication_coordination_glue::SendSwapMainUUIDRpc(client_.RpcClient(), new_main_uuid);
}

auto ReplicationInstanceConnector::StartStateCheck() -> void { client_.StartStateCheck(); }
auto ReplicationInstanceConnector::StopStateCheck() -> void { client_.StopStateCheck(); }
auto ReplicationInstanceConnector::PauseStateCheck() -> void { client_.PauseStateCheck(); }
auto ReplicationInstanceConnector::ResumeStateCheck() -> void { client_.ResumeStateCheck(); }

auto ReplicationInstanceConnector::GetClient() const -> ReplicationInstanceClient const & { return client_; }

}  // namespace memgraph::coordination

#endif
