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

#include "coordination/coordinator_communication_config.hpp"
#include "nuraft/coordinator_log_store.hpp"

#include <spdlog/spdlog.h>
#include <libnuraft/nuraft.hxx>

namespace memgraph::coordination {

using nuraft::cluster_config;
using nuraft::cs_new;
using nuraft::srv_config;
using nuraft::srv_state;
using nuraft::state_mgr;

class CoordinatorStateManager : public state_mgr {
 public:
  explicit CoordinatorStateManager(CoordinatorInstanceInitConfig const &config);

  CoordinatorStateManager(CoordinatorStateManager const &) = delete;
  CoordinatorStateManager &operator=(CoordinatorStateManager const &) = delete;
  CoordinatorStateManager(CoordinatorStateManager &&) = delete;
  CoordinatorStateManager &operator=(CoordinatorStateManager &&) = delete;

  ~CoordinatorStateManager() override = default;

  auto load_config() -> ptr<cluster_config> override;

  auto save_config(cluster_config const &config) -> void override;

  auto save_state(srv_state const &state) -> void override;

  auto read_state() -> ptr<srv_state> override;

  auto load_log_store() -> ptr<log_store> override;

  auto server_id() -> int32 override;

  auto system_exit(int exit_code) -> void override;

  auto GetSrvConfig() const -> ptr<srv_config>;

 private:
  int my_id_;
  ptr<CoordinatorLogStore> cur_log_store_;
  ptr<srv_config> my_srv_config_;
  ptr<cluster_config> cluster_config_;
  ptr<srv_state> saved_state_;
};

}  // namespace memgraph::coordination
#endif
