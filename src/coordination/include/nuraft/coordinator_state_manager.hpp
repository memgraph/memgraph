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
  explicit CoordinatorStateManager(int srv_id, std::string const &endpoint)
      : my_id_(srv_id), my_endpoint_(endpoint), cur_log_store_(cs_new<CoordinatorLogStore>()) {
    my_srv_config_ = cs_new<srv_config>(srv_id, endpoint);

    // Initial cluster config: contains only one server (myself).
    cluster_config_ = cs_new<cluster_config>();
    cluster_config_->get_servers().push_back(my_srv_config_);
  }

  CoordinatorStateManager(CoordinatorStateManager const &) = delete;
  CoordinatorStateManager &operator=(CoordinatorStateManager const &) = delete;
  CoordinatorStateManager(CoordinatorStateManager &&) = delete;
  CoordinatorStateManager &operator=(CoordinatorStateManager &&) = delete;

  ~CoordinatorStateManager() override {}

  ptr<cluster_config> load_config() override {
    // Just return in-memory data in this example.
    // May require reading from disk here, if it has been written to disk.
    return cluster_config_;
  }

  void save_config(const cluster_config &config) override {
    // Just keep in memory in this example.
    // Need to write to disk here, if want to make it durable.
    ptr<buffer> buf = config.serialize();
    cluster_config_ = cluster_config::deserialize(*buf);
  }

  void save_state(const srv_state &state) override {
    // Just keep in memory in this example.
    // Need to write to disk here, if want to make it durable.
    ptr<buffer> buf = state.serialize();
    saved_state_ = srv_state::deserialize(*buf);
  }

  ptr<srv_state> read_state() override {
    // Just return in-memory data in this example.
    // May require reading from disk here, if it has been written to disk.
    return saved_state_;
  }

  ptr<log_store> load_log_store() override { return cur_log_store_; }

  int32 server_id() override { return my_id_; }

  void system_exit(const int exit_code) override {}

  ptr<srv_config> get_srv_config() const { return my_srv_config_; }

 private:
  int my_id_;
  std::string my_endpoint_;
  ptr<CoordinatorLogStore> cur_log_store_;
  ptr<srv_config> my_srv_config_;
  ptr<cluster_config> cluster_config_;
  ptr<srv_state> saved_state_;
};

}  // namespace memgraph::coordination
#endif
