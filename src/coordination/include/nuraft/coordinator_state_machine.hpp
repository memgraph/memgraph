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

#include "nuraft/coordinator_cluster_state.hpp"

#include <spdlog/spdlog.h>
#include <libnuraft/nuraft.hxx>

namespace memgraph::coordination {

using nuraft::async_result;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::cluster_config;
using nuraft::int32;
using nuraft::ptr;
using nuraft::snapshot;
using nuraft::state_machine;

enum class RaftLogAction : uint8_t {
  REGISTER_REPLICATION_INSTANCE,
  UNREGISTER_REPLICATION_INSTANCE,
  SET_INSTANCE_AS_MAIN,
  SET_INSTANCE_AS_REPLICA
};

class CoordinatorStateMachine : public state_machine {
 public:
  CoordinatorStateMachine() = default;
  CoordinatorStateMachine(CoordinatorStateMachine const &) = delete;
  CoordinatorStateMachine &operator=(CoordinatorStateMachine const &) = delete;
  CoordinatorStateMachine(CoordinatorStateMachine &&) = delete;
  CoordinatorStateMachine &operator=(CoordinatorStateMachine &&) = delete;
  ~CoordinatorStateMachine() override {}

  static auto EncodeLogAction(std::string const &instance_name, RaftLogAction log_action) -> ptr<buffer>;

  static auto DecodeLog(buffer &data) -> std::pair<std::string, RaftLogAction>;

  auto pre_commit(ulong log_idx, buffer &data) -> ptr<buffer> override;

  auto commit(ulong log_idx, buffer &data) -> ptr<buffer> override;

  auto commit_config(ulong log_idx, ptr<cluster_config> & /*new_conf*/) -> void override;

  auto rollback(ulong log_idx, buffer &data) -> void override;

  auto read_logical_snp_obj(snapshot & /*snapshot*/, void *& /*user_snp_ctx*/, ulong /*obj_id*/, ptr<buffer> &data_out,
                            bool &is_last_obj) -> int override;

  auto save_logical_snp_obj(snapshot &s, ulong &obj_id, buffer & /*data*/, bool /*is_first_obj*/, bool /*is_last_obj*/)
      -> void override;

  auto apply_snapshot(snapshot &s) -> bool override;

  auto free_user_snp_ctx(void *&user_snp_ctx) -> void override;

  auto last_snapshot() -> ptr<snapshot> override;

  auto last_commit_index() -> ulong override;

  auto create_snapshot(snapshot &s, async_result<bool>::handler_type &when_done) -> void override;

 private:
  CoordinatorClusterState cluster_state_;
  mutable utils::RWLock lock{utils::RWLock::Priority::READ};

  std::atomic<uint64_t> last_committed_idx_{0};

  ptr<snapshot> last_snapshot_;
  std::mutex last_snapshot_lock_;
};

}  // namespace memgraph::coordination
#endif
