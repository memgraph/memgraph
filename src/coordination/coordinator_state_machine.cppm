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

module;

#include "kvstore/kvstore.hpp"
#include "utils/uuid.hpp"

#include <optional>

#include "libnuraft/nuraft.hxx"
#include "nlohmann/json_fwd.hpp"

export module memgraph.coordination.coordinator_state_machine;

#ifdef MG_ENTERPRISE

import memgraph.coordination.coordinator_cluster_state;
import memgraph.coordination.constants;
import memgraph.coordination.coordinator_communication_config;
import memgraph.coordination.coordinator_log_store;
import memgraph.coordination.coordinator_instance_context;
import memgraph.coordination.data_instance_context;
import memgraph.coordination.logger_wrapper;

export namespace memgraph::coordination {

using nuraft::async_result;
using nuraft::buffer;
using nuraft::buffer_serializer;
using nuraft::cluster_config;
using nuraft::int32;
using nuraft::logger;
using nuraft::ptr;
using nuraft::snapshot;
using nuraft::state_machine;

struct SnapshotCtx {
  SnapshotCtx(ptr<snapshot> const &snapshot, CoordinatorClusterState const &cluster_state)
      : snapshot_(snapshot), cluster_state_(cluster_state) {}

  SnapshotCtx() = default;

  ptr<snapshot> snapshot_;
  CoordinatorClusterState cluster_state_;
};

void from_json(nlohmann::json const &j, SnapshotCtx &snapshot_ctx);
void to_json(nlohmann::json &j, SnapshotCtx const &snapshot_ctx);

class CoordinatorStateMachine final : public state_machine {
 public:
  CoordinatorStateMachine(LoggerWrapper logger, LogStoreDurability log_store_durability);
  CoordinatorStateMachine(CoordinatorStateMachine const &) = delete;
  CoordinatorStateMachine &operator=(CoordinatorStateMachine const &) = delete;
  CoordinatorStateMachine(CoordinatorStateMachine &&) = delete;
  CoordinatorStateMachine &operator=(CoordinatorStateMachine &&) = delete;
  ~CoordinatorStateMachine() override = default;

  static auto CreateLog(nlohmann::json const &log) -> ptr<buffer>;

  static auto SerializeUpdateClusterState(CoordinatorClusterStateDelta const &delta_state) -> ptr<buffer>;

  static auto DecodeLog(buffer &data) -> CoordinatorClusterStateDelta;

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

  auto GetDataInstancesContext() const -> std::vector<DataInstanceContext>;
  auto GetCoordinatorInstancesContext() const -> std::vector<CoordinatorInstanceContext>;

  void UpdateStateMachineFromSnapshotDurability();

  // Getters
  auto MainExists() const -> bool;
  auto HasMainState(std::string_view instance_name) const -> bool;
  auto IsCurrentMain(std::string_view instance_name) const -> bool;

  auto GetCurrentMainUUID() const -> utils::UUID;
  auto TryGetCurrentMainName() const -> std::optional<std::string>;

  auto GetEnabledReadsOnMain() const -> bool;
  auto GetSyncFailoverOnly() const -> bool;
  auto GetMaxFailoverReplicaLag() const -> uint64_t;
  auto GetMaxReplicaReadLag() const -> uint64_t;

 private:
  bool HandleMigration(LogStoreVersion stored_version);

  auto CreateSnapshotInternal(ptr<snapshot> const &snapshot) -> void;

  CoordinatorClusterState cluster_state_;
  std::atomic<uint64_t> last_committed_idx_{0};

  std::map<uint64_t, ptr<SnapshotCtx>> snapshots_;
  std::mutex snapshots_lock_;

  LoggerWrapper logger_;
  ptr<snapshot> last_snapshot_;

  std::shared_ptr<kvstore::KVStore> durability_;
};

}  // namespace memgraph::coordination
#endif
