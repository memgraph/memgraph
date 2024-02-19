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

#ifdef MG_ENTERPRISE

#include "nuraft/coordinator_state_machine.hpp"

namespace memgraph::coordination {

auto CoordinatorStateMachine::MainExists() const -> bool { return cluster_state_.MainExists(); }

auto CoordinatorStateMachine::IsMain(std::string const &instance_name) const -> bool {
  return cluster_state_.IsMain(instance_name);
}

auto CoordinatorStateMachine::IsReplica(std::string const &instance_name) const -> bool {
  return cluster_state_.IsReplica(instance_name);
}

auto CoordinatorStateMachine::EncodeLogAction(std::string const &name, RaftLogAction log_action) -> ptr<buffer> {
  auto const str_log = [&name, log_action] {
    switch (log_action) {
      case RaftLogAction::REGISTER_REPLICATION_INSTANCE:
        return "register_" + name;
      case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE:
        return "unregister_" + name;
      case RaftLogAction::SET_INSTANCE_AS_MAIN:
        return "promote_" + name;
      case RaftLogAction::SET_INSTANCE_AS_REPLICA:
        return "demote_" + name;
    }
  }();

  ptr<buffer> log = buffer::alloc(sizeof(uint32_t) + str_log.size());
  buffer_serializer bs(log);
  bs.put_str(str_log);
  return log;
}

auto CoordinatorStateMachine::DecodeLog(buffer &data) -> std::pair<std::string, RaftLogAction> {
  buffer_serializer bs(data);

  auto const log_str = bs.get_str();

  auto const sep = log_str.find('_');
  auto const action = log_str.substr(0, sep);
  auto const name = log_str.substr(sep + 1);

  spdlog::info("Decoding log: {} {}", name, action);

  return {name, ParseRaftLogAction(action)};
}

auto CoordinatorStateMachine::pre_commit(ulong const /*log_idx*/, buffer & /*data*/) -> ptr<buffer> { return nullptr; }

auto CoordinatorStateMachine::commit(ulong const log_idx, buffer &data) -> ptr<buffer> {
  // TODO: (andi) think about locking scheme
  buffer_serializer bs(data);

  auto const [instance_name, log_action] = DecodeLog(data);
  spdlog::info("commit {} : {} {}", log_idx, instance_name, log_action);
  cluster_state_.DoAction(instance_name, log_action);
  // push_back(ReplicationInstance)

  last_committed_idx_ = log_idx;
  return nullptr;
}

auto CoordinatorStateMachine::commit_config(ulong const log_idx, ptr<cluster_config> & /*new_conf*/) -> void {
  last_committed_idx_ = log_idx;
}

auto CoordinatorStateMachine::rollback(ulong const log_idx, buffer &data) -> void {
  // NOTE: Nothing since we don't do anything in pre_commit
}

auto CoordinatorStateMachine::read_logical_snp_obj(snapshot &snapshot, void *& /*user_snp_ctx*/, ulong obj_id,
                                                   ptr<buffer> &data_out, bool &is_last_obj) -> int {
  spdlog::info("read logical snapshot object, obj_id: {}", obj_id);

  ptr<SnapshotCtx> ctx = nullptr;
  {
    auto ll = std::lock_guard{snapshots_lock_};
    auto entry = snapshots_.find(snapshot.get_last_log_idx());
    if (entry == snapshots_.end()) {
      data_out = nullptr;
      is_last_obj = true;
      return 0;
    }
    ctx = entry->second;
  }
  ctx->cluster_state_.Serialize(data_out);
  is_last_obj = true;
  return 0;
}

auto CoordinatorStateMachine::save_logical_snp_obj(snapshot &snapshot, ulong &obj_id, buffer &data, bool is_first_obj,
                                                   bool is_last_obj) -> void {
  spdlog::info("save logical snapshot object, obj_id: {}, is_first_obj: {}, is_last_obj: {}", obj_id, is_first_obj,
               is_last_obj);

  buffer_serializer bs(data);
  auto cluster_state = CoordinatorClusterState::Deserialize(data);

  {
    auto ll = std::lock_guard{snapshots_lock_};
    auto entry = snapshots_.find(snapshot.get_last_log_idx());
    DMG_ASSERT(entry != snapshots_.end());
    entry->second->cluster_state_ = cluster_state;
  }
}

auto CoordinatorStateMachine::apply_snapshot(snapshot &s) -> bool {
  auto ll = std::lock_guard{snapshots_lock_};

  auto entry = snapshots_.find(s.get_last_log_idx());
  if (entry == snapshots_.end()) return false;

  cluster_state_ = entry->second->cluster_state_;
  return true;
}

auto CoordinatorStateMachine::free_user_snp_ctx(void *&user_snp_ctx) -> void {}

auto CoordinatorStateMachine::last_snapshot() -> ptr<snapshot> {
  auto ll = std::lock_guard{snapshots_lock_};
  auto entry = snapshots_.rbegin();
  if (entry == snapshots_.rend()) return nullptr;

  ptr<SnapshotCtx> ctx = entry->second;
  return ctx->snapshot_;
}

auto CoordinatorStateMachine::last_commit_index() -> ulong { return last_committed_idx_; }

auto CoordinatorStateMachine::create_snapshot(snapshot &s, async_result<bool>::handler_type &when_done) -> void {
  ptr<buffer> snp_buf = s.serialize();
  ptr<snapshot> ss = snapshot::deserialize(*snp_buf);
  create_snapshot_internal(ss);

  ptr<std::exception> except(nullptr);
  bool ret = true;
  when_done(ret, except);
}

auto CoordinatorStateMachine::create_snapshot_internal(ptr<snapshot> snapshot) -> void {
  auto ll = std::lock_guard{snapshots_lock_};

  auto ctx = cs_new<SnapshotCtx>(snapshot, cluster_state_);
  snapshots_[snapshot->get_last_log_idx()] = ctx;

  constexpr int MAX_SNAPSHOTS = 3;
  while (snapshots_.size() > MAX_SNAPSHOTS) {
    snapshots_.erase(snapshots_.begin());
  }
}

auto CoordinatorStateMachine::GetInstances() const -> std::vector<std::pair<std::string, std::string>> {
  return cluster_state_.GetInstances();
}

}  // namespace memgraph::coordination
#endif
