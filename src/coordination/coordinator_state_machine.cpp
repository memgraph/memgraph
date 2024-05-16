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

namespace {
constexpr int MAX_SNAPSHOTS = 3;
}  // namespace

namespace memgraph::coordination {

CoordinatorStateMachine::CoordinatorStateMachine(LoggerWrapper logger, std::optional<std::filesystem::path> durability_dir): logger_(logger) {
  if (durability_dir) {
    kv_store_ = std::make_unique<kvstore::KVStore>(durability_dir.value());
  }
}

auto CoordinatorStateMachine::MainExists() const -> bool { return cluster_state_.MainExists(); }

auto CoordinatorStateMachine::HasMainState(std::string_view instance_name) const -> bool {
  return cluster_state_.HasMainState(instance_name);
}

auto CoordinatorStateMachine::HasReplicaState(std::string_view instance_name) const -> bool {
  return cluster_state_.HasReplicaState(instance_name);
}

auto CoordinatorStateMachine::CreateLog(nlohmann::json &&log) -> ptr<buffer> {
  auto const log_dump = log.dump();
  ptr<buffer> log_buf = buffer::alloc(sizeof(uint32_t) + log_dump.size());
  buffer_serializer bs(log_buf);
  bs.put_str(log_dump);
  return log_buf;
}

auto CoordinatorStateMachine::SerializeOpenLock() -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::OPEN_LOCK}, {"info", nullptr}});
}

auto CoordinatorStateMachine::SerializeCloseLock() -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::CLOSE_LOCK}, {"info", nullptr}});
}

auto CoordinatorStateMachine::SerializeRegisterInstance(CoordinatorToReplicaConfig const &config) -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::REGISTER_REPLICATION_INSTANCE}, {"info", config}});
}

auto CoordinatorStateMachine::SerializeUnregisterInstance(std::string_view instance_name) -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::UNREGISTER_REPLICATION_INSTANCE}, {"info", instance_name}});
}

auto CoordinatorStateMachine::SerializeSetInstanceAsMain(InstanceUUIDUpdate const &instance_uuid_change)
    -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::SET_INSTANCE_AS_MAIN}, {"info", instance_uuid_change}});
}

auto CoordinatorStateMachine::SerializeSetInstanceAsReplica(std::string_view instance_name) -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::SET_INSTANCE_AS_REPLICA}, {"info", instance_name}});
}

auto CoordinatorStateMachine::SerializeInstanceNeedsDemote(std::string_view instance_name) -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::INSTANCE_NEEDS_DEMOTE}, {"info", std::string{instance_name}}});
}

auto CoordinatorStateMachine::SerializeUpdateUUIDForNewMain(utils::UUID const &uuid) -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::UPDATE_UUID_OF_NEW_MAIN}, {"info", uuid}});
}

auto CoordinatorStateMachine::SerializeUpdateUUIDForInstance(InstanceUUIDUpdate const &instance_uuid_change)
    -> ptr<buffer> {
  return CreateLog({{"action", RaftLogAction::UPDATE_UUID_FOR_INSTANCE}, {"info", instance_uuid_change}});
}

auto CoordinatorStateMachine::DecodeLog(buffer &data) -> std::pair<TRaftLog, RaftLogAction> {
  buffer_serializer bs(data);
  auto const json = nlohmann::json::parse(bs.get_str());
  auto const action = json["action"].get<RaftLogAction>();
  auto const &info = json.at("info");

  switch (action) {
    case RaftLogAction::OPEN_LOCK:
      [[fallthrough]];
    case RaftLogAction::CLOSE_LOCK: {
      return {std::monostate{}, action};
    }
    case RaftLogAction::REGISTER_REPLICATION_INSTANCE:
      return {info.get<CoordinatorToReplicaConfig>(), action};
    case RaftLogAction::UPDATE_UUID_OF_NEW_MAIN:
      return {info.get<utils::UUID>(), action};
    case RaftLogAction::UPDATE_UUID_FOR_INSTANCE:
    case RaftLogAction::SET_INSTANCE_AS_MAIN:
      return {info.get<InstanceUUIDUpdate>(), action};
    case RaftLogAction::UNREGISTER_REPLICATION_INSTANCE:
    case RaftLogAction::INSTANCE_NEEDS_DEMOTE:
      [[fallthrough]];
    case RaftLogAction::SET_INSTANCE_AS_REPLICA:
      return {info.get<std::string>(), action};
  }
  throw std::runtime_error("Unknown action");
}

auto CoordinatorStateMachine::pre_commit(ulong const /*log_idx*/, buffer & /*data*/) -> ptr<buffer> { return nullptr; }

auto CoordinatorStateMachine::commit(ulong const log_idx, buffer &data) -> ptr<buffer> {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Commit: log_idx={}, data.size()={}", log_idx, data.size()));
  auto const [parsed_data, log_action] = DecodeLog(data);
  cluster_state_.DoAction(parsed_data, log_action);
  last_committed_idx_ = log_idx;

  // Return raft log number
  ptr<buffer> ret = buffer::alloc(sizeof(log_idx));
  buffer_serializer bs_ret(ret);
  bs_ret.put_u64(log_idx);
  return ret;
}

auto CoordinatorStateMachine::commit_config(ulong const log_idx, ptr<cluster_config> & /*new_conf*/) -> void {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Commit config: log_idx={}", log_idx));
  last_committed_idx_ = log_idx;
}

auto CoordinatorStateMachine::rollback(ulong const log_idx, buffer &data) -> void {
  // NOTE: Nothing since we don't do anything in pre_commit
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Rollback: log_idx={}, data.size()={}", log_idx, data.size()));
}

auto CoordinatorStateMachine::read_logical_snp_obj(snapshot &snapshot, void *& /*user_snp_ctx*/, ulong obj_id,
                                                   ptr<buffer> &data_out, bool &is_last_obj) -> int {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Read logical snapshot object, obj_id={}", obj_id));

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

  if (obj_id == 0) {
    // Object ID == 0: first object, put dummy data.
    data_out = buffer::alloc(sizeof(int32));
    buffer_serializer bs(data_out);
    bs.put_i32(0);
    is_last_obj = false;
  } else {
    // Object ID > 0: second object, put actual value.
    ctx->cluster_state_.Serialize(data_out);
    is_last_obj = true;  // TODO(antoniofilipovic) why do we put here true?
  }

  return 0;
}

auto CoordinatorStateMachine::save_logical_snp_obj(snapshot &snapshot, ulong &obj_id, buffer &data, bool is_first_obj,
                                                   bool is_last_obj) -> void {
  logger_.Log(nuraft_log_level::TRACE,
              fmt::format("Save logical snapshot object, obj_id={}, is_first_obj={}, is_last_obj={}", obj_id,
                          is_first_obj, is_last_obj));

  if (obj_id == 0) {
    ptr<buffer> snp_buf = snapshot.serialize();
    auto ss = snapshot::deserialize(*snp_buf);
    create_snapshot_internal(ss);
  } else {
    auto cluster_state = CoordinatorClusterState::Deserialize(data);

    auto ll = std::lock_guard{snapshots_lock_};
    auto entry = snapshots_.find(snapshot.get_last_log_idx());
    MG_ASSERT(entry != snapshots_.end());
    entry->second->cluster_state_ = cluster_state;
  }
  obj_id++;
}

auto CoordinatorStateMachine::apply_snapshot(snapshot &s) -> bool {
  auto ll = std::lock_guard{snapshots_lock_};
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Apply snapshot, last_log_idx={}", s.get_last_log_idx()));

  auto entry = snapshots_.find(s.get_last_log_idx());
  if (entry == snapshots_.end()) return false;

  cluster_state_ = entry->second->cluster_state_;
  return true;
}

auto CoordinatorStateMachine::free_user_snp_ctx(void *&user_snp_ctx) -> void {}

auto CoordinatorStateMachine::last_snapshot() -> ptr<snapshot> {
  auto ll = std::lock_guard{snapshots_lock_};
  logger_.Log(nuraft_log_level::TRACE, "Getting last snapshot from state machine.");
  auto entry = snapshots_.rbegin();
  if (entry == snapshots_.rend()) {
    logger_.Log(nuraft_log_level::TRACE, "There is no snapshot.");
    return nullptr;
  }

  ptr<SnapshotCtx> ctx = entry->second;
  return ctx->snapshot_;
}

auto CoordinatorStateMachine::last_commit_index() -> ulong {
  logger_.Log(nuraft_log_level::TRACE, "Getting last committed index from state machine.");
  return last_committed_idx_;
}

auto CoordinatorStateMachine::create_snapshot(snapshot &s, async_result<bool>::handler_type &when_done) -> void {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Create snapshot, last_log_idx={}", s.get_last_log_idx()));
  ptr<buffer> snp_buf = s.serialize();
  ptr<snapshot> ss = snapshot::deserialize(*snp_buf);
  create_snapshot_internal(ss);

  ptr<std::exception> except(nullptr);
  bool ret = true;
  when_done(ret, except);
}

auto CoordinatorStateMachine::create_snapshot_internal(ptr<snapshot> snapshot) -> void {
  auto ll = std::lock_guard{snapshots_lock_};
  logger_.Log(nuraft_log_level::TRACE,
              fmt::format("Create snapshot internal, last_log_idx={}", snapshot->get_last_log_idx()));

  auto ctx = cs_new<SnapshotCtx>(snapshot, cluster_state_);
  snapshots_[snapshot->get_last_log_idx()] = ctx;
  if (kv_store_) {
    ptr<buffer> data;
    cluster_state_.Serialize(data);
    // TODO serialize snapshot also
    kv_store_->Put("snapshot_id_" + std::to_string(snapshot->get_last_log_idx()), data->get_str());
  }

  while (snapshots_.size() > MAX_SNAPSHOTS) {
    snapshots_.erase(snapshots_.begin());
  }
}

auto CoordinatorStateMachine::GetReplicationInstances() const -> std::vector<ReplicationInstanceState> {
  return cluster_state_.GetReplicationInstances();
}

auto CoordinatorStateMachine::GetCurrentMainUUID() const -> utils::UUID { return cluster_state_.GetCurrentMainUUID(); }

auto CoordinatorStateMachine::IsCurrentMain(std::string_view instance_name) const -> bool {
  return cluster_state_.IsCurrentMain(instance_name);
}

auto CoordinatorStateMachine::GetInstanceUUID(std::string_view instance_name) const -> utils::UUID {
  return cluster_state_.GetInstanceUUID(instance_name);
}

auto CoordinatorStateMachine::IsLockOpened() const -> bool { return cluster_state_.IsLockOpened(); }

auto CoordinatorStateMachine::TryGetCurrentMainName() const -> std::optional<std::string> {
  return cluster_state_.TryGetCurrentMainName();
}

}  // namespace memgraph::coordination
#endif
