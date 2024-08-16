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
#include "nuraft/constants_log_durability.hpp"
#include "nuraft/coordinator_cluster_state.hpp"
#include "nuraft/coordinator_state_manager.hpp"
#include "utils.hpp"
#include "utils/logging.hpp"

#include <regex>

using nuraft::cluster_config;
using nuraft::ptr;
using nuraft::snapshot;

namespace {

constexpr int MAX_SNAPSHOTS = 3;

}  // namespace

namespace memgraph::coordination {

void from_json(nlohmann::json const &j, SnapshotCtx &snapshot_ctx) {
  auto cluster_state_json = j.at(kCoordClusterState.data()).get<std::string>();
  auto last_log_idx = j.at(kLastLogIdx.data()).get<uint64_t>();
  auto last_log_term = j.at(kLastLogTerm.data()).get<uint64_t>();
  auto size = j.at(kSize.data()).get<uint64_t>();
  auto last_config_json = j.at(kLastConfig.data()).get<std::string>();
  auto type = static_cast<nuraft::snapshot::type>((j.at(kType.data()).get<int>()));

  ptr<cluster_config> last_config;
  from_json(nlohmann::json::parse(last_config_json), last_config);

  auto deserialized_snapshot = cs_new<snapshot>(last_log_idx, last_log_term, last_config, size, type);

  CoordinatorClusterState cluster_state;
  from_json(nlohmann::json::parse(cluster_state_json), cluster_state);

  snapshot_ctx = SnapshotCtx{deserialized_snapshot, cluster_state};
}

void to_json(nlohmann::json &j, SnapshotCtx const &snapshot_ctx) {
  nlohmann::json cluster_state_json;
  memgraph::coordination::to_json(cluster_state_json, snapshot_ctx.cluster_state_);

  nlohmann::json last_config_json;
  memgraph::coordination::to_json(last_config_json, *snapshot_ctx.snapshot_->get_last_config());

  j = nlohmann::json{{kCoordClusterState.data(), cluster_state_json.dump()},
                     {kLastLogTerm.data(), snapshot_ctx.snapshot_->get_last_log_term()},
                     {kLastLogIdx.data(), snapshot_ctx.snapshot_->get_last_log_idx()},
                     {kSize.data(), snapshot_ctx.snapshot_->size()},
                     {kLastConfig.data(), last_config_json.dump()},
                     {kType.data(), static_cast<int>((snapshot_ctx.snapshot_->get_type()))}};
}

CoordinatorStateMachine::CoordinatorStateMachine(LoggerWrapper logger,
                                                 std::optional<LogStoreDurability> log_store_durability)
    : logger_(logger) {
  if (log_store_durability.has_value()) {
    durability_ = log_store_durability->durability_store_;
  }
  if (!durability_) {
    logger_.Log(nuraft_log_level::WARNING, "Coordinator state machine stores snapshots only in memory from now on.");
    return;
  }

  logger_.Log(nuraft_log_level::INFO, "Restoring coordinator state machine with durability.");

  bool const successful_migration = HandleMigration(log_store_durability->stored_log_store_version_);

  MG_ASSERT(successful_migration, "Couldn't handle migration of log store version.");
}

void CoordinatorStateMachine::UpdateStateMachineFromSnapshotDurability() {
  auto const end_iter = durability_->end(std::string{kSnapshotIdPrefix});
  for (auto kv_store_snapshot_it = durability_->begin(std::string{kSnapshotIdPrefix}); kv_store_snapshot_it != end_iter;
       ++kv_store_snapshot_it) {
    auto const &[snapshot_key_id, snapshot_ctx_str] = *kv_store_snapshot_it;
    try {
      auto parsed_snapshot_id =
          std::stoul(std::regex_replace(snapshot_key_id, std::regex{kSnapshotIdPrefix.data()}, ""));
      last_committed_idx_ = std::max(last_committed_idx_.load(), parsed_snapshot_id);

      // NOLINTNEXTLINE (misc-const-correctness)
      auto snapshot_ctx = cs_new<SnapshotCtx>();
      from_json(nlohmann::json::parse(snapshot_ctx_str), *snapshot_ctx);
      snapshots_[parsed_snapshot_id] = snapshot_ctx;
    } catch (std::exception &e) {
      LOG_FATAL("Failed to deserialize snapshot with id: {}. Error: {}", snapshot_key_id, e.what());
    }
    logger_.Log(nuraft_log_level::TRACE, fmt::format("Deserialized snapshot with id: {}", snapshot_key_id));
  }

  if (last_committed_idx_ == 0) {
    logger_.Log(nuraft_log_level::TRACE, "Last committed index from snapshots is 0");
    return;
  }
  cluster_state_ = snapshots_[last_committed_idx_]->cluster_state_;
  logger_.Log(nuraft_log_level::TRACE,
              fmt::format("Restored cluster state from snapshot with id: {}", last_committed_idx_));
}

bool CoordinatorStateMachine::HandleMigration(LogStoreVersion stored_version) {
  UpdateStateMachineFromSnapshotDurability();
  if (kActiveVersion == LogStoreVersion::kV2) {
    if (stored_version == LogStoreVersion::kV1) {
      return durability_->Put(kLastCommitedIdx, std::to_string(last_committed_idx_));
    }
    if (stored_version == LogStoreVersion::kV2) {
      auto maybe_last_commited_idx = durability_->Get(kLastCommitedIdx);
      if (!maybe_last_commited_idx.has_value()) {
        logger_.Log(
            nuraft_log_level::ERROR,
            fmt::format(
                "Failed to retrieve last committed index from disk, using last committed index from snapshot {}.",
                last_committed_idx_.load()));
        return durability_->Put(kLastCommitedIdx, std::to_string(last_committed_idx_));
      }
      auto last_committed_idx_value = std::stoul(maybe_last_commited_idx.value());
      if (last_committed_idx_value < last_committed_idx_) {
        logger_.Log(nuraft_log_level::ERROR, fmt::format("Last committed index stored in durability is smaller then "
                                                         "one found from snapshots, using one found in snapshots {}.",
                                                         last_committed_idx_.load()));
        return durability_->Put(kLastCommitedIdx, std::to_string(last_committed_idx_));
      }
      last_committed_idx_ = last_committed_idx_value;
      logger_.Log(nuraft_log_level::TRACE,
                  fmt::format("Restored last committed index from disk: {}", last_committed_idx_));
      return true;
    }
    throw CoordinatorStateMachineVersionMigrationException("Unexpected log store version {} for active version v2.",
                                                           static_cast<int>(stored_version));
  }
  throw CoordinatorStateMachineVersionMigrationException("Unexpected log store version {} for active version {}.",
                                                         static_cast<int>(stored_version),
                                                         static_cast<int>(kActiveVersion));
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
      return std::pair{info.get<InstanceUUIDUpdate>(), action};
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
  auto const &[parsed_data, log_action] = DecodeLog(data);
  cluster_state_.DoAction(parsed_data, log_action);
  if (durability_) {
    durability_->Put(kLastCommitedIdx, std::to_string(log_idx));
  }
  last_committed_idx_ = log_idx;
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Last commit index: {}", last_committed_idx_));
  ptr<buffer> ret = buffer::alloc(sizeof(log_idx));
  buffer_serializer bs_ret(ret);
  bs_ret.put_u64(log_idx);
  return ret;
}

auto CoordinatorStateMachine::commit_config(ulong const log_idx, ptr<cluster_config> & /*new_conf*/) -> void {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Commit config: log_idx={}", log_idx));
  if (durability_) {
    durability_->Put(kLastCommitedIdx, std::to_string(log_idx));
  }
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
    is_last_obj = true;
  }

  return 0;
}

auto CoordinatorStateMachine::save_logical_snp_obj(snapshot &snapshot, ulong &obj_id, buffer &data, bool is_first_obj,
                                                   bool is_last_obj) -> void {
  logger_.Log(nuraft_log_level::TRACE,
              fmt::format("Save logical snapshot object, obj_id={}, is_first_obj={}, is_last_obj={}", obj_id,
                          is_first_obj, is_last_obj));

  ptr<buffer> const snp_buf = snapshot.serialize();
  auto ss = snapshot::deserialize(*snp_buf);
  if (obj_id == 0) {
    CreateSnapshotInternal(ss);
  } else {
    auto cluster_state = CoordinatorClusterState::Deserialize(data);

    auto ll = std::lock_guard{snapshots_lock_};
    auto entry = snapshots_.find(snapshot.get_last_log_idx());
    MG_ASSERT(entry != snapshots_.end());
    auto snapshot_ptr = snapshot::deserialize(*snp_buf);
    if (durability_) {
      nlohmann::json json;
      to_json(json, SnapshotCtx{snapshot_ptr, cluster_state});
      auto const ok =
          durability_->Put(fmt::format("{}{}", kSnapshotIdPrefix, snapshot.get_last_log_idx()), json.dump());
      if (!ok) {
        throw StoreSnapshotToDiskException("Failed to store snapshot to disk.");
      }
    }
    entry->second->snapshot_ = snapshot_ptr;
    entry->second->cluster_state_ = cluster_state;
  }
  obj_id++;
}

auto CoordinatorStateMachine::apply_snapshot(snapshot &s) -> bool {
  auto ll = std::lock_guard{snapshots_lock_};
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Apply snapshot, last_log_idx={}", s.get_last_log_idx()));

  auto entry = snapshots_.find(s.get_last_log_idx());
  if (entry == snapshots_.end()) return false;
  if (durability_) {
    if (!durability_->Get(fmt::format("{}{}", kSnapshotIdPrefix, s.get_last_log_idx())).has_value()) {
      throw NoSnapshotOnDiskException("Failed to retrieve snapshot with id {} from disk.", s.get_last_log_idx());
    }
  }

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
  ptr<snapshot> const ss = snapshot::deserialize(*snp_buf);
  CreateSnapshotInternal(ss);

  ptr<std::exception> except(nullptr);
  bool ret = true;
  when_done(ret, except);
}

auto CoordinatorStateMachine::CreateSnapshotInternal(ptr<snapshot> const &snapshot) -> void {
  auto ll = std::lock_guard{snapshots_lock_};
  logger_.Log(nuraft_log_level::TRACE,
              fmt::format("Create snapshot internal, last_log_idx={}", snapshot->get_last_log_idx()));

  auto ctx = cs_new<SnapshotCtx>(snapshot, cluster_state_);
  if (durability_) {
    nlohmann::json json;
    to_json(json, *ctx);
    auto const ok = durability_->Put(fmt::format("{}{}", kSnapshotIdPrefix, snapshot->get_last_log_idx()), json.dump());
    if (!ok) {
      throw StoreSnapshotToDiskException("Failed to store snapshot to disk.");
    }
  }
  snapshots_[snapshot->get_last_log_idx()] = ctx;

  while (snapshots_.size() > MAX_SNAPSHOTS) {
    auto snapshot_current = snapshots_.begin()->first;
    if (durability_) {
      auto const ok = durability_->Delete("snapshot_id_" + std::to_string(snapshot_current));
      if (!ok) {
        throw DeleteSnapshotFromDiskException("Failed to delete snapshot with id {} from disk.", snapshot_current);
      }
    }
    snapshots_.erase(snapshots_.begin());
  }
}

auto CoordinatorStateMachine::GetReplicationInstances() const -> std::vector<ReplicationInstanceState> {
  return cluster_state_.GetAllReplicationInstances();
}

auto CoordinatorStateMachine::GetCurrentMainUUID() const -> utils::UUID { return cluster_state_.GetCurrentMainUUID(); }

auto CoordinatorStateMachine::IsCurrentMain(std::string_view instance_name) const -> bool {
  return cluster_state_.IsCurrentMain(instance_name);
}

auto CoordinatorStateMachine::GetInstanceUUID(std::string_view instance_name) const -> utils::UUID {
  return cluster_state_.GetInstanceUUID(instance_name);
}

auto CoordinatorStateMachine::IsLockOpened() const -> bool { return cluster_state_.GetIsLockOpened(); }

auto CoordinatorStateMachine::TryGetCurrentMainName() const -> std::optional<std::string> {
  return cluster_state_.TryGetCurrentMainName();
}

}  // namespace memgraph::coordination
#endif
