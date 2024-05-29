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
#include "utils.hpp"
#include "utils/logging.hpp"

#include <regex>

namespace {
constexpr std::string_view kCoordClusterState = "coord_cluster_state";
constexpr std::string_view kLastLogIdx = "last_log_idx";
constexpr std::string_view kLastLogTerm = "last_log_term";
constexpr std::string_view kSize = "size";
constexpr std::string_view kLastConfig = "last_config";
constexpr std::string_view kType = "type";
constexpr std::string_view kSnapshotIdPrefix = "snapshot_id_";
constexpr int MAX_SNAPSHOTS = 3;

nuraft::snapshot::type GetSnapshotType(int val_type) {
  switch (val_type) {
    case 1:
      return nuraft::snapshot::raw_binary;
    case 2:
      return nuraft::snapshot::logical_object;
  }
  LOG_FATAL("Val type unknown");
}

int FromSnapshotType(nuraft::snapshot::type val_type) {
  switch (val_type) {
    case nuraft::snapshot::raw_binary:
      return 1;
    case nuraft::snapshot::logical_object:
      return 2;
  }
  LOG_FATAL("Unknown snapshot type");
}

}  // namespace

namespace memgraph::coordination {

auto CoordinatorStateMachine::DeserializeSnapshotCtxFromDisk(const std::string &snapshot_id,
                                                             memgraph::kvstore::KVStore &kv_store) -> ptr<SnapshotCtx> {
  // Retrieve the JSON string from the key-value store
  auto snapshot_ctx_str = kv_store.Get(snapshot_id);
  if (!snapshot_ctx_str.has_value()) {
    throw std::runtime_error("Failed to retrieve snapshot context from disk");
  }

  // Parse the JSON string into a JSON object
  auto snapshot_ctx_json = nlohmann::json::parse(snapshot_ctx_str.value());

  // Extract each field from the JSON object
  auto data_str = snapshot_ctx_json.at(kCoordClusterState.data()).get<std::string>();
  spdlog::trace("parsed data_str {}", data_str);
  auto last_log_idx = snapshot_ctx_json.at(kLastLogIdx.data()).get<uint64_t>();
  spdlog::trace("parsed last_log_idx: {}", last_log_idx);
  auto last_log_term = snapshot_ctx_json.at(kLastLogTerm.data()).get<uint64_t>();
  spdlog::trace("parsed last_log_term: {}", last_log_term);
  auto size = snapshot_ctx_json.at(kSize.data()).get<uint64_t>();
  spdlog::trace("parsed size: {}", size);
  auto last_config_json = snapshot_ctx_json.at(kLastConfig.data()).get<std::string>();
  spdlog::trace("parsed last_config_json: {}", last_config_json);
  auto type = GetSnapshotType(snapshot_ctx_json.at(kType.data()).get<int>());
  spdlog::trace("parsed type");

  // Deserialize the last_config and data
  auto last_config = memgraph::coordination::DeserializeClusterConfig(nlohmann::json::parse(last_config_json));
  spdlog::trace("deserialized cluster config");
  ptr<buffer> data = buffer::alloc(sizeof(uint32_t) + data_str.size());
  buffer_serializer bs(data);
  bs.put_str(data_str);
  // Create and return the snapshot context
  auto deserialized_snapshot = cs_new<snapshot>(last_log_idx, last_log_term, last_config, size, type);

  auto cluster_state = CoordinatorClusterState::Deserialize(*data);
  spdlog::trace("Deserialized cluster state from disk");
  return cs_new<SnapshotCtx>(deserialized_snapshot, cluster_state);
}


CoordinatorStateMachine::CoordinatorStateMachine(LoggerWrapper logger, std::optional<std::filesystem::path> durability_dir): logger_(logger) {
  if (durability_dir) {
    kv_store_ = std::make_unique<kvstore::KVStore>(durability_dir.value());
  }

  if (kv_store_) {
    for (auto kv_store_snapshot_it = kv_store_->begin(std::string{kSnapshotIdPrefix});
         kv_store_snapshot_it != kv_store_->end(std::string{kSnapshotIdPrefix}); ++kv_store_snapshot_it) {
      auto &[snapshot_key_id, snapshot_key_value] = *kv_store_snapshot_it;
      try {
        auto parsed_snapshot_id =
            std::stoul(std::regex_replace(snapshot_key_id, std::regex{kSnapshotIdPrefix.data()}, ""));
        snapshots_[parsed_snapshot_id] = DeserializeSnapshotCtxFromDisk(snapshot_key_id, *kv_store_);
        MG_ASSERT(parsed_snapshot_id == snapshots_[parsed_snapshot_id]->snapshot_->get_last_log_idx(),
                  "Parsed snapshot id {} does not match last log index {}", parsed_snapshot_id,
                  snapshots_[parsed_snapshot_id]->snapshot_->get_last_log_idx());
      } catch (std::exception &e) {
        LOG_FATAL("Failed to deserialize snapshot with id: {}. Error: {}", snapshot_key_id, e.what());
      }
      spdlog::trace("Deserialized snapshot with id: {}", snapshot_key_id);
    }
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
  if (kv_store_) {
    MG_ASSERT(kv_store_->Get("snapshot_id_" + std::to_string(s.get_last_log_idx())));
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
    auto json_cluster_state = cluster_state_.SerializeJson();
    nlohmann::json snapshot_ctx_to_disk = {{kCoordClusterState, json_cluster_state.dump()},  // json as str
                                           {kLastLogTerm, snapshot->get_last_log_term()},
                                           {kLastLogIdx, snapshot->get_last_log_idx()},
                                           {kSize, snapshot->size()},
                                           {kLastConfig, SerializeClusterConfig(*snapshot->get_last_config())},
                                           {kType, FromSnapshotType(snapshot->get_type())}};
    spdlog::trace("!!!!SERIALIZED coord cluster state");
    kv_store_->Put(std::string{kSnapshotIdPrefix} + std::to_string(snapshot->get_last_log_idx()),
                   snapshot_ctx_to_disk.dump());
  }

  while (snapshots_.size() > MAX_SNAPSHOTS) {
    auto snapshot_current = snapshots_.begin()->first;
    if (kv_store_) {
      MG_ASSERT(kv_store_->Delete("snapshot_id_" + std::to_string(snapshot_current)),
                "Failed to delete snapshot from disk");
    }
    snapshots_.erase(snapshots_.begin());
  }
  spdlog::trace("!!!DONE CREATING SNAPSHOT INTERNAL");
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
