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

#include "coordination/coordinator_state_machine.hpp"

#include "coordination/constants_log_durability.hpp"
#include "coordination/coordinator_cluster_state.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "coordination/coordinator_state_manager.hpp"
#include "utils/logging.hpp"

#include <regex>

using nuraft::cluster_config;
using nuraft::ptr;
using nuraft::snapshot;

namespace {
constexpr int MAX_SNAPSHOTS = 3;
using namespace std::string_view_literals;
constexpr auto kDataInstances =
    "cluster_state"sv;  // called "cluster_state" because at the beginning data instances were considered cluster state
constexpr auto kCoordInstances = "coordinator_instances"sv;
constexpr auto kUuid = "uuid"sv;
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

  auto const deserialized_snapshot = cs_new<snapshot>(last_log_idx, last_log_term, last_config, size, type);

  CoordinatorClusterState cluster_state;
  from_json(nlohmann::json::parse(cluster_state_json), cluster_state);

  snapshot_ctx = SnapshotCtx{deserialized_snapshot, cluster_state};
}

void to_json(nlohmann::json &j, SnapshotCtx const &snapshot_ctx) {
  nlohmann::json cluster_state_json;
  to_json(cluster_state_json, snapshot_ctx.cluster_state_);

  nlohmann::json last_config_json;
  to_json(last_config_json, *snapshot_ctx.snapshot_->get_last_config());

  j = nlohmann::json{{kCoordClusterState.data(), cluster_state_json.dump()},
                     {kLastLogTerm.data(), snapshot_ctx.snapshot_->get_last_log_term()},
                     {kLastLogIdx.data(), snapshot_ctx.snapshot_->get_last_log_idx()},
                     {kSize.data(), snapshot_ctx.snapshot_->size()},
                     {kLastConfig.data(), last_config_json.dump()},
                     {kType.data(), static_cast<int>((snapshot_ctx.snapshot_->get_type()))}};
}

CoordinatorStateMachine::CoordinatorStateMachine(LoggerWrapper const logger, LogStoreDurability log_store_durability)
    : logger_(logger), durability_(std::move(log_store_durability.durability_store_)) {
  logger_.Log(nuraft_log_level::INFO, "Restoring coordinator state machine with durability.");
  MG_ASSERT(HandleMigration(log_store_durability.stored_log_store_version_),
            "Couldn't handle migration of log store version.");
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
      auto snapshot_ctx = std::make_shared<SnapshotCtx>();
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

// Assumes durability exists
bool CoordinatorStateMachine::HandleMigration(LogStoreVersion stored_version) {
  UpdateStateMachineFromSnapshotDurability();
  if constexpr (kActiveVersion == LogStoreVersion::kV2) {
    if (stored_version == LogStoreVersion::kV1) {
      return durability_->Put(kLastCommitedIdx, std::to_string(last_committed_idx_));
    }
    if (stored_version == LogStoreVersion::kV2) {
      const auto maybe_last_commited_idx = durability_->Get(kLastCommitedIdx);
      if (!maybe_last_commited_idx.has_value()) {
        logger_.Log(
            nuraft_log_level::ERROR,
            fmt::format(
                "Failed to retrieve last committed index from disk, using last committed index from snapshot {}.",
                last_committed_idx_.load()));
        return durability_->Put(kLastCommitedIdx, std::to_string(last_committed_idx_));
      }
      const auto last_committed_idx_value = std::stoul(maybe_last_commited_idx.value());
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

auto CoordinatorStateMachine::CreateLog(nlohmann::json &&log) -> ptr<buffer> {
  auto const log_dump = log.dump();
  ptr<buffer> log_buf = buffer::alloc(sizeof(uint32_t) + log_dump.size());
  buffer_serializer bs(log_buf);
  bs.put_str(log_dump);
  return log_buf;
}

auto CoordinatorStateMachine::SerializeUpdateClusterState(std::vector<DataInstanceContext> data_instances,
                                                          std::vector<CoordinatorInstanceContext> coordinator_instances,
                                                          utils::UUID uuid) -> ptr<buffer> {
  return CreateLog({{kDataInstances, data_instances}, {kCoordInstances, coordinator_instances}, {kUuid, uuid}});
}

auto CoordinatorStateMachine::DecodeLog(buffer &data)
    -> std::tuple<std::vector<DataInstanceContext>, std::vector<CoordinatorInstanceContext>, utils::UUID> {
  buffer_serializer bs(data);
  try {
    auto const json = nlohmann::json::parse(bs.get_str());
    auto const data_instances = json.at(kDataInstances.data());
    auto const uuid = json.at(kUuid.data());
    auto const coordinator_instances = json.at(kCoordInstances.data());
    return std::make_tuple(data_instances.get<std::vector<DataInstanceContext>>(),
                           coordinator_instances.get<std::vector<CoordinatorInstanceContext>>(),
                           uuid.get<utils::UUID>());
  } catch (std::exception const &e) {
    LOG_FATAL("Error occurred while decoding log {}.", e.what());
  }
}

auto CoordinatorStateMachine::pre_commit(ulong const /*log_idx*/, buffer & /*data*/) -> ptr<buffer> { return nullptr; }

auto CoordinatorStateMachine::commit(ulong const log_idx, buffer &data) -> ptr<buffer> {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Commit: log_idx={}, data.size()={}", log_idx, data.size()));
  auto [data_instances, coordinator_instances, main_uuid] = DecodeLog(data);
  cluster_state_.DoAction(std::move(data_instances), std::move(coordinator_instances), main_uuid);
  durability_->Put(kLastCommitedIdx, std::to_string(log_idx));
  last_committed_idx_ = log_idx;
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Last commit index: {}", last_committed_idx_));
  ptr<buffer> ret = buffer::alloc(sizeof(log_idx));
  buffer_serializer bs_ret(ret);
  bs_ret.put_u64(log_idx);
  return ret;
}

auto CoordinatorStateMachine::commit_config(ulong const log_idx, ptr<cluster_config> & /*new_conf*/) -> void {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Commit config: log_idx={}", log_idx));
  durability_->Put(kLastCommitedIdx, std::to_string(log_idx));
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
    auto const entry = snapshots_.find(snapshot.get_last_log_idx());
    MG_ASSERT(entry != snapshots_.end());
    auto const snapshot_ptr = snapshot::deserialize(*snp_buf);
    nlohmann::json json;
    to_json(json, SnapshotCtx{snapshot_ptr, cluster_state});
    auto const ok = durability_->Put(fmt::format("{}{}", kSnapshotIdPrefix, snapshot.get_last_log_idx()), json.dump());
    if (!ok) {
      throw StoreSnapshotToDiskException("Failed to store snapshot to disk.");
    }
    entry->second->snapshot_ = snapshot_ptr;
    entry->second->cluster_state_ = cluster_state;
  }
  obj_id++;
}

auto CoordinatorStateMachine::apply_snapshot(snapshot &s) -> bool {
  auto ll = std::lock_guard{snapshots_lock_};
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Apply snapshot, last_log_idx={}", s.get_last_log_idx()));

  auto const entry = snapshots_.find(s.get_last_log_idx());
  if (entry == snapshots_.end()) return false;
  if (!durability_->Get(fmt::format("{}{}", kSnapshotIdPrefix, s.get_last_log_idx())).has_value()) {
    throw NoSnapshotOnDiskException("Failed to retrieve snapshot with id {} from disk.", s.get_last_log_idx());
  }

  cluster_state_ = entry->second->cluster_state_;
  return true;
}

auto CoordinatorStateMachine::free_user_snp_ctx(void *&user_snp_ctx) -> void {}

auto CoordinatorStateMachine::last_snapshot() -> ptr<snapshot> {
  auto ll = std::lock_guard{snapshots_lock_};
  logger_.Log(nuraft_log_level::TRACE, "Getting last snapshot from state machine.");
  auto const entry = snapshots_.rbegin();
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

  auto const ctx = cs_new<SnapshotCtx>(snapshot, cluster_state_);
  nlohmann::json json;
  to_json(json, *ctx);
  if (auto const ok =
          durability_->Put(fmt::format("{}{}", kSnapshotIdPrefix, snapshot->get_last_log_idx()), json.dump());
      !ok) {
    throw StoreSnapshotToDiskException("Failed to store snapshot to disk.");
  }
  snapshots_[snapshot->get_last_log_idx()] = ctx;

  while (snapshots_.size() > MAX_SNAPSHOTS) {
    auto snapshot_current = snapshots_.begin()->first;
    if (auto const ok = durability_->Delete("snapshot_id_" + std::to_string(snapshot_current)); !ok) {
      throw DeleteSnapshotFromDiskException("Failed to delete snapshot with id {} from disk.", snapshot_current);
    }
    snapshots_.erase(snapshots_.begin());
  }
}

auto CoordinatorStateMachine::GetDataInstancesContext() const -> std::vector<DataInstanceContext> {
  return cluster_state_.GetDataInstancesContext();
}

auto CoordinatorStateMachine::GetCoordinatorInstancesContext() const -> std::vector<CoordinatorInstanceContext> {
  return cluster_state_.GetCoordinatorInstancesContext();
}

auto CoordinatorStateMachine::GetCurrentMainUUID() const -> utils::UUID { return cluster_state_.GetCurrentMainUUID(); }

auto CoordinatorStateMachine::IsCurrentMain(std::string_view instance_name) const -> bool {
  return cluster_state_.IsCurrentMain(instance_name);
}

auto CoordinatorStateMachine::TryGetCurrentMainName() const -> std::optional<std::string> {
  return cluster_state_.TryGetCurrentMainName();
}
}  // namespace memgraph::coordination
#endif
