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

#include "dbms/inmemory/replication_handlers.hpp"

#include "dbms/dbms_handler.hpp"
#include "replication/replication_server.hpp"
#include "rpc/utils.hpp"  // Include after all SLK definitions are present
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/schema_info.hpp"
#include "utils/observer.hpp"

#include <spdlog/spdlog.h>
#include <cstdint>
#include <optional>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/transform.hpp>

#include "storage/v2/durability/paths.hpp"

using memgraph::storage::Delta;
using memgraph::storage::EdgeAccessor;
using memgraph::storage::EdgeRef;
using memgraph::storage::LabelIndexStats;
using memgraph::storage::LabelPropertyIndexStats;
using memgraph::storage::PropertyId;
using memgraph::storage::UniqueConstraints;
using memgraph::storage::View;
using memgraph::storage::durability::WalDeltaData;
using namespace std::chrono_literals;

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::dbms {

class SnapshotObserver final : public utils::Observer<void> {
 public:
  explicit SnapshotObserver(slk::Builder *res_builder) : res_builder_(res_builder) {}
  void Update() override {
    auto guard = std::lock_guard{mtx_};
    rpc::SendInProgressMsg(res_builder_);
  }

 private:
  slk::Builder *res_builder_;
  // Mutex is needed because RPC execution could be concurrent
  mutable std::mutex mtx_;
};

namespace {

constexpr auto kWaitForMainLockTimeout = 30s;

auto GenerateOldDir() -> std::string {
  return ".old_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
}

void RemoveDirIfEmpty(std::filesystem::path const &dir) {
  // Exception suppression
  std::error_code ec{};
  std::filesystem::remove(dir, ec);
}

// Move files to backup_dir
void MoveFiles(auto const &files, std::filesystem::path const &backup_dir, utils::FileRetainer *file_retainer) {
  for (auto const &old_path : files) {
    auto const new_path = backup_dir / old_path.filename();
    spdlog::trace("Moving file {} to {}", old_path, new_path);
    file_retainer->RenameFile(old_path, new_path);
  }
}

// Move snapshots and WALs
void MoveDurabilityFiles(std::vector<storage::durability::SnapshotDurabilityInfo> const &snapshot_files,
                         std::filesystem::path const &backup_snapshot_dir,
                         std::vector<storage::durability::WalDurabilityInfo> const &wal_files,
                         std::filesystem::path const &backup_wal_dir, utils::FileRetainer *file_retainer) {
  auto const get_path = [](auto const &durability_info) { return durability_info.path; };
  // Move snapshots
  auto const snapshots_to_move = snapshot_files | rv::transform(get_path) | r::to_vector;
  MoveFiles(snapshots_to_move, backup_snapshot_dir, file_retainer);
  // Move WAL files
  auto const wal_files_to_move = wal_files | rv::transform(get_path) | r::to_vector;
  MoveFiles(wal_files_to_move, backup_wal_dir, file_retainer);
  // Clean DIR
  RemoveDirIfEmpty(backup_snapshot_dir);
  RemoveDirIfEmpty(backup_wal_dir);
}

// Read durability files into optional arguments
auto ReadDurabilityFiles(
    std::optional<std::vector<storage::durability::SnapshotDurabilityInfo>> &maybe_old_snapshot_files,
    std::filesystem::path const &current_snapshot_dir,
    std::optional<std::vector<storage::durability::WalDurabilityInfo>> &maybe_old_wal_files,
    std::filesystem::path const &current_wal_dir) -> bool {
  auto maybe_wal_files = storage::durability::GetWalFiles(current_wal_dir);
  // If there are 0 WAL files, replica will be recovered.
  if (!maybe_wal_files.has_value()) {
    spdlog::warn("Failed to read current WAL files. Replica won't be recovered.");
    return false;
  }
  maybe_old_wal_files.emplace(std::move(*maybe_wal_files));
  // Read all snapshot files
  maybe_old_snapshot_files.emplace(storage::durability::GetSnapshotFiles(current_snapshot_dir));
  return true;
}

struct BackupDirectories {
  std::filesystem::path backup_snapshot_dir;
  std::filesystem::path backup_wal_dir;
};

auto CreateBackupDir(std::filesystem::path const &backup_dir) -> bool {
  std::error_code ec{};
  // Won't fail if directory already exists
  std::filesystem::create_directory(backup_dir, ec);
  if (ec) {
    spdlog::error("Failed to create backup directory {}.", backup_dir);
    return false;
  }
  return true;
}

auto CreateBackupDirectories(std::filesystem::path const &current_snapshot_dir,
                             std::filesystem::path const &current_wal_dir) -> std::optional<BackupDirectories> {
  auto const backup_subdir = GenerateOldDir();
  auto backup_snapshot_dir = current_snapshot_dir / backup_subdir;
  if (!CreateBackupDir(backup_snapshot_dir)) {
    spdlog::error("Failed to create the backup directory for snapshots. Replica won't be recovered.");
    return std::nullopt;
  }

  auto backup_wal_dir = current_wal_dir / backup_subdir;
  if (!CreateBackupDir(backup_wal_dir)) {
    spdlog::error("Failed to create the backup directory for WALs. Replica won't be recovered.");
    return std::nullopt;
  }

  return BackupDirectories{.backup_snapshot_dir = std::move(backup_snapshot_dir),
                           .backup_wal_dir = std::move(backup_wal_dir)};
}

constexpr uint32_t kDeltasBatchProgressSize = 100000;

std::pair<uint64_t, WalDeltaData> ReadDelta(storage::durability::BaseDecoder *decoder, const uint64_t version) {
  try {
    auto timestamp = ReadWalDeltaHeader(decoder);
    spdlog::trace("       Timestamp {}", timestamp);
    auto delta = ReadWalDeltaData(decoder, version);
    return {timestamp, delta};
  } catch (const slk::SlkReaderException &) {
    throw utils::BasicException("Missing data!");
  } catch (const storage::durability::RecoveryFailure &) {
    throw utils::BasicException("Invalid data!");
  }
}

std::optional<DatabaseAccess> GetDatabaseAccessor(dbms::DbmsHandler *dbms_handler, const utils::UUID &uuid) {
  try {
#ifdef MG_ENTERPRISE
    auto acc = dbms_handler->Get(uuid);
    if (!acc) {
      spdlog::error("Failed to get access to UUID ", std::string{uuid});
      return std::nullopt;
    }
#else
    auto acc = dbms_handler->Get();
    if (!acc) {
      spdlog::warn("Failed to get access to the default db.");
      return std::nullopt;
    }
#endif
    auto const *inmem_storage = static_cast<storage::InMemoryStorage *>(acc.get()->storage());
    if (!inmem_storage || inmem_storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      spdlog::error("Database is not IN_MEMORY_TRANSACTIONAL.");
      return std::nullopt;
    }
    return std::optional{std::move(acc)};
  } catch (const dbms::UnknownDatabaseException &) {
    spdlog::warn("No database with UUID \"{}\" on replica!", std::string{uuid});
    return std::nullopt;
  }
}

void LogWrongMain(const std::optional<utils::UUID> &current_main_uuid, const utils::UUID &main_req_id,
                  std::string_view rpc_req) {
  spdlog::error("Received {} with main_id: {} != current_main_uuid: {}", rpc_req, std::string(main_req_id),
                current_main_uuid.has_value() ? std::string(current_main_uuid.value()) : "");
}
}  // namespace

std::unique_ptr<storage::InMemoryStorage::ReplicationAccessor> InMemoryReplicationHandlers::cached_commit_accessor_;

void InMemoryReplicationHandlers::Register(dbms::DbmsHandler *dbms_handler, replication::RoleReplicaData &data) {
  auto &server = *data.server;
  server.rpc_server_.Register<storage::replication::HeartbeatRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::HeartbeatHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::PrepareCommitRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::PrepareCommitHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::FinalizeCommitRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::FinalizeCommitHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::SnapshotRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::SnapshotHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::WalFilesRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::WalFilesHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::CurrentWalRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::CurrentWalHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<replication_coordination_glue::SwapMainUUIDRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::SwapMainUUIDHandler(dbms_handler, data, req_reader, res_builder);
      });
}

void InMemoryReplicationHandlers::SwapMainUUIDHandler(dbms::DbmsHandler *dbms_handler,
                                                      replication::RoleReplicaData &role_replica_data,
                                                      slk::Reader *req_reader, slk::Builder *res_builder) {
  auto replica_state = dbms_handler->ReplicationState();
  if (!replica_state->IsReplica()) {
    spdlog::error("Setting main uuid must be performed on replica.");
    rpc::SendFinalResponse(replication_coordination_glue::SwapMainUUIDRes{false}, res_builder);
    return;
  }

  replication_coordination_glue::SwapMainUUIDReq req;
  slk::Load(&req, req_reader);
  spdlog::info("Set replica data UUID to main uuid {}", std::string(req.uuid));
  replica_state->TryPersistRoleReplica(role_replica_data.config, req.uuid);
  role_replica_data.uuid_ = req.uuid;

  rpc::SendFinalResponse(replication_coordination_glue::SwapMainUUIDRes{true}, res_builder);
}

void InMemoryReplicationHandlers::HeartbeatHandler(dbms::DbmsHandler *dbms_handler,
                                                   const std::optional<utils::UUID> &current_main_uuid,
                                                   slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::HeartbeatReq req;
  slk::Load(&req, req_reader);
  auto const db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);

  if (!current_main_uuid.has_value() || req.main_uuid != *current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::HeartbeatReq::kType.name);
    const storage::replication::HeartbeatRes res{false, 0, ""};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }
  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  if (!db_acc.has_value()) {
    spdlog::warn("No database accessor");
    storage::replication::HeartbeatRes const res{false, 0, ""};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }
  // Move db acc
  auto const *storage = db_acc->get()->storage();
  const storage::replication::HeartbeatRes res{
      true, storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire),
      std::string{storage->repl_storage_state_.epoch_.id()}};
  rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));
}

void InMemoryReplicationHandlers::PrepareCommitHandler(dbms::DbmsHandler *dbms_handler,
                                                       const std::optional<utils::UUID> &current_main_uuid,
                                                       slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::PrepareCommitReq req;
  slk::Load(&req, req_reader);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::PrepareCommitReq::kType.name);
    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  auto db_acc = GetDatabaseAccessor(dbms_handler, req.storage_uuid);
  if (!db_acc) {
    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  spdlog::info("Preparing for commit for db {}", db_acc->get()->name());

  // Read at the beginning so that SLK stream gets cleared even when the request is invalid
  storage::replication::Decoder decoder(req_reader);
  auto maybe_epoch_id = decoder.ReadString();
  if (!maybe_epoch_id) {
    spdlog::error("Invalid replication message, couldn't read epoch id.");
    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  auto &repl_storage_state = storage->repl_storage_state_;
  if (*maybe_epoch_id != repl_storage_state.epoch_.id()) {
    auto prev_epoch = repl_storage_state.epoch_.SetEpoch(*maybe_epoch_id);
    spdlog::trace("Set epoch to {} for db {}", *maybe_epoch_id, storage->name());
    repl_storage_state.AddEpochToHistoryForce(prev_epoch);
  }

  // We do not care about incoming sequence numbers, after a snapshot recovery, the sequence number is 0
  // This is because the snapshots completely wipes the storage and durability
  // It is also the first recovery step, so the WAL chain needs to restart from 0, otherwise the instance won't be
  // able to recover from durable data
  if (storage->wal_file_) {
    if (*maybe_epoch_id != repl_storage_state.epoch_.id()) {
      storage->wal_file_->FinalizeWal();
      storage->wal_file_.reset();
      spdlog::trace("Current WAL file finalized successfully for db {}.", storage->name());
    }
  }

  // last_durable_timestamp could be set by snapshot; so we cannot guarantee exactly what's the previous timestamp
  // TODO: (andi) Not sure if emptying the stream is needed? PR remove-emptying-stream, rebase on it
  if (req.previous_commit_timestamp > repl_storage_state.last_durable_timestamp_.load(std::memory_order_acquire)) {
    // Empty the stream
    bool transaction_complete{false};
    while (!transaction_complete) {
      spdlog::info("Skipping delta");
      const auto [_, delta] = ReadDelta(&decoder, storage::durability::kVersion);
      transaction_complete = IsWalDeltaDataTransactionEnd(delta, storage::durability::kVersion);
    }

    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));
    return;
  }

  spdlog::info("Commit immediately: {}", req.commit_immediately);

  try {
    auto deltas_res = ReadAndApplyDeltasSingleTxn(storage, &decoder, storage::durability::kVersion, res_builder,
                                                  req.commit_immediately);
    cached_commit_accessor_ = std::move(deltas_res.commit_acc);
  } catch (const utils::BasicException &e) {
    spdlog::error(
        "Error occurred while trying to apply deltas because of {}. Replication recovery from append deltas finished "
        "unsuccessfully.",
        e.what());
    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));
    return;
  }

  const storage::replication::PrepareCommitRes res{true};
  rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));
}

void InMemoryReplicationHandlers::FinalizeCommitHandler(dbms::DbmsHandler *dbms_handler,
                                                        const std::optional<utils::UUID> &current_main_uuid,
                                                        slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::FinalizeCommitReq req;
  slk::Load(&req, req_reader);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::FinalizeCommitReq::kType.name);
    storage::replication::FinalizeCommitRes const res(false);
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  auto db_acc = GetDatabaseAccessor(dbms_handler, req.storage_uuid);
  if (!db_acc) {
    storage::replication::FinalizeCommitRes const res(false);
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  spdlog::info("Finalizing commit for db {}", db_acc->get()->name());

  MG_ASSERT(cached_commit_accessor_ != nullptr, "Cached commit accessor became invalid between two phases");

  cached_commit_accessor_->FinalizeCommitPhase(req.durability_commit_timestamp);
  cached_commit_accessor_.reset();
  storage::replication::FinalizeCommitRes const res(true);
  rpc::SendFinalResponse(res, res_builder);
}

// The semantic of snapshot handler is the following: Either handling snapshot request passes or it doesn't. If it
// passes we return the current commit timestamp of the replica. If it doesn't pass, we return optional which will
// signal to the caller that it shouldn't update the commit timestamp value.
void InMemoryReplicationHandlers::SnapshotHandler(DbmsHandler *dbms_handler,
                                                  const std::optional<utils::UUID> &current_main_uuid,
                                                  slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::SnapshotReq req;
  Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.storage_uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in snapshot handler for request with storage_uuid {}",
                  std::string{req.storage_uuid});
    rpc::SendFinalResponse(storage::replication::SnapshotRes{}, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::SnapshotReq::kType.name);
    rpc::SendFinalResponse(storage::replication::SnapshotRes{}, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  // Backup dir
  auto const current_snapshot_dir = storage->recovery_.snapshot_directory_;
  auto const current_wal_directory = storage->recovery_.wal_directory_;

  if (!utils::EnsureDir(current_snapshot_dir)) {
    spdlog::error("Couldn't get access to the current snapshot directory. Recovery won't be done.");
    rpc::SendFinalResponse(storage::replication::SnapshotRes{}, res_builder);
    return;
  }

  auto const maybe_backup_dirs = CreateBackupDirectories(current_snapshot_dir, current_wal_directory);
  if (!maybe_backup_dirs.has_value()) {
    spdlog::error("Couldn't create backup directories. Replica won't be recovered.");
    const storage::replication::SnapshotRes res{{}};
    rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));
    return;
  }
  auto const &[backup_snapshot_dir, backup_wal_dir] = *maybe_backup_dirs;

  // Read durability files
  auto const curr_snapshot_files = storage::durability::GetSnapshotFiles(current_snapshot_dir);
  auto const maybe_curr_wal_files = storage::durability::GetWalFiles(current_wal_directory);
  // If there are 0 WAL files, replica will be recovered.
  if (!maybe_curr_wal_files.has_value()) {
    spdlog::error("Cannot read current WAL files. Replica won't be recovered.");
    rpc::SendFinalResponse(storage::replication::SnapshotRes{}, res_builder, fmt::format("db: {}", storage->name()));
    return;
  }
  auto const &curr_wal_files = *maybe_curr_wal_files;

  storage::replication::Decoder decoder(req_reader);
  const auto maybe_recovery_snapshot_path = decoder.ReadFile(current_snapshot_dir);
  if (!maybe_recovery_snapshot_path.has_value()) {
    spdlog::error("Failed to load snapshot from {}", current_snapshot_dir);
    rpc::SendFinalResponse(storage::replication::SnapshotRes{}, res_builder, fmt::format("db: {}", storage->name()));
    return;
  }
  auto const &recovery_snapshot_path = *maybe_recovery_snapshot_path;

  spdlog::info("Received snapshot saved to {}", recovery_snapshot_path);
  {
    auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
    if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
      spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
      rpc::SendFinalResponse(storage::replication::SnapshotRes{}, res_builder, fmt::format("db: {}", storage->name()));
      return;
    }

    spdlog::trace("Clearing database {} before recovering from snapshot.", storage->name());

    // Clear the database
    storage->Clear();

    std::optional<storage::SnapshotObserverInfo> snapshot_observer_info;
    auto snapshot_progress_observer = std::make_shared<SnapshotObserver>(res_builder);
    snapshot_observer_info.emplace(std::move(snapshot_progress_observer), 1'000'000);

    try {
      spdlog::debug("Loading snapshot for db {}.", storage->name());
      auto [snapshot_info, recovery_info, indices_constraints] = storage::durability::LoadSnapshot(
          recovery_snapshot_path, &storage->vertices_, &storage->edges_, &storage->edges_metadata_,
          &storage->repl_storage_state_.history, storage->name_id_mapper_.get(), &storage->edge_count_,
          storage->config_, &storage->enum_store_,
          storage->config_.salient.items.enable_schema_info ? &storage->schema_info_.Get() : nullptr,
          snapshot_observer_info);
      // If this step is present it should always be the first step of
      // the recovery so we use the UUID we read from snapshot
      storage->uuid().set(snapshot_info.uuid);
      spdlog::trace("Set epoch to {} for db {}", snapshot_info.epoch_id, storage->name());
      storage->repl_storage_state_.epoch_.SetEpoch(std::move(snapshot_info.epoch_id));
      storage->vertex_id_ = recovery_info.next_vertex_id;
      storage->edge_id_ = recovery_info.next_edge_id;
      storage->timestamp_ = std::max(storage->timestamp_, recovery_info.next_timestamp);
      storage->repl_storage_state_.last_durable_timestamp_.store(snapshot_info.durable_timestamp,
                                                                 std::memory_order_release);
      // We are the only active transaction, so mark everything up to the next timestamp
      if (storage->timestamp_ > 0) storage->commit_log_->MarkFinishedInRange(0, storage->timestamp_ - 1);

      RecoverIndicesStatsAndConstraints(&storage->vertices_, storage->name_id_mapper_.get(), &storage->indices_,
                                        &storage->constraints_, storage->config_, recovery_info, indices_constraints,
                                        storage->config_.salient.items.properties_on_edges, snapshot_observer_info);
    } catch (const storage::durability::RecoveryFailure &e) {
      spdlog::error(
          "Couldn't load the snapshot from {} because of: {}. Storage will be cleared. Snapshot and WAL files are "
          "preserved so you can restore your data by restarting instance.",
          *maybe_recovery_snapshot_path, e.what());
      storage->Clear();
      const storage::replication::SnapshotRes res{{}};
      rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));
      return;
    }
  }
  spdlog::debug("Snapshot from {} loaded successfully.", *maybe_recovery_snapshot_path);

  const storage::replication::SnapshotRes res{
      storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire)};
  rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));

  auto const not_recovery_snapshot = [&recovery_snapshot_path](auto const &snapshot_info) {
    return snapshot_info.path != recovery_snapshot_path;
  };

  auto snapshots_to_move = curr_snapshot_files | rv::filter(not_recovery_snapshot) | r::to_vector;

  MoveDurabilityFiles(snapshots_to_move, backup_snapshot_dir, curr_wal_files, backup_wal_dir,
                      &(storage->file_retainer_));

  spdlog::debug("Replication recovery from snapshot finished!");
}

// Commit timestamp on main's side shouldn't be updated if:
// 1.) the database accessor couldn't be obtained
// 2.) UUID sent with the request is not the current MAIN's UUID which replica is listening to
// send after that CurrentWalHandler
// If loading all WAL files succeeded then main can continue recovery if it should send more recovery steps.
// If loading one of WAL files partially succeeded, then recovery cannot be continue but commit timestamp can be
// obtained.
void InMemoryReplicationHandlers::WalFilesHandler(dbms::DbmsHandler *dbms_handler,
                                                  const std::optional<utils::UUID> &current_main_uuid,
                                                  slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::WalFilesReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in wal files handler for request storage_uuid {}",
                  std::string{req.uuid});
    const storage::replication::WalFilesRes res{{}};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::WalFilesReq::kType.name);
    rpc::SendFinalResponse(storage::replication::WalFilesRes{}, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  auto const current_snapshot_dir = storage->recovery_.snapshot_directory_;
  auto const current_wal_directory = storage->recovery_.wal_directory_;

  if (!utils::EnsureDir(current_wal_directory)) {
    spdlog::error("Couldn't get access to the current wal directory. Recovery won't be done.");
    rpc::SendFinalResponse(storage::replication::WalFilesRes{}, res_builder);
    return;
  }

  auto const maybe_backup_dirs = CreateBackupDirectories(current_snapshot_dir, current_wal_directory);
  if (!maybe_backup_dirs.has_value()) {
    spdlog::error("Couldn't create backup directories. Replica won't be recovered.");
    rpc::SendFinalResponse(storage::replication::WalFilesRes{}, res_builder, storage->name());
    return;
  }
  auto const &[backup_snapshot_dir, backup_wal_dir] = *maybe_backup_dirs;

  std::optional<std::vector<storage::durability::SnapshotDurabilityInfo>> maybe_old_snapshot_files;
  std::optional<std::vector<storage::durability::WalDurabilityInfo>> maybe_old_wal_files;

  if (req.reset_needed) {
    {
      auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
      if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
        spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
        rpc::SendFinalResponse(storage::replication::WalFilesRes{}, res_builder, storage->name());
        return;
      }

      spdlog::trace("Clearing replica storage for db {} because the reset is needed while recovering from WalFiles.",
                    storage->name());
      storage->Clear();
    }
    if (!ReadDurabilityFiles(maybe_old_snapshot_files, current_snapshot_dir, maybe_old_wal_files,
                             current_wal_directory)) {
      rpc::SendFinalResponse(storage::replication::WalFilesRes{}, res_builder, fmt::format("db: {}", storage->name()));
      return;
    }
  }

  const auto wal_file_number = req.file_number;
  spdlog::debug("Received {} WAL files.", wal_file_number);
  storage::replication::Decoder decoder(req_reader);

  uint32_t local_batch_counter = 0;
  for (auto i = 0; i < wal_file_number; ++i) {
    auto const [success, current_batch_counter] = LoadWal(storage, &decoder, res_builder, local_batch_counter);
    if (!success) {
      spdlog::debug("Replication recovery from WAL files failed while loading one of WAL files for db {}.",
                    storage->name());
      const storage::replication::WalFilesRes res{{}};
      rpc::SendFinalResponse(res, res_builder);
      return;
    }
    local_batch_counter = current_batch_counter;
  }

  spdlog::debug("Replication recovery from WAL files succeeded for db {}.", storage->name());
  const storage::replication::WalFilesRes res{
      storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire)};
  rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));

  if (req.reset_needed) {
    MoveDurabilityFiles(*maybe_old_snapshot_files, backup_snapshot_dir, *maybe_old_wal_files, backup_wal_dir,
                        &(storage->file_retainer_));
  }
}

// Commit timestamp on MAIN's side shouldn't be updated if:
// 1.) the database accessor couldn't be obtained
// 2.) UUID sent with the request is not the current MAIN's UUID which replica is listening to
// If loading WAL file partially succeeded then we shouldn't continue recovery but commit timestamp can be updated on
// main
void InMemoryReplicationHandlers::CurrentWalHandler(dbms::DbmsHandler *dbms_handler,
                                                    const std::optional<utils::UUID> &current_main_uuid,
                                                    slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::CurrentWalReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in current wal handler for request storage_uuid {}",
                  std::string{req.uuid});
    const storage::replication::CurrentWalRes res{{}};
    rpc::SendFinalResponse(res, res_builder);
    return;
  }

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::CurrentWalReq::kType.name);
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{}, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  auto const current_snapshot_dir = storage->recovery_.snapshot_directory_;
  auto const current_wal_directory = storage->recovery_.wal_directory_;

  if (!utils::EnsureDir(current_wal_directory)) {
    spdlog::error("Couldn't get access to the current wal directory. Recovery won't be done.");
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{}, res_builder);
    return;
  }

  auto const maybe_backup_dirs = CreateBackupDirectories(current_snapshot_dir, current_wal_directory);
  if (!maybe_backup_dirs.has_value()) {
    spdlog::error("Couldn't create backup directories. Replica won't be recovered for db {}.", storage->name());
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{}, res_builder);
    return;
  }
  auto const &[backup_snapshot_dir, backup_wal_dir] = *maybe_backup_dirs;

  std::optional<std::vector<storage::durability::SnapshotDurabilityInfo>> maybe_old_snapshot_files;
  std::optional<std::vector<storage::durability::WalDurabilityInfo>> maybe_old_wal_files;

  if (req.reset_needed) {
    {
      auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
      if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
        spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
        rpc::SendFinalResponse(storage::replication::CurrentWalRes{}, res_builder);
        return;
      }
      spdlog::trace("Clearing replica storage for db {} because the reset is needed while recovering from WalFiles.",
                    storage->name());
      storage->Clear();
    }
    if (!ReadDurabilityFiles(maybe_old_snapshot_files, current_snapshot_dir, maybe_old_wal_files,
                             current_wal_directory)) {
      rpc::SendFinalResponse(storage::replication::CurrentWalRes{}, res_builder,
                             fmt::format("db: {}", storage->name()));
      return;
    }
  }

  storage::replication::Decoder decoder(req_reader);

  // Even if loading wal file failed, we return last_durable_timestamp to the main because it is not a fatal error
  // When loading a single WAL file, we don't care about saving number of deltas
  if (!LoadWal(storage, &decoder, res_builder).first) {
    spdlog::debug(
        "Replication recovery from current WAL didn't end successfully but the error is non-fatal error. DB {}.",
        storage->name());
  } else {
    spdlog::debug("Replication recovery from current WAL ended successfully! DB {}.", storage->name());
  }

  const storage::replication::CurrentWalRes res{
      storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire)};
  rpc::SendFinalResponse(res, res_builder, fmt::format("db: {}", storage->name()));

  if (req.reset_needed) {
    MoveDurabilityFiles(*maybe_old_snapshot_files, backup_snapshot_dir, *maybe_old_wal_files, backup_wal_dir,
                        &(storage->file_retainer_));
  }
}

// The method will return false and hence signal the failure of completely loading the WAL file if:
// 1.) It cannot open WAL file for reading from temporary WAL directory
// 2.) If WAL magic and/or version wasn't loaded successfully
// 3.) If WAL version is invalid
// 4.) If reading WAL info fails
// 5.) If applying some of the deltas failed
// If WAL file doesn't contain any new changes, we ignore it and consider WAL file as successfully applied.
std::pair<bool, uint32_t> InMemoryReplicationHandlers::LoadWal(storage::InMemoryStorage *storage,
                                                               storage::replication::Decoder *decoder,
                                                               slk::Builder *res_builder,
                                                               uint32_t start_batch_counter) {
  const auto temp_wal_directory =
      std::filesystem::temp_directory_path() / "memgraph" / storage::durability::kWalDirectory;

  if (!utils::EnsureDir(temp_wal_directory)) {
    spdlog::error("Couldn't get access to the current tmp directory while loading WAL file.");
    return {false, 0};
  }

  auto maybe_wal_path = decoder->ReadFile(temp_wal_directory);
  if (!maybe_wal_path) {
    spdlog::error("Failed to load WAL file from {}!", temp_wal_directory);
    return {false, 0};
  }
  spdlog::trace("Received WAL saved to {}", *maybe_wal_path);
  try {
    auto wal_info = storage::durability::ReadWalInfo(*maybe_wal_path);

    // We have to check if this is our 1st wal, not what main is sending
    if (storage->wal_seq_num_ == 0) {
      storage->uuid().set(wal_info.uuid);
    }

    // If WAL file doesn't contain any changes that need to be applied, ignore it
    if (wal_info.to_timestamp <= storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire)) {
      spdlog::trace("WAL file won't be applied since all changes already exist.");
      return {true, 0};
    }
    // We trust only WAL files which contain changes we are interested in (newer changes)
    if (auto &replica_epoch = storage->repl_storage_state_.epoch_; wal_info.epoch_id != replica_epoch.id()) {
      spdlog::trace("Set epoch to {} for db {}", wal_info.epoch_id, storage->name());
      auto prev_epoch = replica_epoch.SetEpoch(wal_info.epoch_id);
      storage->repl_storage_state_.AddEpochToHistoryForce(prev_epoch);
    }

    // We do not care about incoming sequence numbers, after a snapshot recovery, the sequence number is 0
    // This is because the snapshots completely wipes the storage and durability
    // It is also the first recovery step, so the WAL chain needs to restart from 0, otherwise the instance won't be
    // able to recover from durable data
    if (storage->wal_file_) {
      storage->wal_file_->FinalizeWal();
      storage->wal_file_.reset();
      spdlog::trace("WAL file {} finalized successfully", *maybe_wal_path);
    }
    spdlog::trace("Loading WAL deltas from {}", *maybe_wal_path);
    storage::durability::Decoder wal_decoder;
    const auto version = wal_decoder.Initialize(*maybe_wal_path, storage::durability::kWalMagic);
    spdlog::debug("WAL file {} loaded successfully", *maybe_wal_path);
    if (!version) {
      spdlog::error("Couldn't read WAL magic and/or version!");
      return {false, 0};
    }
    if (!storage::durability::IsVersionSupported(*version)) {
      spdlog::error("Invalid WAL version!");
      return {false, 0};
    }
    wal_decoder.SetPosition(wal_info.offset_deltas);

    uint32_t local_batch_counter = start_batch_counter;
    for (size_t local_delta_idx = 0; local_delta_idx < wal_info.num_deltas;) {
      // commit_txn_immediately is set true because when loading WAL files, we should commit immediately
      auto const deltas_res =
          ReadAndApplyDeltasSingleTxn(storage, &wal_decoder, *version, res_builder, true, local_batch_counter);
      local_delta_idx += deltas_res.current_delta_idx;
      local_batch_counter = deltas_res.current_batch_counter;
    }

    spdlog::trace("Replication from WAL file {} successful!", *maybe_wal_path);
    return {true, local_batch_counter};
  } catch (const storage::durability::RecoveryFailure &e) {
    spdlog::error("Couldn't recover WAL deltas from {} because of: {}.", *maybe_wal_path, e.what());
    return {false, 0};
  } catch (const utils::BasicException &e) {
    spdlog::error("Loading WAL from {} failed because of {}.", *maybe_wal_path, e.what());
    return {false, 0};
  }
}

// The number of applied deltas also includes skipped deltas.
storage::SingleTxnDeltasProcessingResult InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn(
    storage::InMemoryStorage *storage, storage::durability::BaseDecoder *decoder, const uint64_t version,
    slk::Builder *res_builder, bool const commit_txn_immediately, uint32_t const start_batch_counter) {
  auto edge_acc = storage->edges_.access();
  auto vertex_acc = storage->vertices_.access();

  constexpr auto kSharedAccess = storage::Storage::Accessor::Type::WRITE;
  constexpr auto kUniqueAccess = storage::Storage::Accessor::Type::UNIQUE;

  uint64_t commit_timestamp{0};
  std::unique_ptr<storage::InMemoryStorage::ReplicationAccessor> commit_accessor;

  auto const get_replication_accessor = [storage, &commit_timestamp, &commit_accessor](
                                            uint64_t const local_commit_timestamp,
                                            storage::Storage::Accessor::Type acc_type =
                                                kSharedAccess) -> storage::InMemoryStorage::ReplicationAccessor * {
    if (!commit_accessor) {
      std::unique_ptr<storage::Storage::Accessor> acc = nullptr;
      switch (acc_type) {
        case storage::Storage::Accessor::Type::READ:
          [[fallthrough]];
        case storage::Storage::Accessor::Type::WRITE:
          acc = storage->Access(acc_type);
          break;
        case storage::Storage::Accessor::Type::UNIQUE:
          acc = storage->UniqueAccess();
          break;
        case storage::Storage::Accessor::Type::READ_ONLY:
          acc = storage->ReadOnlyAccess();
          break;
        default:
          throw utils::BasicException("Replica failed to gain storage access! Unknown accessor type.");
      }

      commit_timestamp = local_commit_timestamp;
      commit_accessor.reset(static_cast<storage::InMemoryStorage::ReplicationAccessor *>(acc.release()));

    } else if (commit_timestamp != local_commit_timestamp) {
      throw utils::BasicException("Received more than one transaction!");
    }
    return commit_accessor.get();
  };

  uint64_t current_delta_idx = 0;  // tracks over how many deltas we iterated, includes also skipped deltas.
  uint64_t applied_deltas = 0;     // Non-skipped deltas
  uint32_t current_batch_counter = start_batch_counter;
  auto max_delta_timestamp = storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire);

  auto current_durable_commit_timestamp = max_delta_timestamp;
  spdlog::trace("Current durable commit timestamp: {}", current_durable_commit_timestamp);

  uint64_t prev_printed_timestamp = 0;

  for (bool transaction_complete = false; !transaction_complete; ++current_delta_idx, ++current_batch_counter) {
    if (current_batch_counter == kDeltasBatchProgressSize) {
      spdlog::trace("Sending in progress msg");
      rpc::SendInProgressMsg(res_builder);
      current_batch_counter = 0;
    }

    // End of the stream
    auto const [delta_timestamp, delta] = ReadDelta(decoder, version);
    if (delta_timestamp != prev_printed_timestamp) {
      spdlog::trace("Timestamp: {}", delta_timestamp);
      prev_printed_timestamp = delta_timestamp;
    }

    max_delta_timestamp = std::max(max_delta_timestamp, delta_timestamp);

    transaction_complete = IsWalDeltaDataTransactionEnd(delta, version);

    if (delta_timestamp <= current_durable_commit_timestamp) {
      spdlog::trace("Skipping delta with timestamp: {}", delta_timestamp);
      continue;
    }

    // NOLINTNEXTLINE (google-build-using-namespace)
    using namespace memgraph::storage::durability;
    auto *mapper = storage->name_id_mapper_.get();
    auto delta_apply = utils::Overloaded{
        [&](WalVertexCreate const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("  Delta {}. Create vertex {}", current_delta_idx, gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          if (!transaction->CreateVertexEx(data.gid).has_value()) {
            throw utils::BasicException("Vertex with gid {} already exists at replica.", gid);
          }
        },
        [&](WalVertexDelete const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("  Delta {}. Delete vertex {}", current_delta_idx, gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) {
            throw utils::BasicException("Vertex with gid {} couldn't be found while trying to delete vertex.", gid);
          }
          auto ret = transaction->DeleteVertex(&*vertex);
          if (ret.HasError() || !ret.GetValue()) {
            throw utils::BasicException("Deleting vertex with gid {} failed.", gid);
          }
        },
        [&](WalVertexAddLabel const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("   Delta {}. Vertex {} add label {}", current_delta_idx, gid, data.label);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) {
            throw utils::BasicException("Couldn't find vertex {} when adding label.", gid);
          }
          // NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)
          auto ret = vertex->AddLabel(transaction->NameToLabel(data.label));
          if (ret.HasError() || !ret.GetValue()) {
            throw utils::BasicException("Failed to add label to vertex {}.", gid);
          }
        },
        [&](WalVertexRemoveLabel const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("   Delta {}. Vertex {} remove label {}", current_delta_idx, gid, data.label);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) throw utils::BasicException("Failed to find vertex {} when removing label.", gid);
          // NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)
          auto ret = vertex->RemoveLabel(transaction->NameToLabel(data.label));
          if (ret.HasError() || !ret.GetValue()) {
            throw utils::BasicException("Failed to remove label from vertex {}.", gid);
          }
        },
        [&](WalVertexSetProperty const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("   Delta {}. Vertex {} set property", current_delta_idx, gid);
          // NOLINTNEXTLINE
          auto *transaction = get_replication_accessor(delta_timestamp);
          // NOLINTNEXTLINE
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) {
            throw utils::BasicException("Failed to find vertex {} when setting property.", gid);
          }
          // NOTE: Phase 1 of the text search feature doesn't have replication in scope
          auto ret =
              vertex->SetProperty(transaction->NameToProperty(data.property), ToPropertyValue(data.value, mapper));
          if (ret.HasError()) {
            throw utils::BasicException("Failed to set property label from vertex {}.", gid);
          }
        },
        [&](WalEdgeCreate const &data) {
          auto const edge_gid = data.gid.AsUint();
          auto const from_vertex_gid = data.from_vertex.AsUint();
          auto const to_vertex_gid = data.to_vertex.AsUint();
          spdlog::trace("   Delta {}. Create edge {} of type {} from vertex {} to vertex {}", current_delta_idx,
                        edge_gid, data.edge_type, from_vertex_gid, to_vertex_gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto from_vertex = transaction->FindVertex(data.from_vertex, View::NEW);
          if (!from_vertex) {
            throw utils::BasicException("Failed to find vertex {} when adding edge {}.", from_vertex_gid, edge_gid);
          }
          auto to_vertex = transaction->FindVertex(data.to_vertex, View::NEW);
          if (!to_vertex) {
            throw utils::BasicException("Failed to find vertex {} when adding edge {}.", to_vertex_gid, edge_gid);
          }
          auto edge = transaction->CreateEdgeEx(&*from_vertex, &*to_vertex, transaction->NameToEdgeType(data.edge_type),
                                                data.gid);
          if (edge.HasError()) {
            throw utils::BasicException("Failed to add edge {} between vertices {} and {}.", edge_gid, from_vertex_gid,
                                        to_vertex_gid);
          }
        },
        [&](WalEdgeDelete const &data) {
          auto const edge_gid = data.gid.AsUint();
          auto const from_vertex_gid = data.from_vertex.AsUint();
          auto const to_vertex_gid = data.to_vertex.AsUint();
          spdlog::trace("   Delta {}. Delete edge {} of type {} from vertex {} to vertex {}", current_delta_idx,
                        edge_gid, data.edge_type, from_vertex_gid, to_vertex_gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          auto from_vertex = transaction->FindVertex(data.from_vertex, View::NEW);
          if (!from_vertex) {
            throw utils::BasicException("Failed to find vertex {} when deleting edge {}.", from_vertex_gid, edge_gid);
          }
          auto to_vertex = transaction->FindVertex(data.to_vertex, View::NEW);
          if (!to_vertex) {
            throw utils::BasicException("Failed to find vertex {} when deleting edge {}.", from_vertex_gid, edge_gid);
          }
          auto edgeType = transaction->NameToEdgeType(data.edge_type);
          auto edge = transaction->FindEdge(data.gid, View::NEW, edgeType, &*from_vertex, &*to_vertex);
          if (!edge) {
            throw utils::BasicException("Couldn't find edge {} when deleting edge.", edge_gid);
          }
          if (auto ret = transaction->DeleteEdge(&*edge); ret.HasError()) {
            throw utils::BasicException("Failed to delete edge {} between vertices {} and {}.", edge_gid,
                                        from_vertex_gid, to_vertex_gid);
          }
        },
        [&](WalEdgeSetProperty const &data) {
          auto const edge_gid = data.gid.AsUint();
          spdlog::trace("   Delta {}. Edge {} set property", current_delta_idx, edge_gid);
          if (!storage->config_.salient.items.properties_on_edges)
            throw utils::BasicException(
                "Can't set properties on edges because properties on edges "
                "are disabled!");

          auto *transaction = get_replication_accessor(delta_timestamp);

          // The following block of code effectively implements `FindEdge` and
          // yields an accessor that is only valid for managing the edge's
          // properties.
          auto edge = edge_acc.find(data.gid);
          if (edge == edge_acc.end()) {
            throw utils::BasicException("Failed to find edge {} when setting property.", edge_gid);
          }
          // The edge visibility check must be done here manually because we
          // don't allow direct access to the edges through the public API.
          {
            bool is_visible = true;
            Delta *delta = nullptr;
            {
              auto guard = std::shared_lock{edge->lock};
              is_visible = !edge->deleted;
              delta = edge->delta;
            }
            ApplyDeltasForRead(&transaction->GetTransaction(), delta, View::NEW, [&is_visible](const Delta &delta) {
              switch (delta.action) {
                case Delta::Action::ADD_LABEL:
                case Delta::Action::REMOVE_LABEL:
                case Delta::Action::SET_PROPERTY:
                case Delta::Action::ADD_IN_EDGE:
                case Delta::Action::ADD_OUT_EDGE:
                case Delta::Action::REMOVE_IN_EDGE:
                case Delta::Action::REMOVE_OUT_EDGE:
                  break;
                case Delta::Action::RECREATE_OBJECT: {
                  is_visible = true;
                  break;
                }
                case Delta::Action::DELETE_DESERIALIZED_OBJECT:
                case Delta::Action::DELETE_OBJECT: {
                  is_visible = false;
                  break;
                }
              }
            });
            if (!is_visible) {
              throw utils::BasicException("Edge {} isn't visible when setting property.", edge_gid);
            }
          }

          // Here we create an edge accessor that we will use to get the
          // properties of the edge. The accessor is created with an invalid
          // type and invalid from/to pointers because we don't know them
          // here, but that isn't an issue because we won't use that part of
          // the API here.

          auto [edge_ref, edge_type, from_vertex, vertex_to] = std::invoke([&] {
            if (data.from_gid.has_value()) {
              auto vertex_acc = storage->vertices_.access();
              auto from_vertex = vertex_acc.find(data.from_gid);
              if (from_vertex == vertex_acc.end())
                throw utils::BasicException("Failed to find from vertex {} when setting edge property.",
                                            from_vertex->gid.AsUint());

              auto found_edge = r::find_if(from_vertex->out_edges, [raw_edge_ref = EdgeRef(&*edge)](auto &in) {
                return std::get<2>(in) == raw_edge_ref;
              });
              if (found_edge == from_vertex->out_edges.end()) {
                throw utils::BasicException("Couldn't find edge {} in vertex {}'s out edge collection.", edge_gid,
                                            from_vertex->gid.AsUint());
              }
              const auto &[edge_type, vertex_to, edge_ref] = *found_edge;
              return std::tuple{edge_ref, edge_type, &*from_vertex, vertex_to};
            }
            // fallback if from_gid not available
            auto found_edge = storage->FindEdge(edge->gid);
            if (!found_edge) {
              constexpr auto src_loc{std::source_location()};
              throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", src_loc.file_name(),
                                          src_loc.line());
            }
            const auto &[edge_ref, edge_type, vertex_from, vertex_to] = *found_edge;
            return std::tuple{edge_ref, edge_type, vertex_from, vertex_to};
          });

          auto ea = EdgeAccessor{edge_ref, edge_type, from_vertex, vertex_to, storage, &transaction->GetTransaction()};
          auto ret = ea.SetProperty(transaction->NameToProperty(data.property), ToPropertyValue(data.value, mapper));
          if (ret.HasError()) {
            throw utils::BasicException("Setting property on edge {} failed.", edge_gid);
          }
        },
        [&](WalTransactionStart const &) { spdlog::info("Read WalTransactionStart delta, ignoring it"); },
        [&](WalTransactionEnd const &) {
          spdlog::trace("   Delta {}. Transaction end", current_delta_idx);
          if (!commit_accessor || commit_timestamp != delta_timestamp)
            throw utils::BasicException("Invalid commit data!");
          auto const ret =
              commit_accessor->PrepareForCommitPhase({.desired_commit_timestamp = commit_timestamp, .is_main = false});
          if (ret.HasError()) {
            throw utils::BasicException("Committing failed while trying to prepare for commit on replica.");
          }
          if (commit_txn_immediately) {
            commit_accessor->FinalizeCommitPhase(commit_timestamp);
            commit_accessor.reset();
          }
          spdlog::info("Handled WalTransactionEnd delta");
        },
        [&](WalLabelIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create label index on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateIndex(storage->NameToLabel(data.label)).HasError())
            throw utils::BasicException("Failed to create label index on :{}.", data.label);
        },
        [&](WalLabelIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop label index on :{}", current_delta_idx, data.label);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->DropIndex(storage->NameToLabel(data.label)).HasError())
            throw utils::BasicException("Failed to drop label index on :{}.", data.label);
        },
        [&](WalLabelIndexStatsSet const &data) {
          spdlog::trace("   Delta {}. Set label index statistics on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          const auto label = storage->NameToLabel(data.label);
          LabelIndexStats stats{};
          if (!FromJson(data.json_stats, stats)) {
            throw utils::BasicException("Failed to read statistics!");
          }
          transaction->SetIndexStats(label, stats);
        },
        [&](WalLabelIndexStatsClear const &data) {
          spdlog::trace("   Delta {}. Clear label index statistics on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          if (!transaction->DeleteLabelIndexStats(storage->NameToLabel(data.label))) {
            throw utils::BasicException("Failed to clear label index statistics on :{}.", data.label);
          }
        },
        [&](WalLabelPropertyIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create label+property index on :{} ({})", current_delta_idx, data.label,
                        data.composite_property_paths);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto property_paths = data.composite_property_paths.convert(mapper);
          if (transaction->CreateIndex(storage->NameToLabel(data.label), std::move(property_paths)).HasError())

            throw utils::BasicException("Failed to create label+property index on :{} ({}).", data.label,
                                        data.composite_property_paths);
        },
        [&](WalLabelPropertyIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop label+property index on :{} ({})", current_delta_idx, data.label,
                        data.composite_property_paths);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto property_paths = data.composite_property_paths.convert(mapper);

          if (transaction->DropIndex(storage->NameToLabel(data.label), std::move(property_paths)).HasError()) {
            throw utils::BasicException("Failed to drop label+property index on :{} ({}).", data.label,
                                        data.composite_property_paths);
          }
        },
        [&](WalLabelPropertyIndexStatsSet const &data) {
          spdlog::trace("   Delta {}. Set label-property index statistics on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          const auto label = storage->NameToLabel(data.label);
          auto property_paths = data.composite_property_paths.convert(mapper);
          LabelPropertyIndexStats stats{};
          if (!FromJson(data.json_stats, stats)) {
            throw utils::BasicException("Failed to read statistics!");
          }
          transaction->SetIndexStats(label, std::move(property_paths), stats);
        },
        [&](WalLabelPropertyIndexStatsClear const &data) {
          spdlog::trace("   Delta {}. Clear label-property index statistics on :{}", current_delta_idx, data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          transaction->DeleteLabelPropertyIndexStats(storage->NameToLabel(data.label));
        },
        [&](WalEdgeTypeIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create edge index on :{}", current_delta_idx, data.edge_type);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateIndex(storage->NameToEdgeType(data.edge_type)).HasError()) {
            throw utils::BasicException("Failed to create edge index on :{}.", data.edge_type);
          }
        },
        [&](WalEdgeTypeIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop edge index on :{}", current_delta_idx, data.edge_type);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->DropIndex(storage->NameToEdgeType(data.edge_type)).HasError()) {
            throw utils::BasicException("Failed to drop edge index on :{}.", data.edge_type);
          }
        },
        [&](WalEdgeTypePropertyIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create edge index on :{}({})", current_delta_idx, data.edge_type, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateIndex(storage->NameToEdgeType(data.edge_type), storage->NameToProperty(data.property))
                  .HasError()) {
            throw utils::BasicException("Failed to create edge property index on :{}({}).", data.edge_type,
                                        data.property);
          }
        },
        [&](WalEdgeTypePropertyIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop edge index on :{}({})", current_delta_idx, data.edge_type, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->DropIndex(storage->NameToEdgeType(data.edge_type), storage->NameToProperty(data.property))
                  .HasError()) {
            throw utils::BasicException("Failed to drop edge property index on :{}({}).", data.edge_type,
                                        data.property);
          }
        },
        [&](WalEdgePropertyIndexCreate const &data) {
          spdlog::trace("       Create global edge index on ({})", data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateGlobalEdgeIndex(storage->NameToProperty(data.property)).HasError()) {
            throw utils::BasicException("Failed to create global edge property index on ({}).", data.property);
          }
        },
        [&](WalEdgePropertyIndexDrop const &data) {
          spdlog::trace("       Drop global edge index on ({})", data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateGlobalEdgeIndex(storage->NameToProperty(data.property)).HasError()) {
            throw utils::BasicException("Failed to drop global edge property index on ({}).", data.property);
          }
        },
        [&](WalTextIndexCreate const &) {
          /* NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)*/
        },
        [&](WalTextIndexDrop const &) {
          /* NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)*/
        },
        [&](WalExistenceConstraintCreate const &data) {
          spdlog::trace("   Delta {}. Create existence constraint on :{} ({})", current_delta_idx, data.label,
                        data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->CreateExistenceConstraint(storage->NameToLabel(data.label),
                                                            storage->NameToProperty(data.property));
          if (ret.HasError()) {
            throw utils::BasicException("Failed to create existence constraint on :{} ({}).", data.label,
                                        data.property);
          }
        },
        [&](WalExistenceConstraintDrop const &data) {
          spdlog::trace("   Delta {}. Drop existence constraint on :{} ({})", current_delta_idx, data.label,
                        data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction
                  ->DropExistenceConstraint(storage->NameToLabel(data.label), storage->NameToProperty(data.property))
                  .HasError()) {
            throw utils::BasicException("Failed to drop existence constraint on :{} ({}).", data.label, data.property);
          }
        },
        [&](WalUniqueConstraintCreate const &data) {
          std::stringstream ss;
          utils::PrintIterable(ss, data.properties);
          spdlog::trace("   Delta {}. Create unique constraint on :{} ({})", current_delta_idx, data.label, ss.str());
          std::set<PropertyId> properties;
          for (const auto &prop : data.properties) {
            properties.emplace(storage->NameToProperty(prop));
          }
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->CreateUniqueConstraint(storage->NameToLabel(data.label), properties);
          if (!ret.HasValue() || ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS) {
            throw utils::BasicException("Failed to create unique constraint on :{} ({}).", data.label, ss.str());
          }
        },
        [&](WalUniqueConstraintDrop const &data) {
          std::stringstream ss;
          utils::PrintIterable(ss, data.properties);
          spdlog::trace("   Delta {}. Drop unique constraint on :{} ({})", current_delta_idx, data.label, ss.str());
          std::set<PropertyId> properties;
          for (const auto &prop : data.properties) {
            properties.emplace(storage->NameToProperty(prop));
          }
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->DropUniqueConstraint(storage->NameToLabel(data.label), properties);
          if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
            throw utils::BasicException("Failed to create unique constraint on :{} ({}).", data.label, ss.str());
          }
        },
        [&](WalTypeConstraintCreate const &data) {
          spdlog::trace("   Delta {}. Create IS TYPED {} constraint on :{} ({})", current_delta_idx,
                        storage::TypeConstraintKindToString(data.kind), data.label, data.property);

          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->CreateTypeConstraint(storage->NameToLabel(data.label),
                                                       storage->NameToProperty(data.property), data.kind);
          if (ret.HasError()) {
            throw utils::BasicException("Failed to create IS TYPED {} constraint on :{} ({}).",
                                        TypeConstraintKindToString(data.kind), data.label, data.property);
          }
        },
        [&](WalTypeConstraintDrop const &data) {
          spdlog::trace("   Delta {}. Drop IS TYPED {} constraint on :{} ({})", current_delta_idx,
                        TypeConstraintKindToString(data.kind), data.label, data.property);

          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->DropTypeConstraint(storage->NameToLabel(data.label),
                                                     storage->NameToProperty(data.property), data.kind);
          if (ret.HasError()) {
            throw utils::BasicException("Failed to drop IS TYPED {} constraint on :{} ({}).",
                                        TypeConstraintKindToString(data.kind), data.label, data.property);
          }
        },
        [&](WalEnumCreate const &data) {
          std::stringstream ss;
          utils::PrintIterable(ss, data.evalues);
          spdlog::trace("   Delta {}. Create enum {} with values {}", current_delta_idx, data.etype, ss.str());
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->CreateEnum(data.etype, data.evalues);
          if (res.HasError()) {
            throw utils::BasicException("Failed to create enum {} with values {}.", data.etype, ss.str());
          }
        },
        [&](WalEnumAlterAdd const &data) {
          spdlog::trace("   Delta {}. Alter enum {} add value {}", current_delta_idx, data.etype, data.evalue);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->EnumAlterAdd(data.etype, data.evalue);
          if (res.HasError()) {
            throw utils::BasicException("Failed to alter enum {} add value {}.", data.etype, data.evalue);
          }
        },
        [&](WalEnumAlterUpdate const &data) {
          spdlog::trace("   Delta {}. Alter enum {} update {} to {}", current_delta_idx, data.etype, data.evalue_old,
                        data.evalue_new);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->EnumAlterUpdate(data.etype, data.evalue_old, data.evalue_new);
          if (res.HasError()) {
            throw utils::BasicException("Failed to alter enum {} update {} to {}.", data.etype, data.evalue_old,
                                        data.evalue_new);
          }
        },
        [&](WalPointIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create point index on :{}({})", current_delta_idx, data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto labelId = storage->NameToLabel(data.label);
          auto propId = storage->NameToProperty(data.property);
          auto res = transaction->CreatePointIndex(labelId, propId);
          if (res.HasError()) {
            throw utils::BasicException("Failed to create point index on :{}({})", data.label, data.property);
          }
        },
        [&](WalPointIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop point index on :{}({})", current_delta_idx, data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto labelId = storage->NameToLabel(data.label);
          auto propId = storage->NameToProperty(data.property);
          auto res = transaction->DropPointIndex(labelId, propId);
          if (res.HasError()) {
            throw utils::BasicException("Failed to drop point index on :{}({})", data.label, data.property);
          }
        },
        [&](WalVectorIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create vector index on :{}({})", current_delta_idx, data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto labelId = storage->NameToLabel(data.label);
          auto propId = storage->NameToProperty(data.property);
          auto metric_kind = storage::VectorIndex::MetricFromName(data.metric_kind);

          auto res = transaction->CreateVectorIndex(storage::VectorIndexSpec{
              .index_name = data.index_name,
              .label = labelId,
              .property = propId,
              .metric_kind = metric_kind,
              .dimension = data.dimension,
              .resize_coefficient = data.resize_coefficient,
              .capacity = data.capacity,
          });
          if (res.HasError()) {
            throw utils::BasicException("Failed to create vector index on :{}({})", data.label, data.property);
          }
        },
        [&](WalVectorIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop vector index {} ", current_delta_idx, data.index_name);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->DropVectorIndex(data.index_name);
          if (res.HasError()) {
            throw utils::BasicException("Failed to drop vector index {}", data.index_name);
          }
        },
    };

    std::visit(delta_apply, delta.data_);
    applied_deltas++;
  }

  spdlog::debug("Applied {} deltas", applied_deltas);

  return storage::SingleTxnDeltasProcessingResult{.commit_acc = std::move(commit_accessor),
                                                  .current_delta_idx = current_delta_idx,
                                                  .durability_commit_timestamp = commit_timestamp,
                                                  .current_batch_counter = current_batch_counter};
}
}  // namespace memgraph::dbms