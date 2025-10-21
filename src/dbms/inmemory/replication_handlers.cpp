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
#include "flags/experimental.hpp"
#include "replication/replication_server.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/utils.hpp"  // Include after all SLK definitions are present
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/schema_info.hpp"
#include "utils/file.hpp"
#include "utils/observer.hpp"

#include <spdlog/spdlog.h>
#include <optional>
#include <range/v3/view/filter.hpp>
#include <range/v3/view/join.hpp>
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

void ReadDurabilityFiles(std::vector<storage::durability::SnapshotDurabilityInfo> &old_snapshot_files,
                         std::filesystem::path const &current_snapshot_dir,
                         std::vector<storage::durability::WalDurabilityInfo> &old_wal_files,
                         std::filesystem::path const &current_wal_dir) {
  old_wal_files = storage::durability::GetWalFiles(current_wal_dir);
  old_snapshot_files = storage::durability::GetSnapshotFiles(current_snapshot_dir);
}

struct BackupDirectories {
  std::filesystem::path backup_snapshot_dir;
  std::filesystem::path backup_wal_dir;
};

auto CreateBackupDir(std::filesystem::path const &backup_dir) -> bool {
  std::error_code ec{};

  // Clear old directory (single fallback)
  if (std::filesystem::exists(backup_dir)) {
    std::filesystem::remove_all(backup_dir, ec);
    // Silent failure
  }

  std::filesystem::create_directory(backup_dir, ec);
  if (ec) {
    spdlog::error("Failed to create backup directory {}.", backup_dir);
    return false;
  }
  return true;
}

auto CreateBackupDirectories(std::filesystem::path const &current_snapshot_dir,
                             std::filesystem::path const &current_wal_dir) -> std::optional<BackupDirectories> {
  constexpr std::string_view backup_subdir = ".old";
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

TwoPCCache InMemoryReplicationHandlers::two_pc_cache_;

void InMemoryReplicationHandlers::Register(dbms::DbmsHandler *dbms_handler, replication::RoleReplicaData &data) {
  auto &server = *data.server;
  server.rpc_server_.Register<storage::replication::HeartbeatRpc>(
      [&data, dbms_handler](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                            uint64_t const request_version, auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::HeartbeatHandler(dbms_handler, data.uuid_, request_version, req_reader,
                                                      res_builder);
      });
  server.rpc_server_.Register<storage::replication::PrepareCommitRpc>(
      [&data, dbms_handler](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                            uint64_t const request_version, auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::PrepareCommitHandler(dbms_handler, data.uuid_, request_version, req_reader,
                                                          res_builder);
      });
  server.rpc_server_.Register<storage::replication::FinalizeCommitRpc>(
      [&data, dbms_handler](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                            uint64_t const request_version, auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::FinalizeCommitHandler(dbms_handler, data.uuid_, request_version, req_reader,
                                                           res_builder);
      });
  server.rpc_server_.Register<storage::replication::SnapshotRpc>(
      [&data, dbms_handler](std::optional<rpc::FileReplicationHandler> const &file_replication_handler,
                            uint64_t const request_version, auto *req_reader, auto *res_builder) {
        MG_ASSERT(file_replication_handler.has_value(), "File replication handler not prepared for SnapshotHandler");
        InMemoryReplicationHandlers::SnapshotHandler(*file_replication_handler, dbms_handler, data.uuid_,
                                                     request_version, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::WalFilesRpc>(
      [&data, dbms_handler](std::optional<rpc::FileReplicationHandler> const &file_replication_handler,
                            uint64_t const request_version, auto *req_reader, auto *res_builder) {
        MG_ASSERT(file_replication_handler.has_value(), "File replication handler not prepared for WalFilesHandler");
        InMemoryReplicationHandlers::WalFilesHandler(*file_replication_handler, dbms_handler, data.uuid_,
                                                     request_version, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::CurrentWalRpc>(
      [&data, dbms_handler](std::optional<rpc::FileReplicationHandler> const &file_replication_handler,
                            uint64_t const request_version, auto *req_reader, auto *res_builder) {
        MG_ASSERT(file_replication_handler.has_value(), "File replication handle not prepared for CurrentWalHandler");
        InMemoryReplicationHandlers::CurrentWalHandler(*file_replication_handler, dbms_handler, data.uuid_,
                                                       request_version, req_reader, res_builder);
      });
  server.rpc_server_.Register<replication_coordination_glue::SwapMainUUIDRpc>(
      [&data, dbms_handler](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                            uint64_t const request_version, auto *req_reader, auto *res_builder) {
        InMemoryReplicationHandlers::SwapMainUUIDHandler(dbms_handler, data, request_version, req_reader, res_builder);
      });
}

void InMemoryReplicationHandlers::SwapMainUUIDHandler(dbms::DbmsHandler *dbms_handler,
                                                      replication::RoleReplicaData &role_replica_data,
                                                      uint64_t const request_version, slk::Reader *req_reader,
                                                      slk::Builder *res_builder) {
  auto replica_state = dbms_handler->ReplicationState();
  if (!replica_state->IsReplica()) {
    spdlog::error("Setting main uuid must be performed on replica.");
    rpc::SendFinalResponse(replication_coordination_glue::SwapMainUUIDRes{false}, request_version, res_builder);
    return;
  }

  replication_coordination_glue::SwapMainUUIDReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  spdlog::info("Set replica data UUID to main uuid {}", std::string(req.uuid));
  replica_state->TryPersistRoleReplica(role_replica_data.config, req.uuid);
  role_replica_data.uuid_ = req.uuid;

  rpc::SendFinalResponse(replication_coordination_glue::SwapMainUUIDRes{true}, request_version, res_builder);
}

void InMemoryReplicationHandlers::HeartbeatHandler(dbms::DbmsHandler *dbms_handler,
                                                   const std::optional<utils::UUID> &current_main_uuid,
                                                   uint64_t const request_version, slk::Reader *req_reader,
                                                   slk::Builder *res_builder) {
  storage::replication::HeartbeatReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto const db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);

  if (!current_main_uuid.has_value() || req.main_uuid != *current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::HeartbeatReq::kType.name);
    const storage::replication::HeartbeatRes res{false, 0, "", 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }
  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  if (!db_acc.has_value()) {
    spdlog::warn("No database accessor");
    storage::replication::HeartbeatRes const res{false, 0, "", 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }
  // Move db acc
  auto const *storage = db_acc->get()->storage();
  auto const commit_info = storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);

  auto const last_epoch_with_commit = std::invoke([storage, ldt = commit_info.ldt_]() -> std::string {
    if (auto const &history = storage->repl_storage_state_.history; !history.empty()) {
      auto [history_epoch, history_ldt] = history.back();
      return history_ldt != ldt ? std::string{storage->repl_storage_state_.epoch_.id()} : history_epoch;
    }
    return std::string{storage->repl_storage_state_.epoch_.id()};
  });

  const storage::replication::HeartbeatRes res{true, commit_info.ldt_, last_epoch_with_commit,
                                               commit_info.num_committed_txns_};
  rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", storage->name()));
}

void InMemoryReplicationHandlers::PrepareCommitHandler(dbms::DbmsHandler *dbms_handler,
                                                       const std::optional<utils::UUID> &current_main_uuid,
                                                       uint64_t const request_version, slk::Reader *req_reader,
                                                       slk::Builder *res_builder) {
  storage::replication::PrepareCommitReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::PrepareCommitReq::kType.name);
    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  auto db_acc = GetDatabaseAccessor(dbms_handler, req.storage_uuid);
  if (!db_acc) {
    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // Read at the beginning so that SLK stream gets cleared even when the request is invalid
  storage::replication::Decoder decoder(req_reader);
  auto maybe_epoch_id = decoder.ReadString();
  if (!maybe_epoch_id) {
    spdlog::error("Invalid replication message, couldn't read epoch id.");
    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  auto &repl_storage_state = storage->repl_storage_state_;

  if (*maybe_epoch_id != repl_storage_state.epoch_.id()) {
    // We should first finalize WAL file and then update the epoch
    if (storage->wal_file_) {
      storage->wal_file_->FinalizeWal();
      storage->wal_file_.reset();
    }

    repl_storage_state.SaveLatestHistory();
    repl_storage_state.epoch_.SetEpoch(*maybe_epoch_id);
  }

  // last_durable_timestamp could be set by snapshot; so we cannot guarantee exactly what's the previous timestamp
  if (req.previous_commit_timestamp > repl_storage_state.commit_ts_info_.load(std::memory_order_acquire).ldt_) {
    // Empty the stream
    bool transaction_complete{false};
    while (!transaction_complete) {
      const auto [_, delta] = ReadDelta(&decoder, storage::durability::kVersion);
      transaction_complete = IsWalDeltaDataTransactionEnd(delta, storage::durability::kVersion);
    }

    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", storage->name()));
    return;
  }

  auto deltas_res = ReadAndApplyDeltasSingleTxn(storage, &decoder, storage::durability::kVersion, res_builder,
                                                /*two_phase_commit*/ req.two_phase_commit, /*loading_wal*/ false);

  storage::replication::PrepareCommitRes res{false};
  if (deltas_res.has_value()) {
    two_pc_cache_.commit_accessor_ = std::move(deltas_res->commit_acc);
    two_pc_cache_.durability_commit_timestamp_ = req.durability_commit_timestamp;
    res.success = true;
  }
  rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", storage->name()));
}

void InMemoryReplicationHandlers::FinalizeCommitHandler(dbms::DbmsHandler *dbms_handler,
                                                        const std::optional<utils::UUID> &current_main_uuid,
                                                        uint64_t const request_version, slk::Reader *req_reader,
                                                        slk::Builder *res_builder) {
  storage::replication::FinalizeCommitReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::FinalizeCommitReq::kType.name);
    storage::replication::FinalizeCommitRes const res(false);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  auto db_acc = GetDatabaseAccessor(dbms_handler, req.storage_uuid);
  if (!db_acc) {
    storage::replication::FinalizeCommitRes const res(false);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  // In this handler, we can either commit or abort. If cached accessor is nullptr, it is impossible we should commit
  // because replying to prepare happens after assignment to the accessor
  // If cached accessor is nullptr, and we should abort (e.g. exception was thrown while processing deltas), we can
  // safely return here OK because it means that the abort already happened while destructing accessor during
  // ReadAndApplyDeltasSingleTxn
  if (!two_pc_cache_.commit_accessor_) {
    spdlog::warn("Cached commit accessor became invalid between two phases");
    storage::replication::FinalizeCommitRes const res(true);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (req.durability_commit_timestamp != two_pc_cache_.durability_commit_timestamp_) {
    spdlog::warn("Trying to finalize txn with ldt {} but the last prepared txn is with ldt {}",
                 req.durability_commit_timestamp, two_pc_cache_.durability_commit_timestamp_);
    storage::replication::FinalizeCommitRes const res(true);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  auto *mem_storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  if (req.decision) {
    spdlog::trace("Trying to finalize on replica");
    two_pc_cache_.commit_accessor_->FinalizeCommitPhase(req.durability_commit_timestamp);
    spdlog::trace("Finalized on replica");
  } else {
    two_pc_cache_.commit_accessor_->AbortAndResetCommitTs();
    spdlog::trace("Aborted storage on replica");
  }

  two_pc_cache_.commit_accessor_.reset();
  if (mem_storage->wal_file_) {
    mem_storage->FinalizeWalFile();
  }

  storage::replication::FinalizeCommitRes const res(true);
  rpc::SendFinalResponse(res, request_version, res_builder);
}

void InMemoryReplicationHandlers::AbortPrevTxnIfNeeded(storage::InMemoryStorage *const storage) {
  if (two_pc_cache_.commit_accessor_) {
    two_pc_cache_.commit_accessor_->AbortAndResetCommitTs();
    two_pc_cache_.commit_accessor_.reset();
  }

  if (storage->wal_file_) {
    storage->wal_file_->FinalizeWal();
    storage->wal_file_.reset();
  }
}

// The semantic of snapshot handler is the following: Either handling snapshot request passes or it doesn't. If it
// passes we return the current commit timestamp of the replica. If it doesn't pass, we return optional which will
// signal to the caller that it shouldn't update the commit timestamp value.
void InMemoryReplicationHandlers::SnapshotHandler(rpc::FileReplicationHandler const &file_replication_handler,
                                                  DbmsHandler *dbms_handler,
                                                  const std::optional<utils::UUID> &current_main_uuid,
                                                  uint64_t const request_version, slk::Reader *req_reader,
                                                  slk::Builder *res_builder) {
  storage::replication::SnapshotReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.storage_uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in snapshot handler for request with storage_uuid {}",
                  std::string{req.storage_uuid});
    rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::SnapshotReq::kType.name);
    rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  AbortPrevTxnIfNeeded(storage);

  // Backup dir
  auto const current_snapshot_dir = storage->recovery_.snapshot_directory_;
  auto const current_wal_directory = storage->recovery_.wal_directory_;

  if (!utils::EnsureDir(current_snapshot_dir)) {
    spdlog::error("Couldn't get access to the current snapshot directory. Recovery won't be done.");
    rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  // Read durability files
  auto const curr_snapshot_files = storage::durability::GetSnapshotFiles(current_snapshot_dir);
  auto const curr_wal_files = storage::durability::GetWalFiles(current_wal_directory);

  auto const &active_files = file_replication_handler.GetActiveFileNames();
  MG_ASSERT(active_files.size() == 1, "Received {} snapshot files but expecting only one!", active_files.size());
  auto const &src_snapshot_file = active_files[0];
  auto const dst_snapshot_file = current_snapshot_dir / active_files[0].filename();

  if (!utils::RenamePath(src_snapshot_file, dst_snapshot_file)) {
    spdlog::error("Couldn't copy file from {} to {}", src_snapshot_file, dst_snapshot_file);
    rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder,
                           fmt::format("db: {}", storage->name()));
    return;
  }

  spdlog::info("Received snapshot saved to {}", dst_snapshot_file);
  {
    auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
    if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
      spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
      rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder,
                             fmt::format("db: {}", storage->name()));
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
          dst_snapshot_file, &storage->vertices_, &storage->edges_, &storage->edges_metadata_,
          &storage->repl_storage_state_.history, storage->name_id_mapper_.get(), &storage->edge_count_,
          storage->config_, &storage->enum_store_,
          storage->config_.salient.items.enable_schema_info ? &storage->schema_info_.Get() : nullptr, &storage->ttl_,
          snapshot_observer_info);
      // If this step is present it should always be the first step of
      // the recovery so we use the UUID we read from snapshot
      storage->uuid().set(snapshot_info.uuid);
      spdlog::trace("Set epoch to {} for db {}", snapshot_info.epoch_id, storage->name());
      storage->repl_storage_state_.epoch_.SetEpoch(std::move(snapshot_info.epoch_id));
      storage->vertex_id_ = recovery_info.next_vertex_id;
      storage->edge_id_ = recovery_info.next_edge_id;
      storage->timestamp_ = std::max(storage->timestamp_, recovery_info.next_timestamp);
      storage::CommitTsInfo const new_info{.ldt_ = snapshot_info.durable_timestamp,
                                           .num_committed_txns_ = snapshot_info.num_committed_txns};
      storage->repl_storage_state_.commit_ts_info_.store(new_info, std::memory_order_release);
      spdlog::trace("Set num committed txns to {} after loading snapshot.", snapshot_info.num_committed_txns);
      // We are the only active transaction, so mark everything up to the next timestamp
      if (storage->timestamp_ > 0) storage->commit_log_->MarkFinishedInRange(0, storage->timestamp_ - 1);

      RecoverIndicesStatsAndConstraints(&storage->vertices_, storage->name_id_mapper_.get(), &storage->indices_,
                                        &storage->constraints_, storage->config_, recovery_info, indices_constraints,
                                        storage->config_.salient.items.properties_on_edges, snapshot_observer_info);
    } catch (const storage::durability::RecoveryFailure &e) {
      spdlog::error(
          "Couldn't load the snapshot from {} because of: {}. Storage will be cleared. Snapshot and WAL files are "
          "preserved so you can restore your data by restarting instance.",
          dst_snapshot_file, e.what());
      storage->Clear();
      const storage::replication::SnapshotRes res{std::nullopt, 0};
      rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", storage->name()));
      return;
    }
  }
  spdlog::debug("Snapshot from {} loaded successfully.", dst_snapshot_file);

  auto const [ldt, num_committed_txns] = storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);

  const storage::replication::SnapshotRes res{ldt, num_committed_txns};
  rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", storage->name()));

  auto const not_recovery_snapshot = [&dst_snapshot_file](auto const &snapshot_info) {
    return snapshot_info.path != dst_snapshot_file;
  };

  auto snapshots_to_move = curr_snapshot_files | rv::filter(not_recovery_snapshot) | r::to_vector;

  auto const maybe_backup_dirs = CreateBackupDirectories(current_snapshot_dir, current_wal_directory);
  if (!maybe_backup_dirs.has_value()) {
    spdlog::error("Couldn't create backup directories. Replica won't be recovered.");
    const storage::replication::SnapshotRes res{std::nullopt, 0};
    rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", storage->name()));
    return;
  }
  auto const &[backup_snapshot_dir, backup_wal_dir] = *maybe_backup_dirs;
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
void InMemoryReplicationHandlers::WalFilesHandler(rpc::FileReplicationHandler const &file_replication_handler,
                                                  dbms::DbmsHandler *dbms_handler,
                                                  const std::optional<utils::UUID> &current_main_uuid,
                                                  uint64_t const request_version, slk::Reader *req_reader,
                                                  slk::Builder *res_builder) {
  storage::replication::WalFilesReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in wal files handler for request storage_uuid {}",
                  std::string{req.uuid});
    const storage::replication::WalFilesRes res{std::nullopt, 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::WalFilesReq::kType.name);
    rpc::SendFinalResponse(storage::replication::WalFilesRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  AbortPrevTxnIfNeeded(storage);

  auto const current_snapshot_dir = storage->recovery_.snapshot_directory_;
  auto const current_wal_directory = storage->recovery_.wal_directory_;

  if (!utils::EnsureDir(current_wal_directory)) {
    spdlog::error("Couldn't get access to the current wal directory. Recovery won't be done.");
    rpc::SendFinalResponse(storage::replication::WalFilesRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  std::vector<storage::durability::SnapshotDurabilityInfo> old_snapshot_files;
  std::vector<storage::durability::WalDurabilityInfo> old_wal_files;

  if (req.reset_needed) {
    {
      auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
      if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
        spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
        rpc::SendFinalResponse(storage::replication::WalFilesRes{std::nullopt, 0}, request_version, res_builder,
                               storage->name());
        return;
      }

      spdlog::trace("Clearing replica storage for db {} because the reset is needed while recovering from WalFiles.",
                    storage->name());
      storage->Clear();
    }
    ReadDurabilityFiles(old_snapshot_files, current_snapshot_dir, old_wal_files, current_wal_directory);
  }

  const auto wal_file_number = req.file_number;
  spdlog::debug("Received {} WAL files.", wal_file_number);

  auto const &active_files = file_replication_handler.GetActiveFileNames();

  uint32_t local_batch_counter{0};
  uint64_t num_committed_txns{0};
  for (auto i = 0; i < wal_file_number; ++i) {
    const auto [success, current_batch_counter, num_txns_committed] =
        LoadWal(active_files[i], storage, res_builder, local_batch_counter);
    // Failure to delete the received WAL file isn't fatal since it is saved in the tmp directory so it will eventually
    // get deleted
    utils::DeleteFile(active_files[i]);

    if (!success) {
      spdlog::debug("Replication recovery from WAL files failed while loading one of WAL files for db {}.",
                    storage->name());
      const storage::replication::WalFilesRes res{std::nullopt, 0};
      rpc::SendFinalResponse(res, request_version, res_builder);
      return;
    }
    local_batch_counter = current_batch_counter;
    num_committed_txns += num_txns_committed;
  }

  spdlog::debug("Replication recovery from WAL files succeeded for db {}.", storage->name());
  const storage::replication::WalFilesRes res{
      storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_, num_committed_txns};

  rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", storage->name()));

  if (req.reset_needed) {
    auto const maybe_backup_dirs = CreateBackupDirectories(current_snapshot_dir, current_wal_directory);
    if (!maybe_backup_dirs.has_value()) {
      spdlog::error("Couldn't create backup directories. Replica won't be recovered.");
      rpc::SendFinalResponse(storage::replication::WalFilesRes{std::nullopt, 0}, request_version, res_builder,
                             storage->name());
      return;
    }
    auto const &[backup_snapshot_dir, backup_wal_dir] = *maybe_backup_dirs;
    MoveDurabilityFiles(old_snapshot_files, backup_snapshot_dir, old_wal_files, backup_wal_dir,
                        &(storage->file_retainer_));
  }
}

// Commit timestamp on MAIN's side shouldn't be updated if:
// 1.) the database accessor couldn't be obtained
// 2.) UUID sent with the request is not the current MAIN's UUID which replica is listening to
// If loading WAL file partially succeeded then we shouldn't continue recovery but commit timestamp can be updated on
// main
void InMemoryReplicationHandlers::CurrentWalHandler(rpc::FileReplicationHandler const &file_replication_handler,
                                                    dbms::DbmsHandler *dbms_handler,
                                                    const std::optional<utils::UUID> &current_main_uuid,
                                                    uint64_t const request_version, slk::Reader *req_reader,
                                                    slk::Builder *res_builder) {
  storage::replication::CurrentWalReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in current wal handler for request storage_uuid {}",
                  std::string{req.uuid});
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::CurrentWalReq::kType.name);
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  AbortPrevTxnIfNeeded(storage);

  auto const current_snapshot_dir = storage->recovery_.snapshot_directory_;
  auto const current_wal_directory = storage->recovery_.wal_directory_;

  if (!utils::EnsureDir(current_wal_directory)) {
    spdlog::error("Couldn't get access to the current wal directory. Recovery won't be done.");
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  std::vector<storage::durability::SnapshotDurabilityInfo> old_snapshot_files;
  std::vector<storage::durability::WalDurabilityInfo> old_wal_files;

  if (req.reset_needed) {
    {
      auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
      if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
        spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
        rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
        return;
      }
      spdlog::trace("Clearing replica storage for db {} because the reset is needed while recovering from WalFiles.",
                    storage->name());
      storage->Clear();
    }
    ReadDurabilityFiles(old_snapshot_files, current_snapshot_dir, old_wal_files, current_wal_directory);
  }

  // Even if loading wal file failed, we return last_durable_timestamp to the main because it is not a fatal error
  // When loading a single WAL file, we don't care about saving number of deltas
  auto const &active_files = file_replication_handler.GetActiveFileNames();
  MG_ASSERT(active_files.size() == 1, "Received {} files but expected 1 in CurrentWalHandler", active_files.size());
  auto const load_wal_res = LoadWal(active_files[0], storage, res_builder);
  if (!load_wal_res.success) {
    spdlog::debug(
        "Replication recovery from current WAL didn't end successfully but the error is non-fatal error. DB {}.",
        storage->name());
  } else {
    spdlog::debug("Replication recovery from current WAL ended successfully! DB {}.", storage->name());
  }

  const storage::replication::CurrentWalRes res{
      storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_,
      load_wal_res.num_txns_committed};

  rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", storage->name()));

  if (req.reset_needed) {
    auto const maybe_backup_dirs = CreateBackupDirectories(current_snapshot_dir, current_wal_directory);
    if (!maybe_backup_dirs.has_value()) {
      spdlog::error("Couldn't create backup directories. Replica won't be recovered for db {}.", storage->name());
      rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
      return;
    }
    auto const &[backup_snapshot_dir, backup_wal_dir] = *maybe_backup_dirs;
    MoveDurabilityFiles(old_snapshot_files, backup_snapshot_dir, old_wal_files, backup_wal_dir,
                        &(storage->file_retainer_));
  }

  // Failure to delete the received WAL file isn't fatal since it is saved in the tmp directory so it will eventually
  // get deleted
  utils::DeleteFile(active_files[0]);
}

// The method will return false and hence signal the failure of completely loading the WAL file if:
// 1.) It cannot open WAL file for reading from temporary WAL directory
// 2.) If WAL magic and/or version wasn't loaded successfully
// 3.) If WAL version is invalid
// 4.) If reading WAL info fails
// 5.) If applying some of the deltas failed
// If WAL file doesn't contain any new changes, we ignore it and consider WAL file as successfully applied.
InMemoryReplicationHandlers::LoadWalStatus InMemoryReplicationHandlers::LoadWal(std::filesystem::path const &wal_path,
                                                                                storage::InMemoryStorage *storage,
                                                                                slk::Builder *res_builder,
                                                                                uint32_t const start_batch_counter) {
  spdlog::trace("Received WAL saved to {}", wal_path);

  std::optional<storage::durability::WalInfo> maybe_wal_info;
  try {
    maybe_wal_info.emplace(storage::durability::ReadWalInfo(wal_path));
  } catch (const utils::BasicException &e) {
    spdlog::error("Loading WAL info from {} failed because of {}.", wal_path, e.what());
    return LoadWalStatus{.success = false, .current_batch_counter = 0, .num_txns_committed = 0};
  }

  auto const &wal_info = *maybe_wal_info;

  // We have to check if this is our 1st wal, not what main is sending
  if (storage->wal_seq_num_ == 0) {
    storage->uuid().set(wal_info.uuid);
  }

  // If WAL file doesn't contain any changes that need to be applied, ignore it
  if (wal_info.to_timestamp <= storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_) {
    spdlog::trace("WAL file won't be applied since all changes already exist.");
    return LoadWalStatus{.success = true, .current_batch_counter = 0, .num_txns_committed = 0};
  }

  // We trust only WAL files which contain changes we are interested in (newer changes)
  if (auto &repl_epoch = storage->repl_storage_state_.epoch_; wal_info.epoch_id != repl_epoch.id()) {
    spdlog::trace("Set epoch to {} for db {}", wal_info.epoch_id, storage->name());
    storage->repl_storage_state_.SaveLatestHistory();
    repl_epoch.SetEpoch(wal_info.epoch_id);
  }

  // We do not care about incoming sequence numbers, after a snapshot recovery, the sequence number is 0
  // This is because the snapshots completely wipes the storage and durability
  // It is also the first recovery step, so the WAL chain needs to restart from 0, otherwise the instance won't be
  // able to recover from durable data
  if (storage->wal_file_) {
    storage->wal_file_->FinalizeWal();
    storage->wal_file_.reset();
    spdlog::trace("WAL file {} finalized successfully", wal_path);
  }

  spdlog::trace("Loading WAL deltas from {}", wal_path);
  storage::durability::Decoder wal_decoder;
  const auto version = wal_decoder.Initialize(wal_path, storage::durability::kWalMagic);
  spdlog::debug("WAL file {} loaded successfully", wal_path);
  if (!version) {
    spdlog::error("Couldn't read WAL magic and/or version!");
    return LoadWalStatus{.success = false, .current_batch_counter = 0, .num_txns_committed = 0};
  }
  if (!storage::durability::IsVersionSupported(*version)) {
    spdlog::error("Invalid WAL version!");
    return LoadWalStatus{.success = false, .current_batch_counter = 0, .num_txns_committed = 0};
  }

  wal_decoder.SetPosition(wal_info.offset_deltas);

  uint32_t local_batch_counter = start_batch_counter;
  uint64_t num_txns_committed{0};
  for (size_t local_delta_idx = 0; local_delta_idx < wal_info.num_deltas;) {
    // commit_txn_immediately is set true because when loading WAL files, we should commit immediately
    auto const deltas_res =
        ReadAndApplyDeltasSingleTxn(storage, &wal_decoder, *version, res_builder, /*two_phase_commit*/ false,
                                    /*loading_wal*/ true, local_batch_counter);
    if (deltas_res.has_value()) {
      local_delta_idx += deltas_res->current_delta_idx;
      local_batch_counter = deltas_res->current_batch_counter;
      num_txns_committed += deltas_res->num_txns_committed;
    } else {
      return LoadWalStatus{.success = false, .current_batch_counter = 0, .num_txns_committed = 0};
    }
  }

  spdlog::trace("Replication from WAL file {} successful!", wal_path);
  return LoadWalStatus{
      .success = true, .current_batch_counter = local_batch_counter, .num_txns_committed = num_txns_committed};
}

// The number of applied deltas also includes skipped deltas.
std::optional<storage::SingleTxnDeltasProcessingResult> InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn(
    storage::InMemoryStorage *storage, storage::durability::BaseDecoder *decoder, const uint64_t version,
    slk::Builder *res_builder, bool const two_phase_commit, bool const loading_wal,
    uint32_t const start_batch_counter) {
  auto edge_acc = storage->edges_.access();
  auto vertex_acc = storage->vertices_.access();

  constexpr auto kSharedAccess = storage::StorageAccessType::WRITE;
  constexpr auto kUniqueAccess = storage::StorageAccessType::UNIQUE;

  uint64_t commit_timestamp{0};
  std::unique_ptr<storage::ReplicationAccessor> commit_accessor;

  bool should_commit{true};
  // Replica will use the same storage access type as main did when doing the transaction
  // It is passed through the WalTransactionStart delta
  std::optional<storage::StorageAccessType> access_type;

  auto translate_access_type = [](storage::durability::TransactionAccessType access_type) {
    switch (access_type) {
      case storage::durability::TransactionAccessType::UNIQUE:
        return storage::StorageAccessType::UNIQUE;
      case storage::durability::TransactionAccessType::WRITE:
        return storage::StorageAccessType::WRITE;
      case storage::durability::TransactionAccessType::READ:
        return storage::StorageAccessType::READ;
      case storage::durability::TransactionAccessType::READ_ONLY:
        return storage::StorageAccessType::READ_ONLY;
      default:
        throw std::runtime_error("Unrecognized access type!");
    }
  };

  auto const get_replication_accessor = [&, storage](uint64_t const local_commit_timestamp,
                                                     storage::StorageAccessType acc_hint =
                                                         kSharedAccess) -> storage::ReplicationAccessor * {
    if (!commit_accessor) {
      std::unique_ptr<storage::Storage::Accessor> acc = nullptr;
      // acc_hint only gets used if we are using an older version of WAL (before v3.5.0)
      auto true_access_type = access_type.value_or(acc_hint);
      switch (true_access_type) {
        case storage::StorageAccessType::READ:
          [[fallthrough]];
        case storage::StorageAccessType::WRITE:
          acc = storage->Access(true_access_type);
          break;
        case storage::StorageAccessType::UNIQUE:
          acc = storage->UniqueAccess();
          break;
        case storage::StorageAccessType::READ_ONLY:
          acc = storage->ReadOnlyAccess();
          break;
        default:
          throw utils::BasicException("Replica failed to gain storage access! Unknown accessor type.");
      }

      commit_timestamp = local_commit_timestamp;
      commit_accessor.reset(static_cast<storage::ReplicationAccessor *>(acc.release()));

    } else if (commit_timestamp != local_commit_timestamp) {
      throw utils::BasicException("Received more than one transaction!");
    }
    return commit_accessor.get();
  };

  uint64_t num_committed_txns{0};
  uint64_t current_delta_idx{0};  // tracks over how many deltas we iterated, includes also skipped deltas.
  uint64_t applied_deltas{0};     // Non-skipped deltas
  uint32_t current_batch_counter = start_batch_counter;
  auto max_delta_timestamp = storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;

  auto current_durable_commit_timestamp = max_delta_timestamp;
  spdlog::trace("Current durable commit timestamp: {}", current_durable_commit_timestamp);

  uint64_t prev_printed_timestamp = 0;

  for (bool transaction_complete = false; !transaction_complete; ++current_delta_idx, ++current_batch_counter) {
    if (current_batch_counter == kDeltasBatchProgressSize) {
      rpc::SendInProgressMsg(res_builder);
      current_batch_counter = 0;
    }

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
    using namespace storage::durability;
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
            Delta *local_delta = nullptr;
            {
              auto guard = std::shared_lock{edge->lock};
              is_visible = !edge->deleted;
              local_delta = edge->delta;
            }
            ApplyDeltasForRead(&transaction->GetTransaction(), local_delta, View::NEW,
                               [&is_visible](const Delta &delta) {
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
        [&](WalTransactionStart const &data) {
          spdlog::trace("   Delta {}. Transaction start. Commit txn: {}, Access type: {}", current_delta_idx,
                        data.commit, data.access_type ? static_cast<uint64_t>(*data.access_type) : -1);

          if (loading_wal) {
            // This only gets used when loading a WAL from main
            // Otherwise it doesn't matter what gets sent
            should_commit = data.commit.value_or(true);
          }
          access_type = data.access_type ? std::optional(translate_access_type(*data.access_type)) : std::nullopt;
        },
        [&](WalTransactionEnd const &) {
          spdlog::trace("   Delta {}. Transaction end", current_delta_idx);
          if (!commit_accessor || commit_timestamp != delta_timestamp)
            throw utils::BasicException("Invalid commit data!");
          auto const ret = commit_accessor->PrepareForCommitPhase(
              storage::CommitArgs::make_replica_write(commit_timestamp, two_phase_commit));
          if (ret.HasError()) {
            throw utils::BasicException("Committing failed while trying to prepare for commit on replica.");
          }
          // If not STRICT SYNC replica, reset the commit accessor immediately because the txn is considered committed
          if (!two_phase_commit) {
            commit_accessor.reset();
          }
          // Used to return info to MAIN about how many txns were committed
          if (loading_wal) {
            num_committed_txns++;
          }
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
        [&](WalTextIndexCreate const &data) {
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto label_id = storage->NameToLabel(data.label);
          const auto properties_str = std::invoke([&]() -> std::string {
            if (data.properties && !data.properties->empty()) {
              return fmt::format(" ({})", rv::join(*data.properties, ", ") | r::to<std::string>);
            }
            return {};
          });
          spdlog::trace("   Delta {}. Create text search index {} on :{}{}", current_delta_idx, data.index_name,
                        data.label, properties_str);
          auto prop_ids = std::invoke([&]() -> std::vector<PropertyId> {
            if (!data.properties) {
              return {};
            }
            return *data.properties |
                   rv::transform([&](const auto &prop_name) { return storage->NameToProperty(prop_name); }) |
                   r::to_vector;
          });
          auto ret = transaction->CreateTextIndex(storage::TextIndexSpec{data.index_name, label_id, prop_ids});
          if (ret.HasError()) {
            throw utils::BasicException("Failed to create text search index {} on {}.", data.index_name, data.label);
          }
        },
        [&](WalTextEdgeIndexCreate const &data) {
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          const auto edge_type = storage->NameToEdgeType(data.edge_type);
          const auto properties_str = std::invoke([&]() -> std::string {
            if (!data.properties.empty()) {
              return fmt::format(" ({})", rv::join(data.properties, ", ") | r::to<std::string>);
            }
            return {};
          });
          spdlog::trace("   Delta {}. Create text search index {} on :{}{}", current_delta_idx, data.index_name,
                        data.edge_type, properties_str);
          auto prop_ids = data.properties |
                          rv::transform([&](const auto &prop_name) { return storage->NameToProperty(prop_name); }) |
                          r::to_vector;
          const auto ret =
              transaction->CreateTextEdgeIndex(storage::TextEdgeIndexSpec{data.index_name, edge_type, prop_ids});
          if (ret.HasError()) {
            throw utils::BasicException("Failed to create text search index {} on {}.", data.index_name,
                                        data.edge_type);
          }
        },
        [&](WalTextIndexDrop const &data) {
          spdlog::trace("   Delta {}. Drop text search index {}.", current_delta_idx, data.index_name);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->DropTextIndex(data.index_name).HasError()) {
            throw utils::BasicException("Failed to drop text search index {}.", data.index_name);
          }
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
          auto metric_kind = storage::MetricFromName(data.metric_kind);
          auto scalar_kind = data.scalar_kind ? static_cast<unum::usearch::scalar_kind_t>(*data.scalar_kind)
                                              : unum::usearch::scalar_kind_t::f32_k;

          auto res = transaction->CreateVectorIndex(storage::VectorIndexSpec{
              .index_name = data.index_name,
              .label_id = labelId,
              .property = propId,
              .metric_kind = metric_kind,
              .dimension = data.dimension,
              .resize_coefficient = data.resize_coefficient,
              .capacity = data.capacity,
              .scalar_kind = scalar_kind,
          });
          if (res.HasError()) {
            throw utils::BasicException("Failed to create vector index on :{}({})", data.label, data.property);
          }
        },
        [&](WalVectorEdgeIndexCreate const &data) {
          spdlog::trace("   Delta {}. Create vector index on :{}({})", current_delta_idx, data.edge_type,
                        data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto edgeType = storage->NameToEdgeType(data.edge_type);
          auto propId = storage->NameToProperty(data.property);
          auto metric_kind = storage::MetricFromName(data.metric_kind);

          auto res = transaction->CreateVectorEdgeIndex(storage::VectorEdgeIndexSpec{
              .index_name = data.index_name,
              .edge_type_id = edgeType,
              .property = propId,
              .metric_kind = metric_kind,
              .dimension = data.dimension,
              .resize_coefficient = data.resize_coefficient,
              .capacity = data.capacity,
              .scalar_kind = static_cast<unum::usearch::scalar_kind_t>(data.scalar_kind),
          });
          if (res.HasError()) {
            throw utils::BasicException("Failed to create vector index on :{}({})", data.edge_type, data.property);
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
        [&]([[maybe_unused]] WalTtlOperation const &data) {
#ifdef MG_ENTERPRISE
          spdlog::trace("   Delta {}. TTL operation type {}", current_delta_idx, static_cast<int>(data.operation_type));
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          switch (data.operation_type) {
            case storage::durability::TtlOperationType::ENABLE:
              transaction->StartTtl({.is_main = false});
              break;
            case storage::durability::TtlOperationType::DISABLE:
              transaction->DisableTtl({.is_main = false});
              break;
            case storage::durability::TtlOperationType::CONFIGURE:
              transaction->ConfigureTtl(storage::ttl::TtlInfo{data.period, data.start_time, data.should_run_edge_ttl},
                                        {.is_main = false});
              // Configuration will leave it paused; replicas should not run ttl
              break;
            case storage::durability::TtlOperationType::STOP:
              transaction->StopTtl();
              break;
            default:
              throw utils::BasicException("Invalid TTL operation type: {}", static_cast<int>(data.operation_type));
          }
#else
          spdlog::trace("TTL operation is not supported in community edition");
#endif
        },
    };

    // If I received PrepareCommitRpc, deltas should be applied (loading_wal will be false)
    // If loading WAL file, WalTransactionStart is decision-maker whether to load the txn from WAL or not
    if (loading_wal && !should_commit) continue;

    try {
      std::visit(delta_apply, delta.data_);
    } catch (const std::exception &e) {
      spdlog::error("Applying deltas failed because of {}", e.what());
      return std::nullopt;
    }
    applied_deltas++;
  }

  spdlog::debug("Applied {} deltas. Committed {} txns.", applied_deltas, num_committed_txns);

  return storage::SingleTxnDeltasProcessingResult{.commit_acc = std::move(commit_accessor),
                                                  .current_delta_idx = current_delta_idx,
                                                  .num_txns_committed = num_committed_txns,
                                                  .current_batch_counter = current_batch_counter};
}
}  // namespace memgraph::dbms
