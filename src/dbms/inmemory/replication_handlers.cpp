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
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/schema_info.hpp"

#include <spdlog/spdlog.h>
#include <cstdint>
#include <optional>
#include <usearch/index_plugins.hpp>

using memgraph::replication_coordination_glue::ReplicationRole;
using memgraph::storage::Delta;
using memgraph::storage::EdgeAccessor;
using memgraph::storage::EdgeRef;
using memgraph::storage::EdgeTypeId;
using memgraph::storage::LabelIndexStats;
using memgraph::storage::LabelPropertyIndexStats;
using memgraph::storage::PropertyId;
using memgraph::storage::UniqueConstraints;
using memgraph::storage::View;
using memgraph::storage::durability::WalDeltaData;

namespace memgraph::dbms {
namespace {
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
};

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

void InMemoryReplicationHandlers::Register(dbms::DbmsHandler *dbms_handler, replication::RoleReplicaData &data) {
  auto &server = *data.server;
  server.rpc_server_.Register<storage::replication::HeartbeatRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received HeartbeatRpc");
        InMemoryReplicationHandlers::HeartbeatHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::AppendDeltasRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received AppendDeltasRpc");
        InMemoryReplicationHandlers::AppendDeltasHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::SnapshotRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received SnapshotRpc");
        InMemoryReplicationHandlers::SnapshotHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::WalFilesRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received WalFilesRpc");
        InMemoryReplicationHandlers::WalFilesHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::CurrentWalRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received CurrentWalRpc");
        InMemoryReplicationHandlers::CurrentWalHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::TimestampRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received TimestampRpc");
        InMemoryReplicationHandlers::TimestampHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
  server.rpc_server_.Register<replication_coordination_glue::SwapMainUUIDRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received SwapMainUUIDRpc");
        InMemoryReplicationHandlers::SwapMainUUIDHandler(dbms_handler, data, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::ForceResetStorageRpc>(
      [&data, dbms_handler](auto *req_reader, auto *res_builder) {
        spdlog::debug("Received ForceResetStorageRpc");
        InMemoryReplicationHandlers::ForceResetStorageHandler(dbms_handler, data.uuid_, req_reader, res_builder);
      });
}

void InMemoryReplicationHandlers::SwapMainUUIDHandler(dbms::DbmsHandler *dbms_handler,
                                                      replication::RoleReplicaData &role_replica_data,
                                                      slk::Reader *req_reader, slk::Builder *res_builder) {
  if (!dbms_handler->IsReplica()) {
    spdlog::error("Setting main uuid must be performed on replica.");
    slk::Save(replication_coordination_glue::SwapMainUUIDRes{false}, res_builder);
    return;
  }

  replication_coordination_glue::SwapMainUUIDReq req;
  slk::Load(&req, req_reader);
  spdlog::info("Set replica data UUID to main uuid {}", std::string(req.uuid));
  dbms_handler->ReplicationState().TryPersistRoleReplica(role_replica_data.config, req.uuid);
  role_replica_data.uuid_ = req.uuid;

  slk::Save(replication_coordination_glue::SwapMainUUIDRes{true}, res_builder);
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
    slk::Save(res, res_builder);
    return;
  }
  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  if (!db_acc.has_value()) {
    spdlog::warn("No database accessor");
    storage::replication::HeartbeatRes const res{false, 0, ""};
    slk::Save(res, res_builder);
    return;
  }
  auto const *storage = db_acc->get()->storage();
  const storage::replication::HeartbeatRes res{true, storage->repl_storage_state_.last_durable_timestamp_.load(),
                                               std::string{storage->repl_storage_state_.epoch_.id()}};
  slk::Save(res, res_builder);
}

void InMemoryReplicationHandlers::AppendDeltasHandler(dbms::DbmsHandler *dbms_handler,
                                                      const std::optional<utils::UUID> &current_main_uuid,
                                                      slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::AppendDeltasReq req;
  slk::Load(&req, req_reader);

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::AppendDeltasReq::kType.name);
    const storage::replication::AppendDeltasRes res{false};
    slk::Save(res, res_builder);
    return;
  }

  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    const storage::replication::AppendDeltasRes res{false};
    slk::Save(res, res_builder);
    return;
  }

  storage::replication::Decoder decoder(req_reader);

  auto maybe_epoch_id = decoder.ReadString();
  if (!maybe_epoch_id) {
    spdlog::error("Invalid replication message, couldn't read epoch id.");
    const storage::replication::AppendDeltasRes res{false};
    slk::Save(res, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  auto &repl_storage_state = storage->repl_storage_state_;
  if (*maybe_epoch_id != repl_storage_state.epoch_.id()) {
    spdlog::trace("Set epoch id to {} in AppendDeltasHandler.", *maybe_epoch_id);
    auto prev_epoch = repl_storage_state.epoch_.SetEpoch(*maybe_epoch_id);
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
      spdlog::trace("Current WAL file finalized successfully");
    }
  }

  // last_durable_timestamp could be set by snapshot; so we cannot guarantee exactly what's the previous timestamp
  if (req.previous_commit_timestamp > repl_storage_state.last_durable_timestamp_.load()) {
    // Empty the stream
    bool transaction_complete = false;
    while (!transaction_complete) {
      spdlog::info("Skipping delta");
      const auto [_, delta] = ReadDelta(&decoder, storage::durability::kVersion);
      transaction_complete = IsWalDeltaDataTransactionEnd(delta, storage::durability::kVersion);
    }

    const storage::replication::AppendDeltasRes res{false};
    slk::Save(res, res_builder);
    return;
  }

  try {
    ReadAndApplyDeltas(storage, &decoder, storage::durability::kVersion);
  } catch (const utils::BasicException &e) {
    spdlog::error(
        "Error occurred while trying to apply deltas because of {}. Replication recovery from append deltas finished "
        "unsuccessfully.",
        e.what());
    const storage::replication::AppendDeltasRes res{false};
    slk::Save(res, res_builder);
    return;
  }

  const storage::replication::AppendDeltasRes res{true};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from append deltas finished, replica is now up to date!");
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
    const storage::replication::SnapshotRes res{{}};
    Save(res, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::SnapshotReq::kType.name);
    const storage::replication::SnapshotRes res{{}};
    Save(res, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->recovery_.snapshot_directory_);

  storage::replication::Decoder decoder(req_reader);
  const auto maybe_snapshot_path = decoder.ReadFile(storage->recovery_.snapshot_directory_);
  if (!maybe_snapshot_path.has_value()) {
    spdlog::error("Failed to load snapshot from {}", storage->recovery_.snapshot_directory_);
    const storage::replication::SnapshotRes res{{}};
    Save(res, res_builder);
    return;
  }
  spdlog::info("Received snapshot saved to {}", *maybe_snapshot_path);

  {
    auto storage_guard = std::lock_guard{storage->main_lock_};
    spdlog::trace("Clearing database before recovering from snapshot.");

    // Clear the database
    storage->Clear();

    try {
      spdlog::debug("Loading snapshot");
      auto [snapshot_info, recovery_info, indices_constraints] = storage::durability::LoadSnapshot(
          *maybe_snapshot_path, &storage->vertices_, &storage->edges_, &storage->edges_metadata_,
          storage->repl_storage_state_.history, storage->name_id_mapper_.get(), &storage->edge_count_, storage->config_,
          &storage->enum_store_,
          storage->config_.salient.items.enable_schema_info ? &storage->schema_info_.Get() : nullptr);
      // If this step is present it should always be the first step of
      // the recovery so we use the UUID we read from snapshot
      storage->uuid().set(snapshot_info.uuid);
      storage->repl_storage_state_.epoch_.SetEpoch(std::move(snapshot_info.epoch_id));
      storage->vertex_id_ = recovery_info.next_vertex_id;
      storage->edge_id_ = recovery_info.next_edge_id;
      storage->timestamp_ = std::max(storage->timestamp_, recovery_info.next_timestamp);
      storage->repl_storage_state_.last_durable_timestamp_ = recovery_info.next_timestamp - 1;

      spdlog::trace("Recovering indices and constraints from snapshot.");
      storage::durability::RecoverIndicesStatsAndConstraints(
          &storage->vertices_, storage->name_id_mapper_.get(), &storage->indices_, &storage->constraints_,
          storage->config_, recovery_info, indices_constraints, storage->config_.salient.items.properties_on_edges);
    } catch (const storage::durability::RecoveryFailure &e) {
      spdlog::error("Couldn't load the snapshot from {} because of: {}. Storage will be cleared.", *maybe_snapshot_path,
                    e.what());
      storage->Clear();
      const storage::replication::SnapshotRes res{{}};
      Save(res, res_builder);
      return;
    }
  }
  spdlog::debug("Snapshot from {} loaded successfully.", *maybe_snapshot_path);

  const storage::replication::SnapshotRes res{storage->repl_storage_state_.last_durable_timestamp_.load()};
  Save(res, res_builder);

  spdlog::trace("Deleting old snapshot files due to snapshot recovery.");

  auto uuid_str = std::string{storage->uuid()};
  // Delete other durability files
  auto const snapshot_files = storage::durability::GetSnapshotFiles(storage->recovery_.snapshot_directory_, uuid_str);
  for (const auto &[path, uuid, _] : snapshot_files) {
    if (path != *maybe_snapshot_path) {
      spdlog::trace("Deleting snapshot file {}", path);
      storage->file_retainer_.DeleteFile(path);
    }
  }

  spdlog::trace("Deleting old WAL files due to snapshot recovery.");
  if (auto wal_files = storage::durability::GetWalFiles(storage->recovery_.wal_directory_, uuid_str)) {
    for (const auto &wal_file : *wal_files) {
      spdlog::trace("Deleting WAL file {}", wal_file.path);
      storage->file_retainer_.DeleteFile(wal_file.path);
    }

    storage->wal_file_.reset();
  }
  spdlog::debug("Replication recovery from snapshot finished!");
}

void InMemoryReplicationHandlers::ForceResetStorageHandler(dbms::DbmsHandler *dbms_handler,
                                                           const std::optional<utils::UUID> &current_main_uuid,
                                                           slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::ForceResetStorageReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.db_uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in force reset storage handler for request storage_uuid {}",
                  std::string{req.db_uuid});
    const storage::replication::ForceResetStorageRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::SnapshotReq::kType.name);
    const storage::replication::ForceResetStorageRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  auto storage_guard = std::unique_lock{storage->main_lock_};

  // Clear the database
  storage->Clear();

  const storage::replication::ForceResetStorageRes res{true,
                                                       storage->repl_storage_state_.last_durable_timestamp_.load()};
  slk::Save(res, res_builder);

  spdlog::trace("Deleting old snapshot files.");

  auto const uuid_str = std::string{storage->uuid()};
  // Delete other durability files
  auto snapshot_files = storage::durability::GetSnapshotFiles(storage->recovery_.snapshot_directory_, uuid_str);
  for (const auto &[path, uuid, _] : snapshot_files) {
    spdlog::trace("Deleting snapshot file {}", path);
    storage->file_retainer_.DeleteFile(path);
  }

  spdlog::trace("Deleting old WAL files.");
  if (auto wal_files = storage::durability::GetWalFiles(storage->recovery_.wal_directory_, uuid_str)) {
    for (const auto &wal_file : *wal_files) {
      spdlog::trace("Deleting WAL file {}", wal_file.path);
      storage->file_retainer_.DeleteFile(wal_file.path);
    }

    storage->wal_file_.reset();
  }
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
    slk::Save(res, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::WalFilesReq::kType.name);
    const storage::replication::WalFilesRes res{{}};
    slk::Save(res, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->recovery_.wal_directory_);

  const auto wal_file_number = req.file_number;
  spdlog::debug("Received {} WAL files.", wal_file_number);
  storage::replication::Decoder decoder(req_reader);

  for (auto i = 0; i < wal_file_number; ++i) {
    if (!LoadWal(storage, &decoder)) {
      spdlog::debug("Replication recovery from WAL files failed while loading one of WAL files.");
      const storage::replication::WalFilesRes res{{}};
      slk::Save(res, res_builder);
      return;
    }
  }

  spdlog::debug("Replication recovery from WAL files succeeded");
  const storage::replication::WalFilesRes res{
      storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire)};
  slk::Save(res, res_builder);
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
    slk::Save(res, res_builder);
    return;
  }

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::CurrentWalReq::kType.name);
    const storage::replication::CurrentWalRes res{{}};
    slk::Save(res, res_builder);
    return;
  }

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->recovery_.wal_directory_);

  storage::replication::Decoder decoder(req_reader);

  // Even if loading wal file failed, we return last_durable_timestamp to the main because it is not a fatal error
  if (!LoadWal(storage, &decoder)) {
    spdlog::debug("Replication recovery from current WAL didn't end successfully but the error is non-fatal error.");
  } else {
    spdlog::debug("Replication recovery from current WAL ended successfully!");
  }

  const storage::replication::CurrentWalRes res{
      storage->repl_storage_state_.last_durable_timestamp_.load(std::memory_order_acquire)};
  slk::Save(res, res_builder);
}

// The method will return false and hence signal the failure of completely loading the WAL file if:
// 1.) It cannot open WAL file for reading from temporary WAL directory
// 2.) If WAL magic and/or version wasn't loaded successfully
// 3.) If WAL version is invalid
// 4.) If reading WAL info fails
// 5.) If applying some of the deltas failed
// If WAL file doesn't contain any new changes, we ignore it and consider WAL file as successfully applied.
bool InMemoryReplicationHandlers::LoadWal(storage::InMemoryStorage *storage, storage::replication::Decoder *decoder) {
  const auto temp_wal_directory =
      std::filesystem::temp_directory_path() / "memgraph" / storage::durability::kWalDirectory;
  utils::EnsureDir(temp_wal_directory);
  auto maybe_wal_path = decoder->ReadFile(temp_wal_directory);
  if (!maybe_wal_path) {
    spdlog::error("Failed to load WAL file from {}!", temp_wal_directory);
    return false;
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
      return true;
    }
    // We trust only WAL files which contain changes we are interested in (newer changes)
    if (auto &replica_epoch = storage->repl_storage_state_.epoch_; wal_info.epoch_id != replica_epoch.id()) {
      spdlog::trace("Set epoch id to {} while loading wal file {}.", wal_info.epoch_id, *maybe_wal_path);
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
    storage::durability::Decoder wal;
    const auto version = wal.Initialize(*maybe_wal_path, storage::durability::kWalMagic);
    spdlog::debug("WAL file {} loaded successfully", *maybe_wal_path);
    if (!version) {
      spdlog::error("Couldn't read WAL magic and/or version!");
      return false;
    }
    if (!storage::durability::IsVersionSupported(*version)) {
      spdlog::error("Invalid WAL version!");
      return false;
    }
    wal.SetPosition(wal_info.offset_deltas);

    for (size_t i = 0; i < wal_info.num_deltas;) {
      i += ReadAndApplyDeltas(storage, &wal, *version);
    }

    spdlog::trace("Replication from WAL file {} successful!", *maybe_wal_path);
    return true;
  } catch (const storage::durability::RecoveryFailure &e) {
    spdlog::error("Couldn't recover WAL deltas from {} because of: {}.", *maybe_wal_path, e.what());
    return false;
  } catch (const utils::BasicException &e) {
    spdlog::error("Loading WAL from {} failed because of {}.", *maybe_wal_path, e.what());
    return false;
  }
}

void InMemoryReplicationHandlers::TimestampHandler(dbms::DbmsHandler *dbms_handler,
                                                   const std::optional<utils::UUID> &current_main_uuid,
                                                   slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::TimestampReq req;
  slk::Load(&req, req_reader);
  auto const db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    const storage::replication::TimestampRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::TimestampReq::kType.name);
    const storage::replication::TimestampRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  auto const *storage = db_acc->get()->storage();
  const storage::replication::TimestampRes res{true, storage->repl_storage_state_.last_durable_timestamp_.load()};
  slk::Save(res, res_builder);
}

// The number of applied deltas also includes skipped deltas.
uint64_t InMemoryReplicationHandlers::ReadAndApplyDeltas(storage::InMemoryStorage *storage,
                                                         storage::durability::BaseDecoder *decoder,
                                                         const uint64_t version) {
  auto edge_acc = storage->edges_.access();
  auto vertex_acc = storage->vertices_.access();

  constexpr bool kUniqueAccess = true;
  constexpr bool kSharedAccess = false;

  std::optional<std::pair<uint64_t, storage::InMemoryStorage::ReplicationAccessor>> commit_timestamp_and_accessor;
  auto const get_replication_accessor = [storage, &commit_timestamp_and_accessor](
                                            uint64_t commit_timestamp,
                                            bool const unique =
                                                kSharedAccess) -> storage::InMemoryStorage::ReplicationAccessor * {
    if (!commit_timestamp_and_accessor) {
      std::unique_ptr<storage::Storage::Accessor> acc = nullptr;
      if (unique) {
        acc = storage->UniqueAccess();
      } else {
        acc = storage->Access();
      }
      auto const inmem_acc = std::unique_ptr<storage::InMemoryStorage::InMemoryAccessor>(
          static_cast<storage::InMemoryStorage::InMemoryAccessor *>(acc.release()));
      commit_timestamp_and_accessor.emplace(commit_timestamp, std::move(*inmem_acc));
    } else if (commit_timestamp_and_accessor->first != commit_timestamp) {
      throw utils::BasicException("Received more than one transaction!");
    }
    return &commit_timestamp_and_accessor->second;
  };

  uint64_t current_delta_idx = 0;  // tracks over how many deltas we iterated, includes also skipped deltas.
  uint64_t applied_deltas = 0;     // Non-skipped deltas
  auto max_delta_timestamp = storage->repl_storage_state_.last_durable_timestamp_.load();
  auto current_durable_commit_timestamp = max_delta_timestamp;

  for (bool transaction_complete = false; !transaction_complete; ++current_delta_idx) {
    const auto [delta_timestamp, delta] = ReadDelta(decoder, version);
    max_delta_timestamp = std::max(max_delta_timestamp, delta_timestamp);

    transaction_complete = IsWalDeltaDataTransactionEnd(delta, version);

    if (delta_timestamp <= current_durable_commit_timestamp) {
      spdlog::trace("Skipping delta with timestamp: {}, current durable commit timestamp: {}", delta_timestamp,
                    current_durable_commit_timestamp);
      continue;
    }

    // NOLINTNEXTLINE (google-build-using-namespace)
    using namespace memgraph::storage::durability;
    auto delta_apply = utils::Overloaded{
        [&](WalVertexCreate const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("       Create vertex {}", gid);
          auto *transaction = get_replication_accessor(delta_timestamp);
          if (!transaction->CreateVertexEx(data.gid).has_value()) {
            throw utils::BasicException("Vertex with gid {} already exists at replica.", gid);
          }
        },
        [&](WalVertexDelete const &data) {
          auto const gid = data.gid.AsUint();
          spdlog::trace("       Delete vertex {}", gid);
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
          spdlog::trace("       Vertex {} add label {}", gid, data.label);
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
          spdlog::trace("       Vertex {} remove label {}", gid, data.label);
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
          spdlog::trace("       Vertex {} set property", gid);
          // NOLINTNEXTLINE
          auto *transaction = get_replication_accessor(delta_timestamp);
          // NOLINTNEXTLINE
          auto vertex = transaction->FindVertex(data.gid, View::NEW);
          if (!vertex) {
            throw utils::BasicException("Failed to find vertex {} when setting property.", gid);
          }
          // NOTE: Phase 1 of the text search feature doesn't have replication in scope
          auto ret = vertex->SetProperty(transaction->NameToProperty(data.property), data.value);
          if (ret.HasError()) {
            throw utils::BasicException("Failed to set property label from vertex {}.", gid);
          }
        },
        [&](WalEdgeCreate const &data) {
          auto const edge_gid = data.gid.AsUint();
          auto const from_vertex_gid = data.from_vertex.AsUint();
          auto const to_vertex_gid = data.to_vertex.AsUint();
          spdlog::trace("       Create edge {} of type {} from vertex {} to vertex {}", edge_gid, data.edge_type,
                        from_vertex_gid, to_vertex_gid);
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
          spdlog::trace("       Delete edge {} of type {} from vertex {} to vertex {}", edge_gid, data.edge_type,
                        from_vertex_gid, to_vertex_gid);
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
          spdlog::trace(" Edge {} set property", edge_gid);
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

              auto found_edge = std::ranges::find_if(
                  from_vertex->out_edges,
                  [raw_edge_ref = EdgeRef(&*edge)](auto &in) { return std::get<2>(in) == raw_edge_ref; });
              if (found_edge == from_vertex->out_edges.end()) {
                throw utils::BasicException("Couldn't find edge {} in vertex {}'s out edge collection.", edge_gid,
                                            from_vertex->gid.AsUint());
              }
              const auto &[edge_type, vertex_to, edge_ref] = *found_edge;
              return std::tuple{edge_ref, edge_type, &*from_vertex, vertex_to};
            }
            // fallback if from_gid not available
            auto found_edge = storage->FindEdge(edge->gid);
            if (!found_edge)
              throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
            const auto &[edge_ref, edge_type, vertex_from, vertex_to] = *found_edge;
            return std::tuple{edge_ref, edge_type, vertex_from, vertex_to};
          });

          auto ea = EdgeAccessor{edge_ref, edge_type, from_vertex, vertex_to, storage, &transaction->GetTransaction()};
          auto ret = ea.SetProperty(transaction->NameToProperty(data.property), data.value);
          if (ret.HasError()) {
            throw utils::BasicException("Setting property on edge {} failed.", edge_gid);
          }
        },
        [&](WalTransactionEnd const &) {
          spdlog::trace("       Transaction end");
          if (!commit_timestamp_and_accessor || commit_timestamp_and_accessor->first != delta_timestamp)
            throw utils::BasicException("Invalid commit data!");
          auto ret = commit_timestamp_and_accessor->second.Commit(
              {.desired_commit_timestamp = commit_timestamp_and_accessor->first, .is_main = false});
          if (ret.HasError()) throw utils::BasicException("Committing failed on receiving transaction end delta.");
          commit_timestamp_and_accessor = std::nullopt;
        },
        [&](WalLabelIndexCreate const &data) {
          spdlog::trace("       Create label index on :{}", data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateIndex(storage->NameToLabel(data.label)).HasError())
            throw utils::BasicException("Failed to create label index on :{}.", data.label);
        },
        [&](WalLabelIndexDrop const &data) {
          spdlog::trace("       Drop label index on :{}", data.label);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->DropIndex(storage->NameToLabel(data.label)).HasError())
            throw utils::BasicException("Failed to drop label index on :{}.", data.label);
        },
        [&](WalLabelIndexStatsSet const &data) {
          spdlog::trace("       Set label index statistics on :{}", data.label);
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
          spdlog::trace("       Clear label index statistics on :{}", data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          if (!transaction->DeleteLabelIndexStats(storage->NameToLabel(data.label))) {
            throw utils::BasicException("Failed to clear label index statistics on :{}.", data.label);
          }
        },
        [&](WalLabelPropertyIndexCreate const &data) {
          spdlog::trace("       Create label+property index on :{} ({})", data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateIndex(storage->NameToLabel(data.label), storage->NameToProperty(data.property))
                  .HasError())
            throw utils::BasicException("Failed to create label+property index on :{} ({}).", data.label,
                                        data.property);
        },
        [&](WalLabelPropertyIndexDrop const &data) {
          spdlog::trace("       Drop label+property index on :{} ({})", data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->DropIndex(storage->NameToLabel(data.label), storage->NameToProperty(data.property))
                  .HasError()) {
            throw utils::BasicException("Failed to drop label+property index on :{} ({}).", data.label, data.property);
          }
        },
        [&](WalLabelPropertyIndexStatsSet const &data) {
          spdlog::trace("       Set label-property index statistics on :{}", data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          const auto label = storage->NameToLabel(data.label);
          const auto property = storage->NameToProperty(data.property);
          LabelPropertyIndexStats stats{};
          if (!FromJson(data.json_stats, stats)) {
            throw utils::BasicException("Failed to read statistics!");
          }
          transaction->SetIndexStats(label, property, stats);
        },
        [&](WalLabelPropertyIndexStatsClear const &data) {
          spdlog::trace("       Clear label-property index statistics on :{}", data.label);
          // Need to send the timestamp
          auto *transaction = get_replication_accessor(delta_timestamp);
          transaction->DeleteLabelPropertyIndexStats(storage->NameToLabel(data.label));
        },
        [&](WalEdgeTypeIndexCreate const &data) {
          spdlog::trace("       Create edge index on :{}", data.edge_type);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateIndex(storage->NameToEdgeType(data.edge_type)).HasError()) {
            throw utils::BasicException("Failed to create edge index on :{}.", data.edge_type);
          }
        },
        [&](WalEdgeTypeIndexDrop const &data) {
          spdlog::trace("       Drop edge index on :{}", data.edge_type);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->DropIndex(storage->NameToEdgeType(data.edge_type)).HasError()) {
            throw utils::BasicException("Failed to drop edge index on :{}.", data.edge_type);
          }
        },
        [&](WalEdgeTypePropertyIndexCreate const &data) {
          spdlog::trace("       Create edge index on :{}({})", data.edge_type, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->CreateIndex(storage->NameToEdgeType(data.edge_type), storage->NameToProperty(data.property))
                  .HasError()) {
            throw utils::BasicException("Failed to create edge property index on :{}({}).", data.edge_type,
                                        data.property);
          }
        },
        [&](WalEdgeTypePropertyIndexDrop const &data) {
          spdlog::trace("       Drop edge index on :{}({})", data.edge_type, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          if (transaction->DropIndex(storage->NameToEdgeType(data.edge_type), storage->NameToProperty(data.property))
                  .HasError()) {
            throw utils::BasicException("Failed to drop edge property index on :{}({}).", data.edge_type,
                                        data.property);
          }
        },
        [&](WalTextIndexCreate const &) {
          /* NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)*/
        },
        [&](WalTextIndexDrop const &) {
          /* NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)*/
        },
        [&](WalExistenceConstraintCreate const &data) {
          spdlog::trace("       Create existence constraint on :{} ({})", data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto ret = transaction->CreateExistenceConstraint(storage->NameToLabel(data.label),
                                                            storage->NameToProperty(data.property));
          if (ret.HasError()) {
            throw utils::BasicException("Failed to create existence constraint on :{} ({}).", data.label,
                                        data.property);
          }
        },
        [&](WalExistenceConstraintDrop const &data) {
          spdlog::trace("       Drop existence constraint on :{} ({})", data.label, data.property);
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
          spdlog::trace("       Create unique constraint on :{} ({})", data.label, ss.str());
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
          spdlog::trace("       Drop unique constraint on :{} ({})", data.label, ss.str());
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
          spdlog::trace("       Create IS TYPED {} constraint on :{} ({})",
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
          spdlog::trace("       Drop IS TYPED {} constraint on :{} ({})", TypeConstraintKindToString(data.kind),
                        data.label, data.property);

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
          spdlog::trace("       Create enum {} with values {}", data.etype, ss.str());
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->CreateEnum(data.etype, data.evalues);
          if (res.HasError()) {
            throw utils::BasicException("Failed to create enum {} with values {}.", data.etype, ss.str());
          }
        },
        [&](WalEnumAlterAdd const &data) {
          spdlog::trace("       Alter enum {} add value {}", data.etype, data.evalue);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->EnumAlterAdd(data.etype, data.evalue);
          if (res.HasError()) {
            throw utils::BasicException("Failed to alter enum {} add value {}.", data.etype, data.evalue);
          }
        },
        [&](WalEnumAlterUpdate const &data) {
          spdlog::trace("       Alter enum {} update {} to {}", data.etype, data.evalue_old, data.evalue_new);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->EnumAlterUpdate(data.etype, data.evalue_old, data.evalue_new);
          if (res.HasError()) {
            throw utils::BasicException("Failed to alter enum {} update {} to {}.", data.etype, data.evalue_old,
                                        data.evalue_new);
          }
        },
        [&](WalPointIndexCreate const &data) {
          spdlog::trace("       Create point index on :{}({})", data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto labelId = storage->NameToLabel(data.label);
          auto propId = storage->NameToProperty(data.property);
          auto res = transaction->CreatePointIndex(labelId, propId);
          if (res.HasError()) {
            throw utils::BasicException("Failed to create point index on :{}({})", data.label, data.property);
          }
        },
        [&](WalPointIndexDrop const &data) {
          spdlog::trace("       Drop point index on :{}({})", data.label, data.property);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto labelId = storage->NameToLabel(data.label);
          auto propId = storage->NameToProperty(data.property);
          auto res = transaction->DropPointIndex(labelId, propId);
          if (res.HasError()) {
            throw utils::BasicException("Failed to drop point index on :{}({})", data.label, data.property);
          }
        },
        [&](WalVectorIndexCreate const &data) {
          spdlog::trace("       Create vector index on :{}({})", data.label, data.property);
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
          spdlog::trace("       Drop vector index {} ", data.index_name);
          auto *transaction = get_replication_accessor(delta_timestamp, kUniqueAccess);
          auto res = transaction->DropVectorIndex(data.index_name);
          if (res.HasError()) {
            throw utils::BasicException("Failed to drop vector index {}", data.index_name);
          }
        },
    };

    spdlog::trace("  Delta {}", current_delta_idx);
    std::visit(delta_apply, delta.data_);
    applied_deltas++;
  }

  if (commit_timestamp_and_accessor) throw utils::BasicException("Did not finish the transaction!");

  storage->repl_storage_state_.last_durable_timestamp_ = max_delta_timestamp;

  spdlog::debug("Applied {} deltas", applied_deltas);
  return current_delta_idx;
}
}  // namespace memgraph::dbms
