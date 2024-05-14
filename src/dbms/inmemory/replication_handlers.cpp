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

#include "dbms/inmemory/replication_handlers.hpp"

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "replication/replication_server.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/inmemory/unique_constraints.hpp"

#include <spdlog/spdlog.h>
#include <cstdint>
#include <optional>

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
std::pair<uint64_t, WalDeltaData> ReadDelta(storage::durability::BaseDecoder *decoder) {
  try {
    auto timestamp = ReadWalDeltaHeader(decoder);
    spdlog::trace("       Timestamp {}", timestamp);
    auto delta = ReadWalDeltaData(decoder);
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
    auto *inmem_storage = dynamic_cast<storage::InMemoryStorage *>(acc.get()->storage());
    if (!inmem_storage || inmem_storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      spdlog::error("Database is not IN_MEMORY_TRANSACTIONAL.");
      return std::nullopt;
    }
    return std::optional{std::move(acc)};
  } catch (const dbms::UnknownDatabaseException &e) {
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
  spdlog::info("Set replica data UUID  to main uuid {}", std::string(req.uuid));
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
    storage::replication::HeartbeatRes res{false, 0, ""};
    slk::Save(res, res_builder);
    return;
  }
  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  if (!db_acc.has_value()) {
    spdlog::warn("No database accessor");
    storage::replication::HeartbeatRes res{false, 0, ""};
    slk::Save(res, res_builder);
    return;
  }
  auto const *storage = db_acc->get()->storage();
  storage::replication::HeartbeatRes res{true, storage->repl_storage_state_.last_commit_timestamp_.load(),
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
    storage::replication::AppendDeltasRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    storage::replication::AppendDeltasRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  storage::replication::Decoder decoder(req_reader);

  auto maybe_epoch_id = decoder.ReadString();
  MG_ASSERT(maybe_epoch_id, "Invalid replication message");

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  auto &repl_storage_state = storage->repl_storage_state_;
  if (*maybe_epoch_id != storage->repl_storage_state_.epoch_.id()) {
    auto prev_epoch = storage->repl_storage_state_.epoch_.SetEpoch(*maybe_epoch_id);
    repl_storage_state.AddEpochToHistoryForce(prev_epoch);
  }

  if (storage->wal_file_) {
    if (req.seq_num > storage->wal_file_->SequenceNumber() ||
        *maybe_epoch_id != storage->repl_storage_state_.epoch_.id()) {
      storage->wal_file_->FinalizeWal();
      storage->wal_file_.reset();
      storage->wal_seq_num_ = req.seq_num;
      spdlog::trace("Finalized WAL file");
    } else {
      MG_ASSERT(storage->wal_file_->SequenceNumber() == req.seq_num, "Invalid sequence number of current wal file");
      storage->wal_seq_num_ = req.seq_num + 1;
    }
  } else {
    storage->wal_seq_num_ = req.seq_num;
  }

  if (req.previous_commit_timestamp != repl_storage_state.last_commit_timestamp_.load()) {
    // Empty the stream
    bool transaction_complete = false;
    while (!transaction_complete) {
      SPDLOG_INFO("Skipping delta");
      const auto [timestamp, delta] = ReadDelta(&decoder);
      transaction_complete = storage::durability::IsWalDeltaDataTypeTransactionEnd(
          delta.type,
          storage::durability::kVersion);  // TODO: Check if we are always using the latest version when replicating
    }

    storage::replication::AppendDeltasRes res{false, repl_storage_state.last_commit_timestamp_.load()};
    slk::Save(res, res_builder);
    return;
  }

  ReadAndApplyDelta(
      storage, &decoder,
      storage::durability::kVersion);  // TODO: Check if we are always using the latest version when replicating

  storage::replication::AppendDeltasRes res{true, repl_storage_state.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from append deltas finished, replica is now up to date!");
}

void InMemoryReplicationHandlers::SnapshotHandler(dbms::DbmsHandler *dbms_handler,
                                                  const std::optional<utils::UUID> &current_main_uuid,
                                                  slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::SnapshotReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    storage::replication::SnapshotRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::SnapshotReq::kType.name);
    storage::replication::SnapshotRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  storage::replication::Decoder decoder(req_reader);

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->recovery_.snapshot_directory_);

  const auto maybe_snapshot_path = decoder.ReadFile(storage->recovery_.snapshot_directory_);
  MG_ASSERT(maybe_snapshot_path, "Failed to load snapshot!");
  spdlog::info("Received snapshot saved to {}", *maybe_snapshot_path);

  auto storage_guard = std::unique_lock{storage->main_lock_};
  spdlog::trace("Clearing database since recovering from snapshot.");
  // Clear the database
  storage->vertices_.clear();
  storage->edges_.clear();

  storage->constraints_.existence_constraints_ = std::make_unique<storage::ExistenceConstraints>();
  storage->constraints_.unique_constraints_ = std::make_unique<storage::InMemoryUniqueConstraints>();
  storage->indices_.label_index_ = std::make_unique<storage::InMemoryLabelIndex>();
  storage->indices_.label_property_index_ = std::make_unique<storage::InMemoryLabelPropertyIndex>();
  try {
    spdlog::debug("Loading snapshot");
    auto recovered_snapshot = storage::durability::LoadSnapshot(
        *maybe_snapshot_path, &storage->vertices_, &storage->edges_, &storage->edges_metadata_,
        &storage->repl_storage_state_.history, storage->name_id_mapper_.get(), &storage->edge_count_, storage->config_);
    spdlog::debug("Snapshot loaded successfully");
    // If this step is present it should always be the first step of
    // the recovery so we use the UUID we read from snasphost
    storage->uuid_ = std::move(recovered_snapshot.snapshot_info.uuid);
    storage->repl_storage_state_.epoch_.SetEpoch(std::move(recovered_snapshot.snapshot_info.epoch_id));
    const auto &recovery_info = recovered_snapshot.recovery_info;
    storage->vertex_id_ = recovery_info.next_vertex_id;
    storage->edge_id_ = recovery_info.next_edge_id;
    storage->timestamp_ = std::max(storage->timestamp_, recovery_info.next_timestamp);

    spdlog::trace("Recovering indices and constraints from snapshot.");
    memgraph::storage::durability::RecoverIndicesAndStats(recovered_snapshot.indices_constraints.indices,
                                                          &storage->indices_, &storage->vertices_,
                                                          storage->name_id_mapper_.get());
    memgraph::storage::durability::RecoverConstraints(recovered_snapshot.indices_constraints.constraints,
                                                      &storage->constraints_, &storage->vertices_,
                                                      storage->name_id_mapper_.get());
  } catch (const storage::durability::RecoveryFailure &e) {
    LOG_FATAL("Couldn't load the snapshot because of: {}", e.what());
  }
  storage_guard.unlock();

  storage::replication::SnapshotRes res{true, storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);

  spdlog::trace("Deleting old snapshot files due to snapshot recovery.");
  // Delete other durability files
  auto snapshot_files = storage::durability::GetSnapshotFiles(storage->recovery_.snapshot_directory_, storage->uuid_);
  for (const auto &[path, uuid, _] : snapshot_files) {
    if (path != *maybe_snapshot_path) {
      spdlog::trace("Deleting snapshot file {}", path);
      storage->file_retainer_.DeleteFile(path);
    }
  }

  spdlog::trace("Deleting old WAL files due to snapshot recovery.");
  auto wal_files = storage::durability::GetWalFiles(storage->recovery_.wal_directory_, storage->uuid_);
  if (wal_files) {
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
    storage::replication::ForceResetStorageRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::SnapshotReq::kType.name);
    storage::replication::ForceResetStorageRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  storage::replication::Decoder decoder(req_reader);

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  auto storage_guard = std::unique_lock{storage->main_lock_};

  // Clear the database
  storage->vertices_.clear();
  storage->edges_.clear();
  storage->commit_log_.reset();
  storage->commit_log_.emplace();

  storage->constraints_.existence_constraints_ = std::make_unique<storage::ExistenceConstraints>();
  storage->constraints_.unique_constraints_ = std::make_unique<storage::InMemoryUniqueConstraints>();
  storage->indices_.label_index_ = std::make_unique<storage::InMemoryLabelIndex>();
  storage->indices_.label_property_index_ = std::make_unique<storage::InMemoryLabelPropertyIndex>();

  // Fine since we will force push when reading from WAL just random epoch with 0 timestamp, as it should be if it
  // acted as MAIN before
  storage->repl_storage_state_.epoch_.SetEpoch(std::string(utils::UUID{}));
  storage->repl_storage_state_.last_commit_timestamp_ = 0;

  storage->repl_storage_state_.history.clear();
  storage->vertex_id_ = 0;
  storage->edge_id_ = 0;
  storage->timestamp_ = storage::kTimestampInitialId;

  storage->CollectGarbage<true>(std::move(storage_guard), false);
  storage->vertices_.run_gc();
  storage->edges_.run_gc();

  storage::replication::ForceResetStorageRes res{true, storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);

  spdlog::trace("Deleting old snapshot files.");
  // Delete other durability files
  auto snapshot_files = storage::durability::GetSnapshotFiles(storage->recovery_.snapshot_directory_, storage->uuid_);
  for (const auto &[path, uuid, _] : snapshot_files) {
    spdlog::trace("Deleting snapshot file {}", path);
    storage->file_retainer_.DeleteFile(path);
  }

  spdlog::trace("Deleting old WAL files.");
  auto wal_files = storage::durability::GetWalFiles(storage->recovery_.wal_directory_, storage->uuid_);
  if (wal_files) {
    for (const auto &wal_file : *wal_files) {
      spdlog::trace("Deleting WAL file {}", wal_file.path);
      storage->file_retainer_.DeleteFile(wal_file.path);
    }

    storage->wal_file_.reset();
  }
}

void InMemoryReplicationHandlers::WalFilesHandler(dbms::DbmsHandler *dbms_handler,
                                                  const std::optional<utils::UUID> &current_main_uuid,
                                                  slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::WalFilesReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    storage::replication::WalFilesRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }
  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::WalFilesReq::kType.name);
    storage::replication::WalFilesRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  const auto wal_file_number = req.file_number;
  spdlog::debug("Received WAL files: {}", wal_file_number);

  storage::replication::Decoder decoder(req_reader);

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->recovery_.wal_directory_);

  for (auto i = 0; i < wal_file_number; ++i) {
    LoadWal(storage, &decoder);
  }

  storage::replication::WalFilesRes res{true, storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from WAL files ended successfully, replica is now up to date!");
}

void InMemoryReplicationHandlers::CurrentWalHandler(dbms::DbmsHandler *dbms_handler,
                                                    const std::optional<utils::UUID> &current_main_uuid,
                                                    slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::CurrentWalReq req;
  slk::Load(&req, req_reader);
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    storage::replication::CurrentWalRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::CurrentWalReq::kType.name);
    storage::replication::CurrentWalRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  storage::replication::Decoder decoder(req_reader);

  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());
  utils::EnsureDirOrDie(storage->recovery_.wal_directory_);

  LoadWal(storage, &decoder);

  storage::replication::CurrentWalRes res{true, storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
  spdlog::debug("Replication recovery from current WAL ended successfully, replica is now up to date!");
}

void InMemoryReplicationHandlers::LoadWal(storage::InMemoryStorage *storage, storage::replication::Decoder *decoder) {
  const auto temp_wal_directory =
      std::filesystem::temp_directory_path() / "memgraph" / storage::durability::kWalDirectory;
  utils::EnsureDir(temp_wal_directory);
  auto maybe_wal_path = decoder->ReadFile(temp_wal_directory);
  MG_ASSERT(maybe_wal_path, "Failed to load WAL!");
  spdlog::trace("Received WAL saved to {}", *maybe_wal_path);
  try {
    auto wal_info = storage::durability::ReadWalInfo(*maybe_wal_path);
    if (wal_info.seq_num == 0) {
      storage->uuid_ = wal_info.uuid;
    }
    auto &replica_epoch = storage->repl_storage_state_.epoch_;
    if (wal_info.epoch_id != replica_epoch.id()) {
      // questionable behaviour, we trust that any change in epoch implies change in who is MAIN
      // when we use high availability, this assumption need to be checked.
      auto prev_epoch = replica_epoch.SetEpoch(wal_info.epoch_id);
      storage->repl_storage_state_.AddEpochToHistoryForce(prev_epoch);
    }

    if (storage->wal_file_) {
      if (storage->wal_file_->SequenceNumber() != wal_info.seq_num) {
        storage->wal_file_->FinalizeWal();
        storage->wal_seq_num_ = wal_info.seq_num;
        storage->wal_file_.reset();
        spdlog::trace("WAL file {} finalized successfully", *maybe_wal_path);
      }
    } else {
      storage->wal_seq_num_ = wal_info.seq_num;
    }
    spdlog::trace("Loading WAL deltas from {}", *maybe_wal_path);
    storage::durability::Decoder wal;
    const auto version = wal.Initialize(*maybe_wal_path, storage::durability::kWalMagic);
    spdlog::debug("WAL file {} loaded successfully", *maybe_wal_path);
    if (!version) throw storage::durability::RecoveryFailure("Couldn't read WAL magic and/or version!");
    if (!storage::durability::IsVersionSupported(*version))
      throw storage::durability::RecoveryFailure("Invalid WAL version!");
    wal.SetPosition(wal_info.offset_deltas);

    for (size_t i = 0; i < wal_info.num_deltas;) {
      i += ReadAndApplyDelta(storage, &wal, *version);
    }

    spdlog::debug("Replication from current WAL successful!");
  } catch (const storage::durability::RecoveryFailure &e) {
    LOG_FATAL("Couldn't recover WAL deltas from {} because of: {}", *maybe_wal_path, e.what());
  }
}

void InMemoryReplicationHandlers::TimestampHandler(dbms::DbmsHandler *dbms_handler,
                                                   const std::optional<utils::UUID> &current_main_uuid,
                                                   slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::TimestampReq req;
  slk::Load(&req, req_reader);
  auto const db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    storage::replication::TimestampRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  if (!current_main_uuid.has_value() || req.main_uuid != current_main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::TimestampReq::kType.name);
    storage::replication::CurrentWalRes res{false, 0};
    slk::Save(res, res_builder);
    return;
  }

  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  auto const *storage = db_acc->get()->storage();
  storage::replication::TimestampRes res{true, storage->repl_storage_state_.last_commit_timestamp_.load()};
  slk::Save(res, res_builder);
}

// The number of applied deltas also includes skipped deltas.
uint64_t InMemoryReplicationHandlers::ReadAndApplyDelta(storage::InMemoryStorage *storage,
                                                        storage::durability::BaseDecoder *decoder,
                                                        const uint64_t version) {
  auto edge_acc = storage->edges_.access();
  auto vertex_acc = storage->vertices_.access();

  constexpr bool kUniqueAccess = true;
  constexpr bool kSharedAccess = false;

  std::optional<std::pair<uint64_t, storage::InMemoryStorage::ReplicationAccessor>> commit_timestamp_and_accessor;
  auto const get_transaction = [storage, &commit_timestamp_and_accessor](
                                   uint64_t commit_timestamp,
                                   bool unique = kSharedAccess) -> storage::InMemoryStorage::ReplicationAccessor * {
    if (!commit_timestamp_and_accessor) {
      std::unique_ptr<storage::Storage::Accessor> acc = nullptr;
      if (unique) {
        acc = storage->UniqueAccess(ReplicationRole::REPLICA);
      } else {
        acc = storage->Access(ReplicationRole::REPLICA);
      }
      auto inmem_acc = std::unique_ptr<storage::InMemoryStorage::InMemoryAccessor>(
          static_cast<storage::InMemoryStorage::InMemoryAccessor *>(acc.release()));
      commit_timestamp_and_accessor.emplace(commit_timestamp, std::move(*inmem_acc));
    } else if (commit_timestamp_and_accessor->first != commit_timestamp) {
      throw utils::BasicException("Received more than one transaction!");
    }
    return &commit_timestamp_and_accessor->second;
  };

  uint64_t current_delta_idx = 0;  // tracks over how many deltas we iterated, includes also skipped deltas.
  uint64_t applied_deltas = 0;     // Non-skipped deltas
  auto max_commit_timestamp = storage->repl_storage_state_.last_commit_timestamp_.load();

  for (bool transaction_complete = false; !transaction_complete; ++current_delta_idx) {
    const auto [timestamp, delta] = ReadDelta(decoder);
    if (timestamp > max_commit_timestamp) {
      max_commit_timestamp = timestamp;
    }

    transaction_complete = storage::durability::IsWalDeltaDataTypeTransactionEnd(delta.type, version);

    if (timestamp < storage->timestamp_) {
      spdlog::trace("Skipping delta {} {}", timestamp, storage->timestamp_);
      continue;
    }
    spdlog::trace("  Delta {}", current_delta_idx);
    switch (delta.type) {
      case WalDeltaData::Type::VERTEX_CREATE: {
        spdlog::trace("       Create vertex {}", delta.vertex_create_delete.gid.AsUint());
        auto *transaction = get_transaction(timestamp);
        transaction->CreateVertexEx(delta.vertex_create_delete.gid);
        break;
      }
      case WalDeltaData::Type::VERTEX_DELETE: {
        spdlog::trace("       Delete vertex {}", delta.vertex_create_delete.gid.AsUint());
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_create_delete.gid, View::NEW);
        if (!vertex)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        auto ret = transaction->DeleteVertex(&*vertex);
        if (ret.HasError() || !ret.GetValue())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::VERTEX_ADD_LABEL: {
        spdlog::trace("       Vertex {} add label {}", delta.vertex_add_remove_label.gid.AsUint(),
                      delta.vertex_add_remove_label.label);
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_add_remove_label.gid, View::NEW);
        if (!vertex)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        // NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)
        auto ret = vertex->AddLabel(transaction->NameToLabel(delta.vertex_add_remove_label.label));
        if (ret.HasError() || !ret.GetValue())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::VERTEX_REMOVE_LABEL: {
        spdlog::trace("       Vertex {} remove label {}", delta.vertex_add_remove_label.gid.AsUint(),
                      delta.vertex_add_remove_label.label);
        auto *transaction = get_transaction(timestamp);
        auto vertex = transaction->FindVertex(delta.vertex_add_remove_label.gid, View::NEW);
        if (!vertex)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        // NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)
        auto ret = vertex->RemoveLabel(transaction->NameToLabel(delta.vertex_add_remove_label.label));
        if (ret.HasError() || !ret.GetValue())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::VERTEX_SET_PROPERTY: {
        spdlog::trace("       Vertex {} set property", delta.vertex_edge_set_property.gid.AsUint());
        // NOLINTNEXTLINE
        auto *transaction = get_transaction(timestamp);
        // NOLINTNEXTLINE
        auto vertex = transaction->FindVertex(delta.vertex_edge_set_property.gid, View::NEW);
        if (!vertex)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        // NOTE: Phase 1 of the text search feature doesn't have replication in scope
        auto ret = vertex->SetProperty(transaction->NameToProperty(delta.vertex_edge_set_property.property),
                                       delta.vertex_edge_set_property.value);
        if (ret.HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::EDGE_CREATE: {
        spdlog::trace("       Create edge {} of type {} from vertex {} to vertex {}",
                      delta.edge_create_delete.gid.AsUint(), delta.edge_create_delete.edge_type,
                      delta.edge_create_delete.from_vertex.AsUint(), delta.edge_create_delete.to_vertex.AsUint());
        auto *transaction = get_transaction(timestamp);
        auto from_vertex = transaction->FindVertex(delta.edge_create_delete.from_vertex, View::NEW);
        if (!from_vertex)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        auto to_vertex = transaction->FindVertex(delta.edge_create_delete.to_vertex, View::NEW);
        if (!to_vertex)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        auto edge = transaction->CreateEdgeEx(&*from_vertex, &*to_vertex,
                                              transaction->NameToEdgeType(delta.edge_create_delete.edge_type),
                                              delta.edge_create_delete.gid);
        if (edge.HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::EDGE_DELETE: {
        spdlog::trace("       Delete edge {} of type {} from vertex {} to vertex {}",
                      delta.edge_create_delete.gid.AsUint(), delta.edge_create_delete.edge_type,
                      delta.edge_create_delete.from_vertex.AsUint(), delta.edge_create_delete.to_vertex.AsUint());
        auto *transaction = get_transaction(timestamp);
        auto from_vertex = transaction->FindVertex(delta.edge_create_delete.from_vertex, View::NEW);
        if (!from_vertex)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        auto to_vertex = transaction->FindVertex(delta.edge_create_delete.to_vertex, View::NEW);
        if (!to_vertex)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        auto edgeType = transaction->NameToEdgeType(delta.edge_create_delete.edge_type);
        auto edge =
            transaction->FindEdge(delta.edge_create_delete.gid, View::NEW, edgeType, &*from_vertex, &*to_vertex);
        if (!edge) throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        if (auto ret = transaction->DeleteEdge(&*edge); ret.HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::EDGE_SET_PROPERTY: {
        spdlog::trace(" Edge {} set property", delta.vertex_edge_set_property.gid.AsUint());
        if (!storage->config_.salient.items.properties_on_edges)
          throw utils::BasicException(
              "Can't set properties on edges because properties on edges "
              "are disabled!");

        auto *transaction = get_transaction(timestamp);

        // The following block of code effectively implements `FindEdge` and
        // yields an accessor that is only valid for managing the edge's
        // properties.
        auto edge = edge_acc.find(delta.vertex_edge_set_property.gid);
        if (edge == edge_acc.end())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
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
          if (!is_visible)
            throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        }
        EdgeRef edge_ref(&*edge);
        // Here we create an edge accessor that we will use to get the
        // properties of the edge. The accessor is created with an invalid
        // type and invalid from/to pointers because we don't know them
        // here, but that isn't an issue because we won't use that part of
        // the API here.
        auto ea = EdgeAccessor{edge_ref, EdgeTypeId::FromUint(0UL),     nullptr, nullptr,
                               storage,  &transaction->GetTransaction()};

        auto ret = ea.SetProperty(transaction->NameToProperty(delta.vertex_edge_set_property.property),
                                  delta.vertex_edge_set_property.value);
        if (ret.HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }

      case WalDeltaData::Type::TRANSACTION_END: {
        spdlog::trace("       Transaction end");
        if (!commit_timestamp_and_accessor || commit_timestamp_and_accessor->first != timestamp)
          throw utils::BasicException("Invalid commit data!");
        auto ret = commit_timestamp_and_accessor->second.Commit(
            {.desired_commit_timestamp = commit_timestamp_and_accessor->first, .is_main = false});
        if (ret.HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        commit_timestamp_and_accessor = std::nullopt;
        break;
      }

      case WalDeltaData::Type::LABEL_INDEX_CREATE: {
        spdlog::trace("       Create label index on :{}", delta.operation_label.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction->CreateIndex(storage->NameToLabel(delta.operation_label.label)).HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::LABEL_INDEX_DROP: {
        spdlog::trace("       Drop label index on :{}", delta.operation_label.label);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction->DropIndex(storage->NameToLabel(delta.operation_label.label)).HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::LABEL_INDEX_STATS_SET: {
        spdlog::trace("       Set label index statistics on :{}", delta.operation_label_stats.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        const auto label = storage->NameToLabel(delta.operation_label_stats.label);
        LabelIndexStats stats{};
        if (!FromJson(delta.operation_label_stats.stats, stats)) {
          throw utils::BasicException("Failed to read statistics!");
        }
        transaction->SetIndexStats(label, stats);
        break;
      }
      case WalDeltaData::Type::LABEL_INDEX_STATS_CLEAR: {
        const auto &info = delta.operation_label;
        spdlog::trace("       Clear label index statistics on :{}", info.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        transaction->DeleteLabelIndexStats(storage->NameToLabel(info.label));
        break;
      }
      case WalDeltaData::Type::LABEL_PROPERTY_INDEX_CREATE: {
        spdlog::trace("       Create label+property index on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction
                ->CreateIndex(storage->NameToLabel(delta.operation_label_property.label),
                              storage->NameToProperty(delta.operation_label_property.property))
                .HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::LABEL_PROPERTY_INDEX_DROP: {
        spdlog::trace("       Drop label+property index on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction
                ->DropIndex(storage->NameToLabel(delta.operation_label_property.label),
                            storage->NameToProperty(delta.operation_label_property.property))
                .HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_SET: {
        const auto &info = delta.operation_label_property_stats;
        spdlog::trace("       Set label-property index statistics on :{}", info.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        const auto label = storage->NameToLabel(info.label);
        const auto property = storage->NameToProperty(info.property);
        LabelPropertyIndexStats stats{};
        if (!FromJson(info.stats, stats)) {
          throw utils::BasicException("Failed to read statistics!");
        }
        transaction->SetIndexStats(label, property, stats);
        break;
      }
      case WalDeltaData::Type::LABEL_PROPERTY_INDEX_STATS_CLEAR: {
        const auto &info = delta.operation_label;
        spdlog::trace("       Clear label-property index statistics on :{}", info.label);
        // Need to send the timestamp
        auto *transaction = get_transaction(timestamp);
        transaction->DeleteLabelPropertyIndexStats(storage->NameToLabel(info.label));
        break;
      }
      case WalDeltaData::Type::EDGE_INDEX_CREATE: {
        spdlog::trace("       Create edge index on :{}", delta.operation_edge_type.edge_type);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction->CreateIndex(storage->NameToEdgeType(delta.operation_label.label)).HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::EDGE_INDEX_DROP: {
        spdlog::trace("       Drop edge index on :{}", delta.operation_edge_type.edge_type);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction->DropIndex(storage->NameToEdgeType(delta.operation_label.label)).HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::TEXT_INDEX_CREATE: {
        // NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)
        break;
      }
      case WalDeltaData::Type::TEXT_INDEX_DROP: {
        // NOTE: Text search doesn’t have replication in scope yet (Phases 1 and 2)
        break;
      }
      case WalDeltaData::Type::EXISTENCE_CONSTRAINT_CREATE: {
        spdlog::trace("       Create existence constraint on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        auto ret =
            transaction->CreateExistenceConstraint(storage->NameToLabel(delta.operation_label_property.label),
                                                   storage->NameToProperty(delta.operation_label_property.property));
        if (ret.HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::EXISTENCE_CONSTRAINT_DROP: {
        spdlog::trace("       Drop existence constraint on :{} ({})", delta.operation_label_property.label,
                      delta.operation_label_property.property);
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        if (transaction
                ->DropExistenceConstraint(storage->NameToLabel(delta.operation_label_property.label),
                                          storage->NameToProperty(delta.operation_label_property.property))
                .HasError())
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::UNIQUE_CONSTRAINT_CREATE: {
        std::stringstream ss;
        utils::PrintIterable(ss, delta.operation_label_properties.properties);
        spdlog::trace("       Create unique constraint on :{} ({})", delta.operation_label_properties.label, ss.str());
        std::set<PropertyId> properties;
        for (const auto &prop : delta.operation_label_properties.properties) {
          properties.emplace(storage->NameToProperty(prop));
        }
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        auto ret = transaction->CreateUniqueConstraint(storage->NameToLabel(delta.operation_label_properties.label),
                                                       properties);
        if (!ret.HasValue() || ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS)
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        break;
      }
      case WalDeltaData::Type::UNIQUE_CONSTRAINT_DROP: {
        std::stringstream ss;
        utils::PrintIterable(ss, delta.operation_label_properties.properties);
        spdlog::trace("       Drop unique constraint on :{} ({})", delta.operation_label_properties.label, ss.str());
        std::set<PropertyId> properties;
        for (const auto &prop : delta.operation_label_properties.properties) {
          properties.emplace(storage->NameToProperty(prop));
        }
        auto *transaction = get_transaction(timestamp, kUniqueAccess);
        auto ret =
            transaction->DropUniqueConstraint(storage->NameToLabel(delta.operation_label_properties.label), properties);
        if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
          throw utils::BasicException("Invalid transaction! Please raise an issue, {}:{}", __FILE__, __LINE__);
        }
        break;
      }
    }
    applied_deltas++;
  }

  if (commit_timestamp_and_accessor) throw utils::BasicException("Did not finish the transaction!");

  storage->repl_storage_state_.last_commit_timestamp_ = max_commit_timestamp;

  spdlog::debug("Applied {} deltas", applied_deltas);
  return current_delta_idx;
}
}  // namespace memgraph::dbms
