// Copyright 2026 Memgraph Ltd.
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
#include "dbms/inmemory/two_pc_commit_cache.hpp"
#include "memory/db_arena_fwd.hpp"
#include "rpc/file_replication_handler.hpp"
#include "rpc/progress_heartbeat.hpp"
#include "rpc/protocol.hpp"
#include "rpc/utils.hpp"  // Include after all SLK definitions are present
#include "storage/v2/durability/snapshot.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/file.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/readable_size.hpp"

#include <spdlog/spdlog.h>
#include <optional>
#include <range/v3/view/filter.hpp>

using namespace std::chrono_literals;

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::dbms {

namespace {

// progress hearbeat is turned on every 25s
constexpr auto kWaitForMainLockTimeout = 20s;

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

void DeleteFiles(std::vector<std::filesystem::path> const &files, utils::FileRetainer *file_retainer) {
  for (auto const &path : files) {
    spdlog::trace("Deleting file: {}", path);
    file_retainer->DeleteFile(path);
  }
}

// Move snapshots and WALs
void MoveDurabilityFiles(std::vector<std::filesystem::path> const &snapshot_files,
                         std::filesystem::path const &backup_snapshot_dir,
                         std::vector<std::filesystem::path> const &wal_files,
                         std::filesystem::path const &backup_wal_dir, utils::FileRetainer *file_retainer) {
  // Move snapshots
  MoveFiles(snapshot_files, backup_snapshot_dir, file_retainer);
  // Move WAL files
  MoveFiles(wal_files, backup_wal_dir, file_retainer);
  // Clean DIR
  RemoveDirIfEmpty(backup_snapshot_dir);
  RemoveDirIfEmpty(backup_wal_dir);
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

void ProcessOldDurableFiles(bool const reset_needed, std::filesystem::path const &current_snapshot_dir,
                            std::filesystem::path const &current_wal_dir,
                            std::vector<std::filesystem::path> const &old_wal_files,
                            utils::FileRetainer *file_retainer) {
  if (reset_needed) {
    auto const old_snapshot_files = utils::GetFilesFromDir(current_snapshot_dir);

    if (FLAGS_storage_backup_dir_enabled) {
      auto const maybe_backup_dirs = CreateBackupDirectories(current_snapshot_dir, current_wal_dir);
      if (!maybe_backup_dirs) {
        spdlog::error("Couldn't create backup directories. Old durable files won't be moved to .old directory.");
        return;
      }
      auto const &[backup_snapshot_dir, backup_wal_dir] = *maybe_backup_dirs;

      MoveDurabilityFiles(old_snapshot_files, backup_snapshot_dir, old_wal_files, backup_wal_dir, file_retainer);
    } else {
      DeleteFiles(old_snapshot_files, file_retainer);
      DeleteFiles(old_wal_files, file_retainer);
    }
  }
}

// Size of a durability file received from main, for logging. Unknown when the file cannot be stat'ed.
std::string ReceivedFileSize(std::filesystem::path const &path) {
  std::error_code ec;
  auto const size = std::filesystem::file_size(path, ec);
  if (ec) return "unknown size";
  return fmt::format("{} ({} bytes)", utils::GetReadableSize(static_cast<double>(size)), size);
}

storage::WalApplyOptions MakeApplyOptions(rpc::ProgressHeartbeat &heartbeat, storage::WalApplyMode const mode) {
  return {.mode = mode,
          .on_progress = [&heartbeat]() { heartbeat.RecordProgress(); },
          .should_cancel = [&heartbeat]() { return heartbeat.PeerGone(); }};
}

// Drains the current transaction's remaining deltas from the stream and replies with a failed
// PrepareCommit, so the main sees a clean rejection and (re-)drives recovery. Used when the replica cannot
// apply this transaction (the tenant is broken, or its previous commit timestamp is ahead of the request).
void DrainAndRejectPrepareCommit(storage::replication::Decoder &decoder, uint64_t const request_version,
                                 slk::Builder *res_builder, std::string_view db_name) {
  bool transaction_complete{false};
  while (!transaction_complete) {
    const auto [_, delta] = storage::ReadWalDelta(&decoder, storage::durability::kVersion);
    transaction_complete = IsWalDeltaDataTransactionEnd(delta, storage::durability::kVersion);
  }
  const storage::replication::PrepareCommitRes res{false};
  rpc::SendFinalResponse(res, request_version, res_builder, fmt::format("db: {}", db_name));
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
    const memory::DbArenaScope db_arena_scope{acc.get()};
    auto const *inmem_storage = static_cast<storage::InMemoryStorage *>(acc.get()->storage());
    if (!inmem_storage || inmem_storage->storage_mode_ != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      spdlog::error("Database is not IN_MEMORY_TRANSACTIONAL.");
      return std::nullopt;
    }
    return std::optional{std::move(acc)};
  } catch (const dbms::UnknownDatabaseException &) {
    // A data/recovery RPC referenced a tenant that is not HOT on this replica (absent, or held COLD).
    // We deliberately do NOT reheat it inline. The cluster's correctness model is recovery-driven: when an
    // incoming RPC cannot be executed because this node is in a different state than MAIN expects, the
    // replica FAILS the operation and lets MAIN's recovery converge it. A COLD tenant must be brought HOT
    // only by the ordered RESUME system RPC, never as a side effect of a lagging/reordered data delta —
    // reheating here would let the replica self-heal off MAIN's authoritative hot/cold map and drift.
    // Returning nullopt fails the delta and surfaces the divergence to MAIN.
    spdlog::warn("No HOT database with UUID \"{}\" on replica; failing the delta for MAIN to recover.",
                 std::string{uuid});
    return std::nullopt;
  }
}

void LogWrongMain(utils::UUID const &current_main_uuid, const utils::UUID &main_req_id, std::string_view rpc_req) {
  spdlog::error("Received {} with main_id: {} != current_main_uuid: {}",
                rpc_req,
                std::string(main_req_id),
                std::string(current_main_uuid));
}

}  // namespace

void InMemoryReplicationHandlers::Register(
    dbms::DbmsHandler *dbms_handler,
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
    replication::RoleReplicaData &data) {
  auto &server = *data.server;
  auto const &current_main_uuid = data.uuid_;
  auto &heartbeat = server.progress_heartbeat_;
  server.rpc_server_.Register<storage::replication::HeartbeatRpc>(
      [&current_main_uuid, dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        InMemoryReplicationHandlers::HeartbeatHandler(
            dbms_handler, current_main_uuid, request_version, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::PrepareCommitRpc>(
      [&current_main_uuid, dbms_handler, &repl_state, &heartbeat](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        InMemoryReplicationHandlers::PrepareCommitHandler(
            repl_state, dbms_handler, current_main_uuid, heartbeat, request_version, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::FinalizeCommitRpc>(
      [&current_main_uuid, dbms_handler](
          std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        InMemoryReplicationHandlers::FinalizeCommitHandler(
            dbms_handler, current_main_uuid, request_version, req_reader, res_builder);
      });
  server.rpc_server_.Register<storage::replication::SnapshotRpc>(
      [&current_main_uuid, dbms_handler, &heartbeat](
          std::optional<rpc::FileReplicationHandler> const &file_replication_handler,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        MG_ASSERT(file_replication_handler.has_value(), "File replication handler not prepared for SnapshotHandler");
        InMemoryReplicationHandlers::SnapshotHandler(*file_replication_handler,
                                                     dbms_handler,
                                                     current_main_uuid,
                                                     heartbeat,
                                                     request_version,
                                                     req_reader,
                                                     res_builder);
      });
  server.rpc_server_.Register<storage::replication::WalFilesRpc>(
      [&current_main_uuid, dbms_handler, &repl_state, &heartbeat](
          std::optional<rpc::FileReplicationHandler> const &file_replication_handler,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        MG_ASSERT(file_replication_handler.has_value(), "File replication handler not prepared for WalFilesHandler");
        InMemoryReplicationHandlers::WalFilesHandler(repl_state,
                                                     *file_replication_handler,
                                                     dbms_handler,
                                                     current_main_uuid,
                                                     heartbeat,
                                                     request_version,
                                                     req_reader,
                                                     res_builder);
      });
  server.rpc_server_.Register<storage::replication::CurrentWalRpc>(
      [&current_main_uuid, dbms_handler, &repl_state, &heartbeat](
          std::optional<rpc::FileReplicationHandler> const &file_replication_handler,
          uint64_t const request_version,
          auto *req_reader,
          auto *res_builder) {
        MG_ASSERT(file_replication_handler.has_value(), "File replication handle not prepared for CurrentWalHandler");
        InMemoryReplicationHandlers::CurrentWalHandler(repl_state,
                                                       *file_replication_handler,
                                                       dbms_handler,
                                                       current_main_uuid,
                                                       heartbeat,
                                                       request_version,
                                                       req_reader,
                                                       res_builder);
      });
  server.rpc_server_.Register<replication_coordination_glue::SwapMainUUIDRpc>(
      [&repl_state](std::optional<rpc::FileReplicationHandler> const & /*file_replication_handler*/,
                    uint64_t const request_version,
                    auto *req_reader,
                    auto *res_builder) {
        InMemoryReplicationHandlers::SwapMainUUIDHandler(repl_state, request_version, req_reader, res_builder);
      });
}

// Try to take immediately snapshot lock. If we failed, then try to abort snapshot and take the lock
// again. If we succeeded in taking the lock, return true, otherwise false.
auto InMemoryReplicationHandlers::TakeSnapshotLock(auto &snapshot_guard, storage::InMemoryStorage *storage) -> bool {
  if (snapshot_guard.try_lock()) return true;

  spdlog::info("snapshot lock contention: another snapshot is in progress, requesting abort");

  // abort_snapshot_ will be reset to false in CreateSnapshot in storage.cpp at the end of its execution with
  // OnScopeExit block
  storage->abort_snapshot_.store(true, std::memory_order_release);

  constexpr auto timeout = std::chrono::seconds{10};

  // If after aborting the snapshot we still cannot obtain the snapshot lock, then reply to the main that changes
  // cannot be accepted
  return snapshot_guard.try_lock_for(timeout);
}

void InMemoryReplicationHandlers::SwapMainUUIDHandler(
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
    uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  auto locked_repl_state = repl_state.Lock();

  if (!locked_repl_state->IsReplica()) {
    spdlog::error("Setting main uuid must be performed on replica.");
    rpc::SendFinalResponse(replication_coordination_glue::SwapMainUUIDRes{false}, request_version, res_builder);
    return;
  }

  auto &replica_data = locked_repl_state->GetReplicaRole();

  replication_coordination_glue::SwapMainUUIDReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  spdlog::info("Set replica data UUID to main uuid {}", std::string(req.uuid));
  locked_repl_state->TryPersistRoleReplica(replica_data.config, req.uuid);
  replica_data.uuid_ = req.uuid;

  rpc::SendFinalResponse(replication_coordination_glue::SwapMainUUIDRes{true}, request_version, res_builder);
}

void InMemoryReplicationHandlers::HeartbeatHandler(dbms::DbmsHandler *dbms_handler,
                                                   utils::UUID const &current_main_uuid, uint64_t const request_version,
                                                   slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::HeartbeatReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  // Reject a deposed-MAIN RPC before any tenant work (defence-in-depth; GetDatabaseAccessor does not reheat).
  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::HeartbeatReq::kType.name);
    const storage::replication::HeartbeatRes res{false, 0, "", 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  // TODO: this handler is agnostic of InMemory, move to be reused by on-disk
  if (!db_acc) {
    spdlog::warn("No database accessor");
    storage::replication::HeartbeatRes const res{false, 0, "", 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }
  // Move db acc
  const memory::DbArenaScope db_arena_scope{db_acc->get()};
  auto *storage = db_acc->get()->storage();
  auto const commit_info = storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);

  // A broken tenant is empty (Clear()ed to ldt 0). When the main is also empty (main_commit_timestamp 0) there is
  // nothing to recover: the replica already matches the main, so the recovery-path handlers that normally clear the
  // broken flag are never driven. Reconcile here by clearing the flag so the replica can accept the main's first
  // write. Without this, a STRICT_SYNC write to a never-written tenant would 2PC-abort on the broken guard forever and
  // the main's ldt could never advance to trigger recovery.
  if (storage->IsBroken() && req.main_commit_timestamp == memgraph::storage::kTimestampInitialId &&
      commit_info.ldt_ == memgraph::storage::kTimestampInitialId) [[unlikely]] {
    spdlog::info("Clearing broken flag for db {} after reconciling against an empty main.", storage->name());
    storage->SetBroken(false);
  }

  auto const last_epoch_with_commit = std::invoke([storage, ldt = commit_info.ldt_]() -> std::string {
    if (auto const &history = storage->repl_storage_state_.history; !history.empty()) {
      auto [history_epoch, history_ldt] = history.back();
      return history_ldt != ldt ? std::string{storage->repl_storage_state_.epoch_.id()} : history_epoch;
    }
    return std::string{storage->repl_storage_state_.epoch_.id()};
  });

  const storage::replication::HeartbeatRes res{
      true, commit_info.ldt_, last_epoch_with_commit, commit_info.num_committed_txns_};
  rpc::SendFinalResponse(res, request_version, res_builder, storage->name());
}

void InMemoryReplicationHandlers::PrepareCommitHandler(
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
    dbms::DbmsHandler *dbms_handler, utils::UUID const &current_main_uuid, rpc::ProgressHeartbeat &heartbeat,
    uint64_t const request_version, slk::Reader *req_reader, slk::Builder *res_builder) {
  // It is important to take repl state lock immediately at the start of the handler. In that way it cannot happen that
  // a main promotion starts executing while this handler is executing. In this way we have a guarantee: If I am able to
  // take the read lock on repl state, main promotion will start after committing is finished.
  // If I am unable to take the read lock, then something else of a higher priority is running and I should commit
  // later.
  auto const maybe_locked_repl_state =
      std::invoke([&repl_state]() -> std::optional<decltype(repl_state.TryReadLock())> {
        try {
          return repl_state.TryReadLock();
        } catch (utils::TryLockException const &) {
          spdlog::warn("Failed to take repl state read lock, cannot commit");
          return std::nullopt;
        }
      });

  if (!maybe_locked_repl_state.has_value()) {
    const storage::replication::PrepareCommitRes res{false};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  storage::replication::PrepareCommitReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
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

  const memory::DbArenaScope db_arena_scope{db_acc->get()};
  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  // A broken tenant must never apply incremental deltas: it has been Clear()ed, so its state is empty and any
  // delta would be applied against the wrong base. In normal operation this cannot happen -- a broken tenant
  // reports commit-ts 0 with a fresh epoch, so the main sees it as behind, drives it into RECOVERY and sends
  // recovery steps (a snapshot, WAL files, or both) whose handlers clear the broken flag before any incremental
  // PrepareCommit is sent. This explicit guard is defense-in-depth: it does not rely on that emergent invariant.
  // Drain the delta stream and reject so the main (re-)drives recovery.
  if (storage->IsBroken()) [[unlikely]] {
    DrainAndRejectPrepareCommit(decoder, request_version, res_builder, storage->name());
    return;
  }

  storage::replication::PrepareCommitRes res{false};
  {
    // Started before the abort below, not after: an interrupted 2PC leaves a transaction whose abort is O(deltas), and
    // the first tick only fires one interval after activation. The scope guard deactivates the reusable worker if an
    // exception escapes; normal paths stop it before writing the final response because the socket has no locking.
    heartbeat.Start(res_builder);
    utils::OnScopeExit const stop_heartbeat{[&heartbeat] { heartbeat.Stop(); }};

    // Abort prev txn if needed
    // It could happen that the main instance died before sending finalize for the previous commit and then
    // the new instance becomes main and sends prepare. Scoped to this tenant so an RPC for storage A cannot
    // abort a different tenant's still-pending 2PC (that would strand it and reply commit-OK falsely).
    AbortTwoPCForTenant(storage->uuid(), &heartbeat);
    auto &repl_storage_state = storage->repl_storage_state_;

    if (*maybe_epoch_id != repl_storage_state.epoch_.id()) {
      // We should first finalize WAL file and then update the epoch
      if (storage->wal_file_) {
        storage->wal_file_->FinalizeWal();
        storage->wal_file_.reset();
      }

      // A snapshot being created copies the epoch and history under this lock, so it sees them as one unit. The lock
      // is taken only now so the WAL file's disk I/O above runs outside it.
      auto const engine_guard = std::scoped_lock{storage->engine_lock_};
      repl_storage_state.SaveLatestHistory();
      repl_storage_state.epoch_.SetEpoch(*maybe_epoch_id);
    }

    // last_durable_timestamp could be set by snapshot; so we cannot guarantee exactly what's the previous timestamp
    if (req.previous_commit_timestamp > repl_storage_state.commit_ts_info_.load(std::memory_order_acquire).ldt_) {
      heartbeat.Stop();
      DrainAndRejectPrepareCommit(decoder, request_version, res_builder, storage->name());
      return;
    }

    auto deltas_res = storage->ApplyWalTransaction(
        &decoder,
        storage::durability::kVersion,
        MakeApplyOptions(
            heartbeat, req.two_phase_commit ? storage::WalApplyMode::kStreamTwoPhase : storage::WalApplyMode::kStream));
    heartbeat.Stop();

    if (deltas_res) {
      dbms::TwoPCCommitCache::Store(
          std::move(deltas_res->commit_acc), req.durability_commit_timestamp, storage->uuid());
      res.success = true;
    }
  }
  rpc::SendFinalResponse(res, request_version, res_builder, storage->name());
}

void InMemoryReplicationHandlers::FinalizeCommitHandler(dbms::DbmsHandler *dbms_handler,
                                                        utils::UUID const &current_main_uuid,
                                                        uint64_t const request_version, slk::Reader *req_reader,
                                                        slk::Builder *res_builder) {
  storage::replication::FinalizeCommitReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);

  if (current_main_uuid != req.main_uuid) [[unlikely]] {
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

  const memory::DbArenaScope db_arena_scope{db_acc->get()};

  // Extract the cached accessor out of the slot; everything that walks its deltas or touches
  // engine_lock_ happens below, on the local, with the cache's internal lock already released --
  // see TwoPCCommitCache::TakeMatching's declaration comment for the mismatch-leaves-it-populated
  // contract.
  auto extracted = dbms::TwoPCCommitCache::TakeMatching(req.durability_commit_timestamp);

  // In this handler, we can either commit or abort. If cached accessor is nullptr, it is impossible we should commit
  // because replying to prepare happens after assignment to the accessor
  // If cached accessor is nullptr, and we should abort (e.g. exception was thrown while processing deltas), we can
  // safely return here OK because it means that the abort already happened while destructing accessor during
  // ApplyWalTransaction
  if (!extracted.accessor && !extracted.mismatched_durability_commit_timestamp) {
    spdlog::warn("Cached commit accessor became invalid between two phases");
    storage::replication::FinalizeCommitRes const res(!req.decision);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  if (extracted.mismatched_durability_commit_timestamp) {
    spdlog::warn("Trying to finalize txn with ldt {} but the last prepared txn is with ldt {}",
                 req.durability_commit_timestamp,
                 *extracted.mismatched_durability_commit_timestamp);
    storage::replication::FinalizeCommitRes const res(!req.decision);
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  auto commit_accessor = std::move(extracted.accessor);
  auto *mem_storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  if (req.decision) {
    // If strict sync replica is invoking FinalizeCommitPhase, we need to take the engine lock again
    // and get the new in-memory commit timestamp. Otherwise, the following situation is possible:
    // main executes create() and sends PrepareCommitRpc to replica
    // On replica, we start the explicit txn with BEGIN; and keep executing match (n) return n
    // After receiving FinalizeCommitRpc, replica will show there is a vertex, in that way
    // manifesting non-repeatable reads phenomenon.
    // This has another consequence. A WAL file will contain deltas with commit ts e.g 100 although the last durable
    // timestamp for the transaction which commits these deltas will be different because of the fact that we are
    // taking here another commit timestamp.
    auto &commit_ts = commit_accessor->GetCommitTimestamp();
    DMG_ASSERT(commit_ts.has_value(), "Commit ts without a value");
    // Same committer serialization as the local commit path (see Storage::commit_mutex_).
    auto commit_serializer = mem_storage->LockCommitMutexIfNarrowing();
    auto guard = std::unique_lock{mem_storage->engine_lock_};
    // Mark the old commit ts as finished before emplacing the new one
    mem_storage->commit_log_->MarkFinished(*commit_ts);
    commit_ts.emplace(mem_storage->GetCommitTimestamp());
    commit_accessor->FinalizeCommitPhase(req.durability_commit_timestamp, guard);
    spdlog::trace("Finalized txn on replica");
  } else {
    commit_accessor->AbortAndResetCommitTs();
    spdlog::trace("Aborted txn on replica");
  }

  commit_accessor.reset();
  if (mem_storage->wal_file_) {
    mem_storage->FinalizeWalFile();
  }

  storage::replication::FinalizeCommitRes const res(true);
  rpc::SendFinalResponse(res, request_version, res_builder);
}

void InMemoryReplicationHandlers::DestroyReplAccessor() {
  // Extract under the cache's internal lock, then abort the local outside it -- AbortAndResetCommitTs()
  // walks the transaction's deltas and must not run with the cache mutex held.
  auto accessor = dbms::TwoPCCommitCache::TakeAny();
  if (accessor) {
    accessor->AbortAndResetCommitTs();
  }
}

void InMemoryReplicationHandlers::AbortTwoPCForTenant(utils::UUID const &uuid, rpc::ProgressHeartbeat *heartbeat) {
  // TD-3': single global 2PC slot — only abort it when the cached accessor is this tenant's, else a
  // pending 2PC for a different tenant would be wrongly dropped. See TwoPCCommitCache::TakeForTenant's
  // declaration comment for why the comparison uses the uuid captured at populate time, not one
  // re-derived from the accessor.
  auto accessor = dbms::TwoPCCommitCache::TakeForTenant(uuid);
  if (accessor) {
    // on_progress is reported per delta undone: an interrupted 2PC's abort is O(deltas), and the RPC
    // pre-abort callers run it inside a handler whose peer is timing them.
    auto const on_progress = [heartbeat]() -> storage::ProgressCallback {
      if (heartbeat == nullptr) return {};
      return [heartbeat] { heartbeat->RecordProgress(); };
    }();
    accessor->AbortAndResetCommitTs(on_progress);
  }
}

void InMemoryReplicationHandlers::AbortPrevTxnIfNeeded(storage::InMemoryStorage *const storage,
                                                       rpc::ProgressHeartbeat *heartbeat) {
  AbortTwoPCForTenant(storage->uuid(), heartbeat);
  if (storage->wal_file_) {
    storage->wal_file_->FinalizeWal();
    storage->wal_file_.reset();
  }
}

// The semantic of snapshot handler is the following: Either handling snapshot request passes or it doesn't. If it
// passes we return the current commit timestamp of the replica. If it doesn't pass, we return optional which will
// signal to the caller that it shouldn't update the commit timestamp value.
void InMemoryReplicationHandlers::SnapshotHandler(rpc::FileReplicationHandler const &file_replication_handler,
                                                  DbmsHandler *dbms_handler, utils::UUID const &current_main_uuid,
                                                  rpc::ProgressHeartbeat &heartbeat, uint64_t const request_version,
                                                  slk::Reader *req_reader, slk::Builder *res_builder) {
  storage::replication::SnapshotReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  // Reject a deposed-MAIN RPC before any tenant work (defence-in-depth; GetDatabaseAccessor does not reheat).
  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::SnapshotReq::kType.name);
    rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.storage_uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in snapshot handler for request with storage_uuid {}",
                  std::string{req.storage_uuid});
    rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  const memory::DbArenaScope db_arena_scope{db_acc->get()};
  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  // Creating a snapshot on replica is mutually exclusive with receiving snapshot from main
  // When receiving a snapshot, replica will clear all its durability files and move them into .old directory
  // Snapshot lock needs to be hold for the whole duration of the SnapshotHandler
  auto snapshot_guard = std::unique_lock(storage->snapshot_lock_, std::defer_lock);
  if (!TakeSnapshotLock(snapshot_guard, storage)) {
    rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  // Started before the abort below, not after: an interrupted 2PC leaves a transaction whose abort is O(deltas), and
  // the first tick only fires one interval after activation. It also covers the Clear() further down, which takes
  // long enough on a large tenant to exhaust the peer's budget on its own.
  heartbeat.Start(res_builder);
  utils::OnScopeExit const stop_heartbeat{[&heartbeat] { heartbeat.Stop(); }};
  // Progress only, never cancellation: if the peer goes away mid-recovery this must not abort. Unlike delta
  // application, whose work sits in a transaction that can no longer be reported as committed and will simply be
  // resent, the snapshot is already loaded here. Finishing the derived state advances the commit timestamp, so a
  // reconnecting main may only need WAL deltas from that point instead of resending the whole snapshot.
  auto const record_progress = [&heartbeat] { heartbeat.RecordProgress(); };

  AbortPrevTxnIfNeeded(storage, &heartbeat);

  // Backup dir
  auto const current_snapshot_dir = storage->recovery_.snapshot_directory_;
  if (!utils::EnsureDir(current_snapshot_dir)) {
    spdlog::error("Couldn't get access to the current snapshot directory. Recovery won't be done.");
    heartbeat.Stop();
    rpc::SendFinalResponse(storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  auto const &active_files = file_replication_handler.GetActiveFileNames();
  DMG_ASSERT(active_files.size() == 1, "Received {} snapshot files but expecting only one!", active_files.size());
  auto const &src_snapshot_file = active_files[0];
  auto const dst_snapshot_file = current_snapshot_dir / active_files[0].filename();

  {
    auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
    if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
      spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
      heartbeat.Stop();
      rpc::SendFinalResponse(
          storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder, storage->name());
      return;
    }
    // Acquiring a contended lock is a completed step, so it counts. Recorded on success only -- recording while still
    // blocked would tell the peer we are advancing when we are not.
    record_progress();

    // Moved only once the snapshot is about to be loaded; until then it stays in tmp/, where the request's
    // FileReplicationHandler deletes it if this handler returns early.
    if (!utils::RenamePath(src_snapshot_file, dst_snapshot_file)) {
      spdlog::error("Couldn't copy file from {} to {}", src_snapshot_file, dst_snapshot_file);
      heartbeat.Stop();
      rpc::SendFinalResponse(
          storage::replication::SnapshotRes{std::nullopt, 0}, request_version, res_builder, storage->name());
      return;
    }
    spdlog::info(
        "Received snapshot file {} from main, saved to {}", ReceivedFileSize(dst_snapshot_file), dst_snapshot_file);

    spdlog::info("Clearing database {} before recovering from snapshot.", storage->name());

    // Clear the database
    storage->Clear(record_progress);

    try {
      spdlog::debug("Loading snapshot for db {}.", storage->name());
      auto [snapshot_info, recovery_info, indices_constraints] = storage::durability::LoadSnapshot(
          dst_snapshot_file,
          &storage->vertices_,
          &storage->edges_,
          storage->edges_metadata_index_ ? &*storage->edges_metadata_index_ : nullptr,
          &storage->repl_storage_state_.history,
          storage->name_id_mapper_.get(),
          &storage->edge_count_,
          storage->config_,
          &storage->enum_store_,
          storage->config_.salient.items.enable_schema_info ? &storage->schema_info_.Get() : nullptr,
          &storage->ttl_,
          &storage->description_store_,
          record_progress);
      // If this step is present it should always be the first step of
      // the recovery so we use the UUID we read from snapshot
      storage->uuid().set(snapshot_info.uuid);
      spdlog::info("Set epoch to {} for db {}", snapshot_info.epoch_id, storage->name());
      storage->repl_storage_state_.epoch_.SetEpoch(std::move(snapshot_info.epoch_id));
      storage->vertex_id_ = recovery_info.next_vertex_id;
      storage->edge_id_ = recovery_info.next_edge_id;
      storage->SetTimestampQuiescent(std::max(storage->timestamp_, recovery_info.next_timestamp));
      storage::CommitTsInfo const new_info{.ldt_ = snapshot_info.durable_timestamp,
                                           .num_committed_txns_ = snapshot_info.num_committed_txns};
      storage->repl_storage_state_.commit_ts_info_.store(new_info, std::memory_order_release);
      spdlog::trace("Set num committed txns to {} after loading snapshot.", snapshot_info.num_committed_txns);
      // We are the only active transaction, so mark everything up to the next timestamp
      if (storage->timestamp_ > 0) storage->commit_log_->MarkFinishedInRange(0, storage->timestamp_ - 1);

      RecoverDerivedState(&storage->vertices_,
                          &storage->edges_,
                          storage->name_id_mapper_.get(),
                          &storage->indices_,
                          &storage->constraints_,
                          storage->config_,
                          recovery_info,
                          storage->DbArenaPool(),
                          indices_constraints,
                          storage->edges_metadata_index_ ? &*storage->edges_metadata_index_ : nullptr,
                          storage->config_.salient.items.properties_on_edges,
                          record_progress);
    } catch (const storage::durability::RecoveryFailure &e) {
      spdlog::error(
          "Couldn't load the snapshot from {} because of: {}. Storage will be cleared. Snapshot and WAL files are "
          "preserved so you can restore your data by restarting instance.",
          dst_snapshot_file,
          e.what());
      storage->Clear(record_progress);
      heartbeat.Stop();
      const storage::replication::SnapshotRes res{std::nullopt, 0};
      rpc::SendFinalResponse(res, request_version, res_builder, storage->name());
      return;
    }

    // A successful snapshot load is the moment a broken replica tenant becomes healthy: the main has just full-synced
    // it. Clearing the broken flag re-enables background durability and lets queries touch the tenant again (slice 1
    // guards writes behind IsBroken(), so clearing the flag is sufficient to resume normal operation). Cleared under
    // main_lock_ so the healthy transition is atomic with the recovered state.
    storage->SetBroken(false);
    heartbeat.Stop();
  }
  spdlog::debug("Snapshot from {} loaded successfully.", dst_snapshot_file);

  auto const [ldt, num_committed_txns] = storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire);

  // The old durability files (WALs and snapshots that predate this recovery snapshot) must be removed
  // regardless of whether sending the response below succeeds. If SendFinalResponse throws (e.g. the
  // connection to main drops mid-flush), leaving them behind orphans a stale WAL chain next to the
  // freshly reset seq_num 0 WAL, which corrupts the next recovery. The guard body must never throw
  // because it can run during stack unwinding.
  utils::OnScopeExit const cleanup_old_durability_files{[&] {
    try {
      auto const current_wal_directory = storage->recovery_.wal_directory_;
      auto const curr_snapshot_files = utils::GetFilesFromDir(current_snapshot_dir);
      auto const curr_wal_files = utils::GetFilesFromDir(current_wal_directory);
      auto const not_recovery_snapshot = [&dst_snapshot_file](auto const &snapshot_path) {
        return snapshot_path != dst_snapshot_file;
      };
      auto snapshots_to_process = curr_snapshot_files | rv::filter(not_recovery_snapshot) | r::to_vector;

      if (FLAGS_storage_backup_dir_enabled) {
        auto const maybe_backup_dirs = CreateBackupDirectories(current_snapshot_dir, current_wal_directory);
        if (!maybe_backup_dirs) {
          spdlog::error("Couldn't create backup directories. Old durable files won't be moved to .old directory.");
          return;
        }
        auto const &[backup_snapshot_dir, backup_wal_dir] = *maybe_backup_dirs;
        MoveDurabilityFiles(
            snapshots_to_process, backup_snapshot_dir, curr_wal_files, backup_wal_dir, &(storage->file_retainer_));
      } else {
        DeleteFiles(snapshots_to_process, &storage->file_retainer_);
        DeleteFiles(curr_wal_files, &storage->file_retainer_);
      }
      spdlog::debug("Replication recovery from snapshot finished!");
    } catch (const std::exception &e) {
      spdlog::error("Failed to clean up old durability files after snapshot recovery: {}", e.what());
    }
  }};

  const storage::replication::SnapshotRes res{ldt, num_committed_txns};
  rpc::SendFinalResponse(res, request_version, res_builder, storage->name());
}

// Commit timestamp on main's side shouldn't be updated if:
// 1.) the database accessor couldn't be obtained
// 2.) UUID sent with the request is not the current MAIN's UUID which replica is listening to
// send after that CurrentWalHandler
// If loading all WAL files succeeded then main can continue recovery if it should send more recovery steps.
// If loading one of WAL files partially succeeded, then recovery cannot be continue but commit timestamp can be
// obtained.
void InMemoryReplicationHandlers::WalFilesHandler(
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
    rpc::FileReplicationHandler const &file_replication_handler, dbms::DbmsHandler *dbms_handler,
    utils::UUID const &current_main_uuid, rpc::ProgressHeartbeat &heartbeat, uint64_t const request_version,
    slk::Reader *req_reader, slk::Builder *res_builder) {
  // It is important to take repl state lock immediately at the start of the handler. In that way it cannot happen that
  // a main promotion starts executing while this handler is executing. In this way we have a guarantee: If I am able to
  // take the read lock on repl state, main promotion will start after loading WAL files is finished.
  // If I am unable to take the read lock, then something else of a higher priority is running and I should apply WAL
  // files later
  auto const maybe_locked_repl_state =
      std::invoke([&repl_state]() -> std::optional<decltype(repl_state.TryReadLock())> {
        try {
          return repl_state.TryReadLock();
        } catch (utils::TryLockException const &) {
          spdlog::warn("Failed to take repl state read lock, cannot apply WAL files");
          return std::nullopt;
        }
      });

  if (!maybe_locked_repl_state.has_value()) {
    const storage::replication::WalFilesRes res{std::nullopt, 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  storage::replication::WalFilesReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  // Reject a deposed-MAIN RPC before any tenant work (defence-in-depth; GetDatabaseAccessor does not reheat).
  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::WalFilesReq::kType.name);
    rpc::SendFinalResponse(storage::replication::WalFilesRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in wal files handler for request storage_uuid {}",
                  std::string{req.uuid});
    const storage::replication::WalFilesRes res{std::nullopt, 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  const memory::DbArenaScope db_arena_scope{db_acc->get()};
  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  // Started before the abort below, not after: an interrupted 2PC leaves a transaction whose abort is
  // O(deltas), and the first tick only fires one interval after activation.
  heartbeat.Start(res_builder);
  utils::OnScopeExit const stop_heartbeat{[&heartbeat] { heartbeat.Stop(); }};
  auto const record_progress = [&heartbeat] { heartbeat.RecordProgress(); };

  AbortPrevTxnIfNeeded(storage, &heartbeat);

  auto const current_wal_directory = storage->recovery_.wal_directory_;

  if (!utils::EnsureDir(current_wal_directory)) {
    spdlog::error("Couldn't get access to the current wal directory. Recovery won't be done.");
    heartbeat.Stop();
    rpc::SendFinalResponse(storage::replication::WalFilesRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  std::vector<std::filesystem::path> old_wal_files;

  // Creating a snapshot on replica is mutually exclusive with force resetting from WalFilesHandler
  // When doing a force reset, replica will clear all its durability files and move them into .old directory
  // Snapshot lock needs to be hold for the whole duration of the WalFilesHandler
  auto snapshot_guard = std::unique_lock(storage->snapshot_lock_, std::defer_lock);

  if (req.reset_needed) {
    if (!TakeSnapshotLock(snapshot_guard, storage)) {
      heartbeat.Stop();
      rpc::SendFinalResponse(storage::replication::WalFilesRes{std::nullopt, 0}, request_version, res_builder);
      return;
    }
    // Acquiring a contended lock is a completed step, so it counts: the waits here are the only stretch of this
    // path that reports nothing, and a tick landing in one would find the flag clear and stay silent for another
    // whole interval. Recorded on success only -- recording while still blocked would tell the peer we are
    // advancing when we are not.
    record_progress();
    {
      auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
      if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
        spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
        heartbeat.Stop();
        rpc::SendFinalResponse(
            storage::replication::WalFilesRes{std::nullopt, 0}, request_version, res_builder, storage->name());
        return;
      }
      record_progress();

      spdlog::info("Clearing replica storage for db {} because the reset is needed while recovering from WalFiles.",
                   storage->name());
      storage->Clear(record_progress);
    }

    // Read here because we don't know names of new WAL files
    old_wal_files = utils::GetFilesFromDir(current_wal_directory);
  }

  const auto wal_file_number = req.file_number;
  spdlog::debug("Received {} WAL files.", wal_file_number);

  auto const &active_files = file_replication_handler.GetActiveFileNames();
  for (auto const &wal_file : active_files) {
    spdlog::info("Received WAL file {} from main for db {}: {}",
                 wal_file.filename(),
                 storage->name(),
                 ReceivedFileSize(wal_file));
  }

  // On a force reset the storage was cleared above, so the old durability files must be removed regardless of
  // whether sending the response succeeds; otherwise a SendFinalResponse throw would orphan a stale WAL chain
  // next to the reset seq_num 0 WAL and corrupt the next recovery. ProcessOldDurableFiles is a no-op when the
  // reset wasn't requested. The guard body must never throw because it can run during stack unwinding.
  utils::OnScopeExit const cleanup_old_durability_files{[&] {
    try {
      ProcessOldDurableFiles(req.reset_needed,
                             storage->recovery_.snapshot_directory_,
                             current_wal_directory,
                             old_wal_files,
                             &storage->file_retainer_);
    } catch (const std::exception &e) {
      spdlog::error("Failed to clean up old durability files after WAL files recovery: {}", e.what());
    }
  }};

  uint64_t num_committed_txns{0};
  bool all_applied = true;
  for (auto i = 0UL; i < wal_file_number; ++i) {
    const auto [success, num_txns_committed] = LoadWal(active_files[i], storage, heartbeat);

    if (!success) {
      spdlog::debug("Replication recovery from WAL files failed while loading one of WAL files for db {}.",
                    storage->name());
      all_applied = false;
      break;
    }
    num_committed_txns += num_txns_committed;
  }

  // Every exit from here on writes a response, so the heartbeat must be off first.
  heartbeat.Stop();

  if (!all_applied) {
    const storage::replication::WalFilesRes res{std::nullopt, 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  spdlog::debug("Replication recovery from WAL files succeeded for db {}.", storage->name());

  // A broken replica tenant reports commit-ts 0 and the main heals it during RECOVERY. The recovery steps need not
  // include a snapshot: when the main's WAL chain reaches back to the start (e.g. the main never took a snapshot),
  // GetRecoverySteps sends WAL files only. Having fully applied them, the tenant is consistent with the main, so the
  // broken flag is cleared here too (mirrors SnapshotHandler) to re-enable queries and background durability.
  storage->SetBroken(false);

  const storage::replication::WalFilesRes res{
      storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_, num_committed_txns};

  rpc::SendFinalResponse(res, request_version, res_builder, storage->name());
}

// Commit timestamp on MAIN's side shouldn't be updated if:
// 1.) the database accessor couldn't be obtained
// 2.) UUID sent with the request is not the current MAIN's UUID which replica is listening to
// If loading WAL file partially succeeded then we shouldn't continue recovery but commit timestamp can be updated on
// main
void InMemoryReplicationHandlers::CurrentWalHandler(
    memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
    rpc::FileReplicationHandler const &file_replication_handler, dbms::DbmsHandler *dbms_handler,
    utils::UUID const &current_main_uuid, rpc::ProgressHeartbeat &heartbeat, uint64_t const request_version,
    slk::Reader *req_reader, slk::Builder *res_builder) {
  // It is important to take repl state lock immediately at the start of the handler. In that way it cannot happen that
  // a main promotion starts executing while this handler is executing. In this way we have a guarantee: If I am able to
  // take the read lock on repl state, main promotion will start after loading the current WAL is finished.
  // If I am unable to take the read lock, then something else of a higher priority is running and I should apply this
  // WAL file later
  auto const maybe_locked_repl_state =
      std::invoke([&repl_state]() -> std::optional<decltype(repl_state.TryReadLock())> {
        try {
          return repl_state.TryReadLock();
        } catch (utils::TryLockException const &) {
          spdlog::warn("Failed to take repl state read lock, cannot apply current WAL file");
          return std::nullopt;
        }
      });

  if (!maybe_locked_repl_state.has_value()) {
    const storage::replication::CurrentWalRes res{std::nullopt, 0};
    rpc::SendFinalResponse(res, request_version, res_builder);
    return;
  }

  storage::replication::CurrentWalReq req;
  rpc::LoadWithUpgrade(req, request_version, req_reader);
  // Reject a deposed-MAIN RPC before any tenant work (defence-in-depth; GetDatabaseAccessor does not reheat).
  if (current_main_uuid != req.main_uuid) [[unlikely]] {
    LogWrongMain(current_main_uuid, req.main_uuid, storage::replication::CurrentWalReq::kType.name);
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }
  auto db_acc = GetDatabaseAccessor(dbms_handler, req.uuid);
  if (!db_acc) {
    spdlog::error("Couldn't get database accessor in current wal handler for request storage_uuid {}",
                  std::string{req.uuid});
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  const memory::DbArenaScope db_arena_scope{db_acc->get()};
  auto *storage = static_cast<storage::InMemoryStorage *>(db_acc->get()->storage());

  // Started before the abort below, not after: an interrupted 2PC leaves a transaction whose abort is
  // O(deltas), and the first tick only fires one interval after activation.
  heartbeat.Start(res_builder);
  utils::OnScopeExit const stop_heartbeat{[&heartbeat] { heartbeat.Stop(); }};
  auto const record_progress = [&heartbeat] { heartbeat.RecordProgress(); };

  AbortPrevTxnIfNeeded(storage, &heartbeat);

  auto const current_wal_directory = storage->recovery_.wal_directory_;
  if (!utils::EnsureDir(current_wal_directory)) {
    spdlog::error("Couldn't get access to the current wal directory. Recovery won't be done.");
    heartbeat.Stop();
    rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
    return;
  }

  std::vector<std::filesystem::path> old_wal_files;

  // Creating a snapshot on replica is mutually exclusive with force resetting from CurrentWalHandler
  // When doing a force reset, replica will clear all its durability files and move them into .old directory
  // Snapshot lock needs to be hold for the whole duration of the CurrentWalHandler
  auto snapshot_guard = std::unique_lock(storage->snapshot_lock_, std::defer_lock);

  if (req.reset_needed) {
    if (!TakeSnapshotLock(snapshot_guard, storage)) {
      heartbeat.Stop();
      rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
      return;
    }
    // Acquiring a contended lock is a completed step, so it counts: the waits here are the only stretch of this
    // path that reports nothing, and a tick landing in one would find the flag clear and stay silent for another
    // whole interval. Recorded on success only -- recording while still blocked would tell the peer we are
    // advancing when we are not.
    record_progress();
    {
      auto storage_guard = std::unique_lock{storage->main_lock_, std::defer_lock};
      if (!storage_guard.try_lock_for(kWaitForMainLockTimeout)) {
        spdlog::error("Failed to acquire main lock in {}s", kWaitForMainLockTimeout.count());
        heartbeat.Stop();
        rpc::SendFinalResponse(storage::replication::CurrentWalRes{std::nullopt, 0}, request_version, res_builder);
        return;
      }
      record_progress();
      spdlog::info("Clearing replica storage for db {} because the reset is needed while recovering from WalFiles.",
                   storage->name());
      storage->Clear(record_progress);
    }

    // Read here because we don't know the name of the new WAL file
    old_wal_files = utils::GetFilesFromDir(current_wal_directory);
  }

  // Even if loading wal file failed, we return last_durable_timestamp to the main because it is not a fatal error
  // When loading a single WAL file, we don't care about saving number of deltas
  auto const &active_files = file_replication_handler.GetActiveFileNames();
  MG_ASSERT(active_files.size() == 1, "Received {} files but expected 1 in CurrentWalHandler", active_files.size());
  spdlog::info("Received current WAL file {} from main for db {}: {}",
               active_files[0].filename(),
               storage->name(),
               ReceivedFileSize(active_files[0]));
  auto const load_wal_res = LoadWal(active_files[0], storage, heartbeat);
  heartbeat.Stop();
  if (!load_wal_res.success) {
    spdlog::debug(
        "Replication recovery from current WAL didn't end successfully but the error is non-fatal error. DB {}.",
        storage->name());
  } else {
    spdlog::debug("Replication recovery from current WAL ended successfully! DB {}.", storage->name());
    // The current WAL can be the only recovery step the main sends to heal a broken tenant (commit-ts 0, no snapshot
    // on the main). On success the tenant is consistent with the main, so clear the broken flag here too, just like
    // SnapshotHandler and WalFilesHandler do.
    storage->SetBroken(false);
  }

  const storage::replication::CurrentWalRes res{
      storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_,
      load_wal_res.num_txns_committed};

  // On a force reset the storage was cleared above, so the old durability files must be removed regardless of
  // whether sending the response succeeds; otherwise a SendFinalResponse throw would orphan a stale WAL chain
  // next to the reset seq_num 0 WAL and corrupt the next recovery. ProcessOldDurableFiles is a no-op when the
  // reset wasn't requested. The guard body must never throw because it can run during stack unwinding.
  utils::OnScopeExit const cleanup_old_durability_files{[&] {
    try {
      ProcessOldDurableFiles(req.reset_needed,
                             storage->recovery_.snapshot_directory_,
                             current_wal_directory,
                             old_wal_files,
                             &storage->file_retainer_);
    } catch (const std::exception &e) {
      spdlog::error("Failed to clean up old durability files after current WAL recovery: {}", e.what());
    }
  }};

  rpc::SendFinalResponse(res, request_version, res_builder, storage->name());
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
                                                                                rpc::ProgressHeartbeat &heartbeat) {
  spdlog::trace("Received WAL saved to {}", wal_path);

  // A finalized file states how much it holds, so it is replayed straight from its header. Only main's current WAL,
  // which it is still writing, has no summary; that one is parsed so a transaction main had not finished is not
  // replayed. Either way each transaction's CRC is verified below as it is applied.
  storage::durability::WalInfo wal_info{};
  try {
    wal_info = storage::durability::ReadWalContents(wal_path);
  } catch (const utils::BasicException &e) {
    spdlog::error("Loading WAL info from {} failed because of {}.", wal_path, e.what());
    return LoadWalStatus{.success = false, .num_txns_committed = 0};
  }

  // We have to check if this is our 1st wal, not what main is sending
  if (storage->wal_seq_num_ == 0) {
    storage->uuid().set(wal_info.uuid);
  }

  // If WAL file doesn't contain any changes that need to be applied, ignore it
  if (wal_info.to_timestamp <= storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_) {
    spdlog::trace("WAL file won't be applied since all changes already exist.");
    return LoadWalStatus{.success = true, .num_txns_committed = 0};
  }

  // We trust only WAL files which contain changes we are interested in (newer changes)
  if (auto &repl_epoch = storage->repl_storage_state_.epoch_; wal_info.epoch_id != repl_epoch.id()) {
    spdlog::info("Set epoch to {} for db {}", wal_info.epoch_id, storage->name());
    auto const engine_guard = std::scoped_lock{storage->engine_lock_};
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
    return LoadWalStatus{.success = false, .num_txns_committed = 0};
  }
  if (!storage::durability::IsVersionSupported(*version)) {
    spdlog::error("Invalid WAL version!");
    return LoadWalStatus{.success = false, .num_txns_committed = 0};
  }

  wal_decoder.SetPosition(wal_info.offset_deltas);

  uint64_t num_txns_committed{0};
  size_t local_delta_idx = 0;
  while (local_delta_idx < wal_info.num_deltas) {
    // A delta that won't parse, or a transaction whose CRC doesn't match, throws out of here. The in-flight
    // transaction's accessor is destroyed while unwinding, which aborts it, so nothing half-applied is ever
    // committed. Report failure so the caller stops before the WAL files that follow this one: those build on the
    // transactions that just went missing, and applying them would leave a wrong dataset rather than a stale one.
    std::optional<storage::SingleTxnDeltasProcessingResult> deltas_res;
    try {
      // commit_txn_immediately is set true because when loading WAL files, we should commit immediately
      deltas_res = storage->ApplyWalTransaction(
          &wal_decoder, *version, MakeApplyOptions(heartbeat, storage::WalApplyMode::kWalFile));
    } catch (const utils::BasicException &e) {
      spdlog::error("Aborting WAL file {} at delta {} and skipping the rest of the chain because of: {}",
                    wal_path,
                    local_delta_idx,
                    e.what());
      return LoadWalStatus{.success = false, .num_txns_committed = 0};
    }

    if (deltas_res) {
      local_delta_idx += deltas_res->current_delta_idx;
      num_txns_committed += deltas_res->num_txns_committed;
    } else {
      return LoadWalStatus{.success = false, .num_txns_committed = 0};
    }
  }

  // Each transaction is replayed until its transaction-end delta, not until the count runs out, so consuming more
  // deltas than the file states means the marker that ended one rotted into another delta of the same encoded length:
  // replay ran on into whatever followed. Nothing was committed unverified, since the CRC is checked in the same place
  // the commit happens, but a transaction was silently skipped, so the file must not be reported as applied.
  if (local_delta_idx != wal_info.num_deltas) {
    spdlog::error("WAL file {} states {} deltas but replaying it consumed {}; it ends mid-transaction",
                  wal_path,
                  wal_info.num_deltas,
                  local_delta_idx);
    return LoadWalStatus{.success = false, .num_txns_committed = 0};
  }

  spdlog::trace("Replication from WAL file {} successful!", wal_path);
  return LoadWalStatus{.success = true, .num_txns_committed = num_txns_committed};
}

#ifdef MG_ENTERPRISE
bool InMemoryReplicationHandlers::ReplayWalFile(std::filesystem::path const &wal_path,
                                                storage::durability::WalInfo const &wal_info,
                                                storage::InMemoryStorage *storage,
                                                std::function<void(uint64_t)> const &on_txn_begin) {
  storage::durability::Decoder wal_decoder;
  const auto version = wal_decoder.Initialize(wal_path, storage::durability::kWalMagic);
  if (!version || !storage::durability::IsVersionSupported(*version)) {
    spdlog::error("WAL file {} has no valid magic and version", wal_path);
    return false;
  }
  wal_decoder.SetPosition(wal_info.offset_deltas);
  storage::WalApplyOptions const options{.mode = storage::WalApplyMode::kWalFile, .on_txn_begin = on_txn_begin};
  uint64_t delta_idx{0};
  while (delta_idx < wal_info.num_deltas) {
    auto const res = storage->ApplyWalTransaction(&wal_decoder, *version, options);
    if (!res) return false;
    delta_idx += res->current_delta_idx;
  }
  // Same check as LoadWal: overrunning the stated count means a transaction-end marker was lost.
  return delta_idx == wal_info.num_deltas;
}
#endif

}  // namespace memgraph::dbms
