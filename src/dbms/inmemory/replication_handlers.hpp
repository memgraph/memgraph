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

#pragma once

#include "dbms/dbms_handler_fwd.hpp"
#include "replication/statefwd.hpp"
#include "storage/v2/inmemory/storagefwd.hpp"

#include "storage/v2/durability/serialization.hpp"

#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/uuid.hpp"

namespace memgraph::rpc {
class FileReplicationHandler;
class ProgressHeartbeat;
}  // namespace memgraph::rpc

namespace memgraph::dbms {

class InMemoryReplicationHandlers {
 public:
  // Although it seems a bit unintuitive this is ok. The logic is following:
  // for all RPCs except SwapMainUUID you don't need global repl state, you only need current main UUID. Since you know
  // that the RoleReplicaData won't change for as long as this instance is replica, having a reference is ok.
  // If you do need however a global repl state like in SwapMainUUID, then you lock it and update it in a concurrently
  // safe way.
  // Having a reference to RoleReplicaData serves as an optimization to avoid an additional locking
  static void Register(
      dbms::DbmsHandler *dbms_handler,
      memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
      replication::RoleReplicaData &data);

  // If the connection between MAIN and REPLICA dies just after sending PrepareCommitRes and receiving
  // FinalizeCommitReq, then there is the possibility that the cached_commit_accessor_ will stay alive for too long
  // preventing therefore processing of CurrentWalRpc, WalFilesRpc, SnapshotRpc. Scoped to `storage`'s own uuid
  // (via AbortTwoPCForTenant) because the cached accessor holds main_lock_ on its own storage, so it can only
  // ever block that same tenant's recovery -- nothing is lost by not aborting other tenants' pending 2PCs here.
  // It should also be invoked during the promote
  static void AbortPrevTxnIfNeeded(storage::InMemoryStorage *storage, rpc::ProgressHeartbeat *heartbeat = nullptr);

  // Aborts + destroys whatever accessor is cached, regardless of tenant. Deliberately tenant-oblivious,
  // so only safe once the replica RPC listener pool is joined (process shutdown, main promotion) -- otherwise
  // it can steal another tenant's still-pending accessor and abort it concurrently with that tenant's teardown.
  static void DestroyReplAccessor();

  // Abort + reset the cached 2PC commit accessor ONLY if it belongs to `uuid`. The slot is a single
  // global, not per-tenant, so this is what every tenant-aware call site (AbortPrevTxnIfNeeded,
  // PrepareCommitHandler, the suspend path) uses instead of DestroyReplAccessor to avoid dropping a
  // different tenant's pending 2PC. `heartbeat`, when set, is pinged per delta undone so a large abort
  // (an interrupted 2PC's abort is O(deltas)) does not stall the RPC peer timing the handler. See
  // TwoPCCommitCache (dbms/inmemory/two_pc_commit_cache.hpp) for why the uuid compared against is the
  // one captured at Store time, not re-derived from the accessor.
  static void AbortTwoPCForTenant(utils::UUID const &uuid, rpc::ProgressHeartbeat *heartbeat = nullptr);

 private:
  struct LoadWalStatus {
    bool success{false};
    uint64_t num_txns_committed{0};
  };

  // RPC handlers
  static void HeartbeatHandler(dbms::DbmsHandler *dbms_handler, utils::UUID const &current_main_uuid,
                               uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);

  static void PrepareCommitHandler(
      memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
      dbms::DbmsHandler *dbms_handler, utils::UUID const &current_main_uuid, uint64_t request_version,
      slk::Reader *req_reader, slk::Builder *res_builder);

  static void FinalizeCommitHandler(dbms::DbmsHandler *dbms_handler, utils::UUID const &current_main_uuid,
                                    uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);

  static void SnapshotHandler(rpc::FileReplicationHandler const &file_replication_handler,
                              dbms::DbmsHandler *dbms_handler, utils::UUID const &current_main_uuid,
                              uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);

  static void WalFilesHandler(
      memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
      rpc::FileReplicationHandler const &file_replication_handler, dbms::DbmsHandler *dbms_handler,
      utils::UUID const &current_main_uuid, uint64_t request_version, slk::Reader *req_reader,
      slk::Builder *res_builder);

  static void CurrentWalHandler(
      memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
      rpc::FileReplicationHandler const &file_replication_handler, dbms::DbmsHandler *dbms_handler,
      utils::UUID const &current_main_uuid, uint64_t request_version, slk::Reader *req_reader,
      slk::Builder *res_builder);

  static void SwapMainUUIDHandler(
      memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
      uint64_t request_version, slk::Reader *req_reader, slk::Builder *res_builder);

  static LoadWalStatus LoadWal(std::filesystem::path const &wal_path, storage::InMemoryStorage *storage,
                               rpc::ProgressHeartbeat &heartbeat);

  static auto TakeSnapshotLock(auto &snapshot_guard, storage::InMemoryStorage *storage) -> bool;

  static std::optional<storage::SingleTxnDeltasProcessingResult> ReadAndApplyDeltasSingleTxn(
      storage::InMemoryStorage *storage, storage::durability::BaseDecoder *decoder, uint64_t version,
      rpc::ProgressHeartbeat &heartbeat, bool two_phase_commit, bool loading_wal);
};

}  // namespace memgraph::dbms
