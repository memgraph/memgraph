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
}  // namespace memgraph::rpc

namespace memgraph::dbms {

struct TwoPCCache {
  std::unique_ptr<storage::ReplicationAccessor> commit_accessor_;
  uint64_t durability_commit_timestamp_;
};

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
  // preventing therefore processing of CurrentWalRpc, WalFilesRpc, SnapshotRpc.
  // It should also be invoked during the promote
  static void AbortPrevTxnIfNeeded(storage::InMemoryStorage *storage);

  // Destroys repl accessor needed for 2PC
  static void DestroyReplAccessor();

 private:
  struct LoadWalStatus {
    bool success{false};
    uint32_t current_batch_counter{0};
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

  static LoadWalStatus LoadWal(
      memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> &repl_state,
      std::filesystem::path const &wal_path, storage::InMemoryStorage *storage, slk::Builder *res_builder,
      uint32_t start_batch_counter = 0);

  static auto TakeSnapshotLock(auto &snapshot_guard, storage::InMemoryStorage *storage) -> bool;

  static std::optional<storage::SingleTxnDeltasProcessingResult> ReadAndApplyDeltasSingleTxn(
      storage::InMemoryStorage *storage, storage::durability::BaseDecoder *decoder, uint64_t version, slk::Builder *,
      bool two_phase_commit, bool loading_wal, uint64_t deltas_batch_progress_size, uint32_t start_batch_counter = 0);

  static TwoPCCache two_pc_cache_;
};

}  // namespace memgraph::dbms
