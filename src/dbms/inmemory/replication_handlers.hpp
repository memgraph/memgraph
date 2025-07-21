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

#pragma once

#include "dbms/dbms_handler_fwd.hpp"
#include "replication/statefwd.hpp"
#include "storage/v2/inmemory/storagefwd.hpp"
#include "storage/v2/replication/serialization.hpp"

namespace memgraph::dbms {

struct TwoPCCache {
  std::unique_ptr<storage::ReplicationAccessor> commit_accessor_;
  uint64_t durability_commit_timestamp_;
};

class InMemoryReplicationHandlers {
 public:
  static void Register(dbms::DbmsHandler *dbms_handler, replication::RoleReplicaData &data);

 private:
  // RPC handlers
  static void HeartbeatHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                               slk::Reader *req_reader, slk::Builder *res_builder);

  static void PrepareCommitHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                                   slk::Reader *req_reader, slk::Builder *res_builder);

  static void FinalizeCommitHandler(dbms::DbmsHandler *dbms_handler,
                                    const std::optional<utils::UUID> &current_main_uuid, slk::Reader *req_reader,
                                    slk::Builder *res_builder);

  static void SnapshotHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                              slk::Reader *req_reader, slk::Builder *res_builder);

  static void WalFilesHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                              slk::Reader *req_reader, slk::Builder *res_builder);

  static void CurrentWalHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                                slk::Reader *req_reader, slk::Builder *res_builder);

  static void SwapMainUUIDHandler(dbms::DbmsHandler *dbms_handler, replication::RoleReplicaData &role_replica_data,
                                  slk::Reader *req_reader, slk::Builder *res_builder);

  static std::pair<bool, uint32_t> LoadWal(storage::InMemoryStorage *storage, storage::replication::Decoder *decoder,
                                           slk::Builder *res_builder, uint32_t start_batch_counter = 0);

  // If the connection between MAIN and REPLICA dies just after sending PrepareCommitRes and receiving
  // FinalizeCommitReq, then there is the possibility that the cached_commit_accessor_ will stay alive for too long
  // preventing therefore processing of CurrentWalRpc.
  static void AbortPrevTxnIfNeeded(storage::InMemoryStorage *storage);

  static std::optional<storage::SingleTxnDeltasProcessingResult> ReadAndApplyDeltasSingleTxn(
      storage::InMemoryStorage *storage, storage::durability::BaseDecoder *decoder, uint64_t version, slk::Builder *,
      bool commit_txn_immediately, bool loading_wal, uint32_t start_batch_counter = 0);

  static TwoPCCache two_pc_cache_;
};

}  // namespace memgraph::dbms
