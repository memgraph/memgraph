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

#pragma once

#include "replication/replication_server.hpp"
#include "replication/state.hpp"
#include "storage/v2/storage_replication/serialization.hpp"

namespace memgraph::storage {
class InMemoryStorage;
}
namespace memgraph::dbms {

class DbmsHandler;

class InMemoryReplicationHandlers {
 public:
  static void Register(dbms::DbmsHandler *dbms_handler, replication::RoleReplicaData &data);

 private:
  // RPC handlers
  static void HeartbeatHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                               slk::Reader *req_reader, slk::Builder *res_builder);

  static void AppendDeltasHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                                  slk::Reader *req_reader, slk::Builder *res_builder);

  static void SnapshotHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                              slk::Reader *req_reader, slk::Builder *res_builder);

  static void WalFilesHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                              slk::Reader *req_reader, slk::Builder *res_builder);

  static void CurrentWalHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                                slk::Reader *req_reader, slk::Builder *res_builder);

  static void TimestampHandler(dbms::DbmsHandler *dbms_handler, const std::optional<utils::UUID> &current_main_uuid,
                               slk::Reader *req_reader, slk::Builder *res_builder);

  static void SwapMainUUIDHandler(dbms::DbmsHandler *dbms_handler, replication::RoleReplicaData &role_replica_data,
                                  slk::Reader *req_reader, slk::Builder *res_builder);
  static void ForceResetStorageHandler(dbms::DbmsHandler *dbms_handler,
                                       const std::optional<utils::UUID> &current_main_uuid, slk::Reader *req_reader,
                                       slk::Builder *res_builder);

  static void LoadWal(storage::InMemoryStorage *storage, storage::replication::Decoder *decoder);

  static uint64_t ReadAndApplyDelta(storage::InMemoryStorage *storage, storage::durability::BaseDecoder *decoder,
                                    uint64_t version);
};

}  // namespace memgraph::dbms
