// Copyright 2023 Memgraph Ltd.
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
#include "storage/v2/replication/serialization.hpp"

namespace memgraph::dbms {
class DbmsHandler;
}  // namespace memgraph::dbms
namespace memgraph::storage {

class InMemoryStorage;

class InMemoryReplicationServer {
 public:
  // explicit InMemoryReplicationServer(InMemoryStorage *storage,
  //                                    ReplicationServer &server,
  //                                    memgraph::replication::ReplicationEpoch *epoch);

  static void Register(dbms::DbmsHandler *dbms_handler, ReplicationServer &server);

 private:
  // RPC handlers
  static void HeartbeatHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);

  static void AppendDeltasHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);

  static void SnapshotHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);

  static void WalFilesHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);

  static void CurrentWalHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);

  static void TimestampHandler(dbms::DbmsHandler *dbms_handler, slk::Reader *req_reader, slk::Builder *res_builder);

  static void LoadWal(InMemoryStorage *storage, memgraph::replication::ReplicationEpoch *replica_epoch,
                      replication::Decoder *decoder);

  static uint64_t ReadAndApplyDelta(InMemoryStorage *storage, durability::BaseDecoder *decoder, uint64_t version);

  // static InMemoryStorage *storage_; TODO Redo the cache
  // ReplicationServer& server_; //TODO: interface to global thing

  // memgraph::replication::ReplicationEpoch *replica_epoch_;
};

}  // namespace memgraph::storage
