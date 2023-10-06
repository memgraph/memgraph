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

#include "replication/replication_epoch.hpp"
#include "storage/v2/replication/replication_server.hpp"
#include "storage/v2/replication/serialization.hpp"

namespace memgraph::storage {

class InMemoryStorage;

class InMemoryReplicationServer : public ReplicationServer {
 public:
  explicit InMemoryReplicationServer(InMemoryStorage *storage, const replication::ReplicationServerConfig &config,
                                     memgraph::replication::ReplicationEpoch *repl_epoch);

 private:
  // RPC handlers
  void HeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  void AppendDeltasHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  void SnapshotHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  void WalFilesHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  void CurrentWalHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  void TimestampHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  static void LoadWal(InMemoryStorage *storage, memgraph::replication::ReplicationEpoch &epoch,
                      replication::Decoder *decoder);

  static uint64_t ReadAndApplyDelta(InMemoryStorage *storage, durability::BaseDecoder *decoder, uint64_t version);

  InMemoryStorage *storage_;

  memgraph::replication::ReplicationEpoch *repl_epoch_;
};

}  // namespace memgraph::storage
