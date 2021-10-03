// Copyright 2021 Memgraph Ltd.
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

#include "storage/v2/storage.hpp"

namespace storage {

class Storage::ReplicationServer {
 public:
  explicit ReplicationServer(Storage *storage, io::network::Endpoint endpoint,
                             const replication::ReplicationServerConfig &config);
  ReplicationServer(const ReplicationServer &) = delete;
  ReplicationServer(ReplicationServer &&) = delete;
  ReplicationServer &operator=(const ReplicationServer &) = delete;
  ReplicationServer &operator=(ReplicationServer &&) = delete;

  ~ReplicationServer();

 private:
  // RPC handlers
  void HeartbeatHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void AppendDeltasHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void SnapshotHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void WalFilesHandler(slk::Reader *req_reader, slk::Builder *res_builder);
  void CurrentWalHandler(slk::Reader *req_reader, slk::Builder *res_builder);

  void LoadWal(replication::Decoder *decoder);
  uint64_t ReadAndApplyDelta(durability::BaseDecoder *decoder);

  std::optional<communication::ServerContext> rpc_server_context_;
  std::optional<rpc::Server> rpc_server_;

  Storage *storage_;
};

}  // namespace storage
