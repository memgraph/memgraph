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

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/replication/recovery.hpp"
#include "storage/v2/replication/replication_client.hpp"

namespace memgraph::storage {
class InMemoryStorage;

// Handler for transferring the current WAL file whose data is
// contained in the internal buffer and the file.
class InMemoryCurrentWalHandler {
 public:
  explicit InMemoryCurrentWalHandler(InMemoryStorage const *storage, rpc::Client &rpc_client);
  void AppendFilename(const std::string &filename);

  void AppendSize(size_t size);

  void AppendFileData(utils::InputFile *file);

  void AppendBufferData(const uint8_t *buffer, size_t buffer_size);

  /// @throw rpc::RpcFailedException
  replication::CurrentWalRes Finalize();

 private:
  rpc::Client::StreamHandler<replication::CurrentWalRpc> stream_;
};

////// ReplicationClient Helpers //////

replication::WalFilesRes TransferWalFiles(std::string db_name, rpc::Client &client,
                                          const std::vector<std::filesystem::path> &wal_files);

replication::SnapshotRes TransferSnapshot(std::string db_name, rpc::Client &client, const std::filesystem::path &path);

uint64_t ReplicateCurrentWal(InMemoryCurrentWalHandler &stream, durability::WalFile const &wal_file);

auto GetRecoverySteps(uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker,
                      const InMemoryStorage *storage) -> std::vector<RecoveryStep>;

}  // namespace memgraph::storage
