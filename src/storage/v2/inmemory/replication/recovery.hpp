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

#include "storage/v2/durability/durability.hpp"
#include "storage/v2/replication/recovery.hpp"
#include "storage/v2/replication/replication_client.hpp"

namespace memgraph::storage {
class InMemoryStorage;

////// ReplicationClient Helpers //////

replication::WalFilesRes TransferWalFiles(const utils::UUID &main_uuid, const utils::UUID &uuid, rpc::Client &client,
                                          const std::vector<std::filesystem::path> &wal_files, bool reset_needed);

replication::SnapshotRes TransferSnapshot(const utils::UUID &main_uuid, const utils::UUID &storage_uuid,
                                          rpc::Client &client, const std::filesystem::path &path);

replication::CurrentWalRes TransferCurrentWal(const utils::UUID &main_uuid, const InMemoryStorage *storage,
                                              rpc::Client &client, durability::WalFile const &wal_file,
                                              bool reset_needed);

auto GetRecoverySteps(uint64_t replica_commit, utils::FileRetainer::FileLocker *file_locker,
                      const InMemoryStorage *main_storage) -> std::optional<std::vector<RecoveryStep>>;

}  // namespace memgraph::storage
