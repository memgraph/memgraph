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

#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/replication/replication_handler.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

inline std::unique_ptr<Storage> CreateInMemoryStorage(Config config,
                                                      const ::memgraph::replication::ReplicationState &repl_state) {
  const auto restore_repl = config.durability.restore_replication_state_on_startup;
  const auto wal_mode = config.durability.snapshot_wal_mode;
  auto storage_ = std::make_unique<InMemoryStorage>(std::move(config));
  auto *storage = static_cast<InMemoryStorage *>(storage_.get());

  // Connect replication state and storage
  storage->CreateSnapshotHandler(
      [storage, &repl_state](bool is_periodic) -> utils::BasicResult<InMemoryStorage::CreateSnapshotError> {
        if (repl_state.IsReplica()) {
          return InMemoryStorage::CreateSnapshotError::DisabledForReplica;
        }
        return storage->CreateSnapshot(is_periodic);
      });

  // Handle global replication state
  if (restore_repl) {
    spdlog::info("Replication configuration will be stored and will be automatically restored in case of a crash.");
    // RECOVER REPLICA CONNECTIONS
    RestoreReplication(repl_state, *storage);
  } else {
    spdlog::warn(
        "Replication configuration will NOT be stored. When the server restarts, replication state will be "
        "forgotten.");
  }

  if (wal_mode == Config::Durability::SnapshotWalMode::DISABLED && repl_state.IsMain()) {
    spdlog::warn(
        "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please consider "
        "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
        "without write-ahead logs this instance is not replicating any data.");
  }

  return storage_;
}

}  // namespace memgraph::storage
