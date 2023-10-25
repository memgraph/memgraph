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

#ifdef MG_EXPERIMENTAL_REPLICATION_MULTITENANCY
constexpr bool allow_mt_repl = true;
#else
constexpr bool allow_mt_repl = false;
#endif

#include <variant>

#include "dbms/constants.hpp"
#include "dbms/replication_handler.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

inline std::unique_ptr<Storage> CreateInMemoryStorage(Config config,
                                                      const ::memgraph::replication::ReplicationState &repl_state) {
  const auto wal_mode = config.durability.snapshot_wal_mode;
  const auto name = config.name;
  auto storage = std::make_unique<InMemoryStorage>(std::move(config));

  // Connect replication state and storage
  storage->CreateSnapshotHandler([storage = storage.get(), &repl_state](
                                     bool is_periodic) -> utils::BasicResult<InMemoryStorage::CreateSnapshotError> {
    if (repl_state.IsReplica()) {
      return InMemoryStorage::CreateSnapshotError::DisabledForReplica;
    }
    return storage->CreateSnapshot(is_periodic);
  });

  if (allow_mt_repl || name == dbms::kDefaultDB) {
    // Handle global replication state
    spdlog::info("Replication configuration will be stored and will be automatically restored in case of a crash.");
    // RECOVER REPLICA CONNECTIONS
    memgraph::dbms::RestoreReplication(repl_state, *storage);
  } else if (const ::memgraph::replication::RoleMainData *data =
                 std::get_if<::memgraph::replication::RoleMainData>(&repl_state.ReplicationData());
             data && !data->registered_replicas_.empty()) {
    spdlog::warn("Multi-tenant replication is currently not supported!");
  }

  if (wal_mode == Config::Durability::SnapshotWalMode::DISABLED && repl_state.IsMain()) {
    spdlog::warn(
        "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please consider "
        "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
        "without write-ahead logs this instance is not replicating any data.");
  }

  return std::move(storage);
}

}  // namespace memgraph::storage
