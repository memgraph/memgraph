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

#include "dbms/database.hpp"
#include "flags/storage_mode.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage_mode.hpp"

template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;

namespace memgraph::dbms {

Database::Database(const storage::Config &config, const replication::ReplicationState &repl_state)
    : trigger_store_(config.durability.storage_directory / "triggers"),
      streams_{config.durability.storage_directory / "streams"},
      repl_state_(&repl_state) {
  if (config.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL || config.force_on_disk ||
      utils::DirExists(config.disk.main_storage_directory)) {
    storage_ = std::make_unique<storage::DiskStorage>(config);
  } else {
    storage_ = std::make_unique<storage::InMemoryStorage>(config, config.storage_mode);
    auto *storage = static_cast<storage::InMemoryStorage *>(storage_.get());
    storage->CreateSnapshotHandler(
        [storage, &repl_state](bool is_periodic) -> utils::BasicResult<storage::InMemoryStorage::CreateSnapshotError> {
          if (repl_state.IsReplica()) {
            return storage::InMemoryStorage::CreateSnapshotError::DisabledForReplica;
          }
          return storage->CreateSnapshot(is_periodic);
        });
  }
}

void Database::SwitchToOnDisk() {
  storage_ = std::make_unique<memgraph::storage::DiskStorage>(std::move(storage_->config_));
}

}  // namespace memgraph::dbms
