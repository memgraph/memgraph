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

#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::dbms {

inline std::unique_ptr<storage::Storage> CreateInMemoryStorage(
    storage::Config config,
    const utils::Synchronized<::memgraph::replication::ReplicationState, utils::RWSpinLock> &repl_state,
    storage::PlanInvalidatorPtr invalidator) {
  const auto name = config.salient.name;
  auto storage = std::make_unique<storage::InMemoryStorage>(std::move(config), std::nullopt, std::move(invalidator));

  // Connect replication state and storage
  storage->CreateSnapshotHandler(
      [storage = storage.get(), &repl_state]() -> utils::BasicResult<storage::InMemoryStorage::CreateSnapshotError> {
        // The GetRole should really be done after the Access has been granted.
        // Holding on to the lock for the duration of CreateSnapshot will cause a deadlock
        // Not holding the lock might allow a replica to create the snapshot if the role switch is happening
        const auto role = repl_state.ReadLock()->GetRole();
        return storage->CreateSnapshot(role);
      });

  return std::move(storage);
}

}  // namespace memgraph::dbms
