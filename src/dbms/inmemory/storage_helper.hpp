// Copyright 2026 Memgraph Ltd.
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

#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::dbms {

inline std::unique_ptr<storage::Storage> CreateInMemoryStorage(
    storage::Config config,
    storage::PlanInvalidatorPtr invalidator = std::make_unique<storage::PlanInvalidatorDefault>(),
    std::function<storage::DatabaseProtectorPtr()> database_protector_factory = nullptr) {
  const auto name = config.salient.name;

  // Use default safe factory from Storage constructor for basic usage
  auto storage = std::make_unique<storage::InMemoryStorage>(
      std::move(config), std::nullopt, std::move(invalidator), std::move(database_protector_factory));

  storage->ttl_.SetUserCheck([]() -> bool { return true; });

  // Connect replication state and storage
  storage->CreateSnapshotHandler(
      [storage = storage.get()]() -> std::expected<void, storage::InMemoryStorage::CreateSnapshotError> {
        auto result = storage->CreateSnapshot();
        if (!result) {
          return std::unexpected(result.error());
        }
        return {};
      });

  return std::move(storage);
}

}  // namespace memgraph::dbms
