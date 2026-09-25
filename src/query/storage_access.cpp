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

#include "query/storage_access.hpp"

#include <utility>
#include <variant>

#include "query/exceptions.hpp"
#include "query/frontend/ast/query/query.hpp"
#include "utils/variant_helpers.hpp"

namespace memgraph::query {

namespace {

storage::StorageMode ModeOrThrow(std::optional<storage::StorageMode> storage_mode, char const *message) {
  if (!storage_mode) [[unlikely]] {
    throw DatabaseContextRequiredException(message);
  }
  return *storage_mode;
}

StorageAccessRequirement IndexDdlAccess(bool creating, storage::StorageMode storage_mode) {
  using enum HeldAccess;
  if (storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    // Concurrent population of an index requires snapshot isolation.
    return {.access = creating ? kReadOnly : kRead,
            .mode_dependent = true,
            .isolation_override = storage::IsolationLevel::SNAPSHOT_ISOLATION};
  }
  if (storage_mode == storage::StorageMode::IN_MEMORY_ANALYTICAL) {
    // Read-only either way, so reads run alongside: creation needs writers out for the whole
    // population (see DowngradeToReadIfValid), and a drop is held to the same access.
    return {.access = kReadOnly, .mode_dependent = true};
  }
  // ON_DISK_TRANSACTIONAL requires unique access.
  return {.access = kUnique, .mode_dependent = true};
}

}  // namespace

storage::StorageAccessType ToStorageAccessType(HeldAccess access) {
  switch (access) {
    case HeldAccess::kRead:
      return storage::StorageAccessType::READ;
    case HeldAccess::kWrite:
      return storage::StorageAccessType::WRITE;
    case HeldAccess::kUnique:
      return storage::StorageAccessType::UNIQUE;
    case HeldAccess::kReadOnly:
      return storage::StorageAccessType::READ_ONLY;
  }
  std::unreachable();
}

StorageAccessRequirement RequiredStorageAccess(Query const &query, std::optional<HeldAccess> cypher_access,
                                               std::optional<storage::StorageMode> storage_mode) {
  using enum HeldAccess;
  return std::visit(utils::Overloaded{
                        [](NoAccess) -> StorageAccessRequirement { return {}; },
                        [](FixedAccess settled) -> StorageAccessRequirement { return {.access = settled.access}; },
                        [cypher_access](PlannerShaped shaped) -> StorageAccessRequirement {
                          // The planner settled on no hold, so there is none to take and nothing to commit.
                          if (!cypher_access) return {};
                          return {.access = *cypher_access, .could_commit = shaped.commits};
                        },
                        [storage_mode](IndexDdl ddl) -> StorageAccessRequirement {
                          auto const *subject = ddl.on_edges ? "Database required for edge index query."
                                                             : "Database required for index query.";
                          return IndexDdlAccess(ddl.creating, ModeOrThrow(storage_mode, subject));
                        },
                        [storage_mode](ConstraintDdl) -> StorageAccessRequirement {
                          auto const mode = ModeOrThrow(storage_mode, "Database required for constraint query.");
                          return {.access = mode == storage::StorageMode::ON_DISK_TRANSACTIONAL ? kUnique : kReadOnly,
                                  .mode_dependent = true};
                        },
                    },
                    query.Traits().access);
}

}  // namespace memgraph::query
