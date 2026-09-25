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

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "utils/logging.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::query {

namespace {

storage::StorageMode ModeOrThrow(std::optional<storage::StorageMode> storage_mode, char const *message) {
  if (!storage_mode) [[unlikely]] {
    throw DatabaseContextRequiredException(message);
  }
  return *storage_mode;
}

StorageAccessRequirement IndexDdlAccess(bool creating, storage::StorageMode storage_mode) {
  using enum storage::StorageAccessType;
  if (storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    // Concurrent population of an index requires snapshot isolation.
    return {.access = creating ? READ_ONLY : READ,
            .mode_dependent = true,
            .isolation_override = storage::IsolationLevel::SNAPSHOT_ISOLATION};
  }
  if (storage_mode == storage::StorageMode::IN_MEMORY_ANALYTICAL) {
    // Read-only either way, so reads run alongside: creation needs writers out for the whole
    // population (see DowngradeToReadIfValid), and a drop is held to the same access.
    return {.access = READ_ONLY, .mode_dependent = true};
  }
  // ON_DISK_TRANSACTIONAL requires unique access.
  return {.access = UNIQUE, .mode_dependent = true};
}

}  // namespace

StorageAccessRequirement RequiredStorageAccess(Query const &query, storage::StorageAccessType cypher_access,
                                               std::optional<storage::StorageMode> storage_mode) {
  using enum storage::StorageAccessType;
  switch (query.AccessPolicy()) {
    case StorageAccessPolicy::kNone:
      return {};
    case StorageAccessPolicy::kRead:
      return {.access = READ};
    case StorageAccessPolicy::kUnique:
      return {.access = UNIQUE};
    case StorageAccessPolicy::kCypherShaped:
      // NO_ACCESS opens no storage transaction, and so leaves nothing to commit.
      if (cypher_access == NO_ACCESS) return {};
      return {.access = cypher_access, .could_commit = true};
    case StorageAccessPolicy::kProfiledShaped:
      // Never NO_ACCESS: graph-freedom is decided for a CypherQuery, and a profiled query is not
      // one, so it takes the access its own shape asks for. Which is right either way, since
      // PROFILE reports what an execution did and so needs there to have been one.
      return {.access = cypher_access};
    case StorageAccessPolicy::kIndexDdl: {
      if (auto const *index = utils::Downcast<IndexQuery const>(&query)) {
        return IndexDdlAccess(index->action_ == IndexQuery::Action::CREATE,
                              ModeOrThrow(storage_mode, "Database required for index query."));
      }
      auto const *edge_index = utils::Downcast<EdgeIndexQuery const>(&query);
      MG_ASSERT(edge_index, "A query kind claims index DDL without being one of the index queries");
      return IndexDdlAccess(edge_index->action_ == EdgeIndexQuery::Action::CREATE,
                            ModeOrThrow(storage_mode, "Database required for edge index query."));
    }
    case StorageAccessPolicy::kConstraintDdl:
      return {.access = ModeOrThrow(storage_mode, "Database required for constraint query.") ==
                                storage::StorageMode::ON_DISK_TRANSACTIONAL
                            ? UNIQUE
                            : READ_ONLY,
              .mode_dependent = true};
  }
  std::unreachable();
}

}  // namespace memgraph::query
