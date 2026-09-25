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

#include <optional>

#include "storage/v2/access_type.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/storage_mode.hpp"

namespace memgraph::query {

class Query;

/// What an interpreter takes on the graph in order to prepare one query.
struct StorageAccessRequirement {
  /// Absent means the query is prepared with no storage accessor at all.
  std::optional<storage::StorageAccessType> access{};
  bool could_commit{false};
  /// Whether the storage mode fed `access` or `isolation_override`. Only then is the answer
  /// invalidated by a mode change between reading the mode and the accessor taking its hold, so
  /// only then is a retry worth its cost.
  bool mode_dependent{false};
  std::optional<storage::IsolationLevel> isolation_override{};

  friend bool operator==(StorageAccessRequirement const &, StorageAccessRequirement const &) = default;
};

/// Resolves the query kind's own `StorageAccessPolicy` against the two runtime inputs a policy can
/// need: the access the planner settled on for a Cypher statement, and the storage mode in force.
///
/// Throws `DatabaseContextRequiredException` for a policy the storage mode settles when no mode is
/// given, which is the caller having reached DDL with no current database.
StorageAccessRequirement RequiredStorageAccess(Query const &query, storage::StorageAccessType cypher_access,
                                               std::optional<storage::StorageMode> storage_mode);

}  // namespace memgraph::query
