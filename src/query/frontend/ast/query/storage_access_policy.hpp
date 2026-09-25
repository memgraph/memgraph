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

#include <cstdint>

namespace memgraph::query {

/// What a kind of query needs held on the graph while it is prepared.
///
/// Each kind of query states its own policy, and `RequiredStorageAccess` turns a policy plus the
/// runtime inputs into the access an interpreter takes. The policies from `kCypherShaped` onwards
/// cannot be settled from the kind alone, so they name the rule rather than the answer. Keeping the
/// vocabulary free of storage types is what lets a query kind declare its policy without the AST
/// depending on the storage layer.
enum class StorageAccessPolicy : uint8_t {
  /// Needs no accessor: the query reads or changes instance, session or system state.
  kNone,
  /// Reads graph data, or metadata that tolerates concurrent writers.
  kRead,
  /// Excludes every other accessor for as long as it is held.
  kUnique,
  /// Takes the access the planner settled on for this statement, and has something to commit.
  kCypherShaped,
  /// Takes the same access as `kCypherShaped` with nothing to commit, since profiling reports an
  /// execution rather than performing one of its own.
  kProfiledShaped,
  /// Index DDL: the storage mode decides, and creating differs from dropping.
  kIndexDdl,
  /// Constraint DDL: the storage mode decides.
  kConstraintDdl,
  /// The kind's mutating actions take the graph to themselves; its reading actions read.
  kUniqueWhenMutating,
  /// Only the kind's creating action needs an accessor, and only to read with it.
  kReadWhenCreating,
};

}  // namespace memgraph::query
