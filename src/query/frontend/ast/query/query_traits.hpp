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
#include <variant>

namespace memgraph::query {

/// An access that names a hold on the graph.
///
/// Every value here is a hold that can actually be taken, so the absence of one is not among them:
/// a query that needs no accessor says so with `NoAccess`, and there is no second way to say it.
enum class HeldAccess : uint8_t {
  /// Reads graph data, or metadata that tolerates concurrent writers.
  kRead,
  /// Writes graph data.
  kWrite,
  /// Excludes every other accessor for as long as it is held.
  kUnique,
  /// Ensures writers have gone.
  kReadOnly,
};

/// Needs no accessor: the query reads or changes instance, session or system state.
struct NoAccess {
  friend bool operator==(NoAccess const &, NoAccess const &) = default;
};

/// The query settles its own access, whether from its kind alone or from the action it carries.
struct FixedAccess {
  HeldAccess access;

  friend bool operator==(FixedAccess const &, FixedAccess const &) = default;
};

/// Takes the access the planner settled on for this statement.
struct PlannerShaped {
  /// Whether there will be anything to commit. Profiling reports an execution rather than
  /// performing one of its own, so it commits nothing.
  bool commits;

  friend bool operator==(PlannerShaped const &, PlannerShaped const &) = default;
};

/// Index DDL, whose access the storage mode decides. The same either way for vertices and edges.
struct IndexDdl {
  bool creating;

  friend bool operator==(IndexDdl const &, IndexDdl const &) = default;
};

/// Constraint DDL, whose access the storage mode decides.
struct ConstraintDdl {
  friend bool operator==(ConstraintDdl const &, ConstraintDdl const &) = default;
};

/// What a query needs held on the graph while it is prepared.
///
/// `RequiredStorageAccess` turns one of these plus the runtime inputs into the access an interpreter
/// takes. A case carries whatever settling it needs beyond the kind, so no case has to be recovered
/// by asking what the query was.
using StorageAccessPolicy = std::variant<NoAccess, FixedAccess, PlannerShaped, IndexDdl, ConstraintDdl>;

/// Everything a query states about itself that its callers need before running it.
///
/// One accessor rather than one per fact, so that stating a further fact adds a field here instead
/// of a virtual to every query. A field that most queries answer the same way carries that answer
/// as its default, and the default is whichever answer is safe to inherit.
struct QueryTraits {
  /// Carries no default answer, since what a query needs held is never safe to guess. Value
  /// initialisation still reaches `NoAccess`, so what catches a query that states nothing is the
  /// test holding every query's answer rather than the compiler.
  StorageAccessPolicy access;

  /// Whether the query works on the current database's graph data, including the metadata
  /// describing it, rather than on instance, session or system state. A database that failed
  /// recovery serves none of these until it has been recovered, so the default refuses.
  bool operates_on_graph_data = true;
};

}  // namespace memgraph::query
