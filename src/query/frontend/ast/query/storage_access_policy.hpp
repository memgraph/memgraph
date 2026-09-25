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

#include <variant>

#include "storage/v2/access_type.hpp"

namespace memgraph::query {

/// Needs no accessor: the query reads or changes instance, session or system state.
struct NoAccess {};

/// The query settles its own access, whether from its kind alone or from the action it carries.
struct FixedAccess {
  storage::StorageAccessType access;
};

/// Takes the access the planner settled on for this statement.
struct PlannerShaped {
  /// Whether there will be anything to commit. Profiling reports an execution rather than
  /// performing one of its own, so it commits nothing.
  bool commits;
};

/// Index DDL, whose access the storage mode decides.
struct IndexDdl {
  bool creating;
  /// Indexing edges rather than vertices. The access is the same either way; the two are told apart
  /// so that reaching either without a database names the statement that was refused.
  bool on_edges;
};

/// Constraint DDL, whose access the storage mode decides.
struct ConstraintDdl {};

/// What a query needs held on the graph while it is prepared.
///
/// Each query answers with one of these, and `RequiredStorageAccess` turns an answer plus the
/// runtime inputs into the access an interpreter takes. A case carries whatever settling it needs
/// beyond the kind, so no case has to be recovered by asking what the query was.
using StorageAccessPolicy = std::variant<NoAccess, FixedAccess, PlannerShaped, IndexDdl, ConstraintDdl>;

}  // namespace memgraph::query
