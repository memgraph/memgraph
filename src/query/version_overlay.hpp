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

#include <vector>

#include "storage/v2/versioning/version_delta_store.hpp"

namespace memgraph::query {

class DbAccessor;
class TriggerContextCollector;

// Replays a version's overlay deltas into the live (master) transaction held by `dba`, so the
// subsequent query observes the version's state. The caller is expected to abort the transaction
// afterwards, leaving master untouched. In-memory storage only (uses CreateVertexEx/CreateEdgeEx
// for gid preservation, which the on-disk accessor does not expose); throws otherwise.
//
// When `strict` is true, a delta whose target object is missing (e.g. a SetProperty/AddLabel/Delete
// on a vertex/edge that was never created in the replayed set) is treated as a conflict and throws,
// rather than being silently skipped. Used by REVERT BRANCH COMMIT to dry-run-validate that pruning a
// commit does not orphan surviving deltas; the normal read path uses the lenient default.
void ApplyVersionOverlay(DbAccessor *dba, const std::vector<storage::OverlayDelta> &deltas, bool strict = false);

// Recomputes a version's complete overlay from the changes a query made, as accumulated by
// `collector`. Because replay + the query's own writes are all recorded relative to master
// (View::OLD), the captured set IS the new overlay — callers ReplaceAll() it into the store.
// Final values are read back from `dba` at View::NEW. Property removals are not yet representable
// and are skipped (see OverlayOp — there is no remove-property op).
std::vector<storage::OverlayDelta> CaptureVersionOverlay(DbAccessor *dba, const TriggerContextCollector &collector);

}  // namespace memgraph::query
