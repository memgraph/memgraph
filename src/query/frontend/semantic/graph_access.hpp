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

namespace memgraph::query {

class CypherQuery;

/// True if running `query` reads or writes vertices, edges or their metadata, or resolves anything else
/// that lives in storage.
///
/// Conservative in one direction: a query that needs the graph is never reported as graph-free, so a
/// false answer is safe to act on, while a true answer means only that this could not prove otherwise.
/// `PlanRequiresStorageAccess` answers the same question exactly, once a plan exists.
///
/// Every function counts as needing the graph, because implementations receive the accessor and nothing
/// records which of them use it. That includes the string predicates, which parse as functions.
bool RequiresGraphAccess(const CypherQuery &query);

}  // namespace memgraph::query
