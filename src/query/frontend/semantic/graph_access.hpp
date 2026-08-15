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

/// True if running `query` needs the graph: it reads or writes vertices, edges or their metadata, or
/// resolves anything else that lives in storage. False only if the query can be answered from its own
/// literals, its parameters, and procedures that declare they never touch the graph.
///
/// The answer is conservative in one direction only: a query that needs the graph is never reported as
/// graph-free, while a graph-free query may be reported as needing the graph. Callers may therefore use a
/// false result to skip opening a storage transaction, but must treat a true result as "no opinion" rather
/// than proof that storage is reached.
///
/// This reads the query. The plan built from it is checked separately by `PlanRequiresStorageAccess`,
/// which is exact where this is conservative, and which is what the interpreter acts on: a query this
/// admits still opens a transaction if its plan turns out to need one.
///
/// The widest source of that conservatism is functions. A function implementation receives the accessor
/// and is free to use it, and nothing records which ones do, so calling any function is reported as
/// needing the graph. This also covers the string predicates, which parse as functions, so a `WHERE`
/// using `STARTS WITH` or `CONTAINS` is not graph-free. Narrowing that means declaring per function what
/// a procedure already declares for itself.
bool RequiresGraphAccess(const CypherQuery &query);

}  // namespace memgraph::query
