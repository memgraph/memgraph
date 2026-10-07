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

#include "query/plan/cursor_awaitable_core.hpp"

namespace memgraph::query {
struct ExecutionContext;
}  // namespace memgraph::query

namespace memgraph::query::plan {

/// Drives the root generator forward by EXACTLY ONE step: resume once and report whether a
/// single row was produced (HasRow), the generator is exhausted (Done), or — once cooperative
/// yield is wired (PR10) — the worker yielded (Yielded). This is the SINGLE-RESULT contract:
/// one resume == one co_yield == one row. It is NOT a drain.
///
/// PR4 runs yield-OFF: the resume is a synchronous symmetric-transfer pass down the operator
/// tree and back, so only HasRow/Done are produced here. The ExecutionContext parameter is kept
/// for the forward-compatible signature (PR10 inspects it for scheduler yield).
PullRunResult ResumePullStep(PullAwaitable::ResumeAwaitable &ra, ExecutionContext &ctx);

}  // namespace memgraph::query::plan
