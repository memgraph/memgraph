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

#include "query/plan/cursor_awaitable.hpp"

#include "flags/experimental.hpp"
#include "query/plan/operator.hpp"

namespace memgraph::query::plan {

// Base Cursor seam definitions (kept out-of-line: the ctor needs the flags header, Pull() needs
// ResumePullStep, and Reset() matches the rest of the cursor cpp).

// Capture the routing decision ONCE per cursor (per query). Reading the flag here keeps the
// per-row Pull() router branch on an immutable member instead of a global flag read.
Cursor::Cursor() : use_coroutine_(flags::AreExperimentsEnabled(flags::Experiments::COROUTINE_CURSORS)) {}

void Cursor::Reset() { gen_.reset(); }

bool Cursor::Pull(Frame &f, ExecutionContext &ctx) {
  // DUAL-PATH router. flag OFF -> the cursor's verbatim legacy body (PullLegacy); the call graph
  // stays legacy because PullLegacy calls child->Pull() which re-enters this router.
  if (!use_coroutine_) return PullLegacy(f, ctx);

  // flag ON -> SINGLE-RESULT contract: resume the generator EXACTLY ONCE (one co_yield == one row).
  // Converted cursors override PullCo() to drive gen_.
  auto ra = PullCo(f, ctx);
  return ResumePullStep(ra, ctx).status == PullRunResult::Status::HasRow;
}

PullRunResult ResumePullStep(PullAwaitable::ResumeAwaitable &ra, ExecutionContext & /*ctx*/) {
  // Immediate (legacy, no-frame) or already-exhausted generator: report without resuming.
  if (ra.Done()) {
    ra.RethrowIfException();
    return ra.Result() ? PullRunResult::Row() : PullRunResult::Done();
  }

  // PR4 yield-OFF: a single resume runs the whole symmetric-transfer chain down the operator
  // tree and back, landing on the next co_yield (one row) or co_return (exhausted). Cooperative
  // scheduler yield (PR10) will add the Yielded path here using ctx.
  ra.GetHandle().resume();

  // Surface any exception the coroutine stored via unhandled_exception() before reporting status.
  ra.RethrowIfException();

  return ra.Result() ? PullRunResult::Row() : PullRunResult::Done();
}

}  // namespace memgraph::query::plan
