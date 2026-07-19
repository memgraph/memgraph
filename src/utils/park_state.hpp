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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>

namespace memgraph::utils {

/// Single-owner park descriptor shared by `WorkerResumeEvent` and `DeadlineParkRegistry` (IP-1
/// design doc, opencode-work/resource-lock-starvation/coro-prepare/ip1-design.md, REVISION 2's
/// "ParkState single-owner model" and REVISION 4 R4.1/R4.2/R4.3 -- BINDING on this shape).
///
/// ---------------------------------------------------------------------------------------------
/// Why `on_resume` is a closure, not a bare `std::coroutine_handle<>` (R4.1/R4.2).
/// ---------------------------------------------------------------------------------------------
/// An earlier revision of this design stored a bare `std::coroutine_handle<> handle` here and had
/// every wake source `handle.resume()` it directly. That shape cannot express the real resume
/// contract: resuming a parked Prepare-coroutine is not just "continue this frame" -- it is a
/// SESSION-AWARE continuation that (per R4.2) must keep the owning session alive across a
/// cross-thread resume, detect pool shutdown before touching any per-database state, and, once
/// the frame completes, re-drive the connection's normal post-Execute bookkeeping. None of that
/// can live inside `WorkerResumeEvent`/`DeadlineParkRegistry` themselves (R4.1: they must stay
/// coroutine-agnostic and reusable outside the Prepare-accessor use case), so it is factored out
/// into an opaque `std::function<void()>` supplied by whoever constructs the `ParkState`. From
/// the point of view of this header and its two registries, `on_resume` is a black box: they
/// never need to know it wraps a coroutine handle at all.
///
/// Only the caller that wins `ClaimPark` may invoke `on_resume` -- exactly once, exactly one
/// winner across every wake source that ever races on this `ParkState` (lock-release
/// `WorkerResumeEvent::NotifyAll`, `DeadlineParkRegistry::Sweep`'s deadline sweep, either
/// registry's shutdown `Drain()`, and the awaitable's own abandon-path claim, R4.3). Losers must
/// not call `on_resume` and must not otherwise touch this `ParkState` beyond reading `claimed`.
///
/// ---------------------------------------------------------------------------------------------
/// Why `ParkState` is heap-allocated via `shared_ptr`, not a coroutine-frame local (R2).
/// ---------------------------------------------------------------------------------------------
/// A parked coroutine can be woken by up to three independent sources (see above). Exactly one of
/// them may ever invoke `on_resume`; the others must detect "already taken" and touch nothing
/// else. `claimed` is the single-owner flag that arbitrates this -- but for it to be observable by
/// a *losing* waker, it must remain valid storage even after the *winning* waker has invoked
/// `on_resume` (and thereby possibly driven the coroutine frame to completion and destruction).
/// If `ParkState` lived inside the coroutine frame, a losing waker could race the frame's
/// destruction and read a dangling `claimed` flag. Keeping it on its own heap allocation, kept
/// alive by `shared_ptr` refcounting from every registry it is registered in (plus, in the real
/// integration, the coroutine frame's own reference so it can find its `ParkState` again on a
/// re-probe), avoids that race entirely.
struct ParkState {
  std::function<void()> on_resume;
  size_t worker_id{0};
  std::chrono::steady_clock::time_point deadline;
  std::atomic<bool> claimed{false};
};

/// Attempts to claim `ps` for exactly one wake source. Returns true to EXACTLY ONE caller across
/// all wake sources that ever race on the same `ParkState` -- every other caller, whether racing
/// concurrently or arriving after the fact, gets false. Only the caller that receives `true` may
/// invoke `ps.on_resume()`; a caller that receives `false` must not invoke it and must simply stop
/// (R4.3: this includes the awaitable's own abandon-path claim attempt, which loses exactly when
/// some other wake source got there first and is already driving -- or has already driven --
/// `on_resume` to completion).
inline bool ClaimPark(ParkState &ps) { return !ps.claimed.exchange(true, std::memory_order_acq_rel); }

}  // namespace memgraph::utils
