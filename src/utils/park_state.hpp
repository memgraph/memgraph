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

/// Single-owner park descriptor shared by `WorkerResumeEvent` and `DeadlineParkRegistry`.
///
/// `on_resume` is an opaque closure, not a bare `coroutine_handle`: resuming a parked Prepare is a
/// session-aware continuation (keep the session alive across a cross-thread resume, detect
/// shutdown before touching per-DB state, re-drive post-Execute bookkeeping) that must NOT leak
/// into these coroutine-agnostic registries. To them it is a black box.
///
/// Heap-allocated via `shared_ptr` (not a frame local) because up to three wake sources
/// (NotifyAll, deadline sweep, shutdown Drain, plus the awaitable's own abandon-path claim) race
/// on it: exactly one wins `ClaimPark` and invokes `on_resume`; the losers must still be able to
/// read `claimed` after the winner has driven the frame to destruction, so the flag must outlive
/// the frame.
struct ParkState {
  std::function<void()> on_resume;
  size_t worker_id{0};
  std::chrono::steady_clock::time_point deadline;
  std::atomic<bool> claimed{false};
};

/// Claims `ps` for exactly one wake source: returns true to a single caller ever, false to all
/// others (racing or late). Only the `true` winner may invoke `ps.on_resume()`.
inline bool ClaimPark(ParkState &ps) { return !ps.claimed.exchange(true, std::memory_order_acq_rel); }

}  // namespace memgraph::utils
