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

#include "query/plan/profile.hpp"
#include "utils/tsc.hpp"

namespace memgraph::query {
class DbAccessor;
}

namespace memgraph::query::plan {

/// Small struct threaded through MakeCursor so each cursor's ctor can
/// resolve its `ProfilingStats *` slot once, instead of having every Pull
/// search/create the slot under a shifting parent pointer in
/// ExecutionContext.
///
/// `parent_stats == nullptr` is the "no profiling" sentinel - propagates
/// down through ChildContext so cursors built under it keep their own
/// `profile_slot_` null and skip the timer in Pull entirely.
struct ProfileContext {
  ProfilingStats *parent_stats{nullptr};
  const DbAccessor *db_accessor{nullptr};
};

/// Returns the ProfileContext to pass to the children of an operator that
/// has just appended its own slot. When `child_slot` is null (profiling
/// is off), children get a no-profiling context too.
inline ProfileContext ChildContext(ProfileContext parent, ProfilingStats *child_slot) noexcept {
  parent.parent_stats = child_slot;
  return parent;
}

/// RAII timer over a precomputed slot. The cursor's ctor resolved the
/// slot once; Pull just times the call. When `slot` is null the ctor
/// and dtor are both predicted-out branches.
class ScopedProfile {
 public:
  explicit ScopedProfile(ProfilingStats *slot) noexcept : slot_(slot) {
    if (slot_) {
      slot_->actual_hits++;
      start_time_ = utils::ReadTSC();
    }
  }

  ScopedProfile(const ScopedProfile &) = delete;
  ScopedProfile &operator=(const ScopedProfile &) = delete;
  ScopedProfile(ScopedProfile &&) = delete;
  ScopedProfile &operator=(ScopedProfile &&) = delete;

  ~ScopedProfile() noexcept { Stop(); }

  /// Finalise the timer early (idempotent). Used by parallel-execution branch
  /// scopes that need to attribute time to the branch before running the
  /// surrounding cleanup/accounting in an OnScopeExit handler.
  void Stop() noexcept {
    if (slot_) {
      slot_->num_cycles += utils::ReadTSC() - start_time_;
      slot_ = nullptr;
    }
  }

 private:
  ProfilingStats *slot_;
  uint64_t start_time_{0};
};

}  // namespace memgraph::query::plan
