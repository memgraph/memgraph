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

#include <functional>

namespace memgraph::storage {

// Test-only instrumentation for the commit path (lock-free-read-snapshot experiment).
// A test installs a CommitProbe on the storage; the commit path invokes each hook at the
// corresponding phase boundary so the test can block it on a latch and run another
// transaction at a precise instant. In production the storage's probe pointer is null and
// every call site is a single predictable null-check (near-zero cost). Hooks are optional.
struct CommitProbe {
  std::function<void()> after_mint;         // T minted, engine_lock released, before durability
  std::function<void()> during_durability;  // inside the (unlocked) WAL+replication window
  std::function<void()> before_publish;     // about to reacquire engine_lock to publish
  std::function<void()> after_publish;      // visibility store + watermark advanced
};

// Safe invoker: no-op if the hook is unset. Call sites use InvokeProbe(probe, &CommitProbe::after_mint).
inline void InvokeProbe(CommitProbe *probe, std::function<void()> CommitProbe::*hook) {
  if (probe != nullptr && (probe->*hook)) (probe->*hook)();
}

}  // namespace memgraph::storage
