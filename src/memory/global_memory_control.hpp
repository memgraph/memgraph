// Copyright 2025 Memgraph Ltd.
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

namespace memgraph::memory {

/// Disable jemalloc's internal decay timing (call at startup before SetHooks).
/// This prevents clock_gettime calls during malloc/free operations.
/// After calling this, you should use DecayUnusedMemory() periodically to return
/// memory to the OS.
void DisableInternalDecay();

/// Trigger decay-based purging on all arenas.
/// This is gentler than PurgeUnusedMemory() - it uses jemalloc's decay curve
/// rather than immediately purging all unused pages.
/// Call this periodically from a scheduler to return memory to the OS.
void DecayUnusedMemory();

/// Immediately purge all unused dirty pages from all arenas.
/// This is more aggressive than DecayUnusedMemory().
void PurgeUnusedMemory();

/// Install custom extent hooks for memory tracking.
void SetHooks();

/// Restore original extent hooks.
void UnsetHooks();

}  // namespace memgraph::memory
