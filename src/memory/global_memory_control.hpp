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

namespace memgraph::memory {

void PurgeUnusedMemory();
// Forces jemalloc to perform lazy per-thread state setup (TSD/tcache) while
// OutOfMemoryExceptionBlocker is active.
//
// Why this exists:
// The first allocation or deallocation on a fresh thread can enter jemalloc
// before any Memgraph tracking code has established the thread's allocator
// state. During that first entry jemalloc may lazily initialize internal
// thread-local structures. If our memory-limit tracking rejects allocator-
// internal work during that bootstrap phase, jemalloc can observe inconsistent
// per-thread state and later trip internal assertions.
//
// This helper makes that first jemalloc entry happen under a blocker so
// allocator-internal bootstrap work is never treated as user-visible OOM. It
// is intentionally exposed from mg-memory-utils so the raw malloc/free wrappers
// in mg-memory can call it without depending on utils::MemoryTracker directly.
//
// The function is idempotent per thread. A small thread-local state machine
// prevents recursive warmup when the bootstrap allocation re-enters our malloc
// wrappers.
void EnsureJemallocThreadStateInitialized();
void SetHooks();
void UnsetHooks();
void EnableBackgroundThreads();

}  // namespace memgraph::memory
