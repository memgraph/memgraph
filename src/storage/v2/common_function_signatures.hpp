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
#include <functional>

namespace memgraph::storage {

struct Vertex;

using CheckCancelFunction = std::function<bool()>;
constexpr auto neverCancel = []() { return false; };

// default for when callback not provided
constexpr auto always_invalidate_plan_cache = []<typename... Args>(Args &&...) { return true; };

// Single-pass recovery index population.
// A worker-local callable that offers one vertex to a single index. It is move-only because it
// owns a private skip-list accessor. Vertex indices insert the vertex itself; edge indices insert
// the vertex's matching out-edges.
using IndexVertexInserter = std::move_only_function<void(Vertex &)>;
// Creates a fresh IndexVertexInserter (with its own accessor) for one worker thread. Copyable so
// it can be collected into a vector and shared across workers, each of which calls it once.
using IndexInserterFactory = std::function<IndexVertexInserter()>;

}  // namespace memgraph::storage
