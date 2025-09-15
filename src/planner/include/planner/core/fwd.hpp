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

#include <cstdint>
#include "planner/core/concepts.hpp"
#include "planner/core/eids.hpp"

namespace memgraph::planner::core {

template <typename Symbol>
requires ENodeSymbol<Symbol>
struct ENode;

template <typename Analysis>
struct EClass;

template <typename Symbol, typename Analysis>
struct EGraph;

struct UnionFind;
struct UnionFindContext;

template <typename Symbol>
struct ProcessingContext;

}  // namespace memgraph::planner::core
