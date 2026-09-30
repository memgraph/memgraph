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

#include "planner/pattern/types.hpp"
import rollbear.strong_type;

namespace memgraph::planner::core::pattern::vm {

// SlotIdx is available from the enclosing pattern namespace via unqualified lookup.
// Re-export it so that vm::SlotIdx also works for callers.
using ::memgraph::planner::core::pattern::SlotIdx;

/// Index into the e-class register array.
/// E-class registers hold e-class IDs during pattern matching.
using EClassReg = strong::type<uint8_t, struct EClassReg_, strong::regular>;

/// Index into the e-node register array.
/// E-node registers hold e-node IDs and iteration state during pattern matching.
using ENodeReg = strong::type<uint8_t, struct ENodeReg_, strong::regular>;

/// Instruction address (jump target) in the bytecode.
/// Used for conditional jumps, backtracking, and loop targets.
using InstrAddr = strong::type<uint16_t, struct InstrAddr_, strong::regular>;

}  // namespace memgraph::planner::core::pattern::vm
