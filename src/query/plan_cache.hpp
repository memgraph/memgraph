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

// The plan cache's type, apart from the plan it holds. Naming the cache costs a
// caller the key type and the containers; naming the plan costs it the planner,
// the syntax tree and the symbol table as well. Whoever holds a cache needs the
// first and not the second.

#include <memory>

#include "query/frontend/stripped.hpp"
#include "utils/lru_cache.hpp"
#include "utils/rw_spin_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::query {

// Defined where plans are built. A holder of the cache only ever moves these
// around by pointer, so it does not need the definition.
class PlanWrapper;

// Declared identically where plans are built, so that a file needing the plan
// as well does not have to include this too. Naming the same type twice is
// allowed; the two must be changed together.

using PlanCache_t = utils::LRUCache<frontend::HashedString, std::shared_ptr<PlanWrapper>>;
using PlanCacheLRU = utils::Synchronized<PlanCache_t, utils::RWSpinLock>;

}  // namespace memgraph::query
