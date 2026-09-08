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

// Everything the memgraph.planner.core.egraph global module fragment (primary interface plus the :enode and :eclass
// partitions) includes. Include this before `import memgraph.planner.core.egraph;` so GCC never meets these headers
// textually for the first time after the import.

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <deque>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>
#include <boost/functional/hash.hpp>
#include <boost/unordered/unordered_flat_map.hpp>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/core/concepts.gmf.hpp"
#include "planner/core/constants.gmf.hpp"
#include "planner/core/eids.gmf.hpp"
#include "planner/core/union_find.gmf.hpp"
#include "utils/small_vector.hpp"
