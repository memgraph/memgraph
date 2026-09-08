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

// Everything the memgraph.planner.core.typed_egraph global module fragment includes. Include this before
// `import memgraph.planner.core.typed_egraph;` so GCC never meets these headers textually for the first time after
// the import.

#include <concepts>
#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>

#include "planner/core/egraph.gmf.hpp"
#include "utils/small_vector.hpp"
