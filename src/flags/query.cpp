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
#include "flags/query.hpp"

#include <limits>

#include "utils/flag_validation.hpp"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
// DEFINE_bool(cartesian_product_enabled, true, "Enable cartesian product expansion.");  Moved to run_time_configurable

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_int32(
    hops_limit_recheck_interval, 10,
    "Interval (every how many hops) at which to recheck the hops limit. Used for parallel execution only.",
    FLAG_IN_RANGE(1, std::numeric_limits<int32_t>::max()));
