// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// This test serves as an example of a property-based model test.
// It generates a cluster configuration and a set of operations to
// apply against both the real system and a greatly simplified model.

#include <chrono>

#include <gtest/gtest.h>
#include <rapidcheck.h>
#include <rapidcheck/gtest.h>
#include <spdlog/cfg/env.h>

#include "generated_operations.hpp"

namespace memgraph::tests::simulation {

void my_code(NonEmptyOpVec input) { RC_ASSERT(input.ops.size() < 3); }

RC_GTEST_PROP(PropertyTestDemo, Demo1, (NonEmptyOpVec ops, uint64_t rng_seed)) {
  spdlog::cfg::load_env_levels();

  my_code(ops);

  spdlog::trace("passed stats comparison - all good!");
}

}  // namespace memgraph::tests::simulation
