// Copyright 2022 Memgraph Ltd.
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

#include "generated_operations.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/time.hpp"
#include "test_cluster.hpp"

struct NonEmptyOpVec {
  std::vector<memgraph::tests::simulation::Op> ops;

  friend std::ostream &operator<<(std::ostream &in, const NonEmptyOpVec &op) {
    in << "[";
    bool first = true;
    for (const auto &op : op.ops) {
      if (!first) {
        in << ", ";
      }
      in << op;
      first = false;
    }
    in << "]";

    return in;
  }
};

namespace rc {

template <>
struct Arbitrary<NonEmptyOpVec> {
  static Gen<NonEmptyOpVec> arbitrary() {
    return gen::build<NonEmptyOpVec>(
        gen::set(&NonEmptyOpVec::ops, gen::nonEmpty<std::vector<memgraph::tests::simulation::Op>>()));
  }
};

}  // namespace rc

namespace memgraph::tests::simulation {

using memgraph::io::Time;
using memgraph::io::simulator::SimulatorConfig;

RC_GTEST_PROP(RandomClusterConfig, HappyPath, (ClusterConfig cluster_config, NonEmptyOpVec ops)) {
  SimulatorConfig sim_config{
      .drop_percent = 0,
      .perform_timeouts = false,
      .scramble_messages = true,
      .rng_seed = 0,
      .start_time = Time::min() + std::chrono::microseconds{256 * 1024},
      .abort_time = Time::min() + std::chrono::microseconds{64 * 1024 * 1024},
  };

  RunClusterSimulation(sim_config, cluster_config, ops.ops);
}

}  // namespace memgraph::tests::simulation
