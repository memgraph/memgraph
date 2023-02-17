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
#include <spdlog/cfg/env.h>

#include "generated_operations.hpp"
#include "io/simulator/simulator_config.hpp"
#include "io/time.hpp"
#include "storage/v3/shard_manager.hpp"
#include "test_cluster.hpp"

namespace memgraph::tests::simulation {

using io::Duration;
using io::Time;
using io::simulator::SimulatorConfig;
using storage::v3::kMaximumCronInterval;

RC_GTEST_PROP(RandomClusterConfig, HappyPath, (ClusterConfig cluster_config, NonEmptyOpVec ops, uint64_t rng_seed)) {
  spdlog::cfg::load_env_levels();

  SimulatorConfig sim_config{
      .drop_percent = 0,
      .perform_timeouts = true,
      .scramble_messages = true,
      .rng_seed = rng_seed,
      .start_time = Time::min(),
      // TODO(tyler) set abort_time to something more restrictive than Time::max()
      .abort_time = Time::max(),
  };

  auto [sim_stats_1, latency_stats_1] = RunClusterSimulation(sim_config, cluster_config, ops.ops);
  auto [sim_stats_2, latency_stats_2] = RunClusterSimulation(sim_config, cluster_config, ops.ops);

  if (latency_stats_1 != latency_stats_2) {
    spdlog::error("simulator stats diverged across runs");
    spdlog::error("run 1 simulator stats: {}", sim_stats_1);
    spdlog::error("run 2 simulator stats: {}", sim_stats_2);
    spdlog::error("run 1 latency:\n{}", latency_stats_1.SummaryTable());
    spdlog::error("run 2 latency:\n{}", latency_stats_2.SummaryTable());
    RC_ASSERT(latency_stats_1 == latency_stats_2);
    RC_ASSERT(sim_stats_1 == sim_stats_2);
  }
}

}  // namespace memgraph::tests::simulation
