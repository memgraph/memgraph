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

#pragma once

#include <rapidcheck.h>

#include "testing_constants.hpp"

namespace memgraph::tests::simulation {

struct ClusterConfig {
  int servers;
  int replication_factor;
  int shards;

  friend std::ostream &operator<<(std::ostream &in, const ClusterConfig &cluster) {
    in << "ClusterConfig { servers: " << cluster.servers << ", replication_factor: " << cluster.replication_factor
       << ", shards: " << cluster.shards << " }";
    return in;
  }
};

}  // namespace memgraph::tests::simulation

// Required namespace for rapidcheck generator
namespace rc {

using memgraph::tests::simulation::ClusterConfig;

template <>
struct Arbitrary<ClusterConfig> {
  static Gen<ClusterConfig> arbitrary() {
    return gen::build<ClusterConfig>(
        // gen::inRange is [inclusive min, exclusive max)
        gen::set(&ClusterConfig::servers, gen::inRange(kMinimumServers, kMaximumServers)),
        gen::set(&ClusterConfig::replication_factor,
                 gen::inRange(kMinimumReplicationFactor, kMaximumReplicationFactor)),
        gen::set(&ClusterConfig::shards, gen::inRange(kMinimumShards, kMaximumShards)));
  }
};

}  // namespace rc
