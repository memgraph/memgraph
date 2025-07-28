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
#include <string>

#include "io/network/endpoint.hpp"
#include "replication/config.hpp"
#include "storage/v2/replication/enums.hpp"

// TODO: move to replication namespace and unify
namespace memgraph::storage {

struct TimestampInfo {
  uint64_t current_timestamp_of_replica{0};
  int64_t current_number_of_timestamp_behind_main{0};
};

struct ReplicaInfo {
  std::string name;
  replication_coordination_glue::ReplicationMode mode;
  io::network::Endpoint endpoint;
  replication::ReplicaState state;
  TimestampInfo timestamp_info;
};

}  // namespace memgraph::storage
