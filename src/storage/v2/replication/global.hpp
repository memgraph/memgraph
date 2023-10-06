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

#pragma once

#include <cstdint>
#include <deque>
#include <string>
#include <utility>

#include "io/network/endpoint.hpp"
#include "storage/v2/replication/enums.hpp"
#include "utils/uuid.hpp"

// TODO: move to replication namespace and unify
namespace memgraph::storage {

struct TimestampInfo {
  uint64_t current_timestamp_of_replica;
  uint64_t current_number_of_timestamp_behind_master;
};

struct ReplicaInfo {
  std::string name;
  replication::ReplicationMode mode;
  io::network::Endpoint endpoint;
  replication::ReplicaState state;
  TimestampInfo timestamp_info;
};

}  // namespace memgraph::storage
