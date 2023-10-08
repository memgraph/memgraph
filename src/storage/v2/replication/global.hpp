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

// TODO: Should be at MAIN instance level; shouldn't be connected to storage
struct ReplicationEpoch {
  ReplicationEpoch() : id(utils::GenerateUUID()) {}

  // UUID to distinguish different main instance runs for replication process
  // on SAME storage.
  // Multiple instances can have same storage UUID and be MAIN at the same time.
  // We cannot compare commit timestamps of those instances if one of them
  // becomes the replica of the other so we use epoch_id_ as additional
  // discriminating property.
  // Example of this:
  // We have 2 instances of the same storage, S1 and S2.
  // S1 and S2 are MAIN and accept their own commits and write them to the WAL.
  // At the moment when S1 commited a transaction with timestamp 20, and S2
  // a different transaction with timestamp 15, we change S2's role to REPLICA
  // and register it on S1.
  // Without using the epoch_id, we don't know that S1 and S2 have completely
  // different transactions, we think that the S2 is behind only by 5 commits.
  std::string id;  // TODO: Move to replication level

  // Generates a new epoch id, returning the old one
  std::string NewEpoch() { return std::exchange(id, utils::GenerateUUID()); }

  std::string SetEpoch(std::string new_epoch) { return std::exchange(id, std::move(new_epoch)); }
};

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
