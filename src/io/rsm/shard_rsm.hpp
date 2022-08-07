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

#include <deque>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "coordinator/hybrid_logical_clock.hpp"
#include "io/address.hpp"
#include "io/rsm/raft.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"

using memgraph::coordinator::Hlc;
using memgraph::io::Address;
using memgraph::io::Io;
using memgraph::io::ResponseEnvelope;
using memgraph::io::ResponseFuture;
using memgraph::io::ResponseResult;
using memgraph::io::rsm::Raft;
using memgraph::io::rsm::ReadRequest;
using memgraph::io::rsm::ReadResponse;
using memgraph::io::rsm::WriteRequest;
using memgraph::io::rsm::WriteResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;
using memgraph::storage::PropertyValue;

namespace memgraph::io::rsm {

using ShardRsmKey = std::vector<PropertyValue>;

struct StorageWriteRequest {
  ShardRsmKey key;
  std::optional<int> value;
};

struct StorageWriteResponse {
  bool shard_rsm_success;
  std::optional<int> last_value;
  // Only has a value if the given shard does not contain the requested key
  std::optional<Hlc> latest_known_shard_map_version{std::nullopt};
};

struct StorageGetRequest {
  ShardRsmKey key;
};

struct StorageGetResponse {
  bool shard_rsm_success;
  std::optional<int> value;
  // Only has a value if the given shard does not contain the requested key
  std::optional<Hlc> latest_known_shard_map_version{std::nullopt};
};

class StorageRsm {
  std::map<ShardRsmKey, int> state_;
  ShardRsmKey minimum_key_;
  std::optional<ShardRsmKey> maximum_key_{std::nullopt};
  Hlc shard_map_version_;

  // The key is not located in this shard
  bool IsKeyInRange(const ShardRsmKey &key) {
    if (maximum_key_) [[likely]] {
      return (key >= minimum_key_ && key <= maximum_key_);
    }
    return key >= minimum_key_;
  }

 public:
  StorageGetResponse Read(StorageGetRequest request) {
    StorageGetResponse ret;

    if (IsKeyInRange(request.key)) {
      ret.latest_known_shard_map_version = shard_map_version_;
      ret.shard_rsm_success = false;
    } else if (state_.contains(request.key)) {
      ret.value = state_[request.key];
      ret.shard_rsm_success = true;
    } else {
      ret.shard_rsm_success = false;
      ret.value = std::nullopt;
    }
    return ret;
  }

  StorageWriteResponse Apply(StorageWriteRequest request) {
    StorageWriteResponse ret;

    // Key is outside the prohibited range
    if (IsKeyInRange(request.key)) {
      ret.latest_known_shard_map_version = shard_map_version_;
      ret.shard_rsm_success = false;
    }
    // Key exist
    else if (state_.contains(request.key)) {
      auto &val = state_[request.key];

      /*
       *   Delete
       */
      if (!request.value) {
        ret.shard_rsm_success = true;
        ret.last_value = val;
        state_.erase(state_.find(request.key));
      }

      /*
       *   Update
       */
      // Does old_value match?
      if (request.value == val) {
        ret.last_value = val;
        ret.shard_rsm_success = true;

        val = request.value.value();
      } else {
        ret.last_value = val;
        ret.shard_rsm_success = false;
      }
    }
    /*
     *   Create
     */
    else {
      ret.last_value = std::nullopt;
      ret.shard_rsm_success = true;

      state_.emplace(request.key, std::move(request.value).value());
    }

    return ret;
  }
};

}  // namespace memgraph::io::rsm
