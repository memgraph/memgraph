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

/// The ShardRsm is a simple in-memory raft-backed kv store that can be used for simple testing
/// and implementation of some query engine logic before storage engines are fully implemented.
///
/// To implement multiple read and write commands, change the StorageRead* and StorageWrite* requests
/// and responses to a std::variant of the different options, and route them to specific handlers in
/// the ShardRsm's Read and Apply methods. Remember that Read is called immediately when the Raft
/// leader receives the request, and does not replicate anything over Raft. Apply is called only
/// AFTER the StorageWriteRequest is replicated to a majority of Raft peers, and the result of calling
/// ShardRsm::Apply(StorageWriteRequest) is returned to the client that submitted the request.

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
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "utils/logging.hpp"

namespace memgraph::io::rsm {

using memgraph::coordinator::Hlc;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::PropertyValue;

using ShardRsmKey = std::vector<PropertyValue>;

struct StorageWriteRequest {
  LabelId label_id;
  Hlc transaction_id;
  ShardRsmKey key;
  std::optional<int> value;
};

struct StorageWriteResponse {
  bool shard_rsm_success;
  std::optional<int> last_value;
  // Only has a value if the given shard does not contain the requested key
  std::optional<Hlc> latest_known_shard_map_version{std::nullopt};
};

struct StorageReadRequest {
  LabelId label_id;
  Hlc transaction_id;
  ShardRsmKey key;
};

struct StorageReadResponse {
  bool shard_rsm_success;
  std::optional<int> value;
  // Only has a value if the given shard does not contain the requested key
  std::optional<Hlc> latest_known_shard_map_version{std::nullopt};
};

class ShardRsm {
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
  StorageReadResponse Read(StorageReadRequest request) {
    StorageReadResponse ret;

    if (!IsKeyInRange(request.key)) {
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
    if (!IsKeyInRange(request.key)) {
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

      state_.emplace(request.key, request.value.value());
    }

    return ret;
  }
};

}  // namespace memgraph::io::rsm
