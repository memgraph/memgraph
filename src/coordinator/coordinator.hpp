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

#include <optional>
#include <string>
#include <unordered_set>
#include <variant>
#include <vector>

#include "coordinator/hybrid_logical_clock.hpp"
#include "coordinator/shard_map.hpp"
#include "io/simulator/simulator.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/schemas.hpp"

namespace memgraph::coordinator {

using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::PropertyId;
using Address = memgraph::io::Address;
using SimT = memgraph::io::simulator::SimulatorTransport;
using memgraph::storage::v3::SchemaProperty;

struct HlcRequest {
  Hlc last_shard_map_version;
};

struct HlcResponse {
  Hlc new_hlc;
  std::optional<ShardMap> fresher_shard_map;
};

struct GetShardMapRequest {
  // No state
};

struct GetShardMapResponse {
  ShardMap shard_map;
};

struct AllocateHlcBatchResponse {
  bool success;
  Hlc low;
  Hlc high;
};

struct AllocateEdgeIdBatchRequest {
  size_t batch_size;
};

struct AllocateEdgeIdBatchResponse {
  uint64_t low;
  uint64_t high;
};

struct AllocatePropertyIdsRequest {
  std::vector<std::string> property_names;
};

struct AllocatePropertyIdsResponse {
  std::map<std::string, PropertyId> property_ids;
};

struct SplitShardRequest {
  Hlc previous_shard_map_version;
  LabelId label_id;
  CompoundKey split_key;
};

struct SplitShardResponse {
  bool success;
};

struct RegisterStorageEngineRequest {
  Address address;
};

struct RegisterStorageEngineResponse {
  bool success;
};

struct DeregisterStorageEngineRequest {
  Address address;
};

struct DeregisterStorageEngineResponse {
  bool success;
};

struct InitializeLabelRequest {
  std::string label_name;
  std::vector<SchemaProperty> schema;
  Hlc last_shard_map_version;
};

struct InitializeLabelResponse {
  bool success;
  std::optional<ShardMap> fresher_shard_map;
};

struct HeartbeatRequest {};
struct HeartbeatResponse {};

using CoordinatorWriteRequests =
    std::variant<HlcRequest, AllocateEdgeIdBatchRequest, SplitShardRequest, RegisterStorageEngineRequest,
                 DeregisterStorageEngineRequest, InitializeLabelRequest, AllocatePropertyIdsRequest>;
using CoordinatorWriteResponses =
    std::variant<HlcResponse, AllocateEdgeIdBatchResponse, SplitShardResponse, RegisterStorageEngineResponse,
                 DeregisterStorageEngineResponse, InitializeLabelResponse, AllocatePropertyIdsResponse>;

using CoordinatorReadRequests = std::variant<GetShardMapRequest, HeartbeatRequest>;
using CoordinatorReadResponses = std::variant<GetShardMapResponse, HeartbeatResponse>;

class Coordinator {
 public:
  explicit Coordinator(ShardMap sm) : shard_map_{std::move(sm)} {}

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static
  CoordinatorReadResponses Read(CoordinatorReadRequests requests) {
    // NOLINTNEXTLINE(hicpp-move-const-arg,performance-move-const-arg)
    return std::visit([&](auto &&request) { return HandleRead(std::forward<decltype(request)>(request)); },
                      std::move(requests));
  }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static
  CoordinatorWriteResponses Apply(CoordinatorWriteRequests requests) {
    return std::visit([&](auto &&request) mutable { return ApplyWrite(std::forward<decltype(request)>(request)); },
                      std::move(requests));
  }

 private:
  ShardMap shard_map_;
  uint64_t highest_allocated_timestamp_;

  /// Query engines need to periodically request batches of unique edge IDs.
  uint64_t highest_allocated_edge_id_;

  static CoordinatorReadResponses HandleRead(HeartbeatRequest && /* heartbeat_request */) {
    return HeartbeatResponse{};
  }

  CoordinatorReadResponses HandleRead(GetShardMapRequest && /* get_shard_map_request */) {
    GetShardMapResponse res;
    res.shard_map = shard_map_;
    return res;
  }

  CoordinatorWriteResponses ApplyWrite(HlcRequest &&hlc_request) {
    HlcResponse res{};

    auto hlc_shard_map = shard_map_.GetHlc();

    MG_ASSERT(!(hlc_request.last_shard_map_version.logical_id > hlc_shard_map.logical_id));

    res.new_hlc = Hlc{
        .logical_id = ++highest_allocated_timestamp_,
        // TODO(tyler) probably pass some more context to the Coordinator here
        // so that we can use our wall clock and enforce monotonicity.
        // .coordinator_wall_clock = io_.Now(),
    };

    // Allways return fresher shard_map for now.
    res.fresher_shard_map = std::make_optional(shard_map_);

    return res;
  }

  CoordinatorWriteResponses ApplyWrite(AllocateEdgeIdBatchRequest &&ahr) {
    AllocateEdgeIdBatchResponse res{};

    uint64_t low = highest_allocated_edge_id_;

    highest_allocated_edge_id_ += ahr.batch_size;

    uint64_t high = highest_allocated_edge_id_;

    res.low = low;
    res.high = high;

    return res;
  }

  /// This splits the shard immediately beneath the provided
  /// split key, keeping the assigned peers identical for now,
  /// but letting them be gradually migrated over time.
  CoordinatorWriteResponses ApplyWrite(SplitShardRequest &&split_shard_request) {
    SplitShardResponse res{};

    if (split_shard_request.previous_shard_map_version != shard_map_.shard_map_version) {
      res.success = false;
    } else {
      res.success = shard_map_.SplitShard(split_shard_request.previous_shard_map_version, split_shard_request.label_id,
                                          split_shard_request.split_key);
    }

    return res;
  }

  /// This adds the provided storage engine to the standby storage engine pool,
  /// which can be used to rebalance storage over time.
  static CoordinatorWriteResponses ApplyWrite(RegisterStorageEngineRequest && /* register_storage_engine_request */) {
    RegisterStorageEngineResponse res{};
    // TODO

    return res;
  }

  /// This begins the process of draining the provided storage engine from all raft
  /// clusters that it might be participating in.
  static CoordinatorWriteResponses ApplyWrite(DeregisterStorageEngineRequest && /* register_storage_engine_request */) {
    DeregisterStorageEngineResponse res{};
    // TODO
    // const Address &address = register_storage_engine_request.address;
    // storage_engine_pool_.erase(address);
    // res.success = true;

    return res;
  }

  CoordinatorWriteResponses ApplyWrite(InitializeLabelRequest &&initialize_label_request) {
    InitializeLabelResponse res{};

    bool success = shard_map_.InitializeNewLabel(initialize_label_request.label_name, initialize_label_request.schema,
                                                 initialize_label_request.last_shard_map_version);

    if (success) {
      res.fresher_shard_map = shard_map_;
      res.success = false;
    } else {
      res.fresher_shard_map = std::nullopt;
      res.success = true;
    }

    return res;
  }

  CoordinatorWriteResponses ApplyWrite(AllocatePropertyIdsRequest &&allocate_property_ids_request) {
    AllocatePropertyIdsResponse res{};

    auto property_ids = shard_map_.AllocatePropertyIds(allocate_property_ids_request.property_names);

    res.property_ids = property_ids;

    return res;
  }
};

}  // namespace memgraph::coordinator
