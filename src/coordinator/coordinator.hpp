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
#include <set>
#include <string>
#include <unordered_set>
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include <coordinator/hybrid_logical_clock.hpp>
#include <coordinator/shard_map.hpp>
#include <io/simulator/simulator.hpp>
#include <io/time.hpp>
#include <io/transport.hpp>
#include <storage/v3/id_types.hpp>
#include <storage/v3/schemas.hpp>

namespace memgraph::coordinator {

using memgraph::io::Address;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::PropertyId;
using memgraph::storage::v3::SchemaProperty;
using SimT = memgraph::io::simulator::SimulatorTransport;
using PrimaryKey = std::vector<PropertyValue>;

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
  PrimaryKey split_key;
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
  size_t replication_factor;
  Hlc last_shard_map_version;
};

struct InitializeLabelResponse {
  bool success;
  LabelId new_label_id;
  std::optional<ShardMap> fresher_shard_map;
};

struct HeartbeatRequest {
  Address from_storage_manager;
  std::set<boost::uuids::uuid> initialized_rsms;
};

struct HeartbeatResponse {
  std::vector<ShardToInitialize> shards_to_initialize;
};

using CoordinatorWriteRequests =
    std::variant<HlcRequest, AllocateEdgeIdBatchRequest, SplitShardRequest, RegisterStorageEngineRequest,
                 DeregisterStorageEngineRequest, InitializeLabelRequest, AllocatePropertyIdsRequest, HeartbeatRequest>;
using CoordinatorWriteResponses = std::variant<HlcResponse, AllocateEdgeIdBatchResponse, SplitShardResponse,
                                               RegisterStorageEngineResponse, DeregisterStorageEngineResponse,
                                               InitializeLabelResponse, AllocatePropertyIdsResponse, HeartbeatResponse>;

using CoordinatorReadRequests = std::variant<GetShardMapRequest>;
using CoordinatorReadResponses = std::variant<GetShardMapResponse>;

class Coordinator {
 public:
  explicit Coordinator(ShardMap sm) : shard_map_{std::move(sm)} {}

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static
  CoordinatorReadResponses Read(CoordinatorReadRequests requests) {
    return std::visit([&](auto &&request) { return HandleRead(std::forward<decltype(request)>(request)); },
                      std::move(requests));  // NOLINT(hicpp-move-const-arg,performance-move-const-arg)
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

  CoordinatorReadResponses HandleRead(GetShardMapRequest && /* get_shard_map_request */) {
    GetShardMapResponse res;
    res.shard_map = shard_map_;
    return res;
  }

  CoordinatorWriteResponses ApplyWrite(HeartbeatRequest &&heartbeat_request);

  CoordinatorWriteResponses ApplyWrite(HlcRequest &&hlc_request);

  CoordinatorWriteResponses ApplyWrite(AllocateEdgeIdBatchRequest &&ahr);

  /// This splits the shard immediately beneath the provided
  /// split key, keeping the assigned peers identical for now,
  /// but letting them be gradually migrated over time.
  CoordinatorWriteResponses ApplyWrite(SplitShardRequest &&split_shard_request);

  /// This adds the provided storage engine to the standby storage engine pool,
  /// which can be used to rebalance storage over time.
  static CoordinatorWriteResponses ApplyWrite(RegisterStorageEngineRequest && /* register_storage_engine_request */);

  /// This begins the process of draining the provided storage engine from all raft
  /// clusters that it might be participating in.
  static CoordinatorWriteResponses ApplyWrite(DeregisterStorageEngineRequest && /* register_storage_engine_request */);

  CoordinatorWriteResponses ApplyWrite(InitializeLabelRequest &&initialize_label_request);

  CoordinatorWriteResponses ApplyWrite(AllocatePropertyIdsRequest &&allocate_property_ids_request);
};

}  // namespace memgraph::coordinator
