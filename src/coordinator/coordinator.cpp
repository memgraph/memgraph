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

#include <coordinator/coordinator.hpp>

namespace memgraph::coordinator {

CoordinatorWriteResponses Coordinator::ApplyWrite(HeartbeatRequest &&heartbeat_request) {
  spdlog::info("Coordinator handling HeartbeatRequest");

  // add this storage engine to any under-replicated shards that it is not already a part of

  auto initializing_rsms_for_shard_manager =
      shard_map_.AssignShards(heartbeat_request.from_storage_manager, heartbeat_request.initialized_rsms);

  return HeartbeatResponse{
      .shards_to_initialize = initializing_rsms_for_shard_manager,
  };
}

CoordinatorWriteResponses Coordinator::ApplyWrite(HlcRequest &&hlc_request) {
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

CoordinatorWriteResponses Coordinator::ApplyWrite(AllocateEdgeIdBatchRequest &&ahr) {
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
CoordinatorWriteResponses Coordinator::ApplyWrite(SplitShardRequest &&split_shard_request) {
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
CoordinatorWriteResponses Coordinator::ApplyWrite(
    RegisterStorageEngineRequest && /* register_storage_engine_request */) {
  RegisterStorageEngineResponse res{};
  // TODO

  return res;
}

/// This begins the process of draining the provided storage engine from all raft
/// clusters that it might be participating in.
CoordinatorWriteResponses Coordinator::ApplyWrite(
    DeregisterStorageEngineRequest && /* register_storage_engine_request */) {
  DeregisterStorageEngineResponse res{};
  // TODO

  return res;
}

CoordinatorWriteResponses Coordinator::ApplyWrite(InitializeLabelRequest &&initialize_label_request) {
  InitializeLabelResponse res{};

  std::optional<LabelId> new_label_id = shard_map_.InitializeNewLabel(
      initialize_label_request.label_name, initialize_label_request.schema, initialize_label_request.replication_factor,
      initialize_label_request.last_shard_map_version);

  if (new_label_id) {
    res.new_label_id = new_label_id.value();
    res.fresher_shard_map = std::nullopt;
    res.success = true;
  } else {
    res.fresher_shard_map = shard_map_;
    res.success = false;
  }

  return res;
}

CoordinatorWriteResponses Coordinator::ApplyWrite(AllocatePropertyIdsRequest &&allocate_property_ids_request) {
  AllocatePropertyIdsResponse res{};

  auto property_ids = shard_map_.AllocatePropertyIds(allocate_property_ids_request.property_names);

  res.property_ids = property_ids;

  return res;
}

}  // namespace memgraph::coordinator
