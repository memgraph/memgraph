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

#include "coordinator/coordinator.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/value_conversions.hpp"

namespace memgraph::coordinator {

// 1. try to begin any suggested splits
// 2. mark all initialized RSMs as INITIALIZED in the ShardMap
// 3. assign any valid underreplicated shards to the Heartbeat sender
// 4. send any split requests that the Heartbeat sender should be applying
CoordinatorWriteResponses Coordinator::ApplyWrite(HeartbeatRequest &&heartbeat_request) {
  spdlog::info("Coordinator handling HeartbeatRequest");

  HeartbeatResponse ret{};

  bool initiated_split = false;

  // 1. try to begin any suggested splits
  for (const auto &suggested_split_info : heartbeat_request.suggested_splits) {
    const LabelId label_id = suggested_split_info.label_id;
    auto &label_space = shard_map_.label_spaces.at(label_id);

    const PrimaryKey splitting_shard_low_key =
        storage::conversions::ConvertPropertyVector(suggested_split_info.splitting_shard_low_key);
    const PrimaryKey split_key = storage::conversions::ConvertPropertyVector(suggested_split_info.split_key);

    auto &shard = label_space.shards.at(splitting_shard_low_key);
    const ShardId splitting_shard_id = std::make_pair(label_id, splitting_shard_low_key);

    if (shard.pending_split.has_value() || shard.version != suggested_split_info.shard_version) {
      spdlog::info("skipping split, already splitting: {}, shard.version: {}, suggested shard_version: {}",
                   shard.pending_split.has_value(), shard.version, suggested_split_info.shard_version);
      continue;
    }

    // begin the split process for this shard
    const auto new_uuid_lhs = shard_map_.GetHlc();
    const auto new_uuid_rhs = shard_map_.GetHlc();
    spdlog::warn(
        "Coordinator beginning new split process for shard {} after receiving a pending split. splitting into lhs: {} "
        "and rhs: {}",
        shard.version.logical_id, new_uuid_lhs.logical_id, new_uuid_rhs.logical_id);

    // bump current shard version and store pending split info
    shard.version = new_uuid_lhs;
    shard.pending_split = suggested_split_info;
    MG_ASSERT(!splitting_shards_.contains(splitting_shard_id));
    splitting_shards_.insert(splitting_shard_id);
    initiated_split = true;

    // copy this shard and store it in the ShardMap
    ShardMetadata duplicated_shard{shard};
    duplicated_shard.version = new_uuid_rhs;
    duplicated_shard.pending_split.reset();
    std::map<boost::uuids::uuid, boost::uuids::uuid> split_mapping = {};
    for (auto &peer_metadata : duplicated_shard.peers) {
      peer_metadata.status = Status::PENDING_SPLIT;
      peer_metadata.split_from = peer_metadata.address.unique_id;

      const auto new_uuid = shard_map_.NewShardUuid();

      spdlog::info("Coordinator allocating new rsm uuid: {}", new_uuid);

      // store new uuid for the right side of each shard
      rsm_split_from_.insert({new_uuid, splitting_shard_id});
      split_mapping.emplace(peer_metadata.address.unique_id, new_uuid);

      peer_metadata.address.unique_id = new_uuid;
    }

    // TODO(tyler) fix this good assertion
    // if (high_key) {
    //   MG_ASSERT(converted_pk < *high_key, "Split point is beyond low key of the next shard");
    // }
    label_space.shards.insert({split_key, duplicated_shard});

    // we will send the heartbeater its split request in stage 4 below
  }

  // 2. mark all initialized RSMs as INITIALIZED in the ShardMap
  for (const auto &[initialized_rsm, shard_id] : heartbeat_request.initialized_rsms) {
    spdlog::info("looking at rsm {}", initialized_rsm);
    auto [label_id, low_key] = shard_id;
    auto &label_space = shard_map_.label_spaces.at(label_id);
    auto &shard = label_space.shards.at(low_key);

    // if even a single shard has been initialized after a split, its raft log has
    // reached consensus and we can allow more splits again and remove the pending
    // split state.
    if (rsm_split_from_.contains(initialized_rsm)) {
      auto split_shard_id = rsm_split_from_.at(initialized_rsm);

      auto [split_label_id, split_low_key] = split_shard_id;
      auto &split_shard = label_space.shards.at(split_low_key);

      spdlog::warn("Coordinator clearing split for shard {}", split_shard.version);
      MG_ASSERT(split_shard.pending_split.has_value());
      MG_ASSERT(splitting_shards_.contains(split_shard_id));

      split_shard.pending_split.reset();
      splitting_shards_.erase(split_shard_id);
      rsm_split_from_.erase(initialized_rsm);
    }

    size_t initialized_count = 0;
    bool found = false;
    for (auto &peer : shard.peers) {
      if (peer.address.last_known_ip == heartbeat_request.from_storage_manager.last_known_ip &&
          peer.address.last_known_port == heartbeat_request.from_storage_manager.last_known_port) {
        // TODO(tyler) switch the conditional to match on rsm uuid and update the peer last_known_* address always
        const int low_key_int = low_key[0].ValueInt();
        MG_ASSERT(peer.address.unique_id == initialized_rsm,
                  "expected Coordinator uuid {} to equal peer uuid {} for shard version {} with low key {}",
                  peer.address.unique_id, initialized_rsm, shard.version, low_key_int);
        spdlog::warn("Coordinator marking rsm {} for shard {} as initialized", initialized_rsm, shard.version);
        peer.status = Status::CONSENSUS_PARTICIPANT;
        ret.acknowledged_initialized_rsms.push_back(initialized_rsm);
        found = true;
      }

      if (peer.status == Status::CONSENSUS_PARTICIPANT) {
        initialized_count++;
      }
    }

    MG_ASSERT(found, "did not find peer {} in shard version {}", heartbeat_request.from_storage_manager.unique_id,
              shard.version);

    if (initialized_count >= label_space.replication_factor) {
      spdlog::info("clearing underreplicated shard with {} initialized peers", initialized_count);
      underreplicated_shards_.erase(shard_id);
    }
  }

  // 3. assign any valid underreplicated shards to the Heartbeat sender
  for (auto &underreplicated_shard_id : underreplicated_shards_) {
    auto [label_id, low_key] = underreplicated_shard_id;
    auto &label_space = shard_map_.label_spaces.at(label_id);
    auto &shard = label_space.shards.at(low_key);

    // make sure we're not already a member
    for (const auto &peer : shard.peers) {
      if (peer.address.last_known_ip == heartbeat_request.from_storage_manager.last_known_ip &&
          peer.address.last_known_port == heartbeat_request.from_storage_manager.last_known_port) {
        // we are already a member of this shard. continue to next underreplicated shard to consider
        continue;
      }
    }

    // we are not already a member, so we can be assigned to this shard
    Address address = heartbeat_request.from_storage_manager;
    address.unique_id = shard_map_.NewShardUuid();

    std::optional<PrimaryKey> high_key;
    auto next = std::next(label_space.shards.find(low_key));
    if (next != label_space.shards.end()) {
      high_key = next->first;
    }

    spdlog::info("assigning shard manager to shard");

    ret.shards_to_initialize.push_back(ShardToInitialize{
        .new_shard_version = shard.version,
        .uuid = address.unique_id,
        .label_id = label_id,
        .min_key = low_key,
        .max_key = high_key,
        .schema = shard_map_.schemas[label_id],
        .config = Config{.split =
                             Config::Split{
                                 .max_shard_vertex_size = label_space.split_threshold,
                             }},
        .id_to_names = shard_map_.IdToNames(),
    });

    PeerMetadata peer_metadata = {
        .address = address,
        .status = Status::INITIALIZING,
    };

    shard.peers.emplace_back(peer_metadata);
  }

  // 4. send any split requests that the Heartbeat sender should be applying
  spdlog::info("0");
  for (const auto &[label_id, low_key] : splitting_shards_) {
    // see if this machine is a PENDING_SPLIT peer for any of the splitting shards
    const auto &label_space = shard_map_.label_spaces.at(label_id);
    const auto &splitting_shard = label_space.shards.at(low_key);
    MG_ASSERT(splitting_shard.pending_split.has_value());
    const auto &suggested_split_info = splitting_shard.pending_split.value();
    const auto split_key = storage::conversions::ConvertPropertyVector(suggested_split_info.split_key);
    const auto &new_shard = label_space.shards.at(split_key);

    spdlog::info("1");
    for (const auto &peer : new_shard.peers) {
      spdlog::info("2");
      if (peer.status != Status::PENDING_SPLIT ||
          peer.address.last_known_ip != heartbeat_request.from_storage_manager.last_known_ip ||
          peer.address.last_known_port != heartbeat_request.from_storage_manager.last_known_port) {
        spdlog::warn("not splitting peer: status is PENDING_SPLIT: {}, peer address: {} storage manager address: {}",
                     peer.status == Status::PENDING_SPLIT, peer.address, heartbeat_request.from_storage_manager);
        // not splitting or not us
        continue;
      }

      spdlog::warn("Coordinator expecting heartbeating peer to split, so it will reply with a ShardToSplit message");

      std::map<boost::uuids::uuid, boost::uuids::uuid> uuid_mapping{};

      // need to iterate over all peers again to build the full uuid_mapping to
      // send to this machine
      for (const auto &peer_metadata2 : new_shard.peers) {
        uuid_mapping.emplace(peer_metadata2.split_from, peer_metadata2.address.unique_id);
      }

      ret.shards_to_split.push_back(ShardToSplit{
          .split_key = split_key,
          .old_shard_version = suggested_split_info.shard_version,
          .new_lhs_shard_version = splitting_shard.version,
          .new_rhs_shard_version = new_shard.version,
          .uuid_mapping = uuid_mapping,
      });
    }
  }

  if (initiated_split) {
    MG_ASSERT(!ret.shards_to_split.empty(), "did not send a split request to the shard that suggested a split");
  }

  return ret;
}

CoordinatorWriteResponses Coordinator::ApplyWrite(HlcRequest &&hlc_request) {
  HlcResponse res{};

  auto hlc_shard_map = shard_map_.GetHlc();

  MG_ASSERT(!(hlc_request.last_shard_map_version.logical_id > hlc_shard_map.logical_id));

  res.new_hlc = Hlc{
      .logical_id = ++highest_allocated_timestamp_,
      // TODO(tyler) probably pass some more context to the Coordinator here
      // so that we can use our wall clock and enforce monotonicity.
      // Check it to ensure it's +/- 1 day of the coordinator's io_.Now()
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
      initialize_label_request.split_threshold, initialize_label_request.last_shard_map_version);

  if (new_label_id) {
    res.new_label_id = new_label_id.value();
    res.fresher_shard_map = std::nullopt;
    res.success = true;

    auto min_key = SchemaToMinKey(initialize_label_request.schema);
    auto shard_id = std::make_pair(new_label_id.value(), min_key);
    underreplicated_shards_.insert(shard_id);
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
