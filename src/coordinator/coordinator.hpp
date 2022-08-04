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

#include "coordinator/hybrid_logical_clock.hpp"
#include "coordinator/shard_map.hpp"
#include "io/simulator/simulator.hpp"
#include "io/time.hpp"
#include "io/transport.hpp"

namespace memgraph::coordinator {

using Address = memgraph::io::Address;
using SimT = memgraph::io::simulator::SimulatorTransport;

struct HlcRequest {
  Hlc last_shard_map_version;
};

struct HlcResponse {
  Hlc new_hlc;
  std::optional<ShardMap> fresher_shard_map;
};

struct AllocateHlcBatchRequest {
  Hlc low;
  Hlc high;
};

struct AllocateHlcBatchResponse {
  bool success;
  Hlc low;
  Hlc high;
};

struct SplitShardRequest {
  Hlc previous_shard_map_version;
  Label label;
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

using WriteRequests = std::variant<AllocateHlcBatchRequest, SplitShardRequest, RegisterStorageEngineRequest,
                                   DeregisterStorageEngineRequest>;
using WriteResponses = std::variant<AllocateHlcBatchResponse, SplitShardResponse, RegisterStorageEngineResponse,
                                    DeregisterStorageEngineResponse>;

using ReadRequests = std::variant<HlcRequest>;
using ReadResponses = std::variant<HlcResponse>;

class Coordinator {
  ShardMap shard_map_;

  WriteResponses Apply(AllocateHlcBatchRequest &&ahr) {
    AllocateHlcBatchResponse res{};

    return res;
  }

  /// This splits the shard immediately beneath the provided
  /// split key, keeping the assigned peers identical for now,
  /// but letting them be gradually migrated over time.
  WriteResponses Apply(SplitShardRequest &&split_shard_request) {
    SplitShardResponse res{};

    if (split_shard_request.previous_shard_map_version != shard_map_.shard_map_version) {
      // TODO reply with failure
    }

    return res;
  }

  WriteResponses Apply(RegisterStorageEngineRequest &&register_storage_engine_request) {
    RegisterStorageEngineResponse res{};

    return res;
  }

  WriteResponses Apply(DeregisterStorageEngineRequest &&register_storage_engine_request) {
    DeregisterStorageEngineResponse res{};

    return res;
  }

 public:
  ReadResponses Read(ReadRequests requests) { return HlcResponse{}; }

  WriteResponses Apply(WriteRequests requests) {
    return std::visit([&](auto &&requests) { return Apply(requests); }, std::move(requests));
  }
};

}  // namespace memgraph::coordinator
