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

#include <chrono>
#include <deque>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <unordered_map>
#include <vector>

#include "coordinator/coordinator_client.hpp"
#include "coordinator/coordinator_rsm.hpp"
#include "coordinator/shard_map.hpp"
#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/rsm/raft.hpp"
#include "io/rsm/rsm_client.hpp"
#include "io/rsm/shard_rsm.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "query/v2/requests.hpp"
#include "utils/result.hpp"

template <typename TStorageClient>
class RsmStorageClientManager {
 public:
  using CompoundKey = memgraph::io::rsm::ShardRsmKey;
  RsmStorageClientManager() = default;
  RsmStorageClientManager(const RsmStorageClientManager &) = delete;
  RsmStorageClientManager(RsmStorageClientManager &&) = delete;

  void AddClient(const std::string &label, CompoundKey cm_k, TStorageClient client) {
    cli_cache_[label].insert({std::move(cm_k), std::move(client)});
  }

  bool Exists(const std::string &label, const CompoundKey &cm_k) { return cli_cache_[label].contains(cm_k); }

  void PurgeCache() { cli_cache_.clear(); }
  // void EvictFromCache(std::vector<TStorageClient>);
  TStorageClient &GetClient(const std::string &label, CompoundKey key) { return cli_cache_[label].find(key)->second; }

 private:
  std::unordered_map<std::string, std::map<CompoundKey, TStorageClient>> cli_cache_;
};

// In execution context an object exists
struct ExecutionState {
  using CompoundKey = memgraph::io::rsm::ShardRsmKey;
  using Shard = memgraph::coordinator::Shard;
  std::optional<std::vector<Shard>> state_;
  std::string label;
  //  using CompoundKey = memgraph::coordinator::CompoundKey;
  std::optional<CompoundKey> key;
};

namespace rsm = memgraph::io::rsm;

// TODO(kostasrim)rename this class template
template <typename TTransport>
class QueryEngineMiddleware {
 public:
  using StorageClient =
      memgraph::coordinator::RsmClient<TTransport, rsm::StorageWriteRequest, rsm::StorageWriteResponse,
                                       rsm::StorageReadRequest, rsm::StorageReadResponse>;
  using CoordinatorClient = memgraph::coordinator::CoordinatorClient<TTransport>;
  using Address = memgraph::io::Address;
  using Shard = memgraph::coordinator::Shard;
  using ShardMap = memgraph::coordinator::ShardMap;
  using CompoundKey = memgraph::coordinator::CompoundKey;
  using memgraph::io::Io;
  QueryEngineMiddleware(CoordinatorClient coord, Io<TTransport> &&io)
      : coord_cli_(std::move(coord)), io_(std::move(io)) {}

  std::vector<ScanVerticesResponse> Request(ScanVerticesRequest rqst, ExecutionState &state) {
    MaybeUpdateShardMap();
    MaybeUpdateExecutionState(state);
    std::vector<ScanVerticesResponse> responses;
    for (const auto &shard : *state.state_) {
      auto &storage_client = GetStorageClientForShard(state.label, *state.key);
      auto read_response_result = storage_client.SendReadRequest(rqst);
      // RETRY on timeouts?
      // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map test
      if (read_response_result.HasError()) {
        throw std::runtime_error("Handle gracefully!");
      }
      responses.push_back(read_response_result.Value());
    }
    // TODO(kostasrim) Update state accordingly
    return responses;
    // For a future based API. Also maybe introduce a `Retry` function that accepts a lambda which is the request
    // and a number denoting the number of times the request is retried until an exception or an error is returned.
    // std::vector<memgraph::io::future<ScanAllVerticesRequest>> requests;
    //     for (const auto &shard : state.state_) {
    //       auto &storage_client = GetStorageClientForShard(state.Label, rqst.label);
    //       requests.push_back(client->Request(rqst));
    //     }
    //
    //     std::vector<ScanAllVerticesResponse> responses;
    //     for (auto &f : requests) {
    //       f.wait();
    //       if (f.HasError()) {
    //         // handle error
    //       }
    //       responses.push_back(std::move(f).Value());
    //     }
  }

  CreateVerticesResponse Request(CreateVerticesRequest rqst, ExecutionState &state) {
    //    MaybeUpdateShardMap();
    //    MaybeUpdateExecutionState();
  }

  size_t TestRequest(ExecutionState &state) {
    MaybeUpdateShardMap();
    MaybeUpdateExecutionState(state);
    for (auto &st : *state.state_) {
      auto &storage_client = GetStorageClientForShard(state.label, *state.key);

      rsm::StorageWriteRequest storage_req;
      storage_req.key = *state.key;
      storage_req.value = 469;
      auto write_response_result = storage_client.SendWriteRequest(storage_req);
      if (write_response_result.HasError()) {
        throw std::runtime_error("Handle gracefully!");
      }
      auto write_response = write_response_result.GetValue();

      bool cas_succeeded = write_response.shard_rsm_success;

      if (!cas_succeeded) {
        throw std::runtime_error("Handler gracefully!");
      }
      rsm::StorageReadRequest storage_get_req;
      storage_get_req.key = *state.key;

      auto get_response_result = storage_client.SendReadRequest(storage_get_req);
      if (get_response_result.HasError()) {
        throw std::runtime_error("Handler gracefully!");
      }
      auto get_response = get_response_result.GetValue();
      auto val = get_response.value.value();
      return val;
    }
    return 0;
  }

 private:
  void MaybeUpdateShardMap() {
    memgraph::coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    auto read_res = coord_cli_.SendReadRequest(req);
    if (read_res.HasError()) {
      // handle error gracefully
      // throw some error
    }
    auto coordinator_read_response = read_res.GetValue();
    auto hlc_response = std::get<memgraph::coordinator::HlcResponse>(coordinator_read_response);
    if (hlc_response.fresher_shard_map) {
      // error here new shard map shouldn't exist
    }

    // Transaction ID to be used later...
    auto transaction_id = hlc_response.new_hlc;

    if (hlc_response.fresher_shard_map) {
      shards_map_ = hlc_response.fresher_shard_map.value();
    } else {
      throw std::runtime_error("Should handle gracefully!");
    }
  }

  void MaybeUpdateExecutionState(ExecutionState &state) {
    if (state.state_) {
      return;
    }
    state.state_ = std::make_optional<std::vector<Shard>>();
    const auto &shards = shards_map_.shards[state.label];
    if (state.key) {
      if (auto it = shards.find(*state.key); it != shards.end()) {
        state.state_->push_back(it->second);
        return;
      }
      // throw here
    }

    for (const auto &[key, shard] : shards) {
      state.state_->push_back(shard);
    }
  }

  //  std::vector<storageclient> GetStorageClientFromShardforRange(const std::string &label, const CompoundKey &start,
  //                                                               const CompoundKey &end);
  StorageClient &GetStorageClientForShard(const std::string &label, const CompoundKey &cm_k) {
    if (storage_cli_manager_.Exists(label, cm_k)) {
      return storage_cli_manager_.GetClient(label, cm_k);
    }
    auto target_shard = shards_map_.GetShardForKey(label, cm_k);
    AddStorageClientToManager(std::move(target_shard), label, cm_k);
    return storage_cli_manager_.GetClient(label, cm_k);
  }

  void AddStorageClientToManager(Shard target_shard, const std::string &label, const CompoundKey &cm_k) {
    MG_ASSERT(!target_shard.empty());
    auto leader_addr = target_shard.front();
    std::vector<Address> addresses;
    for (auto &address : target_shard) {
      addresses.push_back(std::move(address.address));
    }
    auto cli = StorageClient(io_, std::move(leader_addr.address), std::move(addresses));
    storage_cli_manager_.AddClient(label, cm_k, std::move(cli));
  }

  ShardMap shards_map_;
  CoordinatorClient coord_cli_;
  RsmStorageClientManager<StorageClient> storage_cli_manager_;
  Io<TTransport> io_;
  // TODO(kostasrim) Add batch prefetching
};
