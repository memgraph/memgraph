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
#include <random>
#include <set>
#include <stdexcept>
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
#include "storage/v3/id_types.hpp"
#include "utils/result.hpp"

template <typename TStorageClient>
class RsmStorageClientManager {
 public:
  using CompoundKey = memgraph::io::rsm::ShardRsmKey;
  using Shard = memgraph::coordinator::Shard;
  RsmStorageClientManager() = default;
  RsmStorageClientManager(const RsmStorageClientManager &) = delete;
  RsmStorageClientManager(RsmStorageClientManager &&) = delete;

  void AddClient(const std::string &label, Shard key, TStorageClient client) {
    cli_cache_[label].insert({std::move(key), std::move(client)});
  }

  bool Exists(const std::string &label, const Shard &key) { return cli_cache_[label].contains(key); }

  void PurgeCache() { cli_cache_.clear(); }

  TStorageClient &GetClient(const std::string &label, const Shard &key) { return cli_cache_[label].find(key)->second; }

 private:
  std::map<std::string, std::map<Shard, TStorageClient>> cli_cache_;
};

template <typename TRequest>
struct ExecutionState {
  using CompoundKey = memgraph::io::rsm::ShardRsmKey;
  using Shard = memgraph::coordinator::Shard;

  enum State : int8_t { INITIALIZING, EXECUTING, COMPLETED };
  // label is optional because some operators can create/remove etc, vertices. These kind of requests contain the label
  // on the request itself.
  std::optional<std::string> label;
  // CompoundKey is optional because some operators require to iterate over all the available keys
  // of a shard. One example is ScanAll, where we only require the field label.
  std::optional<CompoundKey> key;
  // Transaction id to be filled by the ShardRequestManager implementation
  memgraph::coordinator::Hlc transaction_id;
  // Initialized by ShardRequestManager implementation. This vector is filled with the shards that
  // the ShardRequestManager impl will send requests to. When a request to a shard exhausts it, meaning that
  // it pulled all the requested data from the given Shard, it will be removed from the Vector. When the Vector becomes
  // empty, it means that all of the requests have completed succefully.
  std::vector<Shard> shard_cache;
  // 1-1 mapping with `shard_cache`.
  // A vector that tracks request metatdata for each shard (For example, next_id for a ScanAll on Shard A)
  std::vector<TRequest> requests;
  State state = INITIALIZING;
};

class ShardRequestManagerInterface {
 public:
  ShardRequestManagerInterface() = default;
  virtual void StartTransaction() = 0;
  virtual std::vector<ScanVerticesResponse> Request(ExecutionState<ScanVerticesRequest> &state) = 0;
  virtual ~ShardRequestManagerInterface() {}
  ShardRequestManagerInterface(const ShardRequestManagerInterface &) = delete;
  ShardRequestManagerInterface(ShardRequestManagerInterface &&) = delete;
};

// TODO(kostasrim)rename this class template
template <typename TTransport, typename... Rest>
class ShardRequestManager : public ShardRequestManagerInterface {
 public:
  using StorageWriteRequest = memgraph::io::rsm::StorageWriteRequest;
  using StorageWriteResponse = memgraph::io::rsm::StorageWriteResponse;
  using StorageClient =
      memgraph::coordinator::RsmClient<TTransport, StorageWriteRequest, StorageWriteResponse, Rest...>;
  using CoordinatorClient = memgraph::coordinator::CoordinatorClient<TTransport>;
  using Address = memgraph::io::Address;
  using Shard = memgraph::coordinator::Shard;
  using ShardMap = memgraph::coordinator::ShardMap;
  using CompoundKey = memgraph::coordinator::CompoundKey;
  ShardRequestManager(CoordinatorClient coord, memgraph::io::Io<TTransport> &&io)
      : coord_cli_(std::move(coord)), io_(std::move(io)) {}

  ~ShardRequestManager() override {}

  void StartTransaction() override {
    memgraph::coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    auto read_res = coord_cli_.SendReadRequest(req);
    if (read_res.HasError()) {
      throw std::runtime_error("HLC request failed");
    }
    auto coordinator_read_response = read_res.GetValue();
    auto hlc_response = std::get<memgraph::coordinator::HlcResponse>(coordinator_read_response);

    // Transaction ID to be used later...
    transaction_id_ = hlc_response.new_hlc;

    if (!hlc_response.fresher_shard_map) {
      throw std::runtime_error("Should handle gracefully!");
    }
    shards_map_ = hlc_response.fresher_shard_map.value();
  }

  // TODO(kostasrim) Simplify return result
  std::vector<ScanVerticesResponse> Request(ExecutionState<ScanVerticesRequest> &state) override {
    MaybeInitializeExecutionState(state);
    std::vector<ScanVerticesResponse> responses;
    auto &shard_cacheref = state.shard_cache;
    size_t id = 0;
    for (auto shard_it = shard_cacheref.begin(); shard_it != shard_cacheref.end(); ++id) {
      auto &storage_client = GetStorageClientForShard(*state.label, state.requests[id].start_id.second);
      // TODO(kostasrim) Currently requests return the result directly. Adjust this when the API works MgFuture instead.
      auto read_response_result = storage_client.SendReadRequest(state.requests[id]);
      // RETRY on timeouts?
      // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map test
      if (read_response_result.HasError()) {
        throw std::runtime_error("Read request error");
      }
      if (read_response_result.GetValue().success == false) {
        throw std::runtime_error("Request did not succeed");
      }
      responses.push_back(read_response_result.GetValue());
      if (!read_response_result.GetValue().next_start_id) {
        shard_it = shard_cacheref.erase(shard_it);
      } else {
        state.requests[id].start_id.second = read_response_result.GetValue().next_start_id->second;
        ++shard_it;
      }
    }
    // We are done with this state
    MaybeCompleteState(state);
    // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as return
    // result of storage_client.SendReadRequest()).
    return responses;
  }

  std::vector<CreateVerticesResponse> Request(ExecutionState<CreateVerticesRequest> &state,
                                              std::vector<NewVertexLabel> new_vertices) {
    MG_ASSERT(!new_vertices.empty());
    MaybeInitializeExecutionState(state, std::move(new_vertices));
    std::vector<CreateVerticesResponse> responses;
    auto &shard_cache_ref = state.shard_cache;
    size_t id = 0;
    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
      // This is fine because all new_vertices of each request end up on the same shard
      Label label = state.requests[id].new_vertices[0].label_ids;
      auto primary_key = state.requests[id].new_vertices[0].primary_key;
      auto &storage_client = GetStorageClientForShard(label, primary_key);
      auto read_response_result = storage_client.SendReadRequest(state.requests[id]);
      // RETRY on timeouts?
      // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map test
      if (read_response_result.HasError()) {
        throw std::runtime_error("Write request error");
      }
      if (read_response_result.GetValue().success == false) {
        throw std::runtime_error("Write request did not succeed");
      }
      responses.push_back(read_response_result.GetValue());
      shard_it = shard_cache_ref.erase(shard_it);
    }
    // We are done with this state
    MaybeCompleteState(state);
    // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as return
    // result of storage_client.SendReadRequest()).
    return responses;
  }

 private:
  template <typename ExecutionState>
  void ThrowIfStateCompleted(ExecutionState &state) const {
    if (state.state == ExecutionState::COMPLETED) [[unlikely]] {
      throw std::runtime_error("State is completed and must be reset");
    }
  }

  template <typename ExecutionState>
  void MaybeCompleteState(ExecutionState &state) const {
    if (state.requests.empty()) {
      state.state = ExecutionState::COMPLETED;
    }
  }

  template <typename ExecutionState>
  bool ShallNotInitializeState(ExecutionState &state) const {
    return state.state != ExecutionState::INITIALIZING;
  }

  template <typename TRequest>
  void MaybeUpdateExecutionState(TRequest &state) {
    if (state.shard_cache) {
      return;
    }
    state.transaction_id = transaction_id_;
    state.shard_cache = std::make_optional<std::vector<Shard>>();
    const auto &shards = shards_map_.shards[shards_map_.labels[state.label]];
    if (state.key) {
      if (auto it = shards.find(*state.key); it != shards.end()) {
        state.shard_cache->push_back(it->second);
        return;
      }
      // throw here
    }

    for (const auto &[key, shard] : shards) {
      state.shard_cache->push_back(shard);
    }
  }

  void MaybeInitializeExecutionState(ExecutionState<CreateVerticesRequest> &state,
                                     std::vector<NewVertexLabel> new_vertices) {
    ThrowIfStateCompleted(state);
    if (ShallNotInitializeState(state)) {
      return;
    }
    state.transaction_id = transaction_id_;

    std::map<Shard, CreateVerticesRequest> per_shard_request_table;

    for (auto &new_vertex : new_vertices) {
      auto shard = shards_map_.GetShardForKey(new_vertex.label, new_vertex.primary_key);
      if (!per_shard_request_table.contains(shard)) {
        CreateVerticesRequest create_v_rqst{.transaction_id = transaction_id_};
        per_shard_request_table.insert(std::pair(shard, std::move(create_v_rqst)));
        state.shard_cache.push_back(shard);
      }
      per_shard_request_table[shard].new_vertices.push_back(
          NewVertex{.label_ids = shards_map_.GetLabelId(new_vertex.label),
                    .primary_key = std::move(new_vertex.primary_key),
                    .properties = std::move(new_vertex.properties)});
    }

    for (auto &[shard, rqst] : per_shard_request_table) {
      state.requests.push_back(std::move(rqst));
    }
    state.state = ExecutionState<CreateVerticesRequest>::EXECUTING;
  }

  void MaybeInitializeExecutionState(ExecutionState<ScanVerticesRequest> &state) {
    ThrowIfStateCompleted(state);
    if (ShallNotInitializeState(state)) {
      return;
    }
    state.transaction_id = transaction_id_;
    const auto &shards = shards_map_.shards[shards_map_.labels[*state.label]];
    for (const auto &[key, shard] : shards) {
      state.shard_cache.push_back(shard);
      ScanVerticesRequest rqst;
      rqst.transaction_id = transaction_id_;
      rqst.start_id.second = key;
      state.requests.push_back(std::move(rqst));
    }
    state.state = ExecutionState<ScanVerticesRequest>::EXECUTING;
  }

  //  std::vector<storageclient> GetStorageClientFromShardforRange(const std::string &label, const CompoundKey &start,
  //                                                               const CompoundKey &end);

  template <typename TLabel>
  StorageClient &GetStorageClientForShard(const TLabel &label, const CompoundKey &key) {
    auto shard = shards_map_.GetShardForKey(label, key);
    if (!storage_cli_manager_.Exists(label, shard)) {
      AddStorageClientToManager(shard, label);
    }
    return storage_cli_manager_.GetClient(label, shard);
  }

  void AddStorageClientToManager(Shard target_shard, const std::string &label) {
    MG_ASSERT(!target_shard.empty());
    auto leader_addr = target_shard.front();
    std::vector<Address> addresses;
    addresses.reserve(target_shard.size());
    for (auto &address : target_shard) {
      addresses.push_back(std::move(address.address));
    }
    auto cli = StorageClient(io_, std::move(leader_addr.address), std::move(addresses));
    storage_cli_manager_.AddClient(label, target_shard, std::move(cli));
  }

  ShardMap shards_map_;
  CoordinatorClient coord_cli_;
  RsmStorageClientManager<StorageClient> storage_cli_manager_;
  memgraph::io::Io<TTransport> io_;
  memgraph::coordinator::Hlc transaction_id_;
  // TODO(kostasrim) Add batch prefetching
};
