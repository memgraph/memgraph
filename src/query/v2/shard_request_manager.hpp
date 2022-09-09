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

namespace requests {
template <typename TStorageClient>
class RsmStorageClientManager {
 public:
  using CompoundKey = memgraph::io::rsm::ShardRsmKey;
  using Shard = memgraph::coordinator::Shard;
  using LabelId = memgraph::storage::v3::LabelId;
  RsmStorageClientManager() = default;
  RsmStorageClientManager(const RsmStorageClientManager &) = delete;
  RsmStorageClientManager(RsmStorageClientManager &&) = delete;

  void AddClient(const LabelId label_id, Shard key, TStorageClient client) {
    cli_cache_[label_id].insert({std::move(key), std::move(client)});
  }

  bool Exists(const LabelId label_id, const Shard &key) { return cli_cache_[label_id].contains(key); }

  void PurgeCache() { cli_cache_.clear(); }

  TStorageClient &GetClient(const LabelId label_id, const Shard &key) { return cli_cache_[label_id].find(key)->second; }

 private:
  std::map<LabelId, std::map<Shard, TStorageClient>> cli_cache_;
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
  using WriteRequests = CreateVerticesRequest;
  using WriteResponses = CreateVerticesResponse;
  using StorageClient = memgraph::coordinator::RsmClient<TTransport, WriteRequests, WriteResponses, Rest...>;
  using CoordinatorClient = memgraph::coordinator::CoordinatorClient<TTransport>;
  using Address = memgraph::io::Address;
  using Shard = memgraph::coordinator::Shard;
  using ShardMap = memgraph::coordinator::ShardMap;
  using CompoundKey = memgraph::coordinator::CompoundKey;
  ShardRequestManager(CoordinatorClient coord, memgraph::io::Io<TTransport> &&io)
      : coord_cli_(std::move(coord)), io_(std::move(io)) {}

  ShardRequestManager(const ShardRequestManager &) = delete;
  ShardRequestManager(ShardRequestManager &&) = delete;

  ~ShardRequestManager() override {}

  void StartTransaction() override {
    memgraph::coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    auto write_res = coord_cli_.SendWriteRequest(req);
    if (write_res.HasError()) {
      throw std::runtime_error("HLC request failed");
    }
    auto coordinator_write_response = write_res.GetValue();
    auto hlc_response = std::get<memgraph::coordinator::HlcResponse>(coordinator_write_response);

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
      auto &storage_client = GetStorageClientForShard(*state.label, state.requests[id].start_id.primary_key);
      // TODO(kostasrim) Currently requests return the result directly. Adjust this when the API works MgFuture instead.
      auto read_response_result = storage_client.SendReadRequest(state.requests[id]);
      // RETRY on timeouts?
      // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map test
      if (read_response_result.HasError()) {
        throw std::runtime_error("ScanAll request timedout");
      }
      if (read_response_result.GetValue().success == false) {
        throw std::runtime_error("ScanAll request did not succeed");
      }
      responses.push_back(read_response_result.GetValue());
      if (!read_response_result.GetValue().next_start_id) {
        shard_it = shard_cacheref.erase(shard_it);
      } else {
        state.requests[id].start_id.primary_key = read_response_result.GetValue().next_start_id->primary_key;
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
    MaybeInitializeExecutionState(state, new_vertices);
    std::vector<CreateVerticesResponse> responses;
    auto &shard_cache_ref = state.shard_cache;
    size_t id = 0;
    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
      // This is fine because all new_vertices of each request end up on the same shard
      const Label label = state.requests[id].new_vertices[0].label_ids;
      auto primary_key = state.requests[id].new_vertices[0].primary_key;
      auto &storage_client = GetStorageClientForShard(*shard_it, label.id);
      auto write_response_result = storage_client.SendWriteRequest(state.requests[id]);
      // RETRY on timeouts?
      // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map test
      if (write_response_result.HasError()) {
        throw std::runtime_error("CreateVertices request timedout");
      }
      if (write_response_result.GetValue().success == false) {
        throw std::runtime_error("CreateVertices request did not succeed");
      }
      responses.push_back(write_response_result.GetValue());
      shard_it = shard_cache_ref.erase(shard_it);
    }
    // We are done with this state
    MaybeCompleteState(state);
    // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as return
    // result of storage_client.SendReadRequest()).
    return responses;
  }

  std::vector<ExpandOneResponse> Request(ExecutionState<ExpandOneRequest> &state) {
    // TODO(kostasrim)Update to limit the batch size here
    // Expansions of the destination must be handled by the caller. For example
    // match (u:L1 { prop : 1 })-[:Friend]-(v:L1)
    // For each vertex U, the ExpandOne will result in <U, Edges>. The destination vertex and its properties
    // must be fetched again with an ExpandOne(Edges.dst)
    MaybeInitializeExecutionState(state);
    std::vector<ExpandOneResponse> responses;
    auto &shard_cache_ref = state.shard_cache;
    size_t id = 0;
    // pending_requests on shards
    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
      const Label primary_label = state.requests[id].src_vertices[0].primary_label;
      auto &storage_client = GetStorageClientForShard(*shard_it, primary_label.id);
      auto read_response_result = storage_client.SendReadRequest(state.requests[id]);
      // RETRY on timeouts?
      // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map
      if (read_response_result.HasError()) {
        throw std::runtime_error("ExpandOne request timedout");
      }
      if (read_response_result.GetValue().success == false) {
        throw std::runtime_error("ExpandOne request did not succeed");
      }
      responses.push_back(read_response_result.GetValue());
    }
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
          NewVertex{.label_ids = {shards_map_.GetLabelId(new_vertex.label)},
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
    const auto shards = shards_map_.GetShards(*state.label);
    for (const auto &[key, shard] : shards) {
      state.shard_cache.push_back(std::move(shard));
      ScanVerticesRequest rqst;
      rqst.transaction_id = transaction_id_;
      rqst.start_id.primary_key = std::move(key);
      state.requests.push_back(std::move(rqst));
    }
    state.state = ExecutionState<ScanVerticesRequest>::EXECUTING;
  }

  void MaybeInitializeExecutionState(ExecutionState<ExpandOneRequest> &state) {
    ThrowIfStateCompleted(state);
    if (ShallNotInitializeState(state)) {
      return;
    }
    state.transaction_id = transaction_id_;

    std::map<Shard, ExpandOneRequest> per_shard_request_table;
    MG_ASSERT(state.requests.size() == 1);
    auto top_level_rqst = std::move(*state.requests.begin());
    auto top_level_rqst_template = top_level_rqst;
    top_level_rqst_template.src_vertices.clear();
    top_level_rqst_template.edge_types.clear();
    state.requests.clear();
    size_t id = 0;
    for (const auto &vertex : top_level_rqst.src_vertices) {
      auto shard = shards_map_.GetShardForKey(vertex.primary_label.id, vertex.primary_key);
      if (!per_shard_request_table.contains(shard)) {
        ExpandOneRequest expand_v_rqst = top_level_rqst_template;
        per_shard_request_table.insert(std::pair(shard, std::move(expand_v_rqst)));
        state.shard_cache.push_back(shard);
      }
      per_shard_request_table[shard].src_vertices.push_back(vertex);
      per_shard_request_table[shard].edge_types.push_back(top_level_rqst.edge_types[id]);
      ++id;
    }

    for (auto &[shard, rqst] : per_shard_request_table) {
      state.requests.push_back(std::move(rqst));
    }
    state.state = ExecutionState<ExpandOneRequest>::EXECUTING;
  }

  StorageClient &GetStorageClientForShard(Shard shard, LabelId label_id) {
    if (!storage_cli_manager_.Exists(label_id, shard)) {
      AddStorageClientToManager(shard, label_id);
    }
    return storage_cli_manager_.GetClient(label_id, shard);
  }

  StorageClient &GetStorageClientForShard(const std::string &label, const CompoundKey &key) {
    auto shard = shards_map_.GetShardForKey(label, key);
    auto label_id = shards_map_.GetLabelId(label);
    return GetStorageClientForShard(std::move(shard), label_id);
  }

  void AddStorageClientToManager(Shard target_shard, const LabelId &label_id) {
    MG_ASSERT(!target_shard.empty());
    auto leader_addr = target_shard.front();
    std::vector<Address> addresses;
    addresses.reserve(target_shard.size());
    for (auto &address : target_shard) {
      addresses.push_back(std::move(address.address));
    }
    auto cli = StorageClient(io_, std::move(leader_addr.address), std::move(addresses));
    storage_cli_manager_.AddClient(label_id, target_shard, std::move(cli));
  }

  ShardMap shards_map_;
  CoordinatorClient coord_cli_;
  RsmStorageClientManager<StorageClient> storage_cli_manager_;
  memgraph::io::Io<TTransport> io_;
  memgraph::coordinator::Hlc transaction_id_;
  // TODO(kostasrim) Add batch prefetching
};
}  // namespace requests
