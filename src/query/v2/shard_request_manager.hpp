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

#include "coordinator/coordinator.hpp"
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
#include "query/v2/accessors.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/value_conversions.hpp"
#include "utils/result.hpp"

namespace memgraph::msgs {
template <typename TStorageClient>
class RsmStorageClientManager {
 public:
  using CompoundKey = memgraph::io::rsm::ShardRsmKey;
  using Shard = memgraph::coordinator::Shard;
  using LabelId = memgraph::storage::v3::LabelId;
  RsmStorageClientManager() = default;
  RsmStorageClientManager(const RsmStorageClientManager &) = delete;
  RsmStorageClientManager(RsmStorageClientManager &&) = delete;
  RsmStorageClientManager &operator=(const RsmStorageClientManager &) = delete;
  RsmStorageClientManager &operator=(RsmStorageClientManager &&) = delete;
  ~RsmStorageClientManager() = default;

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
  using VertexAccessor = memgraph::query::v2::accessors::VertexAccessor;
  ShardRequestManagerInterface() = default;
  ShardRequestManagerInterface(const ShardRequestManagerInterface &) = delete;
  ShardRequestManagerInterface(ShardRequestManagerInterface &&) = delete;
  ShardRequestManagerInterface &operator=(const ShardRequestManagerInterface &) = delete;
  ShardRequestManagerInterface &&operator=(ShardRequestManagerInterface &&) = delete;

  virtual ~ShardRequestManagerInterface() = default;

  virtual void StartTransaction() = 0;
  virtual std::vector<VertexAccessor> Request(ExecutionState<ScanVerticesRequest> &state) = 0;
  virtual std::vector<CreateVerticesResponse> Request(ExecutionState<CreateVerticesRequest> &state,
                                                      std::vector<NewVertex> new_vertices) = 0;
  virtual std::vector<ExpandOneResponse> Request(ExecutionState<ExpandOneRequest> &state) = 0;
  virtual memgraph::storage::v3::PropertyId NameToProperty(const std::string &name) const = 0;
  virtual memgraph::storage::v3::LabelId LabelNameToLabelId(const std::string &name) const = 0;
  virtual bool IsPrimaryKey(PropertyId name) const = 0;
};

// TODO(kostasrim)rename this class template
template <typename TTransport>
class ShardRequestManager : public ShardRequestManagerInterface {
 public:
  using StorageClient =
      memgraph::coordinator::RsmClient<TTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;
  using CoordinatorWriteRequests = memgraph::coordinator::CoordinatorWriteRequests;
  using CoordinatorClient = memgraph::coordinator::CoordinatorClient<TTransport>;
  using Address = memgraph::io::Address;
  using Shard = memgraph::coordinator::Shard;
  using ShardMap = memgraph::coordinator::ShardMap;
  using CompoundKey = memgraph::coordinator::PrimaryKey;
  using VertexAccessor = memgraph::query::v2::accessors::VertexAccessor;
  ShardRequestManager(CoordinatorClient coord, memgraph::io::Io<TTransport> &&io)
      : coord_cli_(std::move(coord)), io_(std::move(io)) {}

  ShardRequestManager(const ShardRequestManager &) = delete;
  ShardRequestManager(ShardRequestManager &&) = delete;
  ShardRequestManager &operator=(const ShardRequestManager &) = delete;
  ShardRequestManager &operator=(ShardRequestManager &&) = delete;

  ~ShardRequestManager() override {}

  void StartTransaction() override {
    memgraph::coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    CoordinatorWriteRequests write_req = req;
    auto write_res = coord_cli_.SendWriteRequest(write_req);
    if (write_res.HasError()) {
      throw std::runtime_error("HLC request failed");
    }
    auto coordinator_write_response = write_res.GetValue();
    auto hlc_response = std::get<memgraph::coordinator::HlcResponse>(coordinator_write_response);

    // Transaction ID to be used later...
    transaction_id_ = hlc_response.new_hlc;

    if (hlc_response.fresher_shard_map) {
      shards_map_ = hlc_response.fresher_shard_map.value();
    }
  }

  memgraph::storage::v3::PropertyId NameToProperty(const std::string &name) const override {
    return *shards_map_.GetPropertyId(name);
  }

  memgraph::storage::v3::LabelId LabelNameToLabelId(const std::string &name) const override {
    return shards_map_.GetLabelId(name);
  }

  bool IsPrimaryKey(const PropertyId name) const override {
    return std::find_if(shards_map_.properties.begin(), shards_map_.properties.end(),
                        [name](auto &pr) { return pr.second == name; }) != shards_map_.properties.end();
  }

  // TODO(Tyler / Gabor) stages to using async RsmClient
  //     methods for each of the 3 existing Request methods:
  // ~~~~ stage A: launch requests
  // for each request:
  //   1. get the client in the same way as below
  //      using GetStorageClientForShard
  //   2. call storage_client.SendAsyncReadRequest(state.requests[id])
  //      to send the request and load a Future into the RsmClient
  // ~~~~ stage B: drive requests to completion
  // while requests exist in the unfinished request structure:
  //   for each request:
  //     3. call storage_client.PollAsyncReadRequest() to see
  //        if the request is ready
  //   if requests exist:
  //     4. call shard_cache.begin().AwaitAsyncReadRequest() to block on it
  //   if we get a paginated response, launch a new async request for it
  //   if we get a non-paginated response, remove it from the unfinished request structure
  // ~~~~ notes:
  // * it's possible that some responses will return
  //   before the first one that we call Await... on returns,
  //   but it's OK for now if we're not optimal. The main
  //   point is that we're still parallelizing.

  // TODO(kostasrim) Simplify return result
  std::vector<VertexAccessor> Request(ExecutionState<ScanVerticesRequest> &state) override {
    MaybeInitializeExecutionState(state);
    std::vector<ScanVerticesResponse> responses;

    // TODO(gvolfing)
    // Figure out redirection. nullopt can mean so many things...
    // discuss with Tyler. Redirection should be handled on the
    // ShardRsmside only? or resend from here?

    // 1. Send the requests.
    for (const auto &request : state.requests) {
      auto &storage_client =
          GetStorageClientForShard(*state.label, storage::conversions::ConvertPropertyVector(request.start_id.second));
      ReadRequests req = request;
      storage_client.SendAsyncReadRequest(request);
    }

    // 2. Loop on poll
    do {
      bool at_least_one_result = PollOnRequests(state, responses);
      if (!at_least_one_result && !state.shard_cache.empty()) {
        // 3. Await on the first req if none of the futures have
        //    been filled yet.
        AwaitOnFirstRequest(state, responses);
      }
    } while (!state.shard_cache.empty());

    // We are done with this state
    MaybeCompleteState(state);
    // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as return
    // result of storage_client.SendReadRequest()).
    return PostProcess(std::move(responses));
  }

  // // TODO(kostasrim) Simplify return result
  // std::vector<VertexAccessor> Request(ExecutionState<ScanVerticesRequest> &state) override {
  //   MaybeInitializeExecutionState(state);
  //   std::vector<ScanVerticesResponse> responses;
  //   auto &shard_cache_ref = state.shard_cache;
  //   size_t id = 0;
  //   for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
  //     auto &storage_client = GetStorageClientForShard(
  //         *state.label, storage::conversions::ConvertPropertyVector(state.requests[id].start_id.second));
  //     // TODO(kostasrim) Currently requests return the result directly. Adjust this when the API works MgFuture
  //     // instead.
  //     ReadRequests req = state.requests[id];
  //     auto read_response_result = storage_client.SendReadRequest(req);
  //     // RETRY on timeouts?
  //     // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map test
  //     if (read_response_result.HasError()) {
  //       throw std::runtime_error("ScanAll request timedout");
  //     }
  //     ReadResponses read_response_variant = read_response_result.GetValue();
  //     auto &response = std::get<ScanVerticesResponse>(read_response_variant);
  //     if (!response.success) {
  //       throw std::runtime_error("ScanAll request did not succeed");
  //     }
  //     if (!response.next_start_id) {
  //       shard_it = shard_cache_ref.erase(shard_it);
  //     } else {
  //       state.requests[id].start_id.second = response.next_start_id->second;
  //       ++shard_it;
  //     }
  //     responses.push_back(std::move(response));
  //   }
  //   // We are done with this state
  //   MaybeCompleteState(state);
  //   // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as
  //   return
  //   // result of storage_client.SendReadRequest()).
  //   return PostProcess(std::move(responses));
  // }

  std::vector<CreateVerticesResponse> Request(ExecutionState<CreateVerticesRequest> &state,
                                              std::vector<NewVertex> new_vertices) override {
    MG_ASSERT(!new_vertices.empty());
    MaybeInitializeExecutionState(state, new_vertices);
    std::vector<CreateVerticesResponse> responses;
    auto &shard_cache_ref = state.shard_cache;
    size_t id = 0;

    std::vector<std::vector<memgraph::query::v2::accessors::VertexAccessor::Label>> prim_label_cache;

    // 1. Send the requests.
    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
      // This is fine because all new_vertices of each request end up on the same shard
      // TODO(gvolfing) what happens here if we have to resend the Request?
      // The execution state is modified here, The Primary Label will be lost.
      // Do we recreate the execution state upon resending?
      const auto labels = state.requests[id].new_vertices[0].label_ids;

      // before we would have a problem at this step, because the original state of
      // the execution state would have been allready modified at this point hence
      // the deep copy.
      auto req_deep_copy = state.requests[id];

      for (auto &new_vertex : req_deep_copy.new_vertices) {
        new_vertex.label_ids.erase(new_vertex.label_ids.begin());
      }

      auto &storage_client = GetStorageClientForShard(*shard_it, labels[0].id);

      WriteRequests req = req_deep_copy;
      storage_client.SendAsyncWriteRequest(req);
    }
    // 2. Loop on Poll

    // We are done with this state
    MaybeCompleteState(state);
    // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as return
    // result of storage_client.SendReadRequest()).
    return responses;
  }

  // std::vector<CreateVerticesResponse> Request(ExecutionState<CreateVerticesRequest> &state,
  //                                             std::vector<NewVertex> new_vertices) override {
  //   MG_ASSERT(!new_vertices.empty());
  //   MaybeInitializeExecutionState(state, new_vertices);
  //   std::vector<CreateVerticesResponse> responses;
  //   auto &shard_cache_ref = state.shard_cache;
  //   size_t id = 0;
  //   for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
  //     // This is fine because all new_vertices of each request end up on the same shard
  //     const auto labels = state.requests[id].new_vertices[0].label_ids;
  //     for (auto &new_vertex : state.requests[id].new_vertices) {
  //       new_vertex.label_ids.erase(new_vertex.label_ids.begin());
  //     }
  //     auto primary_key = state.requests[id].new_vertices[0].primary_key;
  //     auto &storage_client = GetStorageClientForShard(*shard_it, labels[0].id);
  //     WriteRequests req = state.requests[id];
  //     auto write_response_result = storage_client.SendWriteRequest(req);
  //     // RETRY on timeouts?
  //     // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map test
  //     if (write_response_result.HasError()) {
  //       throw std::runtime_error("CreateVertices request timedout");
  //     }
  //     WriteResponses response_variant = write_response_result.GetValue();
  //     CreateVerticesResponse mapped_response = std::get<CreateVerticesResponse>(response_variant);

  //     if (!mapped_response.success) {
  //       throw std::runtime_error("CreateVertices request did not succeed");
  //     }
  //     responses.push_back(mapped_response);
  //     shard_it = shard_cache_ref.erase(shard_it);
  //   }
  //   // We are done with this state
  //   MaybeCompleteState(state);
  //   // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as
  //   return
  //   // result of storage_client.SendReadRequest()).
  //   return responses;
  // }

  std::vector<ExpandOneResponse> Request(ExecutionState<ExpandOneRequest> &state) override {
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
      const Label primary_label = state.requests[id].src_vertices[0].first;
      auto &storage_client = GetStorageClientForShard(*shard_it, primary_label.id);
      ReadRequests req = state.requests[id];
      auto read_response_result = storage_client.SendReadRequest(req);
      // RETRY on timeouts?
      // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map
      if (read_response_result.HasError()) {
        throw std::runtime_error("ExpandOne request timedout");
      }
      auto &response = std::get<ExpandOneResponse>(read_response_result.GetValue());
      responses.push_back(std::move(response));
    }
    return responses;
  }

 private:
  std::vector<VertexAccessor> PostProcess(std::vector<ScanVerticesResponse> &&responses) const {
    std::vector<VertexAccessor> accessors;
    for (auto &response : responses) {
      for (auto &result_row : response.results) {
        accessors.emplace_back(VertexAccessor(std::move(result_row.vertex), std::move(result_row.props)));
      }
    }
    return accessors;
  }

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
                                     std::vector<NewVertex> new_vertices) {
    ThrowIfStateCompleted(state);
    if (ShallNotInitializeState(state)) {
      return;
    }
    state.transaction_id = transaction_id_;

    std::map<Shard, CreateVerticesRequest> per_shard_request_table;

    for (auto &new_vertex : new_vertices) {
      MG_ASSERT(!new_vertex.label_ids.empty(), "This is error!");
      auto shard = shards_map_.GetShardForKey(new_vertex.label_ids[0].id,
                                              storage::conversions::ConvertPropertyVector(new_vertex.primary_key));
      if (!per_shard_request_table.contains(shard)) {
        CreateVerticesRequest create_v_rqst{.transaction_id = transaction_id_};
        per_shard_request_table.insert(std::pair(shard, std::move(create_v_rqst)));
        state.shard_cache.push_back(shard);
      }
      per_shard_request_table[shard].new_vertices.push_back(std::move(new_vertex));
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
    auto shards = shards_map_.GetShards(*state.label);
    for (auto &[key, shard] : shards) {
      MG_ASSERT(!shard.empty());
      state.shard_cache.push_back(std::move(shard));
      ScanVerticesRequest rqst;
      rqst.transaction_id = transaction_id_;
      rqst.start_id.second = storage::conversions::ConvertValueVector(key);
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
      auto shard =
          shards_map_.GetShardForKey(vertex.first.id, storage::conversions::ConvertPropertyVector(vertex.second));
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

  bool PollOnRequests(ExecutionState<ScanVerticesRequest> &state, std::vector<ScanVerticesResponse> &responses) {
    auto &shard_cache_ref = state.shard_cache;
    size_t id = 0;
    bool at_least_one_result = false;

    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
      auto &storage_client = GetStorageClientForShard(
          *state.label, storage::conversions::ConvertPropertyVector(state.requests[id].start_id.second));

      auto poll_result = storage_client.PollAsyncReadRequest();
      if (!poll_result) {
        // Result is not ready yet.
        continue;
      }

      at_least_one_result = true;

      if (poll_result->HasError()) {
        throw std::runtime_error("ScanAll request timedout");
      }

      ReadResponses read_response_variant = poll_result->GetValue();
      auto &response = std::get<ScanVerticesResponse>(read_response_variant);
      if (!response.success) {
        throw std::runtime_error("ScanAll request did not succeed");
      }

      // TODO(gvolfing) still verify if this is how it should be working
      if (!response.next_start_id) {
        shard_it = shard_cache_ref.erase(shard_it);
      } else {
        state.requests[id].start_id.second = response.next_start_id->second;
        ++shard_it;  // why increment here? will we ever visit the shard again for the second round of values? + the id
                     // of the request is also being incremented in this case.
      }
      responses.push_back(std::move(response));
    }

    return at_least_one_result;
  }

  // These are not working with possibly paginated responses
  bool PollOnRequests(ExecutionState<CreateVerticesRequest> &state, std::vector<CreateVerticesResponse> &responses) {
    auto &shard_cache_ref = state.shard_cache;
    size_t id = 0;
    bool at_least_one_result = false;

    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
      // This is fine because all new_vertices of each request end up on the same shard
      const auto labels = state.requests[id].new_vertices[0].label_ids;

      auto &storage_client = GetStorageClientForShard(*shard_it, labels[0].id);

      auto poll_result = storage_client.PollAsyncReadRequest();
      if (!poll_result) {
        // Result is not ready yet.
        continue;
      }

      at_least_one_result = true;

      if (poll_result->HasError()) {
        throw std::runtime_error("ScanAll request timedout");
      }

      WriteResponses response_variant = poll_result->GetValue();
      CreateVerticesResponse mapped_response = std::get<CreateVerticesResponse>(response_variant);

      if (!mapped_response.success) {
        throw std::runtime_error("CreateVertices request did not succeed");
      }
      responses.push_back(mapped_response);
      shard_it = shard_cache_ref.erase(shard_it);
    }

    return at_least_one_result;
  }

  void AwaitOnFirstRequest(ExecutionState<ScanVerticesRequest> &state, std::vector<ScanVerticesResponse> &responses) {
    auto &shard_cache_ref = state.shard_cache;
    auto &storage_client = GetStorageClientForShard(
        *state.label, storage::conversions::ConvertPropertyVector(state.requests.front().start_id.second));
    auto await_result = storage_client.AwaitAsyncReadRequest();

    if (!await_result) {
      // Redirection has occured.
    }

    if (await_result->HasError()) {
      throw std::runtime_error("ScanAll request timedout");
    }

    ReadResponses read_response_variant = await_result->GetValue();
    auto &response = std::get<ScanVerticesResponse>(read_response_variant);
    if (!response.success) {
      throw std::runtime_error("ScanAll request did not succeed");
    }

    // TODO(gvolfing) Verify if this is working as intended.
    //  Assumption -> The first req in 'requests' is associated with the first shard in 'shard_cache_ref'.
    if (!response.next_start_id) {
      shard_cache_ref.erase(shard_cache_ref.begin());
    } else {
      state.requests[0].start_id.second = response.next_start_id->second;
      //++shard_it; // why increment here? will we ever visit the shard again for the second round of values? + the id
      // of the request is also being incremented in this case.
    }
    responses.push_back(std::move(response));
  }
  void AwaitOnFirstRequest(ExecutionState<CreateVerticesRequest> &state,
                           std::vector<CreateVerticesResponse> &responses) {
    auto &shard_cache_ref = state.shard_cache;
    auto &storage_client = GetStorageClientForShard(
        *state.label, storage::conversions::ConvertPropertyVector(state.requests.front().new_vertices[0].primary_key));
    auto await_result = storage_client.AwaitAsyncWriteRequest();

    if (!await_result) {
      // Redirection has occured.
    }

    if (await_result->HasError()) {
      throw std::runtime_error("ScanAll request timedout");
    }

    WriteResponses write_response_variant = await_result->GetValue();
    auto &response = std::get<CreateVerticesResponse>(write_response_variant);

    if (!response.success) {
      throw std::runtime_error("CreateVertices request did not succeed");
    }
    responses.push_back(response);
    shard_cache_ref.erase(shard_cache_ref.begin());
  }

  ShardMap shards_map_;
  CoordinatorClient coord_cli_;
  RsmStorageClientManager<StorageClient> storage_cli_manager_;
  memgraph::io::Io<TTransport> io_;
  memgraph::coordinator::Hlc transaction_id_;
  // TODO(kostasrim) Add batch prefetching
};
}  // namespace memgraph::msgs
