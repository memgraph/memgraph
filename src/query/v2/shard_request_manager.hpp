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
#include <iterator>
#include <map>
#include <numeric>
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

  void AddClient(Shard key, TStorageClient client) { cli_cache_.emplace(std::move(key), std::move(client)); }

  bool Exists(const Shard &key) { return cli_cache_.contains(key); }

  void PurgeCache() { cli_cache_.clear(); }

  TStorageClient &GetClient(const Shard &key) {
    auto it = cli_cache_.find(key);
    MG_ASSERT(it != cli_cache_.end(), "Non-existing shard client");
    return it->second;
  }

 private:
  std::map<Shard, TStorageClient> cli_cache_;
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
  // TODO(gvolfing)
  // Maybe make this into a more complex object to be able to keep track of paginated resutls. E.g. instead of a vector
  // of Shards make it into a std::vector<std::pair<Shard, PaginatedResultType>> (probably a struct instead of a pair)
  // where PaginatedResultType is an enum signaling the progress on the given request. This way we can easily check if
  // a partial response on a shard(if there is one) is finished and we can send off the request for the next batch.
  std::vector<Shard> shard_cache;
  // 1-1 mapping with `shard_cache`.
  // A vector that tracks request metadata for each shard (For example, next_id for a ScanAll on Shard A)
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
  virtual void Commit() = 0;
  virtual std::vector<VertexAccessor> Request(ExecutionState<ScanVerticesRequest> &state) = 0;
  virtual std::vector<CreateVerticesResponse> Request(ExecutionState<CreateVerticesRequest> &state,
                                                      std::vector<NewVertex> new_vertices) = 0;
  virtual std::vector<ExpandOneResultRow> Request(ExecutionState<ExpandOneRequest> &state,
                                                  ExpandOneRequest request) = 0;
  virtual std::vector<CreateExpandResponse> Request(ExecutionState<CreateExpandRequest> &state,
                                                    std::vector<NewExpand> new_edges) = 0;

  virtual storage::v3::EdgeTypeId NameToEdgeType(const std::string &name) const = 0;
  virtual storage::v3::PropertyId NameToProperty(const std::string &name) const = 0;
  virtual storage::v3::LabelId NameToLabel(const std::string &name) const = 0;
  virtual const std::string &PropertyToName(memgraph::storage::v3::PropertyId prop) const = 0;
  virtual const std::string &LabelToName(memgraph::storage::v3::LabelId label) const = 0;
  virtual const std::string &EdgeTypeToName(memgraph::storage::v3::EdgeTypeId type) const = 0;
  virtual bool IsPrimaryKey(LabelId primary_label, PropertyId property) const = 0;
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

  void Commit() override {
    memgraph::coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    CoordinatorWriteRequests write_req = req;
    auto write_res = coord_cli_.SendWriteRequest(write_req);
    if (write_res.HasError()) {
      throw std::runtime_error("HLC request for commit failed");
    }
    auto coordinator_write_response = write_res.GetValue();
    auto hlc_response = std::get<memgraph::coordinator::HlcResponse>(coordinator_write_response);

    if (hlc_response.fresher_shard_map) {
      shards_map_ = hlc_response.fresher_shard_map.value();
    }
    auto commit_timestamp = hlc_response.new_hlc;

    msgs::CommitRequest commit_req{.transaction_id = transaction_id_, .commit_timestamp = commit_timestamp};

    for (const auto &[label, space] : shards_map_.label_spaces) {
      for (const auto &[key, shard] : space.shards) {
        auto &storage_client = GetStorageClientForShard(shard);
        // TODO(kostasrim) Currently requests return the result directly. Adjust this when the API works MgFuture
        // instead.
        auto commit_response = storage_client.SendWriteRequest(commit_req);
        // RETRY on timeouts?
        // Sometimes this produces a timeout. Temporary solution is to use a while(true) as was done in shard_map test
        if (commit_response.HasError()) {
          throw std::runtime_error("Commit request timed out");
        }
        WriteResponses write_response_variant = commit_response.GetValue();
        auto &response = std::get<CommitResponse>(write_response_variant);
        if (!response.success) {
          throw std::runtime_error("Commit request did not succeed");
        }
      }
    }
  }

  storage::v3::EdgeTypeId NameToEdgeType(const std::string &name) const override {
    return shards_map_.GetEdgeTypeId(name).value();
  }

  storage::v3::PropertyId NameToProperty(const std::string &name) const override {
    return shards_map_.GetPropertyId(name).value();
  }

  storage::v3::LabelId NameToLabel(const std::string &name) const override {
    return shards_map_.GetLabelId(name).value();
  }

  const std::string &PropertyToName(memgraph::storage::v3::PropertyId /*prop*/) const override {
    static std::string str{"dummy__prop"};
    return str;
  }
  const std::string &LabelToName(memgraph::storage::v3::LabelId /*label*/) const override {
    static std::string str{"dummy__label"};
    return str;
  }
  const std::string &EdgeTypeToName(memgraph::storage::v3::EdgeTypeId /*type*/) const override {
    static std::string str{"dummy__edgetype"};
    return str;
  }

  bool IsPrimaryKey(LabelId primary_label, PropertyId property) const override {
    const auto schema_it = shards_map_.schemas.find(primary_label);
    MG_ASSERT(schema_it != shards_map_.schemas.end(), "Invalid primary label id: {}", primary_label.AsUint());

    return std::find_if(schema_it->second.begin(), schema_it->second.end(), [property](const auto &schema_prop) {
             return schema_prop.property_id == property;
           }) != schema_it->second.end();
  }

  // TODO(kostasrim) Simplify return result
  std::vector<VertexAccessor> Request(ExecutionState<ScanVerticesRequest> &state) override {
    spdlog::info("shards_map_.size(): {}", shards_map_.GetShards(*state.label).size());
    MaybeInitializeExecutionState(state);
    std::vector<ScanVerticesResponse> responses;

    SendAllRequests(state);
    auto all_requests_gathered = [](auto &paginated_rsp_tracker) {
      return std::ranges::all_of(paginated_rsp_tracker, [](const auto &state) {
        return state.second == PaginatedResponseState::PartiallyFinished;
      });
    };

    std::map<Shard, PaginatedResponseState> paginated_response_tracker;
    for (const auto &shard : state.shard_cache) {
      paginated_response_tracker.insert(std::make_pair(shard, PaginatedResponseState::Pending));
    }

    do {
      AwaitOnPaginatedRequests(state, responses, paginated_response_tracker);
    } while (!all_requests_gathered(paginated_response_tracker));

    MaybeCompleteState(state);
    // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as return
    // result of storage_client.SendReadRequest()).
    return PostProcess(std::move(responses));
  }

  std::vector<CreateVerticesResponse> Request(ExecutionState<CreateVerticesRequest> &state,
                                              std::vector<NewVertex> new_vertices) override {
    MG_ASSERT(!new_vertices.empty());
    MaybeInitializeExecutionState(state, new_vertices);
    std::vector<CreateVerticesResponse> responses;
    auto &shard_cache_ref = state.shard_cache;

    // 1. Send the requests.
    SendAllRequests(state, shard_cache_ref);

    // 2. Block untill all the futures are exhausted
    do {
      AwaitOnResponses(state, responses);
    } while (!state.shard_cache.empty());

    MaybeCompleteState(state);
    // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as return
    // result of storage_client.SendReadRequest()).
    return responses;
  }

  std::vector<CreateExpandResponse> Request(ExecutionState<CreateExpandRequest> &state,
                                            std::vector<NewExpand> new_edges) override {
    MG_ASSERT(!new_edges.empty());
    MaybeInitializeExecutionState(state, new_edges);
    std::vector<CreateExpandResponse> responses;
    auto &shard_cache_ref = state.shard_cache;
    size_t id{0};
    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++id) {
      auto &storage_client = GetStorageClientForShard(*shard_it);
      WriteRequests req = state.requests[id];
      auto write_response_result = storage_client.SendWriteRequest(std::move(req));
      if (write_response_result.HasError()) {
        throw std::runtime_error("CreateVertices request timedout");
      }
      WriteResponses response_variant = write_response_result.GetValue();
      CreateExpandResponse mapped_response = std::get<CreateExpandResponse>(response_variant);

      if (!mapped_response.success) {
        throw std::runtime_error("CreateExpand request did not succeed");
      }
      responses.push_back(mapped_response);
      shard_it = shard_cache_ref.erase(shard_it);
    }
    // We are done with this state
    MaybeCompleteState(state);
    return responses;
  }

  std::vector<ExpandOneResultRow> Request(ExecutionState<ExpandOneRequest> &state, ExpandOneRequest request) override {
    // TODO(kostasrim)Update to limit the batch size here
    // Expansions of the destination must be handled by the caller. For example
    // match (u:L1 { prop : 1 })-[:Friend]-(v:L1)
    // For each vertex U, the ExpandOne will result in <U, Edges>. The destination vertex and its properties
    // must be fetched again with an ExpandOne(Edges.dst)
    MaybeInitializeExecutionState(state, std::move(request));
    std::vector<ExpandOneResponse> responses;
    auto &shard_cache_ref = state.shard_cache;

    // 1. Send the requests.
    SendAllRequests(state, shard_cache_ref);

    // 2. Block untill all the futures are exhausted
    do {
      AwaitOnResponses(state, responses);
    } while (!state.shard_cache.empty());
    std::vector<ExpandOneResultRow> result_rows;
    const auto total_row_count = std::accumulate(
        responses.begin(), responses.end(), 0,
        [](const int64_t partial_count, const ExpandOneResponse &resp) { return partial_count + resp.result.size(); });
    result_rows.reserve(total_row_count);

    for (auto &response : responses) {
      result_rows.insert(result_rows.end(), std::make_move_iterator(response.result.begin()),
                         std::make_move_iterator(response.result.end()));
    }
    MaybeCompleteState(state);
    return result_rows;
  }

 private:
  enum class PaginatedResponseState { Pending, PartiallyFinished };

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

  void MaybeInitializeExecutionState(ExecutionState<CreateExpandRequest> &state, std::vector<NewExpand> new_expands) {
    ThrowIfStateCompleted(state);
    if (ShallNotInitializeState(state)) {
      return;
    }
    state.transaction_id = transaction_id_;

    std::map<Shard, CreateExpandRequest> per_shard_request_table;
    auto ensure_shard_exists_in_table = [&per_shard_request_table,
                                         transaction_id = transaction_id_](const Shard &shard) {
      if (!per_shard_request_table.contains(shard)) {
        CreateExpandRequest create_expand_request{.transaction_id = transaction_id};
        per_shard_request_table.insert({shard, std::move(create_expand_request)});
      }
    };

    for (auto &new_expand : new_expands) {
      const auto shard_src_vertex = shards_map_.GetShardForKey(
          new_expand.src_vertex.first.id, storage::conversions::ConvertPropertyVector(new_expand.src_vertex.second));
      const auto shard_dest_vertex = shards_map_.GetShardForKey(
          new_expand.dest_vertex.first.id, storage::conversions::ConvertPropertyVector(new_expand.dest_vertex.second));

      ensure_shard_exists_in_table(shard_src_vertex);

      if (shard_src_vertex != shard_dest_vertex) {
        ensure_shard_exists_in_table(shard_dest_vertex);
        per_shard_request_table[shard_dest_vertex].new_expands.push_back(new_expand);
      }
      per_shard_request_table[shard_src_vertex].new_expands.push_back(std::move(new_expand));
    }

    for (auto &[shard, request] : per_shard_request_table) {
      state.shard_cache.push_back(shard);
      state.requests.push_back(std::move(request));
    }
    state.state = ExecutionState<CreateExpandRequest>::EXECUTING;
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

  void MaybeInitializeExecutionState(ExecutionState<ExpandOneRequest> &state, ExpandOneRequest request) {
    ThrowIfStateCompleted(state);
    if (ShallNotInitializeState(state)) {
      return;
    }
    state.transaction_id = transaction_id_;

    std::map<Shard, ExpandOneRequest> per_shard_request_table;
    auto top_level_rqst_template = request;
    top_level_rqst_template.transaction_id = transaction_id_;
    top_level_rqst_template.src_vertices.clear();
    state.requests.clear();
    for (auto &vertex : request.src_vertices) {
      auto shard =
          shards_map_.GetShardForKey(vertex.first.id, storage::conversions::ConvertPropertyVector(vertex.second));
      if (!per_shard_request_table.contains(shard)) {
        per_shard_request_table.insert(std::pair(shard, top_level_rqst_template));
        state.shard_cache.push_back(shard);
      }
      per_shard_request_table[shard].src_vertices.push_back(vertex);
    }

    for (auto &[shard, rqst] : per_shard_request_table) {
      state.requests.push_back(std::move(rqst));
    }
    state.state = ExecutionState<ExpandOneRequest>::EXECUTING;
  }

  StorageClient &GetStorageClientForShard(Shard shard) {
    if (!storage_cli_manager_.Exists(shard)) {
      AddStorageClientToManager(shard);
    }
    return storage_cli_manager_.GetClient(shard);
  }

  StorageClient &GetStorageClientForShard(const std::string &label, const CompoundKey &key) {
    auto shard = shards_map_.GetShardForKey(label, key);
    return GetStorageClientForShard(std::move(shard));
  }

  void AddStorageClientToManager(Shard target_shard) {
    MG_ASSERT(!target_shard.empty());
    auto leader_addr = target_shard.front();
    std::vector<Address> addresses;
    addresses.reserve(target_shard.size());
    for (auto &address : target_shard) {
      addresses.push_back(std::move(address.address));
    }
    auto cli = StorageClient(io_, std::move(leader_addr.address), std::move(addresses));
    storage_cli_manager_.AddClient(target_shard, std::move(cli));
  }

  void SendAllRequests(ExecutionState<ScanVerticesRequest> &state) {
    for (const auto &request : state.requests) {
      auto &storage_client =
          GetStorageClientForShard(*state.label, storage::conversions::ConvertPropertyVector(request.start_id.second));
      ReadRequests req = request;
      storage_client.SendAsyncReadRequest(request);
    }
  }

  void SendAllRequests(ExecutionState<CreateVerticesRequest> &state,
                       std::vector<memgraph::coordinator::Shard> &shard_cache_ref) {
    size_t id = 0;
    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++shard_it) {
      // This is fine because all new_vertices of each request end up on the same shard
      const auto labels = state.requests[id].new_vertices[0].label_ids;
      auto req_deep_copy = state.requests[id];

      for (auto &new_vertex : req_deep_copy.new_vertices) {
        new_vertex.label_ids.erase(new_vertex.label_ids.begin());
      }

      auto &storage_client = GetStorageClientForShard(*shard_it);

      WriteRequests req = req_deep_copy;
      storage_client.SendAsyncWriteRequest(req);
      ++id;
    }
  }

  void SendAllRequests(ExecutionState<ExpandOneRequest> &state,
                       std::vector<memgraph::coordinator::Shard> &shard_cache_ref) {
    size_t id = 0;
    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end(); ++shard_it) {
      auto &storage_client = GetStorageClientForShard(*shard_it);
      ReadRequests req = state.requests[id];
      storage_client.SendAsyncReadRequest(req);
      ++id;
    }
  }

  void AwaitOnResponses(ExecutionState<CreateVerticesRequest> &state, std::vector<CreateVerticesResponse> &responses) {
    auto &shard_cache_ref = state.shard_cache;
    int64_t request_idx = 0;

    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end();) {
      // This is fine because all new_vertices of each request end up on the same shard
      const auto labels = state.requests[request_idx].new_vertices[0].label_ids;

      auto &storage_client = GetStorageClientForShard(*shard_it);

      auto poll_result = storage_client.AwaitAsyncWriteRequest();
      if (!poll_result) {
        ++shard_it;
        ++request_idx;

        continue;
      }

      if (poll_result->HasError()) {
        throw std::runtime_error("CreateVertices request timed out");
      }

      WriteResponses response_variant = poll_result->GetValue();
      auto response = std::get<CreateVerticesResponse>(response_variant);

      if (!response.success) {
        throw std::runtime_error("CreateVertices request did not succeed");
      }
      responses.push_back(response);

      shard_it = shard_cache_ref.erase(shard_it);
      // Needed to maintain the 1-1 mapping between the ShardCache and the requests.
      auto it = state.requests.begin() + request_idx;
      state.requests.erase(it);
    }
  }

  void AwaitOnResponses(ExecutionState<ExpandOneRequest> &state, std::vector<ExpandOneResponse> &responses) {
    auto &shard_cache_ref = state.shard_cache;
    int64_t request_idx = 0;

    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end();) {
      auto &storage_client = GetStorageClientForShard(*shard_it);

      auto poll_result = storage_client.PollAsyncReadRequest();
      if (!poll_result) {
        ++shard_it;
        ++request_idx;
        continue;
      }

      if (poll_result->HasError()) {
        throw std::runtime_error("ExpandOne request timed out");
      }

      ReadResponses response_variant = poll_result->GetValue();
      auto response = std::get<ExpandOneResponse>(response_variant);
      // -NOTE-
      // Currently a boolean flag for signaling the overall success of the
      // ExpandOne request does not exist. But it should, so here we assume
      // that it is already in place.
      if (!response.success) {
        throw std::runtime_error("ExpandOne request did not succeed");
      }

      responses.push_back(std::move(response));
      shard_it = shard_cache_ref.erase(shard_it);
      // Needed to maintain the 1-1 mapping between the ShardCache and the requests.
      auto it = state.requests.begin() + request_idx;
      state.requests.erase(it);
    }
  }

  void AwaitOnPaginatedRequests(ExecutionState<ScanVerticesRequest> &state,
                                std::vector<ScanVerticesResponse> &responses,
                                std::map<Shard, PaginatedResponseState> &paginated_response_tracker) {
    auto &shard_cache_ref = state.shard_cache;

    // Find the first request that is not holding a paginated response.
    int64_t request_idx = 0;
    for (auto shard_it = shard_cache_ref.begin(); shard_it != shard_cache_ref.end();) {
      if (paginated_response_tracker.at(*shard_it) != PaginatedResponseState::Pending) {
        ++shard_it;
        ++request_idx;
        continue;
      }

      auto &storage_client = GetStorageClientForShard(
          *state.label, storage::conversions::ConvertPropertyVector(state.requests[request_idx].start_id.second));
      auto await_result = storage_client.AwaitAsyncReadRequest();

      if (!await_result) {
        // Redirection has occured.
        ++shard_it;
        ++request_idx;
        continue;
      }

      if (await_result->HasError()) {
        throw std::runtime_error("ScanAll request timed out");
      }

      ReadResponses read_response_variant = await_result->GetValue();
      auto response = std::get<ScanVerticesResponse>(read_response_variant);
      if (!response.success) {
        throw std::runtime_error("ScanAll request did not succeed");
      }

      if (!response.next_start_id) {
        paginated_response_tracker.erase((*shard_it));
        shard_cache_ref.erase(shard_it);
        // Needed to maintain the 1-1 mapping between the ShardCache and the requests.
        auto it = state.requests.begin() + request_idx;
        state.requests.erase(it);

      } else {
        state.requests[request_idx].start_id.second = response.next_start_id->second;
        paginated_response_tracker[*shard_it] = PaginatedResponseState::PartiallyFinished;
      }
      responses.push_back(std::move(response));
    }
  }

  ShardMap shards_map_;
  CoordinatorClient coord_cli_;
  RsmStorageClientManager<StorageClient> storage_cli_manager_;
  memgraph::io::Io<TTransport> io_;
  memgraph::coordinator::Hlc transaction_id_;
  // TODO(kostasrim) Add batch prefetching
};
}  // namespace memgraph::msgs
