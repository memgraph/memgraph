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

namespace memgraph::query::v2 {
template <typename TStorageClient>
class RsmStorageClientManager {
 public:
  using CompoundKey = io::rsm::ShardRsmKey;
  using Shard = coordinator::Shard;
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
struct ShardRequestState {
  memgraph::coordinator::Shard shard;
  TRequest request;
  std::optional<io::rsm::AsyncRequestToken> async_request_token;
};

template <typename TRequest>
struct ExecutionState {
  using CompoundKey = io::rsm::ShardRsmKey;
  using Shard = coordinator::Shard;

  // label is optional because some operators can create/remove etc, vertices. These kind of requests contain the label
  // on the request itself.
  std::optional<std::string> label;
  // Transaction id to be filled by the RequestRouter implementation
  coordinator::Hlc transaction_id;
  // Initialized by RequestRouter implementation. This vector is filled with the shards that
  // the RequestRouter impl will send requests to. When a request to a shard exhausts it, meaning that
  // it pulled all the requested data from the given Shard, it will be removed from the Vector. When the Vector becomes
  // empty, it means that all of the requests have completed succefully.
  std::vector<ShardRequestState<TRequest>> requests;
};

class RequestRouterInterface {
 public:
  using VertexAccessor = query::v2::accessors::VertexAccessor;
  RequestRouterInterface() = default;
  RequestRouterInterface(const RequestRouterInterface &) = delete;
  RequestRouterInterface(RequestRouterInterface &&) = delete;
  RequestRouterInterface &operator=(const RequestRouterInterface &) = delete;
  RequestRouterInterface &&operator=(RequestRouterInterface &&) = delete;

  virtual ~RequestRouterInterface() = default;

  virtual void StartTransaction() = 0;
  virtual void Commit() = 0;
  virtual std::vector<VertexAccessor> ScanVertices(std::optional<std::string> label) = 0;
  virtual std::vector<msgs::CreateVerticesResponse> CreateVertices(std::vector<msgs::NewVertex> new_vertices) = 0;
  virtual std::vector<msgs::ExpandOneResultRow> ExpandOne(msgs::ExpandOneRequest request) = 0;
  virtual std::vector<msgs::CreateExpandResponse> CreateExpand(std::vector<msgs::NewExpand> new_edges) = 0;

  virtual storage::v3::EdgeTypeId NameToEdgeType(const std::string &name) const = 0;
  virtual storage::v3::PropertyId NameToProperty(const std::string &name) const = 0;
  virtual storage::v3::LabelId NameToLabel(const std::string &name) const = 0;
  virtual const std::string &PropertyToName(memgraph::storage::v3::PropertyId prop) const = 0;
  virtual const std::string &LabelToName(memgraph::storage::v3::LabelId label) const = 0;
  virtual const std::string &EdgeTypeToName(memgraph::storage::v3::EdgeTypeId type) const = 0;
  virtual std::optional<storage::v3::PropertyId> MaybeNameToProperty(const std::string &name) const = 0;
  virtual std::optional<storage::v3::EdgeTypeId> MaybeNameToEdgeType(const std::string &name) const = 0;
  virtual std::optional<storage::v3::LabelId> MaybeNameToLabel(const std::string &name) const = 0;
  virtual bool IsPrimaryLabel(storage::v3::LabelId label) const = 0;
  virtual bool IsPrimaryKey(storage::v3::LabelId primary_label, storage::v3::PropertyId property) const = 0;
};

// TODO(kostasrim)rename this class template
template <typename TTransport>
class RequestRouter : public RequestRouterInterface {
 public:
  using StorageClient = coordinator::RsmClient<TTransport, msgs::WriteRequests, msgs::WriteResponses,
                                               msgs::ReadRequests, msgs::ReadResponses>;
  using CoordinatorWriteRequests = coordinator::CoordinatorWriteRequests;
  using CoordinatorClient = coordinator::CoordinatorClient<TTransport>;
  using Address = io::Address;
  using Shard = coordinator::Shard;
  using ShardMap = coordinator::ShardMap;
  using CompoundKey = coordinator::PrimaryKey;
  using VertexAccessor = query::v2::accessors::VertexAccessor;
  RequestRouter(CoordinatorClient coord, io::Io<TTransport> &&io) : coord_cli_(std::move(coord)), io_(std::move(io)) {}

  RequestRouter(const RequestRouter &) = delete;
  RequestRouter(RequestRouter &&) = delete;
  RequestRouter &operator=(const RequestRouter &) = delete;
  RequestRouter &operator=(RequestRouter &&) = delete;

  ~RequestRouter() override {}

  void StartTransaction() override {
    coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    CoordinatorWriteRequests write_req = req;
    auto write_res = coord_cli_.SendWriteRequest(write_req);
    if (write_res.HasError()) {
      throw std::runtime_error("HLC request failed");
    }
    auto coordinator_write_response = write_res.GetValue();
    auto hlc_response = std::get<coordinator::HlcResponse>(coordinator_write_response);

    // Transaction ID to be used later...
    transaction_id_ = hlc_response.new_hlc;

    if (hlc_response.fresher_shard_map) {
      shards_map_ = hlc_response.fresher_shard_map.value();
      SetUpNameIdMappers();
    }
  }

  void Commit() override {
    coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    CoordinatorWriteRequests write_req = req;
    auto write_res = coord_cli_.SendWriteRequest(write_req);
    if (write_res.HasError()) {
      throw std::runtime_error("HLC request for commit failed");
    }
    auto coordinator_write_response = write_res.GetValue();
    auto hlc_response = std::get<coordinator::HlcResponse>(coordinator_write_response);

    if (hlc_response.fresher_shard_map) {
      shards_map_ = hlc_response.fresher_shard_map.value();
      SetUpNameIdMappers();
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
        msgs::WriteResponses write_response_variant = commit_response.GetValue();
        auto &response = std::get<msgs::CommitResponse>(write_response_variant);
        if (response.error) {
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

  const std::string &PropertyToName(storage::v3::PropertyId id) const override {
    return properties_.IdToName(id.AsUint());
  }
  const std::string &LabelToName(storage::v3::LabelId id) const override { return labels_.IdToName(id.AsUint()); }
  const std::string &EdgeTypeToName(storage::v3::EdgeTypeId id) const override {
    return edge_types_.IdToName(id.AsUint());
  }

  bool IsPrimaryKey(storage::v3::LabelId primary_label, storage::v3::PropertyId property) const override {
    const auto schema_it = shards_map_.schemas.find(primary_label);
    MG_ASSERT(schema_it != shards_map_.schemas.end(), "Invalid primary label id: {}", primary_label.AsUint());

    return std::find_if(schema_it->second.begin(), schema_it->second.end(), [property](const auto &schema_prop) {
             return schema_prop.property_id == property;
           }) != schema_it->second.end();
  }

  bool IsPrimaryLabel(storage::v3::LabelId label) const override { return shards_map_.label_spaces.contains(label); }

  // TODO(kostasrim) Simplify return result
  std::vector<VertexAccessor> ScanVertices(std::optional<std::string> label) override {
    ExecutionState<msgs::ScanVerticesRequest> state = {};
    state.label = label;

    // create requests
    InitializeExecutionState(state);

    // begin all requests in parallel
    for (auto &request : state.requests) {
      auto &storage_client = GetStorageClientForShard(request.shard);
      msgs::ReadRequests req = request.request;

      request.async_request_token = storage_client.SendAsyncReadRequest(request.request);
    }

    // drive requests to completion
    std::vector<msgs::ScanVerticesResponse> responses;
    do {
      DriveReadResponses(state, responses);
    } while (!state.requests.empty());

    // convert responses into VertexAccessor objects to return
    std::vector<VertexAccessor> accessors;
    for (auto &response : responses) {
      for (auto &result_row : response.results) {
        accessors.emplace_back(VertexAccessor(std::move(result_row.vertex), std::move(result_row.props), this));
      }
    }

    return accessors;
  }

  std::vector<msgs::CreateVerticesResponse> CreateVertices(std::vector<msgs::NewVertex> new_vertices) override {
    ExecutionState<msgs::CreateVerticesRequest> state = {};
    MG_ASSERT(!new_vertices.empty());

    // create requests
    InitializeExecutionState(state, new_vertices);

    // begin all requests in parallel
    for (auto &request : state.requests) {
      auto req_deep_copy = request.request;

      for (auto &new_vertex : req_deep_copy.new_vertices) {
        new_vertex.label_ids.erase(new_vertex.label_ids.begin());
      }

      auto &storage_client = GetStorageClientForShard(request.shard);

      msgs::WriteRequests req = req_deep_copy;
      request.async_request_token = storage_client.SendAsyncWriteRequest(req);
    }

    // drive requests to completion
    std::vector<msgs::CreateVerticesResponse> responses;
    do {
      DriveWriteResponses(state, responses);
    } while (!state.requests.empty());

    return responses;
  }

  std::vector<msgs::CreateExpandResponse> CreateExpand(std::vector<msgs::NewExpand> new_edges) override {
    ExecutionState<msgs::CreateExpandRequest> state = {};
    MG_ASSERT(!new_edges.empty());

    // create requests
    InitializeExecutionState(state, new_edges);

    // begin all requests in parallel
    for (auto &request : state.requests) {
      auto &storage_client = GetStorageClientForShard(request.shard);
      msgs::WriteRequests req = request.request;
      request.async_request_token = storage_client.SendAsyncWriteRequest(req);
    }

    // drive requests to completion
    std::vector<msgs::CreateExpandResponse> responses;
    do {
      DriveWriteResponses(state, responses);
    } while (!state.requests.empty());

    return responses;
  }

  std::vector<msgs::ExpandOneResultRow> ExpandOne(msgs::ExpandOneRequest request) override {
    ExecutionState<msgs::ExpandOneRequest> state = {};
    // TODO(kostasrim)Update to limit the batch size here
    // Expansions of the destination must be handled by the caller. For example
    // match (u:L1 { prop : 1 })-[:Friend]-(v:L1)
    // For each vertex U, the ExpandOne will result in <U, Edges>. The destination vertex and its properties
    // must be fetched again with an ExpandOne(Edges.dst)

    // create requests
    InitializeExecutionState(state, std::move(request));

    // begin all requests in parallel
    for (auto &request : state.requests) {
      auto &storage_client = GetStorageClientForShard(request.shard);
      msgs::ReadRequests req = request.request;
      request.async_request_token = storage_client.SendAsyncReadRequest(req);
    }

    // drive requests to completion
    std::vector<msgs::ExpandOneResponse> responses;
    do {
      DriveReadResponses(state, responses);
    } while (!state.requests.empty());

    // post-process responses
    std::vector<msgs::ExpandOneResultRow> result_rows;
    const auto total_row_count = std::accumulate(responses.begin(), responses.end(), 0,
                                                 [](const int64_t partial_count, const msgs::ExpandOneResponse &resp) {
                                                   return partial_count + resp.result.size();
                                                 });
    result_rows.reserve(total_row_count);

    for (auto &response : responses) {
      result_rows.insert(result_rows.end(), std::make_move_iterator(response.result.begin()),
                         std::make_move_iterator(response.result.end()));
    }

    return result_rows;
  }

  std::optional<storage::v3::PropertyId> MaybeNameToProperty(const std::string &name) const override {
    return shards_map_.GetPropertyId(name);
  }

  std::optional<storage::v3::EdgeTypeId> MaybeNameToEdgeType(const std::string &name) const override {
    return shards_map_.GetEdgeTypeId(name);
  }

  std::optional<storage::v3::LabelId> MaybeNameToLabel(const std::string &name) const override {
    return shards_map_.GetLabelId(name);
  }

 private:
  void InitializeExecutionState(ExecutionState<msgs::CreateVerticesRequest> &state,
                                std::vector<msgs::NewVertex> new_vertices) {
    state.transaction_id = transaction_id_;

    std::map<Shard, msgs::CreateVerticesRequest> per_shard_request_table;

    for (auto &new_vertex : new_vertices) {
      MG_ASSERT(!new_vertex.label_ids.empty(), "No label_ids provided for new vertex in RequestRouter::CreateVertices");
      auto shard = shards_map_.GetShardForKey(new_vertex.label_ids[0].id,
                                              storage::conversions::ConvertPropertyVector(new_vertex.primary_key));
      if (!per_shard_request_table.contains(shard)) {
        msgs::CreateVerticesRequest create_v_rqst{.transaction_id = transaction_id_};
        per_shard_request_table.insert(std::pair(shard, std::move(create_v_rqst)));
      }
      per_shard_request_table[shard].new_vertices.push_back(std::move(new_vertex));
    }

    for (auto &[shard, request] : per_shard_request_table) {
      ShardRequestState<msgs::CreateVerticesRequest> shard_request_state{
          .shard = shard,
          .request = request,
          .async_request_token = std::nullopt,
      };
      state.requests.emplace_back(std::move(shard_request_state));
    }
  }

  void InitializeExecutionState(ExecutionState<msgs::CreateExpandRequest> &state,
                                std::vector<msgs::NewExpand> new_expands) {
    state.transaction_id = transaction_id_;

    std::map<Shard, msgs::CreateExpandRequest> per_shard_request_table;
    auto ensure_shard_exists_in_table = [&per_shard_request_table,
                                         transaction_id = transaction_id_](const Shard &shard) {
      if (!per_shard_request_table.contains(shard)) {
        msgs::CreateExpandRequest create_expand_request{.transaction_id = transaction_id};
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
      ShardRequestState<msgs::CreateExpandRequest> shard_request_state{
          .shard = shard,
          .request = request,
          .async_request_token = std::nullopt,
      };
      state.requests.emplace_back(std::move(shard_request_state));
    }
  }

  void InitializeExecutionState(ExecutionState<msgs::ScanVerticesRequest> &state) {
    std::vector<coordinator::Shards> multi_shards;
    state.transaction_id = transaction_id_;
    if (!state.label) {
      multi_shards = shards_map_.GetAllShards();
    } else {
      const auto label_id = shards_map_.GetLabelId(*state.label);
      MG_ASSERT(label_id);
      MG_ASSERT(IsPrimaryLabel(*label_id));
      multi_shards = {shards_map_.GetShardsForLabel(*state.label)};
    }
    for (auto &shards : multi_shards) {
      for (auto &[key, shard] : shards) {
        MG_ASSERT(!shard.empty());

        msgs::ScanVerticesRequest request;
        request.transaction_id = transaction_id_;
        request.start_id.second = storage::conversions::ConvertValueVector(key);

        ShardRequestState<msgs::ScanVerticesRequest> shard_request_state{
            .shard = shard,
            .request = std::move(request),
            .async_request_token = std::nullopt,
        };

        state.requests.emplace_back(std::move(shard_request_state));
      }
    }
  }

  void InitializeExecutionState(ExecutionState<msgs::ExpandOneRequest> &state, msgs::ExpandOneRequest request) {
    state.transaction_id = transaction_id_;

    std::map<Shard, msgs::ExpandOneRequest> per_shard_request_table;
    auto top_level_rqst_template = request;
    top_level_rqst_template.transaction_id = transaction_id_;
    top_level_rqst_template.src_vertices.clear();
    state.requests.clear();
    for (auto &vertex : request.src_vertices) {
      auto shard =
          shards_map_.GetShardForKey(vertex.first.id, storage::conversions::ConvertPropertyVector(vertex.second));
      if (!per_shard_request_table.contains(shard)) {
        per_shard_request_table.insert(std::pair(shard, top_level_rqst_template));
      }
      per_shard_request_table[shard].src_vertices.push_back(vertex);
    }

    for (auto &[shard, request] : per_shard_request_table) {
      ShardRequestState<msgs::ExpandOneRequest> shard_request_state{
          .shard = shard,
          .request = request,
          .async_request_token = std::nullopt,
      };

      state.requests.emplace_back(std::move(shard_request_state));
    }
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

  template <typename RequestT, typename ResponseT>
  void DriveReadResponses(ExecutionState<RequestT> &state, std::vector<ResponseT> &responses) {
    for (auto &request : state.requests) {
      auto &storage_client = GetStorageClientForShard(request.shard);

      auto poll_result = storage_client.AwaitAsyncReadRequest(request.async_request_token.value());
      while (!poll_result) {
        poll_result = storage_client.AwaitAsyncReadRequest(request.async_request_token.value());
      }

      if (poll_result->HasError()) {
        throw std::runtime_error("RequestRouter Read request timed out");
      }

      msgs::ReadResponses response_variant = poll_result->GetValue();
      auto response = std::get<ResponseT>(response_variant);
      if (response.error) {
        throw std::runtime_error("RequestRouter Read request did not succeed");
      }

      responses.push_back(std::move(response));
    }
    state.requests.clear();
  }

  template <typename RequestT, typename ResponseT>
  void DriveWriteResponses(ExecutionState<RequestT> &state, std::vector<ResponseT> &responses) {
    for (auto &request : state.requests) {
      auto &storage_client = GetStorageClientForShard(request.shard);

      auto poll_result = storage_client.AwaitAsyncWriteRequest(request.async_request_token.value());
      while (!poll_result) {
        poll_result = storage_client.AwaitAsyncWriteRequest(request.async_request_token.value());
      }

      if (poll_result->HasError()) {
        throw std::runtime_error("RequestRouter Write request timed out");
      }

      msgs::WriteResponses response_variant = poll_result->GetValue();
      auto response = std::get<ResponseT>(response_variant);
      if (response.error) {
        throw std::runtime_error("RequestRouter Write request did not succeed");
      }

      responses.push_back(std::move(response));
    }
    state.requests.clear();
  }

  void SetUpNameIdMappers() {
    std::unordered_map<uint64_t, std::string> id_to_name;
    for (const auto &[name, id] : shards_map_.labels) {
      id_to_name.emplace(id.AsUint(), name);
    }
    labels_.StoreMapping(std::move(id_to_name));
    id_to_name.clear();
    for (const auto &[name, id] : shards_map_.properties) {
      id_to_name.emplace(id.AsUint(), name);
    }
    properties_.StoreMapping(std::move(id_to_name));
    id_to_name.clear();
    for (const auto &[name, id] : shards_map_.edge_types) {
      id_to_name.emplace(id.AsUint(), name);
    }
    edge_types_.StoreMapping(std::move(id_to_name));
  }

  ShardMap shards_map_;
  storage::v3::NameIdMapper properties_;
  storage::v3::NameIdMapper edge_types_;
  storage::v3::NameIdMapper labels_;
  CoordinatorClient coord_cli_;
  RsmStorageClientManager<StorageClient> storage_cli_manager_;
  io::Io<TTransport> io_;
  coordinator::Hlc transaction_id_;
  // TODO(kostasrim) Add batch prefetching
};
}  // namespace memgraph::query::v2
