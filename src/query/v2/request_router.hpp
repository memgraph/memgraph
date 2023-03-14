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

#pragma once

#include <algorithm>
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
#include <variant>
#include <vector>

#include <boost/uuid/uuid.hpp>

#include "coordinator/coordinator.hpp"
#include "coordinator/coordinator_client.hpp"
#include "coordinator/coordinator_rsm.hpp"
#include "coordinator/shard_map.hpp"
#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/local_transport/local_transport.hpp"
#include "io/notifier.hpp"
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
  using ShardMetadata = coordinator::ShardMetadata;
  RsmStorageClientManager() = default;
  RsmStorageClientManager(const RsmStorageClientManager &) = delete;
  RsmStorageClientManager(RsmStorageClientManager &&) = delete;
  RsmStorageClientManager &operator=(const RsmStorageClientManager &) = delete;
  RsmStorageClientManager &operator=(RsmStorageClientManager &&) = delete;
  ~RsmStorageClientManager() = default;

  void AddClient(ShardMetadata key, TStorageClient client) { cli_cache_.emplace(std::move(key), std::move(client)); }

  bool Exists(const ShardMetadata &key) { return cli_cache_.contains(key); }

  void PurgeCache() { cli_cache_.clear(); }

  TStorageClient &GetClient(const ShardMetadata &key) {
    auto it = cli_cache_.find(key);
    MG_ASSERT(it != cli_cache_.end(), "Non-existing shard client");
    return it->second;
  }

 private:
  std::map<ShardMetadata, TStorageClient> cli_cache_;
};

template <typename TRequest>
struct ShardRequestState {
  memgraph::coordinator::ShardMetadata shard;
  TRequest request;
};

// maps from ReadinessToken's internal size_t to the associated state
template <typename TRequest>
using RunningRequests = std::unordered_map<size_t, ShardRequestState<TRequest>>;

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
  virtual std::vector<msgs::GetPropertiesResultRow> GetProperties(msgs::GetPropertiesRequest request) = 0;
  virtual std::vector<msgs::GraphResponse> GetGraph(msgs::GraphRequest req) = 0;

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
  virtual bool IsPrimaryProperty(storage::v3::LabelId primary_label, storage::v3::PropertyId property) const = 0;

  virtual std::optional<std::pair<uint64_t, uint64_t>> AllocateInitialEdgeIds(io::Address coordinator_address) = 0;
  virtual void InstallSimulatorTicker(std::function<bool()> tick_simulator) = 0;
  virtual const std::vector<coordinator::SchemaProperty> &GetSchemaForLabel(storage::v3::LabelId label) const = 0;
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
  using ShardMetadata = coordinator::ShardMetadata;
  using ShardMap = coordinator::ShardMap;
  using CompoundKey = coordinator::PrimaryKey;
  using VertexAccessor = query::v2::accessors::VertexAccessor;
  RequestRouter(CoordinatorClient coord, io::Io<TTransport> &&io) : coord_cli_(std::move(coord)), io_(std::move(io)) {}

  RequestRouter(const RequestRouter &) = delete;
  RequestRouter(RequestRouter &&) = delete;
  RequestRouter &operator=(const RequestRouter &) = delete;
  RequestRouter &operator=(RequestRouter &&) = delete;

  ~RequestRouter() override {}

  void InstallSimulatorTicker(std::function<bool()> tick_simulator) override {
    notifier_.InstallSimulatorTicker(tick_simulator);
  }

  void StartTransaction() override {
    coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    CoordinatorWriteRequests write_req = req;
    spdlog::trace("sending hlc request to start transaction");
    auto write_res = coord_cli_.SendWriteRequest(write_req);
    spdlog::trace("received hlc response to start transaction");
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
    spdlog::trace("sending hlc request before committing transaction");
    auto write_res = coord_cli_.SendWriteRequest(write_req);
    spdlog::trace("received hlc response before committing transaction");
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

  bool IsPrimaryProperty(storage::v3::LabelId primary_label, storage::v3::PropertyId property) const override {
    const auto schema_it = shards_map_.schemas.find(primary_label);
    MG_ASSERT(schema_it != shards_map_.schemas.end(), "Invalid primary label id: {}", primary_label.AsUint());

    return std::find_if(schema_it->second.begin(), schema_it->second.end(), [property](const auto &schema_prop) {
             return schema_prop.property_id == property;
           }) != schema_it->second.end();
  }

  const std::vector<coordinator::SchemaProperty> &GetSchemaForLabel(storage::v3::LabelId label) const override {
    return shards_map_.schemas.at(label);
  }

  bool IsPrimaryLabel(storage::v3::LabelId label) const override { return shards_map_.label_spaces.contains(label); }

  // TODO(kostasrim) Simplify return result
  std::vector<VertexAccessor> ScanVertices(std::optional<std::string> label) override {
    // create requests
    auto requests_to_be_sent = RequestsForScanVertices(label);

    spdlog::trace("created {} ScanVertices requests", requests_to_be_sent.size());

    // begin all requests in parallel
    RunningRequests<msgs::ScanVerticesRequest> running_requests = {};
    running_requests.reserve(requests_to_be_sent.size());
    for (size_t i = 0; i < requests_to_be_sent.size(); i++) {
      auto &request = requests_to_be_sent[i];
      io::ReadinessToken readiness_token{i};
      auto &storage_client = GetStorageClientForShard(request.shard);
      storage_client.SendAsyncReadRequest(request.request, notifier_, readiness_token);
      running_requests.emplace(readiness_token.GetId(), request);
    }
    spdlog::trace("sent {} ScanVertices requests in parallel", running_requests.size());

    // drive requests to completion
    auto responses = DriveReadResponses<msgs::ScanVerticesRequest, msgs::ScanVerticesResponse>(running_requests);
    spdlog::trace("got back {} ScanVertices responses after driving to completion", responses.size());

    // convert responses into VertexAccessor objects to return
    std::vector<VertexAccessor> accessors;
    accessors.reserve(responses.size());
    for (auto &response : responses) {
      for (auto &result_row : response.results) {
        accessors.emplace_back(VertexAccessor(std::move(result_row.vertex), std::move(result_row.props), this));
      }
    }

    return accessors;
  }

  std::vector<msgs::CreateVerticesResponse> CreateVertices(std::vector<msgs::NewVertex> new_vertices) override {
    MG_ASSERT(!new_vertices.empty());

    // create requests
    std::vector<ShardRequestState<msgs::CreateVerticesRequest>> requests_to_be_sent =
        RequestsForCreateVertices(new_vertices);
    spdlog::trace("created {} CreateVertices requests", requests_to_be_sent.size());

    // begin all requests in parallel
    RunningRequests<msgs::CreateVerticesRequest> running_requests = {};
    running_requests.reserve(requests_to_be_sent.size());
    for (size_t i = 0; i < requests_to_be_sent.size(); i++) {
      auto &request = requests_to_be_sent[i];
      io::ReadinessToken readiness_token{i};
      for (auto &new_vertex : request.request.new_vertices) {
        new_vertex.label_ids.erase(new_vertex.label_ids.begin());
      }
      auto &storage_client = GetStorageClientForShard(request.shard);
      storage_client.SendAsyncWriteRequest(request.request, notifier_, readiness_token);
      running_requests.emplace(readiness_token.GetId(), request);
    }
    spdlog::trace("sent {} CreateVertices requests in parallel", running_requests.size());

    // drive requests to completion
    return DriveWriteResponses<msgs::CreateVerticesRequest, msgs::CreateVerticesResponse>(running_requests);
  }

  std::vector<msgs::CreateExpandResponse> CreateExpand(std::vector<msgs::NewExpand> new_edges) override {
    MG_ASSERT(!new_edges.empty());

    // create requests
    std::vector<ShardRequestState<msgs::CreateExpandRequest>> requests_to_be_sent =
        RequestsForCreateExpand(std::move(new_edges));

    // begin all requests in parallel
    RunningRequests<msgs::CreateExpandRequest> running_requests = {};
    running_requests.reserve(requests_to_be_sent.size());
    for (size_t i = 0; i < requests_to_be_sent.size(); i++) {
      auto &request = requests_to_be_sent[i];
      io::ReadinessToken readiness_token{i};
      auto &storage_client = GetStorageClientForShard(request.shard);
      msgs::WriteRequests req = request.request;
      storage_client.SendAsyncWriteRequest(req, notifier_, readiness_token);
      running_requests.emplace(readiness_token.GetId(), request);
    }

    // drive requests to completion
    return DriveWriteResponses<msgs::CreateExpandRequest, msgs::CreateExpandResponse>(running_requests);
  }

  std::vector<msgs::ExpandOneResultRow> ExpandOne(msgs::ExpandOneRequest request) override {
    // TODO(kostasrim)Update to limit the batch size here
    // Expansions of the destination must be handled by the caller. For example
    // match (u:L1 { prop : 1 })-[:Friend]-(v:L1)
    // For each vertex U, the ExpandOne will result in <U, Edges>. The destination vertex and its properties
    // must be fetched again with an ExpandOne(Edges.dst)

    // create requests
    std::vector<ShardRequestState<msgs::ExpandOneRequest>> requests_to_be_sent = RequestsForExpandOne(request);

    // begin all requests in parallel
    RunningRequests<msgs::ExpandOneRequest> running_requests = {};
    running_requests.reserve(requests_to_be_sent.size());
    for (size_t i = 0; i < requests_to_be_sent.size(); i++) {
      auto &request = requests_to_be_sent[i];
      io::ReadinessToken readiness_token{i};
      auto &storage_client = GetStorageClientForShard(request.shard);
      msgs::ReadRequests req = request.request;
      storage_client.SendAsyncReadRequest(req, notifier_, readiness_token);
      running_requests.emplace(readiness_token.GetId(), request);
    }

    // drive requests to completion
    auto responses = DriveReadResponses<msgs::ExpandOneRequest, msgs::ExpandOneResponse>(running_requests);

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

  std::vector<msgs::GetPropertiesResultRow> GetProperties(msgs::GetPropertiesRequest requests) override {
    requests.transaction_id = transaction_id_;
    // create requests
    std::vector<ShardRequestState<msgs::GetPropertiesRequest>> requests_to_be_sent =
        RequestsForGetProperties(std::move(requests));

    // begin all requests in parallel
    RunningRequests<msgs::GetPropertiesRequest> running_requests = {};
    running_requests.reserve(requests_to_be_sent.size());
    for (size_t i = 0; i < requests_to_be_sent.size(); i++) {
      auto &request = requests_to_be_sent[i];
      io::ReadinessToken readiness_token{i};
      auto &storage_client = GetStorageClientForShard(request.shard);
      msgs::ReadRequests req = request.request;
      storage_client.SendAsyncReadRequest(req, notifier_, readiness_token);
      running_requests.emplace(readiness_token.GetId(), request);
    }

    // drive requests to completion
    auto responses = DriveReadResponses<msgs::GetPropertiesRequest, msgs::GetPropertiesResponse>(running_requests);

    // post-process responses
    std::vector<msgs::GetPropertiesResultRow> result_rows;

    for (auto &&response : responses) {
      std::move(response.result_row.begin(), response.result_row.end(), std::back_inserter(result_rows));
    }

    return result_rows;
  }

  std::vector<msgs::GraphResponse> GetGraph(msgs::GraphRequest req) override {
    SPDLOG_WARN("RequestRouter::GetGraph(GraphRequest) not fully implemented");

    // InitializeRequests
    // TODO(gitbuda): This seems quite expensive because potentially a lot of requests has to be initialized.
    auto multi_shards = shards_map_.GetAllShards();
    std::vector<ShardRequestState<msgs::GraphRequest>> requests = {};
    for (auto &shards : multi_shards) {
      for (auto &[key, shard] : shards) {
        MG_ASSERT(!shard.peers.empty());
        msgs::GraphRequest request;
        request.transaction_id = transaction_id_;
        ShardRequestState<msgs::GraphRequest> shard_request_state{
            .shard = shard,
            .request = std::move(request),
        };
        requests.emplace_back(std::move(shard_request_state));
      }
    }
    spdlog::trace("created {} Graph requests", requests.size());

    // SendRequests and CollectResponses
    RunningRequests<msgs::GraphRequest> running_requests = {};
    running_requests.reserve(requests.size());
    for (size_t i = 0; i < requests.size(); i++) {
      auto &request = requests[i];
      io::ReadinessToken readiness_token{i};
      auto &storage_client = GetStorageClientForShard(request.shard);
      storage_client.SendAsyncReadRequest(request.request, notifier_, readiness_token);
      running_requests.emplace(readiness_token.GetId(), request);
    }
    spdlog::trace("sent {} Graph requests in parallel", running_requests.size());
    auto responses = DriveReadResponses<msgs::GraphRequest, msgs::GraphResponse>(running_requests);
    spdlog::trace("got back {} Graph responses after driving to completion", responses.size());

    return responses;
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
  std::vector<ShardRequestState<msgs::CreateVerticesRequest>> RequestsForCreateVertices(
      const std::vector<msgs::NewVertex> &new_vertices) {
    std::map<ShardMetadata, msgs::CreateVerticesRequest> per_shard_request_table;

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

    std::vector<ShardRequestState<msgs::CreateVerticesRequest>> requests = {};

    for (auto &[shard, request] : per_shard_request_table) {
      ShardRequestState<msgs::CreateVerticesRequest> shard_request_state{
          .shard = shard,
          .request = request,
      };
      requests.emplace_back(std::move(shard_request_state));
    }

    return requests;
  }

  std::vector<ShardRequestState<msgs::CreateExpandRequest>> RequestsForCreateExpand(
      std::vector<msgs::NewExpand> new_expands) {
    std::map<ShardMetadata, msgs::CreateExpandRequest> per_shard_request_table;
    auto ensure_shard_exists_in_table = [&per_shard_request_table,
                                         transaction_id = transaction_id_](const ShardMetadata &shard) {
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

    std::vector<ShardRequestState<msgs::CreateExpandRequest>> requests = {};

    for (auto &[shard, request] : per_shard_request_table) {
      ShardRequestState<msgs::CreateExpandRequest> shard_request_state{
          .shard = shard,
          .request = request,
      };
      requests.emplace_back(std::move(shard_request_state));
    }

    return requests;
  }

  std::vector<ShardRequestState<msgs::ScanVerticesRequest>> RequestsForScanVertices(
      const std::optional<std::string> &label) {
    std::vector<coordinator::Shards> multi_shards;
    if (label) {
      const auto label_id = shards_map_.GetLabelId(*label);
      MG_ASSERT(label_id);
      MG_ASSERT(IsPrimaryLabel(*label_id));
      multi_shards = {shards_map_.GetShardsForLabel(*label)};
    } else {
      multi_shards = shards_map_.GetAllShards();
    }

    std::vector<ShardRequestState<msgs::ScanVerticesRequest>> requests = {};

    for (auto &shards : multi_shards) {
      for (auto &[key, shard] : shards) {
        MG_ASSERT(!shard.peers.empty());

        msgs::ScanVerticesRequest request;
        request.transaction_id = transaction_id_;
        request.start_id.second = storage::conversions::ConvertValueVector(key);

        ShardRequestState<msgs::ScanVerticesRequest> shard_request_state{
            .shard = shard,
            .request = std::move(request),
        };

        requests.emplace_back(std::move(shard_request_state));
      }
    }

    return requests;
  }

  std::vector<ShardRequestState<msgs::ExpandOneRequest>> RequestsForExpandOne(const msgs::ExpandOneRequest &request) {
    std::map<ShardMetadata, msgs::ExpandOneRequest> per_shard_request_table;
    msgs::ExpandOneRequest top_level_rqst_template = request;
    top_level_rqst_template.transaction_id = transaction_id_;
    top_level_rqst_template.src_vertices.clear();

    for (auto &vertex : request.src_vertices) {
      auto shard =
          shards_map_.GetShardForKey(vertex.first.id, storage::conversions::ConvertPropertyVector(vertex.second));
      if (!per_shard_request_table.contains(shard)) {
        per_shard_request_table.insert(std::pair(shard, top_level_rqst_template));
      }
      per_shard_request_table[shard].src_vertices.push_back(vertex);
    }

    std::vector<ShardRequestState<msgs::ExpandOneRequest>> requests = {};

    for (auto &[shard, request] : per_shard_request_table) {
      ShardRequestState<msgs::ExpandOneRequest> shard_request_state{
          .shard = shard,
          .request = request,
      };

      requests.emplace_back(std::move(shard_request_state));
    }

    return requests;
  }

  std::vector<ShardRequestState<msgs::GetPropertiesRequest>> RequestsForGetProperties(
      msgs::GetPropertiesRequest &&request) {
    std::map<ShardMetadata, msgs::GetPropertiesRequest> per_shard_request_table;
    auto top_level_rqst_template = request;
    top_level_rqst_template.transaction_id = transaction_id_;
    top_level_rqst_template.vertex_ids.clear();
    top_level_rqst_template.vertices_and_edges.clear();

    for (auto &&vertex : request.vertex_ids) {
      auto shard =
          shards_map_.GetShardForKey(vertex.first.id, storage::conversions::ConvertPropertyVector(vertex.second));
      if (!per_shard_request_table.contains(shard)) {
        per_shard_request_table.insert(std::pair(shard, top_level_rqst_template));
      }
      per_shard_request_table[shard].vertex_ids.emplace_back(std::move(vertex));
    }

    for (auto &[vertex, maybe_edge] : request.vertices_and_edges) {
      auto shard =
          shards_map_.GetShardForKey(vertex.first.id, storage::conversions::ConvertPropertyVector(vertex.second));
      if (!per_shard_request_table.contains(shard)) {
        per_shard_request_table.insert(std::pair(shard, top_level_rqst_template));
      }
      per_shard_request_table[shard].vertices_and_edges.emplace_back(std::move(vertex), maybe_edge);
    }

    std::vector<ShardRequestState<msgs::GetPropertiesRequest>> requests;

    for (auto &[shard, rqst] : per_shard_request_table) {
      ShardRequestState<msgs::GetPropertiesRequest> shard_request_state{
          .shard = shard,
          .request = std::move(rqst),
      };

      requests.emplace_back(std::move(shard_request_state));
    }

    return requests;
  }

  StorageClient &GetStorageClientForShard(ShardMetadata shard) {
    if (!storage_cli_manager_.Exists(shard)) {
      AddStorageClientToManager(shard);
    }
    return storage_cli_manager_.GetClient(shard);
  }

  StorageClient &GetStorageClientForShard(const std::string &label, const CompoundKey &key) {
    auto shard = shards_map_.GetShardForKey(label, key);
    return GetStorageClientForShard(std::move(shard));
  }

  void AddStorageClientToManager(ShardMetadata target_shard) {
    MG_ASSERT(!target_shard.peers.empty());
    auto leader_addr = target_shard.peers.front();
    std::vector<Address> addresses;
    addresses.reserve(target_shard.peers.size());
    for (auto &address : target_shard.peers) {
      addresses.push_back(std::move(address.address));
    }
    auto cli = StorageClient(io_, std::move(leader_addr.address), std::move(addresses));
    storage_cli_manager_.AddClient(target_shard, std::move(cli));
  }

  template <typename RequestT, typename ResponseT>
  std::vector<ResponseT> DriveReadResponses(RunningRequests<RequestT> &running_requests) {
    // Store responses in a map based on the corresponding request
    // offset, so that they can be reassembled in the correct order
    // even if they came back in randomized orders.
    std::map<size_t, ResponseT> response_map;

    spdlog::trace("waiting on readiness for token");
    while (response_map.size() < running_requests.size()) {
      auto ready = notifier_.Await();
      spdlog::trace("got readiness for token {}", ready.GetId());
      auto &request = running_requests.at(ready.GetId());
      auto &storage_client = GetStorageClientForShard(request.shard);

      std::optional<utils::BasicResult<io::TimedOut, msgs::ReadResponses>> poll_result =
          storage_client.PollAsyncReadRequest(ready);

      if (!poll_result.has_value()) {
        continue;
      }

      if (poll_result->HasError()) {
        throw std::runtime_error("RequestRouter Read request timed out");
      }

      msgs::ReadResponses response_variant = poll_result->GetValue();
      auto response = std::get<ResponseT>(response_variant);
      if (response.error) {
        throw std::runtime_error("RequestRouter Read request did not succeed");
      }

      // the readiness token has an ID based on the request vector offset
      response_map.emplace(ready.GetId(), std::move(response));
    }

    std::vector<ResponseT> responses;
    responses.reserve(running_requests.size());

    int last = -1;
    for (auto &&[offset, response] : response_map) {
      MG_ASSERT(last + 1 == offset);
      responses.emplace_back(std::forward<ResponseT>(response));
      last = offset;
    }

    return responses;
  }

  template <typename RequestT, typename ResponseT>
  std::vector<ResponseT> DriveWriteResponses(RunningRequests<RequestT> &running_requests) {
    // Store responses in a map based on the corresponding request
    // offset, so that they can be reassembled in the correct order
    // even if they came back in randomized orders.
    std::map<size_t, ResponseT> response_map;

    while (response_map.size() < running_requests.size()) {
      auto ready = notifier_.Await();
      auto &request = running_requests.at(ready.GetId());
      auto &storage_client = GetStorageClientForShard(request.shard);

      std::optional<utils::BasicResult<io::TimedOut, msgs::WriteResponses>> poll_result =
          storage_client.PollAsyncWriteRequest(ready);

      if (!poll_result.has_value()) {
        continue;
      }

      if (poll_result->HasError()) {
        throw std::runtime_error("RequestRouter Write request timed out");
      }

      msgs::WriteResponses response_variant = poll_result->GetValue();
      auto response = std::get<ResponseT>(response_variant);
      if (response.error) {
        throw std::runtime_error("RequestRouter Write request did not succeed");
      }

      // the readiness token has an ID based on the request vector offset
      response_map.emplace(ready.GetId(), std::move(response));
    }

    std::vector<ResponseT> responses;
    responses.reserve(running_requests.size());

    int last = -1;
    for (auto &&[offset, response] : response_map) {
      MG_ASSERT(last + 1 == offset);
      responses.emplace_back(std::forward<ResponseT>(response));
      last = offset;
    }

    return responses;
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

  std::optional<std::pair<uint64_t, uint64_t>> AllocateInitialEdgeIds(io::Address coordinator_address) override {
    coordinator::CoordinatorWriteRequests requests{coordinator::AllocateEdgeIdBatchRequest{.batch_size = 1000000}};

    io::rsm::WriteRequest<coordinator::CoordinatorWriteRequests> ww;
    ww.operation = requests;
    auto resp =
        io_.template Request<io::rsm::WriteRequest<coordinator::CoordinatorWriteRequests>,
                             io::rsm::WriteResponse<coordinator::CoordinatorWriteResponses>>(coordinator_address, ww)
            .Wait();
    if (resp.HasValue()) {
      const auto alloc_edge_id_reps =
          std::get<coordinator::AllocateEdgeIdBatchResponse>(resp.GetValue().message.write_return);
      return std::make_pair(alloc_edge_id_reps.low, alloc_edge_id_reps.high);
    }
    return {};
  }

  ShardMap shards_map_;
  storage::v3::NameIdMapper properties_;
  storage::v3::NameIdMapper edge_types_;
  storage::v3::NameIdMapper labels_;
  CoordinatorClient coord_cli_;
  RsmStorageClientManager<StorageClient> storage_cli_manager_;
  io::Io<TTransport> io_;
  coordinator::Hlc transaction_id_;
  io::Notifier notifier_ = {};
  // TODO(kostasrim) Add batch prefetching
};

class RequestRouterFactory {
 public:
  RequestRouterFactory() = default;
  RequestRouterFactory(const RequestRouterFactory &) = delete;
  RequestRouterFactory &operator=(const RequestRouterFactory &) = delete;
  RequestRouterFactory(RequestRouterFactory &&) = delete;
  RequestRouterFactory &operator=(RequestRouterFactory &&) = delete;

  virtual ~RequestRouterFactory() = default;

  virtual std::unique_ptr<RequestRouterInterface> CreateRequestRouter(
      const coordinator::Address &coordinator_address) const = 0;
};

class LocalRequestRouterFactory : public RequestRouterFactory {
  using LocalTransportIo = io::Io<io::local_transport::LocalTransport>;
  LocalTransportIo &io_;

 public:
  explicit LocalRequestRouterFactory(LocalTransportIo &io) : io_(io) {}

  std::unique_ptr<RequestRouterInterface> CreateRequestRouter(
      const coordinator::Address &coordinator_address) const override {
    using TransportType = io::local_transport::LocalTransport;

    auto query_io = io_.ForkLocal(boost::uuids::uuid{boost::uuids::random_generator()()});
    auto local_transport_io = io_.ForkLocal(boost::uuids::uuid{boost::uuids::random_generator()()});

    return std::make_unique<RequestRouter<TransportType>>(
        coordinator::CoordinatorClient<TransportType>(query_io, coordinator_address, {coordinator_address}),
        std::move(local_transport_io));
  }
};

class SimulatedRequestRouterFactory : public RequestRouterFactory {
  io::simulator::Simulator *simulator_;

 public:
  explicit SimulatedRequestRouterFactory(io::simulator::Simulator &simulator) : simulator_(&simulator) {}

  std::unique_ptr<RequestRouterInterface> CreateRequestRouter(
      const coordinator::Address &coordinator_address) const override {
    using TransportType = io::simulator::SimulatorTransport;
    auto actual_transport_handle = simulator_->GetSimulatorHandle();

    boost::uuids::uuid random_uuid;
    io::Address unique_local_addr_query;

    // The simulated RR should not introduce stochastic behavior.
    random_uuid = boost::uuids::uuid{3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    unique_local_addr_query = {.unique_id = boost::uuids::uuid{4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}};

    auto io = simulator_->Register(unique_local_addr_query);
    auto query_io = io.ForkLocal(random_uuid);

    return std::make_unique<RequestRouter<TransportType>>(
        coordinator::CoordinatorClient<TransportType>(query_io, coordinator_address, {coordinator_address}),
        std::move(io));
  }
};

}  // namespace memgraph::query::v2
