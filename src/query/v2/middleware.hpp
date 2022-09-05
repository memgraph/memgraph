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
  RsmStorageClientManager() = default;
  RsmStorageClientManager(const RsmStorageClientManager &) = delete;
  RsmStorageClientManager(RsmStorageClientManager &&) = delete;

  void AddClient(const std::string &label, CompoundKey key, TStorageClient client) {
    cli_cache_[label].insert({std::move(key), std::move(client)});
  }

  bool Exists(const std::string &label, const CompoundKey &key) { return cli_cache_[label].contains(key); }

  void PurgeCache() { cli_cache_.clear(); }
  TStorageClient &GetClient(const std::string &label, const CompoundKey &key) {
    return cli_cache_[label].find(key)->second;
  }

 private:
  std::unordered_map<std::string, std::map<CompoundKey, TStorageClient>> cli_cache_;
};

template <typename TRequest>
struct ExecutionState {
  using CompoundKey = memgraph::io::rsm::ShardRsmKey;
  using Shard = memgraph::coordinator::Shard;
  const std::string label;
  // CompoundKey is optional because some operators require to iterate over all the available keys
  // of a shard. One example is ScanAll, where we only require the field label.
  std::optional<CompoundKey> key;
  // Transaction id to be filled by the ShardRequestManager implementation
  memgraph::coordinator::Hlc transaction_id;
  // Initialized by ShardRequestManager implementation. This vector is filled with the shards that
  // the ShardRequestManager impl will send requests to. When a request to a shard exhausts it, meaning that
  // it pulled all the requested data from the given Shard, it will be removed from the Vector. When the Vector becomes
  // empty, it means that all of the requests have completed succefully.
  std::optional<std::vector<Shard>> state_;
  // 1-1 mapping with `state_`.
  // A vector that tracks request metatdata for each shard (For example, next_id for a ScanAll on Shard A)
  std::vector<TRequest> requests;
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

  std::vector<ScanVerticesResponse> Request(ExecutionState<ScanVerticesRequest> &state) override {
    MaybeInitializeExecutionState(state);
    std::vector<ScanVerticesResponse> responses;
    auto &state_ref = *state.state_;
    size_t id = 0;
    for (auto shard_it = state_ref.begin(); shard_it != state_ref.end(); ++id) {
      auto &storage_client = GetStorageClientForShard(state.label, state.requests[id].start_id.second);
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
        shard_it = state_ref.erase(shard_it);
      } else {
        state.requests[id].start_id.second = read_response_result.GetValue().next_start_id->second;
        ++shard_it;
      }
    }
    // TODO(kostasrim) Before returning start prefetching the batch (this shall be done once we get MgFuture as return
    // result of storage_client.SendReadRequest()).
    return responses;
  }

  std::vector<CreateVerticesResponse> Request(ExecutionState<CreateVerticesRequest> &state) {}

  //  size_t TestRequest(ExecutionState &state) {
  //    MaybeUpdateShardMap(state);
  //    MaybeUpdateExecutionState(state);
  //    for (auto &st : *state.state_) {
  //      auto &storage_client = GetStorageClientForShard(state.label, *state.key);
  //
  //      memgraph::storage::v3::LabelId label_id = shards_map_.labels.at(state.label);
  //
  //      rsm::StorageWriteRequest storage_req;
  //      storage_req.label_id = label_id;
  //      storage_req.transaction_id = state.transaction_id;
  //      storage_req.key = *state.key;
  //      storage_req.value = 469;
  //      auto write_response_result = storage_client.SendWriteRequest(storage_req);
  //      if (write_response_result.HasError()) {
  //        throw std::runtime_error("Handle gracefully!");
  //      }
  //      auto write_response = write_response_result.GetValue();
  //
  //      bool cas_succeeded = write_response.shard_rsm_success;
  //
  //      if (!cas_succeeded) {
  //        throw std::runtime_error("Handler gracefully!");
  //      }
  //      rsm::StorageReadRequest storage_get_req;
  //      storage_get_req.key = *state.key;
  //
  //      auto get_response_result = storage_client.SendReadRequest(storage_get_req);
  //      if (get_response_result.HasError()) {
  //        throw std::runtime_error("Handler gracefully!");
  //      }
  //      auto get_response = get_response_result.GetValue();
  //      auto val = get_response.value.value();
  //      return val;
  //    }
  //    return 0;
  //  }

 private:
  template <typename TRequest>
  void MaybeUpdateShardMap(TRequest &state) {
    memgraph::coordinator::HlcRequest req{.last_shard_map_version = shards_map_.GetHlc()};
    auto read_res = coord_cli_.SendReadRequest(req);
    if (read_res.HasError()) {
      throw std::runtime_error("HLC request failed");
    }
    auto coordinator_read_response = read_res.GetValue();
    auto hlc_response = std::get<memgraph::coordinator::HlcResponse>(coordinator_read_response);
    if (hlc_response.fresher_shard_map) {
      // throw std::runtime_error("Shouldn'");
      //  error here new shard map shouldn't exist
    }

    // Transaction ID to be used later...
    state.transaction_id = hlc_response.new_hlc;

    if (hlc_response.fresher_shard_map) {
      shards_map_ = hlc_response.fresher_shard_map.value();
    } else {
      throw std::runtime_error("Should handle gracefully!");
    }
  }

  template <typename TRequest>
  void MaybeUpdateExecutionState(TRequest &state) {
    if (state.state_) {
      return;
    }
    state.transaction_id = transaction_id_;
    state.state_ = std::make_optional<std::vector<Shard>>();
    const auto &shards = shards_map_.shards[shards_map_.labels[state.label]];
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

  void MaybeInitializeExecutionState(ExecutionState<ScanVerticesRequest> &state) {
    if (state.state_) {
      return;
    }
    state.transaction_id = transaction_id_;
    state.state_ = std::make_optional<std::vector<Shard>>();
    const auto &shards = shards_map_.shards[shards_map_.labels[state.label]];
    for (const auto &[key, shard] : shards) {
      state.state_->push_back(shard);
      ScanVerticesRequest rqst;
      rqst.transaction_id = transaction_id_;
      rqst.start_id.second = key;
      state.requests.push_back(std::move(rqst));
    }
  }

  //  std::vector<storageclient> GetStorageClientFromShardforRange(const std::string &label, const CompoundKey &start,
  //                                                               const CompoundKey &end);
  StorageClient &GetStorageClientForShard(const std::string &label, const CompoundKey &key) {
    if (storage_cli_manager_.Exists(label, key)) {
      return storage_cli_manager_.GetClient(label, key);
    }
    auto target_shard = shards_map_.GetShardForKey(label, key);
    AddStorageClientToManager(std::move(target_shard), label, key);
    return storage_cli_manager_.GetClient(label, key);
  }

  void AddStorageClientToManager(Shard target_shard, const std::string &label, const CompoundKey &cm_k) {
    MG_ASSERT(!target_shard.empty());
    auto leader_addr = target_shard.front();
    std::vector<Address> addresses;
    addresses.reserve(target_shard.size());
    for (auto &address : target_shard) {
      addresses.push_back(std::move(address.address));
    }
    auto cli = StorageClient(io_, std::move(leader_addr.address), std::move(addresses));
    storage_cli_manager_.AddClient(label, cm_k, std::move(cli));
  }

  ShardMap shards_map_;
  CoordinatorClient coord_cli_;
  RsmStorageClientManager<StorageClient> storage_cli_manager_;
  memgraph::io::Io<TTransport> io_;
  memgraph::coordinator::Hlc transaction_id_;
  // TODO(kostasrim) Add batch prefetching
};
