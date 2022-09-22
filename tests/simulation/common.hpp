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

/// The ShardRsm is a simple in-memory raft-backed kv store that can be used for simple testing
/// and implementation of some query engine logic before storage engines are fully implemented.
///
/// To implement multiple read and write commands, change the StorageRead* and StorageWrite* requests
/// and responses to a std::variant of the different options, and route them to specific handlers in
/// the ShardRsm's Read and Apply methods. Remember that Read is called immediately when the Raft
/// leader receives the request, and does not replicate anything over Raft. Apply is called only
/// AFTER the StorageWriteRequest is replicated to a majority of Raft peers, and the result of calling
/// ShardRsm::Apply(StorageWriteRequest) is returned to the client that submitted the request.

#include <algorithm>
#include <deque>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include <iostream>
#include "coordinator/hybrid_logical_clock.hpp"
#include "io/address.hpp"
#include "io/rsm/raft.hpp"
#include "io/rsm/shard_rsm.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/value_conversions.hpp"
#include "utils/logging.hpp"

using memgraph::coordinator::Hlc;
using memgraph::io::rsm::StorageWriteRequest;
using memgraph::io::rsm::StorageWriteResponse;
using memgraph::io::simulator::Simulator;
using memgraph::io::simulator::SimulatorConfig;
using memgraph::io::simulator::SimulatorStats;
using memgraph::io::simulator::SimulatorTransport;
using memgraph::msgs::CreateVerticesRequest;
using memgraph::msgs::CreateVerticesResponse;
using memgraph::msgs::ExpandOneRequest;
using memgraph::msgs::ExpandOneResponse;
using memgraph::msgs::ListedValues;
using memgraph::msgs::ScanVerticesRequest;
using memgraph::msgs::ScanVerticesResponse;
using memgraph::msgs::Value;
using memgraph::msgs::VertexId;
using memgraph::storage::v3::LabelId;
using memgraph::storage::v3::PropertyValue;

using ShardRsmKey = std::vector<memgraph::storage::v3::PropertyValue>;

class MockedShardRsm {
  std::map<ShardRsmKey, int> state_;
  ShardRsmKey minimum_key_;
  std::optional<ShardRsmKey> maximum_key_{std::nullopt};
  Hlc shard_map_version_;

  // The key is not located in this shard
  bool IsKeyInRange(const ShardRsmKey &key) {
    if (maximum_key_) [[likely]] {
      return (key >= minimum_key_ && key <= maximum_key_);
    }
    return key >= minimum_key_;
  }

 public:
  //  ExpandOneResponse Read(ExpandOneRequest rqst);
  //  GetPropertiesResponse Read(GetPropertiesRequest rqst);
  ScanVerticesResponse ReadImpl(ScanVerticesRequest rqst) {
    ScanVerticesResponse ret;
    auto as_prop_val = memgraph::storage::conversions::ConvertPropertyVector(rqst.start_id.second);
    if (!IsKeyInRange(as_prop_val)) {
      ret.success = false;
    } else if (as_prop_val == ShardRsmKey{PropertyValue(0), PropertyValue(0)}) {
      Value val(int64_t(0));
      ret.next_start_id = std::make_optional<VertexId>();
      ret.next_start_id->second =
          memgraph::storage::conversions::ConvertValueVector(ShardRsmKey{PropertyValue(1), PropertyValue(0)});
      memgraph::msgs::ScanResultRow result;
      result.props.push_back(std::make_pair(memgraph::msgs::PropertyId::FromUint(0), val));
      ret.results.push_back(std::move(result));
      ret.success = true;
    } else if (as_prop_val == ShardRsmKey{PropertyValue(1), PropertyValue(0)}) {
      memgraph::msgs::ScanResultRow result;
      Value val(int64_t(1));
      result.props.push_back(std::make_pair(memgraph::msgs::PropertyId::FromUint(0), val));
      ret.results.push_back(std::move(result));
      ret.success = true;
    } else if (as_prop_val == ShardRsmKey{PropertyValue(12), PropertyValue(13)}) {
      memgraph::msgs::ScanResultRow result;
      Value val(int64_t(444));
      result.props.push_back(std::make_pair(memgraph::msgs::PropertyId::FromUint(0), val));
      ret.results.push_back(std::move(result));
      ret.success = true;
    } else {
      ret.success = false;
    }
    return ret;
  }

  ExpandOneResponse ReadImpl(ExpandOneRequest rqst) { return {}; }
  using ReadRequests = std::variant<ScanVerticesRequest, ExpandOneRequest>;
  using ReadResponses = std::variant<ScanVerticesResponse, ExpandOneResponse>;

  ReadResponses Read(ReadRequests read_requests) {
    return {std::visit([this](auto &&request) { return ReadResponses{ReadImpl(std::move(request))}; },
                       std::move(read_requests))};
  }

  CreateVerticesResponse Apply(CreateVerticesRequest request) { return CreateVerticesResponse{.success = true}; }
};
