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

#include <chrono>
#include <cstdint>
#include <iostream>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

#include "io/address.hpp"
#include "io/errors.hpp"
#include "io/rsm/raft.hpp"
#include "io/rsm/rsm_client.hpp"
#include "io/simulator/simulator.hpp"
#include "io/simulator/simulator_transport.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/shard.hpp"
#include "storage/v3/shard_rsm.hpp"
#include "storage/v3/view.hpp"
#include "utils/result.hpp"

namespace memgraph::storage::v3::tests {

using io::Address;
using io::Io;
using io::ResponseEnvelope;
using io::ResponseFuture;
using io::Time;
using io::TimedOut;
using io::rsm::Raft;
using io::rsm::ReadRequest;
using io::rsm::ReadResponse;
using io::rsm::RsmClient;
using io::rsm::WriteRequest;
using io::rsm::WriteResponse;
using io::simulator::Simulator;
using io::simulator::SimulatorConfig;
using io::simulator::SimulatorStats;
using io::simulator::SimulatorTransport;
using utils::BasicResult;

using msgs::ReadRequests;
using msgs::ReadResponses;
using msgs::WriteRequests;
using msgs::WriteResponses;

using ShardClient = RsmClient<SimulatorTransport, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

using ConcreteShardRsm = Raft<SimulatorTransport, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses>;

// TODO(gvolfing) test vertex deletion with DETACH_DELETE as well
template <typename IoImpl>
void RunShardRaft(Raft<IoImpl, ShardRsm, WriteRequests, WriteResponses, ReadRequests, ReadResponses> server) {
  server.Run();
}

namespace {

uint64_t GetTransactionId() {
  static uint64_t transaction_id = 0;
  return transaction_id++;
}

uint64_t GetUniqueInteger() {
  static uint64_t prop_val_val = 1001;
  return prop_val_val++;
}

constexpr LabelId get_primary_label() { return LabelId::FromUint(1); }

constexpr SchemaProperty get_schema_property() {
  return {.property_id = PropertyId::FromUint(2), .type = common::SchemaType::INT};
}

msgs::PrimaryKey GetPrimaryKey(int64_t value) {
  msgs::Value prop_val(static_cast<int64_t>(value));
  msgs::PrimaryKey primary_key = {prop_val};
  return primary_key;
}

msgs::NewVertex GetNewVertex(int64_t value) {
  // Specify Labels.
  msgs::Label label1 = {.id = LabelId::FromUint(3)};
  std::vector<msgs::Label> label_ids = {label1};

  // Specify primary key.
  msgs::PrimaryKey primary_key = GetPrimaryKey(value);

  // Specify properties
  auto val1 = msgs::Value(static_cast<int64_t>(value));
  auto prop1 = std::make_pair(PropertyId::FromUint(4), val1);

  auto val3 = msgs::Value(static_cast<int64_t>(value));
  auto prop3 = std::make_pair(PropertyId::FromUint(5), val3);

  //(VERIFY) does the schema has to be specified with the properties or the primarykey?
  auto val2 = msgs::Value(static_cast<int64_t>(value));
  auto prop2 = std::make_pair(PropertyId::FromUint(6), val2);

  std::vector<std::pair<PropertyId, msgs::Value>> properties{prop1, prop2, prop3};

  // NewVertex
  return {.label_ids = label_ids, .primary_key = primary_key, .properties = properties};
}

// TODO(gvolfing) maybe rename that something that makes sense.
std::vector<std::vector<msgs::Value>> GetValuePrimaryKeysWithValue(int64_t value) {
  msgs::Value val(static_cast<int64_t>(value));
  return {{val}};
}

void Commit(ShardClient &client, const coordinator::Hlc &transaction_timestamp) {
  coordinator::Hlc commit_timestamp{.logical_id = GetTransactionId()};

  msgs::CommitRequest commit_req{};
  commit_req.transaction_id = transaction_timestamp;
  commit_req.commit_timestamp = commit_timestamp;

  while (true) {
    auto write_res = client.SendWriteRequest(commit_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::CommitResponse>(write_response_result);
    MG_ASSERT(!write_response.error.has_value(), "Commit expected to be successful, but it is failed");

    break;
  }
}

}  // namespace

// attempts to sending different requests
namespace {

bool AttemptToCreateVertex(ShardClient &client, int64_t value) {
  msgs::NewVertex vertex = GetNewVertex(value);

  auto create_req = msgs::CreateVerticesRequest{};
  create_req.new_vertices = {vertex};
  create_req.transaction_id.logical_id = GetTransactionId();

  auto write_res = client.SendWriteRequest(create_req);
  MG_ASSERT(write_res.HasValue() && !std::get<msgs::CreateVerticesResponse>(write_res.GetValue()).error.has_value(),
            "Unexpected failure");

  Commit(client, create_req.transaction_id);
  return true;
}

bool AttemptToDeleteVertex(ShardClient &client, int64_t value) {
  auto delete_req = msgs::DeleteVerticesRequest{};
  delete_req.deletion_type = msgs::DeleteVerticesRequest::DeletionType::DELETE;
  delete_req.primary_keys = GetValuePrimaryKeysWithValue(value);
  delete_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto write_res = client.SendWriteRequest(delete_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::DeleteVerticesResponse>(write_response_result);

    Commit(client, delete_req.transaction_id);
    return !write_response.error.has_value();
  }
}

bool AttemptToUpdateVertex(ShardClient &client, int64_t vertex_primary_key, std::vector<LabelId> add_labels = {},
                           std::vector<LabelId> remove_labels = {}) {
  auto vertex_id = GetValuePrimaryKeysWithValue(vertex_primary_key)[0];

  std::vector<std::pair<PropertyId, msgs::Value>> property_updates;
  auto property_update = std::make_pair(PropertyId::FromUint(5), msgs::Value(static_cast<int64_t>(10000)));

  msgs::UpdateVertex update_vertex;
  update_vertex.primary_key = vertex_id;
  update_vertex.property_updates = {property_update};
  update_vertex.add_labels = add_labels;
  update_vertex.remove_labels = remove_labels;

  msgs::UpdateVerticesRequest update_req;
  update_req.transaction_id.logical_id = GetTransactionId();
  update_req.update_vertices = {update_vertex};

  while (true) {
    auto write_res = client.SendWriteRequest(update_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::UpdateVerticesResponse>(write_response_result);

    Commit(client, update_req.transaction_id);
    return !write_response.error.has_value();
  }
}

bool AttemptToRemoveVertexProperty(ShardClient &client, int64_t primary_key, std::vector<LabelId> add_labels = {},
                                   std::vector<LabelId> remove_labels = {}) {
  auto vertex_id = GetValuePrimaryKeysWithValue(primary_key)[0];

  std::vector<std::pair<PropertyId, msgs::Value>> property_updates;
  auto property_update = std::make_pair(PropertyId::FromUint(5), msgs::Value());

  msgs::UpdateVertex update_vertex;
  update_vertex.primary_key = vertex_id;
  update_vertex.property_updates = {property_update};
  update_vertex.add_labels = add_labels;
  update_vertex.remove_labels = remove_labels;

  msgs::UpdateVerticesRequest update_req;
  update_req.transaction_id.logical_id = GetTransactionId();
  update_req.update_vertices = {update_vertex};

  while (true) {
    auto write_res = client.SendWriteRequest(update_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::UpdateVerticesResponse>(write_response_result);

    Commit(client, update_req.transaction_id);
    return !write_response.error.has_value();
  }
}

bool AttemptToAddEdge(ShardClient &client, int64_t value_of_vertex_1, int64_t value_of_vertex_2, int64_t edge_gid,
                      EdgeTypeId edge_type_id) {
  auto id = msgs::EdgeId{};
  msgs::Label label = {.id = get_primary_label()};

  auto src = std::make_pair(label, GetPrimaryKey(value_of_vertex_1));
  auto dst = std::make_pair(label, GetPrimaryKey(value_of_vertex_2));
  id.gid = edge_gid;

  auto type = msgs::EdgeType{};
  type.id = edge_type_id;

  msgs::NewExpand edge;
  edge.id = id;
  edge.type = type;
  edge.src_vertex = src;
  edge.dest_vertex = dst;

  msgs::CreateExpandRequest create_req{};
  create_req.new_expands = {edge};
  create_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto write_res = client.SendWriteRequest(create_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::CreateExpandResponse>(write_response_result);

    Commit(client, create_req.transaction_id);

    return !write_response.error.has_value();
  }
  return true;
}

bool AttemptToAddEdgeWithProperties(ShardClient &client, int64_t value_of_vertex_1, int64_t value_of_vertex_2,
                                    int64_t edge_gid, uint64_t edge_prop_id, int64_t edge_prop_val,
                                    const std::vector<EdgeTypeId> &edge_type_id) {
  msgs::EdgeId id1;
  msgs::Label label = {.id = get_primary_label()};

  auto src = std::make_pair(label, GetPrimaryKey(value_of_vertex_1));
  auto dst = std::make_pair(label, GetPrimaryKey(value_of_vertex_2));
  id1.gid = edge_gid;

  auto type1 = msgs::EdgeType{};
  type1.id = edge_type_id[0];

  auto edge_prop = std::make_pair(PropertyId::FromUint(edge_prop_id), msgs::Value(edge_prop_val));

  auto expand = msgs::NewExpand{};
  expand.id = id1;
  expand.type = type1;
  expand.src_vertex = src;
  expand.dest_vertex = dst;
  expand.properties = {edge_prop};

  msgs::CreateExpandRequest create_req{};
  create_req.new_expands = {expand};
  create_req.transaction_id.logical_id = GetTransactionId();

  auto write_res = client.SendWriteRequest(create_req);
  MG_ASSERT(write_res.HasValue() && !std::get<msgs::CreateExpandResponse>(write_res.GetValue()).error.has_value(),
            "Unexpected failure");

  Commit(client, create_req.transaction_id);
  return true;
}

bool AttemptToDeleteEdge(ShardClient &client, int64_t value_of_vertex_1, int64_t value_of_vertex_2, int64_t edge_gid,
                         EdgeTypeId edge_type_id) {
  auto id = msgs::EdgeId{};
  msgs::Label label = {.id = get_primary_label()};

  auto src = std::make_pair(label, GetPrimaryKey(value_of_vertex_1));
  auto dst = std::make_pair(label, GetPrimaryKey(value_of_vertex_2));

  id.gid = edge_gid;

  auto type = msgs::EdgeType{};
  type.id = edge_type_id;

  auto edge = msgs::Edge{};
  edge.id = id;
  edge.type = type;
  edge.src = {src};
  edge.dst = {dst};

  msgs::DeleteEdgesRequest delete_req{};
  delete_req.edges = {edge};
  delete_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto write_res = client.SendWriteRequest(delete_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::DeleteEdgesResponse>(write_response_result);

    Commit(client, delete_req.transaction_id);
    return !write_response.error.has_value();
  }
}

bool AttemptToUpdateEdge(ShardClient &client, int64_t value_of_vertex_1, int64_t value_of_vertex_2, int64_t edge_gid,
                         EdgeTypeId edge_type_id, uint64_t edge_prop_id, int64_t edge_prop_val) {
  auto id = msgs::EdgeId{};
  msgs::Label label = {.id = get_primary_label()};

  auto src = std::make_pair(label, GetPrimaryKey(value_of_vertex_1));
  auto dst = std::make_pair(label, GetPrimaryKey(value_of_vertex_2));

  id.gid = edge_gid;

  auto type = msgs::EdgeType{};
  type.id = edge_type_id;

  auto edge = msgs::Edge{};
  edge.id = id;
  edge.type = type;

  auto edge_prop = std::vector<std::pair<PropertyId, msgs::Value>>{
      std::make_pair(PropertyId::FromUint(edge_prop_id), msgs::Value(edge_prop_val))};

  msgs::UpdateEdgeProp update_props{.edge_id = id, .src = src, .dst = dst, .property_updates = edge_prop};

  msgs::UpdateEdgesRequest update_req{};
  update_req.transaction_id.logical_id = GetTransactionId();
  update_req.new_properties = {update_props};

  while (true) {
    auto write_res = client.SendWriteRequest(update_req);
    if (write_res.HasError()) {
      continue;
    }

    auto write_response_result = write_res.GetValue();
    auto write_response = std::get<msgs::UpdateEdgesResponse>(write_response_result);

    Commit(client, update_req.transaction_id);
    return !write_response.error.has_value();
  }
}

std::tuple<size_t, std::optional<msgs::VertexId>> AttemptToScanAllWithoutBatchLimit(ShardClient &client,
                                                                                    msgs::VertexId start_id) {
  msgs::ScanVerticesRequest scan_req{};
  scan_req.batch_limit = {};
  scan_req.filter_expressions.clear();
  scan_req.props_to_return = std::nullopt;
  scan_req.start_id = start_id;
  scan_req.storage_view = msgs::StorageView::OLD;
  scan_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(scan_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ScanVerticesResponse>(write_response_result);

    MG_ASSERT(write_response.error == std::nullopt);

    return {write_response.results.size(), write_response.next_start_id};
  }
}

std::tuple<size_t, std::optional<msgs::VertexId>> AttemptToScanAllWithBatchLimit(ShardClient &client,
                                                                                 msgs::VertexId start_id,
                                                                                 uint64_t batch_limit) {
  msgs::ScanVerticesRequest scan_req{};
  scan_req.batch_limit = batch_limit;
  scan_req.filter_expressions.clear();
  scan_req.props_to_return = std::nullopt;
  scan_req.start_id = start_id;
  scan_req.storage_view = msgs::StorageView::OLD;
  scan_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(scan_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ScanVerticesResponse>(write_response_result);

    MG_ASSERT(!write_response.error.has_value());

    return {write_response.results.size(), write_response.next_start_id};
  }
}

std::tuple<size_t, std::optional<msgs::VertexId>> AttemptToScanAllWithExpression(ShardClient &client,
                                                                                 msgs::VertexId start_id,
                                                                                 uint64_t batch_limit,
                                                                                 uint64_t prop_val_to_check_against) {
  std::string filter_expr1 = "MG_SYMBOL_NODE.prop1 = " + std::to_string(prop_val_to_check_against);
  std::vector<std::string> filter_expressions = {filter_expr1};

  std::string regular_expr1 = "2+2";
  std::vector<std::string> vertex_expressions = {regular_expr1};

  msgs::ScanVerticesRequest scan_req{};
  scan_req.batch_limit = batch_limit;
  scan_req.filter_expressions = filter_expressions;
  scan_req.vertex_expressions = vertex_expressions;
  scan_req.props_to_return = std::nullopt;
  scan_req.start_id = start_id;
  scan_req.storage_view = msgs::StorageView::NEW;
  scan_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(scan_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ScanVerticesResponse>(write_response_result);

    MG_ASSERT(!write_response.error.has_value());
    MG_ASSERT(!write_response.results.empty(), "There are no results!");
    MG_ASSERT(write_response.results[0].evaluated_vertex_expressions[0].int_v == 4);
    return {write_response.results.size(), write_response.next_start_id};
  }
}

msgs::GetPropertiesResponse AttemptToGetProperties(
    ShardClient &client, std::optional<std::vector<PropertyId>> properties, std::vector<msgs::VertexId> vertices,
    std::vector<msgs::EdgeId> edges, std::optional<size_t> limit = std::nullopt,
    std::optional<uint64_t> filter_prop = std::nullopt, bool edge = false,
    std::optional<std::string> order_by = std::nullopt) {
  msgs::GetPropertiesRequest req{};
  req.transaction_id.logical_id = GetTransactionId();
  req.property_ids = std::move(properties);

  if (filter_prop) {
    std::string filter_expr = (!edge) ? "MG_SYMBOL_NODE.prop1 >= " : "MG_SYMBOL_EDGE.e_prop = ";
    filter_expr += std::to_string(*filter_prop);
    req.filter = std::make_optional(std::move(filter_expr));
  }
  if (order_by) {
    std::string filter_expr = (!edge) ? "MG_SYMBOL_NODE." : "MG_SYMBOL_EDGE.";
    filter_expr += *order_by;
    msgs::OrderBy order_by{.expression = {std::move(filter_expr)}, .direction = msgs::OrderingDirection::DESCENDING};
    std::vector<msgs::OrderBy> request_order_by;
    request_order_by.push_back(std::move(order_by));
    req.order_by = std::move(request_order_by);
  }
  if (limit) {
    req.limit = limit;
  }
  req.expressions = {std::string("5 = 5")};
  std::vector<msgs::VertexId> req_v;
  std::vector<msgs::EdgeId> req_e;
  for (auto &v : vertices) {
    req_v.push_back(std::move(v));
  }
  for (auto &e : edges) {
    req_e.push_back(std::move(e));
  }

  if (!edges.empty()) {
    MG_ASSERT(edges.size() == vertices.size());
    size_t id = 0;
    req.vertices_and_edges.reserve(req_v.size());
    for (auto &v : req_v) {
      req.vertices_and_edges.push_back({std::move(v), std::move(req_e[id++])});
    }
  } else {
    req.vertex_ids = std::move(req_v);
  }

  while (true) {
    auto read_res = client.SendReadRequest(req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::GetPropertiesResponse>(write_response_result);

    return write_response;
  }
}

void AttemptToScanAllWithOrderByOnPrimaryProperty(ShardClient &client, msgs::VertexId start_id, uint64_t batch_limit) {
  msgs::ScanVerticesRequest scan_req;
  scan_req.batch_limit = batch_limit;
  scan_req.order_bys = {{msgs::Expression{"MG_SYMBOL_NODE.prop1"}, msgs::OrderingDirection::DESCENDING}};
  scan_req.props_to_return = std::nullopt;
  scan_req.start_id = start_id;
  scan_req.storage_view = msgs::StorageView::NEW;
  scan_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(scan_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ScanVerticesResponse>(write_response_result);

    MG_ASSERT(!write_response.error.has_value());
    MG_ASSERT(write_response.results.size() == 5, "Expecting 5 results!");
    for (int64_t i{0}; i < 5; ++i) {
      const auto expected_primary_key = std::vector{msgs::Value(1023 - i)};
      const auto actual_primary_key = write_response.results[i].vertex.id.second;
      MG_ASSERT(expected_primary_key == actual_primary_key, "The order of vertices is not correct");
    }
    break;
  }
}

void AttemptToScanAllWithOrderByOnSecondaryProperty(ShardClient &client, msgs::VertexId start_id,
                                                    uint64_t batch_limit) {
  msgs::ScanVerticesRequest scan_req;
  scan_req.batch_limit = batch_limit;
  scan_req.order_bys = {{msgs::Expression{"MG_SYMBOL_NODE.prop4"}, msgs::OrderingDirection::DESCENDING}};
  scan_req.props_to_return = std::nullopt;
  scan_req.start_id = start_id;
  scan_req.storage_view = msgs::StorageView::NEW;
  scan_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(scan_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ScanVerticesResponse>(write_response_result);

    MG_ASSERT(!write_response.error.has_value());
    MG_ASSERT(write_response.results.size() == 5, "Expecting 5 results!");
    for (int64_t i{0}; i < 5; ++i) {
      const auto expected_prop4 = std::vector{msgs::Value(1023 - i)};
      const auto actual_prop4 = std::invoke([&write_response, i]() {
        const auto res = std::ranges::find_if(write_response.results[i].props, [](const auto &id_value_prop_pair) {
          return id_value_prop_pair.first.AsInt() == 4;
        });
        MG_ASSERT(res != write_response.results[i].props.end(), "Property does not exist!");
        return std::vector{res->second};
      });

      MG_ASSERT(expected_prop4 == actual_prop4, "The order of vertices is not correct");
    }
    break;
  }
}

void AttemptToExpandOneWithWrongEdgeType(ShardClient &client, uint64_t src_vertex_val, EdgeTypeId edge_type_id) {
  // Source vertex
  msgs::Label label = {.id = get_primary_label()};
  auto src_vertex = std::make_pair(label, GetPrimaryKey(src_vertex_val));

  // Edge type
  auto edge_type = msgs::EdgeType{};
  edge_type.id = edge_type_id;

  // Edge direction
  auto edge_direction = msgs::EdgeDirection::OUT;

  // Source Vertex properties to look for
  std::optional<std::vector<PropertyId>> src_vertex_properties = {};

  // Edge properties to look for
  std::optional<std::vector<PropertyId>> edge_properties = {};

  std::vector<std::string> expressions;
  std::vector<msgs::OrderBy> order_by_vertices = {};
  std::vector<msgs::OrderBy> order_by_edges = {};
  std::optional<size_t> limit = {};
  std::vector<std::string> filter = {};

  msgs::ExpandOneRequest expand_one_req{};

  expand_one_req.direction = edge_direction;
  expand_one_req.edge_properties = edge_properties;
  expand_one_req.edge_types = {edge_type};
  expand_one_req.vertex_expressions = expressions;
  expand_one_req.filters = filter;
  expand_one_req.limit = limit;
  expand_one_req.order_by_vertices = order_by_vertices;
  expand_one_req.order_by_edges = order_by_edges;
  expand_one_req.src_vertex_properties = src_vertex_properties;
  expand_one_req.src_vertices = {src_vertex};
  expand_one_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(expand_one_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ExpandOneResponse>(write_response_result);
    MG_ASSERT(write_response.result.size() == 1);

    MG_ASSERT(write_response.result[0].in_edges_with_all_properties.empty());
    MG_ASSERT(write_response.result[0].out_edges_with_all_properties.empty());
    MG_ASSERT(write_response.result[0].in_edges_with_specific_properties.empty());
    MG_ASSERT(write_response.result[0].out_edges_with_specific_properties.empty());

    break;
  }
}

void AttemptToExpandOneSimple(ShardClient &client, uint64_t src_vertex_val, EdgeTypeId edge_type_id) {
  // Source vertex
  msgs::Label label = {.id = get_primary_label()};
  auto src_vertex = std::make_pair(label, GetPrimaryKey(src_vertex_val));

  // Edge type
  auto edge_type = msgs::EdgeType{};
  edge_type.id = edge_type_id;

  // Edge direction
  auto edge_direction = msgs::EdgeDirection::OUT;

  // Source Vertex properties to look for
  std::optional<std::vector<PropertyId>> src_vertex_properties = {};

  // Edge properties to look for
  std::optional<std::vector<PropertyId>> edge_properties = {};

  std::vector<std::string> expressions;
  std::vector<msgs::OrderBy> order_by_vertices = {};
  std::vector<msgs::OrderBy> order_by_edges = {};
  std::optional<size_t> limit = {};
  std::vector<std::string> filter = {};

  msgs::ExpandOneRequest expand_one_req{};

  expand_one_req.direction = edge_direction;
  expand_one_req.edge_properties = edge_properties;
  expand_one_req.edge_types = {edge_type};
  expand_one_req.vertex_expressions = expressions;
  expand_one_req.filters = filter;
  expand_one_req.limit = limit;
  expand_one_req.order_by_vertices = order_by_vertices;
  expand_one_req.order_by_edges = order_by_edges;
  expand_one_req.src_vertex_properties = src_vertex_properties;
  expand_one_req.src_vertices = {src_vertex};
  expand_one_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(expand_one_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ExpandOneResponse>(write_response_result);
    MG_ASSERT(write_response.result.size() == 1);
    MG_ASSERT(write_response.result[0].out_edges_with_all_properties.size() == 2);
    MG_ASSERT(write_response.result[0].in_edges_with_all_properties.empty());
    MG_ASSERT(write_response.result[0].in_edges_with_specific_properties.empty());
    MG_ASSERT(write_response.result[0].out_edges_with_specific_properties.empty());
    const auto number_of_properties_on_edge =
        (write_response.result[0].out_edges_with_all_properties[0]).properties.size();
    MG_ASSERT(number_of_properties_on_edge == 1);
    break;
  }
}

void AttemptToExpandOneWithUniqueEdges(ShardClient &client, uint64_t src_vertex_val, EdgeTypeId edge_type_id) {
  // Source vertex
  msgs::Label label = {.id = get_primary_label()};
  auto src_vertex = std::make_pair(label, GetPrimaryKey(src_vertex_val));

  // Edge type
  auto edge_type = msgs::EdgeType{};
  edge_type.id = edge_type_id;

  // Edge direction
  auto edge_direction = msgs::EdgeDirection::OUT;

  // Source Vertex properties to look for
  std::optional<std::vector<PropertyId>> src_vertex_properties = {};

  // Edge properties to look for
  std::optional<std::vector<PropertyId>> edge_properties = {};

  std::vector<std::string> expressions;
  std::vector<msgs::OrderBy> order_by_vertices = {};
  std::vector<msgs::OrderBy> order_by_edges = {};
  std::optional<size_t> limit = {};
  std::vector<std::string> filter = {};

  msgs::ExpandOneRequest expand_one_req{};

  expand_one_req.direction = edge_direction;
  expand_one_req.edge_properties = edge_properties;
  expand_one_req.edge_types = {edge_type};
  expand_one_req.vertex_expressions = expressions;
  expand_one_req.filters = filter;
  expand_one_req.limit = limit;
  expand_one_req.order_by_vertices = order_by_vertices;
  expand_one_req.order_by_edges = order_by_edges;
  expand_one_req.src_vertex_properties = src_vertex_properties;
  expand_one_req.src_vertices = {src_vertex};
  expand_one_req.only_unique_neighbor_rows = true;
  expand_one_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(expand_one_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ExpandOneResponse>(write_response_result);
    MG_ASSERT(write_response.result.size() == 1);
    MG_ASSERT(write_response.result[0].out_edges_with_all_properties.size() == 1);
    MG_ASSERT(write_response.result[0].in_edges_with_all_properties.empty());
    MG_ASSERT(write_response.result[0].in_edges_with_specific_properties.empty());
    MG_ASSERT(write_response.result[0].out_edges_with_specific_properties.empty());
    const auto number_of_properties_on_edge =
        (write_response.result[0].out_edges_with_all_properties[0]).properties.size();
    MG_ASSERT(number_of_properties_on_edge == 1);
    break;
  }
}

void AttemptToExpandOneLimitAndOrderBy(ShardClient &client, uint64_t src_vertex_val, uint64_t other_src_vertex_val,
                                       EdgeTypeId edge_type_id) {
  // Source vertex
  msgs::Label label = {.id = get_primary_label()};
  auto src_vertex = std::make_pair(label, GetPrimaryKey(src_vertex_val));
  auto other_src_vertex = std::make_pair(label, GetPrimaryKey(other_src_vertex_val));

  // Edge type
  auto edge_type = msgs::EdgeType{};
  edge_type.id = edge_type_id;

  // Edge direction
  auto edge_direction = msgs::EdgeDirection::OUT;

  // Source Vertex properties to look for
  std::optional<std::vector<PropertyId>> src_vertex_properties = {};

  // Edge properties to look for
  std::optional<std::vector<PropertyId>> edge_properties = {};

  std::vector<msgs::OrderBy> order_by_vertices = {
      {msgs::Expression{"MG_SYMBOL_NODE.prop1"}, msgs::OrderingDirection::ASCENDING}};
  std::vector<msgs::OrderBy> order_by_edges = {
      {msgs::Expression{"MG_SYMBOL_EDGE.prop4"}, msgs::OrderingDirection::DESCENDING}};

  size_t limit = 1;
  std::vector<std::string> filters = {"MG_SYMBOL_NODE.prop1 != -1"};

  msgs::ExpandOneRequest expand_one_req{};

  expand_one_req.direction = edge_direction;
  expand_one_req.edge_properties = edge_properties;
  expand_one_req.edge_types = {edge_type};
  expand_one_req.filters = filters;
  expand_one_req.limit = limit;
  expand_one_req.order_by_vertices = order_by_vertices;
  expand_one_req.order_by_edges = order_by_edges;
  expand_one_req.src_vertex_properties = src_vertex_properties;
  expand_one_req.src_vertices = {src_vertex, other_src_vertex};
  expand_one_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(expand_one_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ExpandOneResponse>(write_response_result);

    // We check that we do not have more results than the limit. Based on the data in the graph, we know that we should
    // receive exactly limit responses.
    auto expected_number_of_rows = std::min(expand_one_req.src_vertices.size(), limit);
    MG_ASSERT(expected_number_of_rows == 1);
    MG_ASSERT(write_response.result.size() == expected_number_of_rows);

    // We know there are 1 out-going edges from V1->V2
    // We know there are 10 out-going edges from V2->V3
    // Since we sort on prop1 and limit 1, we will have a single response
    // with two edges corresponding to V1->V2 and V1->V3
    const auto expected_number_of_edges = 2;
    MG_ASSERT(write_response.result[0].out_edges_with_all_properties.size() == expected_number_of_edges);
    MG_ASSERT(write_response.result[0]
                  .out_edges_with_specific_properties.empty());  // We are not asking for specific properties

    // We also check that the vertices are ordered by prop1 DESC

    auto is_sorted = std::is_sorted(write_response.result.cbegin(), write_response.result.cend(),
                                    [](const auto &vertex, const auto &other_vertex) {
                                      const auto primary_key = vertex.src_vertex.id.second;
                                      const auto other_primary_key = other_vertex.src_vertex.id.second;

                                      MG_ASSERT(primary_key.size() == 1);
                                      MG_ASSERT(other_primary_key.size() == 1);
                                      return primary_key[0].int_v > other_primary_key[0].int_v;
                                    });

    MG_ASSERT(is_sorted);
    break;
  }
}

void AttemptToExpandOneWithSpecifiedSrcVertexProperties(ShardClient &client, uint64_t src_vertex_val,
                                                        EdgeTypeId edge_type_id) {
  // Source vertex
  msgs::Label label = {.id = get_primary_label()};
  auto src_vertex = std::make_pair(label, GetPrimaryKey(src_vertex_val));

  // Edge type
  auto edge_type = msgs::EdgeType{};
  edge_type.id = edge_type_id;

  // Edge direction
  auto edge_direction = msgs::EdgeDirection::OUT;

  // Source Vertex properties to look for
  std::vector<PropertyId> desired_src_vertex_props{PropertyId::FromUint(2)};
  std::optional<std::vector<PropertyId>> src_vertex_properties = desired_src_vertex_props;

  // Edge properties to look for
  std::optional<std::vector<PropertyId>> edge_properties = {};

  std::vector<std::string> expressions;
  std::vector<msgs::OrderBy> order_by_vertices = {};
  std::vector<msgs::OrderBy> order_by_edges = {};
  std::optional<size_t> limit = {};
  std::vector<std::string> filter = {};

  msgs::ExpandOneRequest expand_one_req{};

  expand_one_req.direction = edge_direction;
  expand_one_req.edge_properties = edge_properties;
  expand_one_req.edge_types = {edge_type};
  expand_one_req.vertex_expressions = expressions;
  expand_one_req.filters = filter;
  expand_one_req.limit = limit;
  expand_one_req.order_by_vertices = order_by_vertices;
  expand_one_req.order_by_edges = order_by_edges;
  expand_one_req.src_vertex_properties = src_vertex_properties;
  expand_one_req.src_vertices = {src_vertex};
  expand_one_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(expand_one_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ExpandOneResponse>(write_response_result);
    MG_ASSERT(write_response.result.size() == 1);
    auto src_vertex_props_size = write_response.result[0].src_vertex_properties.size();
    MG_ASSERT(src_vertex_props_size == 1);
    MG_ASSERT(write_response.result[0].out_edges_with_all_properties.size() == 2);
    MG_ASSERT(write_response.result[0].in_edges_with_all_properties.empty());
    MG_ASSERT(write_response.result[0].in_edges_with_specific_properties.empty());
    MG_ASSERT(write_response.result[0].out_edges_with_specific_properties.empty());
    const auto number_of_properties_on_edge =
        (write_response.result[0].out_edges_with_all_properties[0]).properties.size();
    MG_ASSERT(number_of_properties_on_edge == 1);
    break;
  }
}

void AttemptToExpandOneWithSpecifiedEdgeProperties(ShardClient &client, uint64_t src_vertex_val,
                                                   EdgeTypeId edge_type_id, uint64_t edge_prop_id) {
  // Source vertex
  msgs::Label label = {.id = get_primary_label()};
  auto src_vertex = std::make_pair(label, GetPrimaryKey(src_vertex_val));

  // Edge type
  auto edge_type = msgs::EdgeType{};
  edge_type.id = edge_type_id;

  // Edge direction
  auto edge_direction = msgs::EdgeDirection::OUT;

  // Source Vertex properties to look for
  std::optional<std::vector<PropertyId>> src_vertex_properties = {};

  // Edge properties to look for
  std::vector<PropertyId> specified_edge_prop{PropertyId::FromUint(edge_prop_id)};
  std::optional<std::vector<PropertyId>> edge_properties = {specified_edge_prop};

  std::vector<std::string> expressions;
  std::vector<msgs::OrderBy> order_by_vertices = {};
  std::vector<msgs::OrderBy> order_by_edges = {};
  std::optional<size_t> limit = {};
  std::vector<std::string> filter = {};

  msgs::ExpandOneRequest expand_one_req{};

  expand_one_req.direction = edge_direction;
  expand_one_req.edge_properties = edge_properties;
  expand_one_req.edge_types = {edge_type};
  expand_one_req.vertex_expressions = expressions;
  expand_one_req.filters = filter;
  expand_one_req.limit = limit;
  expand_one_req.order_by_vertices = order_by_vertices;
  expand_one_req.order_by_edges = order_by_edges;
  expand_one_req.src_vertex_properties = src_vertex_properties;
  expand_one_req.src_vertices = {src_vertex};
  expand_one_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(expand_one_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ExpandOneResponse>(write_response_result);
    MG_ASSERT(write_response.result.size() == 1);
    MG_ASSERT(write_response.result[0].out_edges_with_specific_properties.size() == 2);
    MG_ASSERT(write_response.result[0].in_edges_with_specific_properties.empty());
    MG_ASSERT(write_response.result[0].in_edges_with_all_properties.empty());
    MG_ASSERT(write_response.result[0].out_edges_with_all_properties.empty());
    const auto specific_properties_size =
        (write_response.result[0].out_edges_with_specific_properties[0]).properties.size();
    MG_ASSERT(specific_properties_size == 1);
    break;
  }
}

void AttemptToExpandOneWithFilters(ShardClient &client, uint64_t src_vertex_val, EdgeTypeId edge_type_id,
                                   uint64_t edge_prop_id, uint64_t prop_val_to_check_against) {
  std::string filter_expr1 = "MG_SYMBOL_NODE.prop1 = " + std::to_string(prop_val_to_check_against);

  // Source vertex
  msgs::Label label = {.id = get_primary_label()};
  auto src_vertex = std::make_pair(label, GetPrimaryKey(src_vertex_val));

  // Edge type
  auto edge_type = msgs::EdgeType{};
  edge_type.id = edge_type_id;

  // Edge direction
  auto edge_direction = msgs::EdgeDirection::OUT;

  // Source Vertex properties to look for
  std::optional<std::vector<PropertyId>> src_vertex_properties = {};

  // Edge properties to look for
  std::optional<std::vector<PropertyId>> edge_properties = {};

  std::vector<std::string> expressions;
  std::vector<msgs::OrderBy> order_by_vertices = {};
  std::vector<msgs::OrderBy> order_by_edges = {};
  std::optional<size_t> limit = {};
  std::vector<std::string> filter = {};

  msgs::ExpandOneRequest expand_one_req{};

  expand_one_req.direction = edge_direction;
  expand_one_req.edge_properties = edge_properties;
  expand_one_req.edge_types = {edge_type};
  expand_one_req.vertex_expressions = expressions;
  expand_one_req.filters = {filter_expr1};
  expand_one_req.limit = limit;
  expand_one_req.order_by_vertices = order_by_vertices;
  expand_one_req.order_by_edges = order_by_edges;
  expand_one_req.src_vertex_properties = src_vertex_properties;
  expand_one_req.src_vertices = {src_vertex};
  expand_one_req.transaction_id.logical_id = GetTransactionId();

  while (true) {
    auto read_res = client.SendReadRequest(expand_one_req);
    if (read_res.HasError()) {
      continue;
    }

    auto write_response_result = read_res.GetValue();
    auto write_response = std::get<msgs::ExpandOneResponse>(write_response_result);
    MG_ASSERT(write_response.result.size() == 1);
    MG_ASSERT(write_response.result[0].out_edges_with_specific_properties.empty());
    MG_ASSERT(write_response.result[0].in_edges_with_specific_properties.empty());
    MG_ASSERT(write_response.result[0].in_edges_with_all_properties.empty());
    MG_ASSERT(write_response.result[0].out_edges_with_all_properties.size() == 2);
    break;
  }
}

}  // namespace

// tests
namespace {

void TestCreateVertices(ShardClient &client) { MG_ASSERT(AttemptToCreateVertex(client, GetUniqueInteger())); }

void TestCreateAndDeleteVertices(ShardClient &client) {
  auto unique_prop_val = GetUniqueInteger();

  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val));
  MG_ASSERT(AttemptToDeleteVertex(client, unique_prop_val));
}

void TestCreateAndUpdateVertices(ShardClient &client) {
  auto unique_prop_val = GetUniqueInteger();

  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val));
  MG_ASSERT(AttemptToUpdateVertex(client, unique_prop_val, {LabelId::FromInt(3)}, {}));
  MG_ASSERT(AttemptToUpdateVertex(client, unique_prop_val, {}, {LabelId::FromInt(3)}));
  MG_ASSERT(AttemptToRemoveVertexProperty(client, unique_prop_val));
}

void TestCreateEdge(ShardClient &client) {
  auto unique_prop_val_1 = GetUniqueInteger();
  auto unique_prop_val_2 = GetUniqueInteger();

  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_2));

  auto edge_gid = GetUniqueInteger();
  auto edge_type_id = EdgeTypeId::FromUint(GetUniqueInteger());

  MG_ASSERT(AttemptToAddEdge(client, unique_prop_val_1, unique_prop_val_2, edge_gid, edge_type_id));
}

void TestCreateAndDeleteEdge(ShardClient &client) {
  // Add the Edge
  auto unique_prop_val_1 = GetUniqueInteger();
  auto unique_prop_val_2 = GetUniqueInteger();

  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_2));

  auto edge_gid = GetUniqueInteger();
  auto edge_type_id = EdgeTypeId::FromUint(GetUniqueInteger());

  MG_ASSERT(AttemptToAddEdge(client, unique_prop_val_1, unique_prop_val_2, edge_gid, edge_type_id));

  // Delete the Edge
  MG_ASSERT(AttemptToDeleteEdge(client, unique_prop_val_1, unique_prop_val_2, edge_gid, edge_type_id));
}

void TestUpdateEdge(ShardClient &client) {
  // Add the Edge
  auto unique_prop_val_1 = GetUniqueInteger();
  auto unique_prop_val_2 = GetUniqueInteger();

  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_2));

  auto edge_gid = GetUniqueInteger();
  auto edge_type_id = EdgeTypeId::FromUint(GetUniqueInteger());

  auto edge_prop_id = GetUniqueInteger();
  auto edge_prop_val_old = GetUniqueInteger();
  auto edge_prop_val_new = GetUniqueInteger();

  MG_ASSERT(AttemptToAddEdgeWithProperties(client, unique_prop_val_1, unique_prop_val_2, edge_gid, edge_prop_id,
                                           edge_prop_val_old, {edge_type_id}));

  // Update the Edge
  MG_ASSERT(AttemptToUpdateEdge(client, unique_prop_val_1, unique_prop_val_2, edge_gid, edge_type_id, edge_prop_id,
                                edge_prop_val_new));
}

void TestScanAllOneGo(ShardClient &client) {
  auto unique_prop_val_1 = GetUniqueInteger();
  auto unique_prop_val_2 = GetUniqueInteger();
  auto unique_prop_val_3 = GetUniqueInteger();
  auto unique_prop_val_4 = GetUniqueInteger();
  auto unique_prop_val_5 = GetUniqueInteger();

  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_2));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_3));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_4));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_5));

  msgs::Label prim_label = {.id = get_primary_label()};
  msgs::PrimaryKey prim_key = {msgs::Value(static_cast<int64_t>(unique_prop_val_1))};

  msgs::VertexId v_id = {prim_label, prim_key};

  auto [result_size_2, next_id_2] = AttemptToScanAllWithExpression(client, v_id, 5, unique_prop_val_2);
  MG_ASSERT(result_size_2 == 1);

  AttemptToScanAllWithOrderByOnPrimaryProperty(client, v_id, 5);
  AttemptToScanAllWithOrderByOnSecondaryProperty(client, v_id, 5);

  auto [result_size_with_batch, next_id_with_batch] = AttemptToScanAllWithBatchLimit(client, v_id, 5);
  auto [result_size_without_batch, next_id_without_batch] = AttemptToScanAllWithoutBatchLimit(client, v_id);

  MG_ASSERT(result_size_with_batch == 5);
  MG_ASSERT(result_size_without_batch == 5);
}

void TestScanAllWithSmallBatchSize(ShardClient &client) {
  auto unique_prop_val_1 = GetUniqueInteger();
  auto unique_prop_val_2 = GetUniqueInteger();
  auto unique_prop_val_3 = GetUniqueInteger();
  auto unique_prop_val_4 = GetUniqueInteger();
  auto unique_prop_val_5 = GetUniqueInteger();
  auto unique_prop_val_6 = GetUniqueInteger();
  auto unique_prop_val_7 = GetUniqueInteger();
  auto unique_prop_val_8 = GetUniqueInteger();
  auto unique_prop_val_9 = GetUniqueInteger();
  auto unique_prop_val_10 = GetUniqueInteger();

  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_2));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_3));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_4));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_5));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_6));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_7));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_8));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_9));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_10));

  msgs::Label prim_label = {.id = get_primary_label()};
  msgs::PrimaryKey prim_key1 = {msgs::Value(static_cast<int64_t>(unique_prop_val_1))};

  msgs::VertexId v_id_1 = {prim_label, prim_key1};

  auto [result_size1, next_id1] = AttemptToScanAllWithBatchLimit(client, v_id_1, 3);
  MG_ASSERT(result_size1 == 3);

  auto [result_size2, next_id2] = AttemptToScanAllWithBatchLimit(client, next_id1.value(), 3);
  MG_ASSERT(result_size2 == 3);

  auto [result_size3, next_id3] = AttemptToScanAllWithBatchLimit(client, next_id2.value(), 3);
  MG_ASSERT(result_size3 == 3);

  auto [result_size4, next_id4] = AttemptToScanAllWithBatchLimit(client, next_id3.value(), 3);
  MG_ASSERT(result_size4 == 1);
  MG_ASSERT(!next_id4);
}

void TestExpandOneGraphOne(ShardClient &client) {
  {
    // ExpandOneSimple
    auto unique_prop_val_1 = GetUniqueInteger();
    auto unique_prop_val_2 = GetUniqueInteger();
    auto unique_prop_val_3 = GetUniqueInteger();

    MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_1));
    MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_2));
    MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_3));

    auto edge_type_id = EdgeTypeId::FromUint(GetUniqueInteger());
    auto wrong_edge_type_id = EdgeTypeId::FromUint(GetUniqueInteger());

    auto edge_gid_1 = GetUniqueInteger();
    auto edge_gid_2 = GetUniqueInteger();

    auto edge_prop_id = GetUniqueInteger();
    auto edge_prop_val = GetUniqueInteger();

    std::vector<uint64_t> edges_ids(10);
    std::generate(edges_ids.begin(), edges_ids.end(), GetUniqueInteger);

    // (V1)-[edge_type_id]->(V2)
    MG_ASSERT(AttemptToAddEdgeWithProperties(client, unique_prop_val_1, unique_prop_val_2, edge_gid_1, edge_prop_id,
                                             edge_prop_val, {edge_type_id}));
    // (V1)-[edge_type_id]->(V3)
    MG_ASSERT(AttemptToAddEdgeWithProperties(client, unique_prop_val_1, unique_prop_val_3, edge_gid_2, edge_prop_id,
                                             edge_prop_val, {edge_type_id}));

    // (V2)-[edge_type_id]->(V3) x 10
    std::for_each(edges_ids.begin(), edges_ids.end(), [&](const auto &edge_id) {
      MG_ASSERT(AttemptToAddEdgeWithProperties(client, unique_prop_val_2, unique_prop_val_3, edge_id, edge_prop_id,
                                               edge_prop_val, {edge_type_id}));
    });

    AttemptToExpandOneSimple(client, unique_prop_val_1, edge_type_id);
    AttemptToExpandOneLimitAndOrderBy(client, unique_prop_val_1, unique_prop_val_2, edge_type_id);
    AttemptToExpandOneWithWrongEdgeType(client, unique_prop_val_1, wrong_edge_type_id);
    AttemptToExpandOneWithSpecifiedSrcVertexProperties(client, unique_prop_val_1, edge_type_id);
    AttemptToExpandOneWithSpecifiedEdgeProperties(client, unique_prop_val_1, edge_type_id, edge_prop_id);
    AttemptToExpandOneWithFilters(client, unique_prop_val_1, edge_type_id, edge_prop_id, unique_prop_val_1);
  }
}

void TestExpandOneGraphTwo(ShardClient &client) {
  {
    // ExpandOneSimple
    auto unique_prop_val_1 = GetUniqueInteger();
    auto unique_prop_val_2 = GetUniqueInteger();

    MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_1));
    MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_2));

    auto edge_type_id = EdgeTypeId::FromUint(GetUniqueInteger());

    auto edge_gid_1 = GetUniqueInteger();
    auto edge_gid_2 = GetUniqueInteger();

    auto edge_prop_id = GetUniqueInteger();
    auto edge_prop_val = GetUniqueInteger();

    // (V1)-[edge_type_id]->(V2)
    MG_ASSERT(AttemptToAddEdgeWithProperties(client, unique_prop_val_1, unique_prop_val_2, edge_gid_1, edge_prop_id,
                                             edge_prop_val, {edge_type_id}));
    // (V1)-[edge_type_id]->(V3)
    MG_ASSERT(AttemptToAddEdgeWithProperties(client, unique_prop_val_1, unique_prop_val_2, edge_gid_2, edge_prop_id,
                                             edge_prop_val, {edge_type_id}));
    // AttemptToExpandOneSimple(client, unique_prop_val_1, edge_type_id);
    AttemptToExpandOneWithUniqueEdges(client, unique_prop_val_1, edge_type_id);
  }
}

void TestGetProperties(ShardClient &client) {
  const auto unique_prop_val_1 = GetUniqueInteger();
  const auto unique_prop_val_2 = GetUniqueInteger();
  const auto unique_prop_val_3 = GetUniqueInteger();
  const auto unique_prop_val_4 = GetUniqueInteger();
  const auto unique_prop_val_5 = GetUniqueInteger();

  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_1));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_2));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_3));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_4));
  MG_ASSERT(AttemptToCreateVertex(client, unique_prop_val_5));

  const msgs::Label prim_label = {.id = get_primary_label()};
  const msgs::PrimaryKey prim_key = {msgs::Value(static_cast<int64_t>(unique_prop_val_1))};
  const msgs::VertexId v_id = {prim_label, prim_key};
  const msgs::PrimaryKey prim_key_2 = {msgs::Value(static_cast<int64_t>(unique_prop_val_2))};
  const msgs::VertexId v_id_2 = {prim_label, prim_key_2};
  const msgs::PrimaryKey prim_key_3 = {msgs::Value(static_cast<int64_t>(unique_prop_val_3))};
  const msgs::VertexId v_id_3 = {prim_label, prim_key_3};
  const msgs::PrimaryKey prim_key_4 = {msgs::Value(static_cast<int64_t>(unique_prop_val_4))};
  const msgs::VertexId v_id_4 = {prim_label, prim_key_4};
  const msgs::PrimaryKey prim_key_5 = {msgs::Value(static_cast<int64_t>(unique_prop_val_5))};
  const msgs::VertexId v_id_5 = {prim_label, prim_key_5};
  const auto prop_id_2 = PropertyId::FromUint(2);
  const auto prop_id_4 = PropertyId::FromUint(4);
  const auto prop_id_5 = PropertyId::FromUint(5);
  // No properties
  {
    const auto result = AttemptToGetProperties(client, {{}}, {v_id, v_id_2}, {});
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 2);
    for (const auto &elem : result.result_row) {
      MG_ASSERT(elem.props.size() == 0);
    }
  }
  // All properties
  {
    const auto result = AttemptToGetProperties(client, std::nullopt, {v_id, v_id_2}, {});
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 2);
    for (const auto &elem : result.result_row) {
      MG_ASSERT(elem.props.size() == 4);
    }
  }
  {
    // Specific properties
    const auto result =
        AttemptToGetProperties(client, std::vector{prop_id_2, prop_id_4, prop_id_5}, {v_id, v_id_2, v_id_3}, {});
    MG_ASSERT(!result.error);
    MG_ASSERT(!result.result_row.empty());
    MG_ASSERT(result.result_row.size() == 3);
    for (const auto &elem : result.result_row) {
      MG_ASSERT(elem.props.size() == 3);
    }
  }
  {
    // Two properties from two vertices with a filter on unique_prop_5
    const auto result = AttemptToGetProperties(client, std::vector{prop_id_2, prop_id_4}, {v_id, v_id_2, v_id_5}, {},
                                               std::nullopt, unique_prop_val_5);
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 1);
  }
  {
    // One property from three vertices.
    const auto result = AttemptToGetProperties(client, std::vector{prop_id_2}, {v_id, v_id_2, v_id_3}, {});
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 3);
    MG_ASSERT(result.result_row[0].props.size() == 1);
    MG_ASSERT(result.result_row[1].props.size() == 1);
    MG_ASSERT(result.result_row[2].props.size() == 1);
  }
  {
    // Same as before but with limit of 1 row
    const auto result = AttemptToGetProperties(client, std::vector{prop_id_2}, {v_id, v_id_2, v_id_3}, {},
                                               std::make_optional<size_t>(1));
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 1);
  }
  {
    // Same as before but with a limit greater than the elements returned
    const auto result = AttemptToGetProperties(client, std::vector{prop_id_2}, std::vector{v_id, v_id_2, v_id_3}, {},
                                               std::make_optional<size_t>(5));
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 3);
  }
  {
    // Order by on `prop1` (descending)
    const auto result = AttemptToGetProperties(client, std::vector{prop_id_2}, {v_id, v_id_2, v_id_3}, {}, std::nullopt,
                                               std::nullopt, false, "prop1");
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 3);
    MG_ASSERT(result.result_row[0].vertex == v_id_3);
    MG_ASSERT(result.result_row[1].vertex == v_id_2);
    MG_ASSERT(result.result_row[2].vertex == v_id);
  }
  {
    // Order by and filter on >= unique_prop_val_3 && assert result row data members
    const auto result = AttemptToGetProperties(client, std::vector{prop_id_2}, {v_id, v_id_2, v_id_3, v_id_4, v_id_5},
                                               {}, std::nullopt, unique_prop_val_3, false, "prop1");
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 3);
    MG_ASSERT(result.result_row[0].vertex == v_id_5);
    MG_ASSERT(result.result_row[0].props.size() == 1);
    MG_ASSERT(result.result_row[0].props.front().second == prim_key_5.front());
    MG_ASSERT(result.result_row[0].props.size() == 1);
    MG_ASSERT(result.result_row[0].props.front().first == prop_id_2);
    MG_ASSERT(result.result_row[0].evaluated_expressions.size() == 1);
    MG_ASSERT(result.result_row[0].evaluated_expressions.front() == msgs::Value(true));

    MG_ASSERT(result.result_row[1].vertex == v_id_4);
    MG_ASSERT(result.result_row[1].props.size() == 1);
    MG_ASSERT(result.result_row[1].props.front().second == prim_key_4.front());
    MG_ASSERT(result.result_row[1].props.size() == 1);
    MG_ASSERT(result.result_row[1].props.front().first == prop_id_2);
    MG_ASSERT(result.result_row[1].evaluated_expressions.size() == 1);
    MG_ASSERT(result.result_row[1].evaluated_expressions.front() == msgs::Value(true));

    MG_ASSERT(result.result_row[2].vertex == v_id_3);
    MG_ASSERT(result.result_row[2].props.size() == 1);
    MG_ASSERT(result.result_row[2].props.front().second == prim_key_3.front());
    MG_ASSERT(result.result_row[2].props.size() == 1);
    MG_ASSERT(result.result_row[2].props.front().first == prop_id_2);
    MG_ASSERT(result.result_row[2].evaluated_expressions.size() == 1);
    MG_ASSERT(result.result_row[2].evaluated_expressions.front() == msgs::Value(true));
  }

  // Edges
  const auto edge_gid = GetUniqueInteger();
  const auto edge_type_id = EdgeTypeId::FromUint(GetUniqueInteger());
  const auto unique_edge_prop_id = 7;
  const auto edge_prop_val = GetUniqueInteger();
  MG_ASSERT(AttemptToAddEdgeWithProperties(client, unique_prop_val_1, unique_prop_val_2, edge_gid, unique_edge_prop_id,
                                           edge_prop_val, {edge_type_id}));
  const auto edge_gid_2 = GetUniqueInteger();
  const auto edge_prop_val_2 = GetUniqueInteger();
  MG_ASSERT(AttemptToAddEdgeWithProperties(client, unique_prop_val_3, unique_prop_val_4, edge_gid_2,
                                           unique_edge_prop_id, edge_prop_val_2, {edge_type_id}));
  const auto edge_prop_id = PropertyId::FromUint(unique_edge_prop_id);
  std::vector<msgs::EdgeId> edge_ids = {{edge_gid}, {edge_gid_2}};
  // No properties
  {
    const auto result = AttemptToGetProperties(client, {{}}, {v_id_2, v_id_3}, edge_ids);
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 2);
    for (const auto &elem : result.result_row) {
      MG_ASSERT(elem.props.size() == 0);
    }
  }
  // All properties
  {
    const auto result = AttemptToGetProperties(client, std::nullopt, {v_id_2, v_id_3}, edge_ids);
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 2);
    for (const auto &elem : result.result_row) {
      MG_ASSERT(elem.props.size() == 1);
    }
  }
  // Properties for two vertices
  {
    const auto result = AttemptToGetProperties(client, std::vector{edge_prop_id}, {v_id_2, v_id_3}, edge_ids);
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 2);
  }
  // Filter
  {
    const auto result = AttemptToGetProperties(client, std::vector{edge_prop_id}, {v_id_2, v_id_3}, edge_ids, {},
                                               {edge_prop_val}, true);
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 1);
    MG_ASSERT(result.result_row.front().edge);
    MG_ASSERT(result.result_row.front().edge.value().gid == edge_gid);
    MG_ASSERT(result.result_row.front().props.size() == 1);
    MG_ASSERT(result.result_row.front().props.front().second == msgs::Value(static_cast<int64_t>(edge_prop_val)));
  }
  // Order by
  {
    const auto result =
        AttemptToGetProperties(client, std::vector{edge_prop_id}, {v_id_2, v_id_3}, edge_ids, {}, {}, true, "e_prop");
    MG_ASSERT(!result.error);
    MG_ASSERT(result.result_row.size() == 2);
    MG_ASSERT(result.result_row[0].vertex == v_id_3);
    MG_ASSERT(result.result_row[0].edge);
    MG_ASSERT(result.result_row[0].edge.value().gid == edge_gid_2);
    MG_ASSERT(result.result_row[0].props.size() == 1);
    MG_ASSERT(result.result_row[0].props.front().second == msgs::Value(static_cast<int64_t>(edge_prop_val_2)));
    MG_ASSERT(result.result_row[0].evaluated_expressions.size() == 1);
    MG_ASSERT(result.result_row[0].evaluated_expressions.front() == msgs::Value(true));

    MG_ASSERT(result.result_row[1].vertex == v_id_2);
    MG_ASSERT(result.result_row[1].edge);
    MG_ASSERT(result.result_row[1].edge.value().gid == edge_gid);
    MG_ASSERT(result.result_row[1].props.size() == 1);
    MG_ASSERT(result.result_row[1].props.front().second == msgs::Value(static_cast<int64_t>(edge_prop_val)));
    MG_ASSERT(result.result_row[1].evaluated_expressions.size() == 1);
    MG_ASSERT(result.result_row[1].evaluated_expressions.front() == msgs::Value(true));
  }
}

}  // namespace

int TestMessages() {
  SimulatorConfig config{
      .drop_percent = 0,
      .perform_timeouts = false,
      .scramble_messages = false,
      .rng_seed = 0,
      .start_time = Time::min() + std::chrono::microseconds{256 * 1024},
      .abort_time = Time::max(),
  };

  auto simulator = Simulator(config);

  Io<SimulatorTransport> shard_server_io_1 = simulator.RegisterNew();
  shard_server_io_1.SetDefaultTimeout(std::chrono::seconds(1));
  const auto shard_server_1_address = shard_server_io_1.GetAddress();
  Io<SimulatorTransport> shard_server_io_2 = simulator.RegisterNew();
  shard_server_io_2.SetDefaultTimeout(std::chrono::seconds(1));
  const auto shard_server_2_address = shard_server_io_2.GetAddress();
  Io<SimulatorTransport> shard_server_io_3 = simulator.RegisterNew();
  shard_server_io_3.SetDefaultTimeout(std::chrono::seconds(1));
  const auto shard_server_3_address = shard_server_io_3.GetAddress();
  Io<SimulatorTransport> shard_client_io = simulator.RegisterNew();
  shard_client_io.SetDefaultTimeout(std::chrono::seconds(1));

  PropertyValue min_pk(static_cast<int64_t>(0));
  std::vector<PropertyValue> min_prim_key = {min_pk};

  PropertyValue max_pk(static_cast<int64_t>(10000000));
  std::vector<PropertyValue> max_prim_key = {max_pk};

  std::vector<SchemaProperty> schema_prop = {get_schema_property()};

  const coordinator::Hlc shard_version{0};
  auto shard_ptr1 =
      std::make_unique<Shard>(get_primary_label(), min_prim_key, max_prim_key, schema_prop, shard_version);
  auto shard_ptr2 =
      std::make_unique<Shard>(get_primary_label(), min_prim_key, max_prim_key, schema_prop, shard_version);
  auto shard_ptr3 =
      std::make_unique<Shard>(get_primary_label(), min_prim_key, max_prim_key, schema_prop, shard_version);

  shard_ptr1->StoreMapping(
      {{1, "label"}, {2, "prop1"}, {3, "label1"}, {4, "prop2"}, {5, "prop3"}, {6, "prop4"}, {7, "e_prop"}});
  shard_ptr2->StoreMapping(
      {{1, "label"}, {2, "prop1"}, {3, "label1"}, {4, "prop2"}, {5, "prop3"}, {6, "prop4"}, {7, "e_prop"}});
  shard_ptr3->StoreMapping(
      {{1, "label"}, {2, "prop1"}, {3, "label1"}, {4, "prop2"}, {5, "prop3"}, {6, "prop4"}, {7, "e_prop"}});

  std::vector<Address> address_for_1{shard_server_2_address, shard_server_3_address};
  std::vector<Address> address_for_2{shard_server_1_address, shard_server_3_address};
  std::vector<Address> address_for_3{shard_server_1_address, shard_server_2_address};

  utils::Sender<msgs::InitializeSplitShard> local_shard_manager_sender{[](const auto &elem) {}};
  ConcreteShardRsm shard_server1(std::move(shard_server_io_1), address_for_1,
                                 ShardRsm(std::move(shard_ptr1), local_shard_manager_sender));
  ConcreteShardRsm shard_server2(std::move(shard_server_io_2), address_for_2,
                                 ShardRsm(std::move(shard_ptr2), local_shard_manager_sender));
  ConcreteShardRsm shard_server3(std::move(shard_server_io_3), address_for_3,
                                 ShardRsm(std::move(shard_ptr3), local_shard_manager_sender));

  auto server_thread1 = std::jthread([&shard_server1]() { shard_server1.Run(); });
  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_1_address);

  auto server_thread2 = std::jthread([&shard_server2]() { shard_server2.Run(); });
  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_2_address);

  auto server_thread3 = std::jthread([&shard_server3]() { shard_server3.Run(); });
  simulator.IncrementServerCountAndWaitForQuiescentState(shard_server_3_address);

  std::cout << "Beginning test after servers have become quiescent." << std::endl;

  std::vector server_addrs = {shard_server_1_address, shard_server_2_address, shard_server_3_address};
  ShardClient client(shard_client_io, shard_server_1_address, server_addrs);

  // Vertex tests
  TestCreateVertices(client);
  TestCreateAndDeleteVertices(client);
  TestCreateAndUpdateVertices(client);

  // Edge tests
  TestCreateEdge(client);
  TestCreateAndDeleteEdge(client);
  TestUpdateEdge(client);

  // ScanAll tests
  TestScanAllOneGo(client);
  TestScanAllWithSmallBatchSize(client);

  // ExpandOne tests
  TestExpandOneGraphOne(client);
  TestExpandOneGraphTwo(client);

  // GetProperties tests
  TestGetProperties(client);
  simulator.ShutDown();

  SimulatorStats stats = simulator.Stats();

  std::cout << "total messages:     " << stats.total_messages << std::endl;
  std::cout << "dropped messages:   " << stats.dropped_messages << std::endl;
  std::cout << "timed out requests: " << stats.timed_out_requests << std::endl;
  std::cout << "total requests:     " << stats.total_requests << std::endl;
  std::cout << "total responses:    " << stats.total_responses << std::endl;
  std::cout << "simulator ticks:    " << stats.simulator_ticks << std::endl;

  std::cout << "========================== SUCCESS :) ==========================" << std::endl;

  return 0;
}

}  // namespace memgraph::storage::v3::tests

int main() { return memgraph::storage::v3::tests::TestMessages(); }
