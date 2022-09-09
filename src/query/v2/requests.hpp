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
#include <iostream>
#include <map>
#include <optional>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "coordinator/hybrid_logical_clock.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"

using memgraph::coordinator::Hlc;
using memgraph::storage::v3::LabelId;
namespace requests {

struct Label {
  LabelId id;
  friend bool operator==(const Label &lhs, const Label &rhs) { return lhs.id == rhs.id; }
};

// TODO(kostasrim) update this with CompoundKey, same for the rest of the file.
using PrimaryKey = std::vector<memgraph::storage::v3::PropertyValue>;

struct VertexId {
  Label primary_label;
  PrimaryKey primary_key;
  friend bool operator==(const VertexId &lhs, const VertexId &rhs) {
    return (lhs.primary_label == rhs.primary_label) && (lhs.primary_key == rhs.primary_key);
  }
};

using Gid = size_t;
using PropertyId = memgraph::storage::v3::PropertyId;

struct EdgeType {
  uint64_t id;
  friend bool operator==(const EdgeType &lhs, const EdgeType &rhs) = default;
};

struct EdgeId {
  VertexId src;
  VertexId dst;
  Gid gid;
};

struct Vertex {
  VertexId id;
  std::vector<Label> labels;
  friend bool operator==(const Vertex &lhs, const Vertex &rhs) {
    return (lhs.id == rhs.id) && (lhs.labels == rhs.labels);
  }
};

struct Edge {
  EdgeId edge_id;
  EdgeType type;
  friend bool operator==(const Edge &lhs, const Edge &rhs) {
    return (lhs.edge_id.src == rhs.edge_id.src) && (lhs.edge_id.dst == rhs.edge_id.dst) && (lhs.type == rhs.type);
  }
};

struct PathPart {
  Vertex dst;
  Gid edge;
};

struct Path {
  Vertex src;
  std::vector<PathPart> parts;
};

struct Null {};

struct Value {
  Value() : type(NILL), null_v{} {};
  ~Value(){};

  enum Type { NILL, BOOL, INT64, DOUBLE, STRING, LIST, MAP, VERTEX, EDGE, PATH };
  union {
    Null null_v;
    bool bool_v;
    int64_t int_v;
    double double_v;
    std::string string_v;
    std::vector<Value> list_v;
    std::map<std::string, Value> map_v;
    Vertex vertex_v;
    Edge edge_v;
    Path path_v;
  };

  Type type;

  // copy ctor needed.
  Value(const Value &value) : type(value.type) {
    switch (value.type) {
      case NILL:
        null_v = {};
        break;
      case BOOL:
        bool_v = value.bool_v;
        break;
      case INT64:
        int_v = value.int_v;
        break;
      case DOUBLE:
        double_v = value.double_v;
        break;
      case STRING:
        string_v = value.string_v;
        break;
      case LIST:
        list_v = value.list_v;
        break;
      case MAP:
        map_v = value.map_v;
        break;
      case VERTEX:
        vertex_v = value.vertex_v;
        break;
      case EDGE:
        edge_v = value.edge_v;
        break;
      case PATH:
        path_v = value.path_v;
        break;
    }
  }
  //  Value(Value &&other) noexcept {};

  explicit Value(const bool val) : bool_v(val), type(BOOL){};
  explicit Value(const int64_t val) : int_v(val), type(INT64){};
  explicit Value(const double val) : double_v(val), type(DOUBLE){};
  explicit Value(const std::string &val) : string_v(val), type(STRING){};

  explicit Value(std::vector<Value> &&val) : list_v(std::move(val)), type(LIST){};
  explicit Value(std::map<std::string, Value> &&val) : map_v(std::move(val)), type(MAP){};

  explicit Value(const Vertex &val) : vertex_v(val), type(VERTEX){};
  explicit Value(const Edge &val) : edge_v(val), type(EDGE){};
  explicit Value(const Path &val) : path_v(val), type(PATH){};

  Value &operator=(const Value &value) {
    if (&value == this) {
      return *this;
    }
    type = value.type;
    switch (value.type) {
      case NILL:
        null_v = {};
        break;
      case BOOL:
        bool_v = value.bool_v;
        break;
      case INT64:
        int_v = value.int_v;
        break;
      case DOUBLE:
        double_v = value.double_v;
        break;
      case STRING:
        string_v = value.string_v;
        break;
      case LIST:
        list_v = value.list_v;
        break;
      case MAP:
        map_v = value.map_v;
        break;
      case VERTEX:
        vertex_v = value.vertex_v;
        break;
      case EDGE:
        edge_v = value.edge_v;
        break;
      case PATH:
        path_v = value.path_v;
        break;
    }
    return *this;
  }

  friend bool operator==(const Value &lhs, const Value &rhs) {
    if (lhs.type != rhs.type) {
      return false;
    }
    switch (lhs.type) {
      case NILL:
        return true;
      case BOOL:
        return lhs.bool_v == rhs.bool_v;
      case INT64:
        return lhs.int_v == rhs.int_v;
      case DOUBLE:
        return lhs.double_v == rhs.double_v;
      case STRING:
        return lhs.string_v == rhs.string_v;
      case LIST:
        return lhs.list_v == rhs.list_v;
      case MAP:
        return lhs.map_v == rhs.map_v;
      case VERTEX:
        return lhs.vertex_v == rhs.vertex_v;
      case EDGE:
        return lhs.edge_v == rhs.edge_v;
      case PATH:
        return true;
    }
  }
};

// this one
struct ValuesMap {
  std::unordered_map<PropertyId, Value> values_map;
};

struct MappedValues {
  std::vector<ValuesMap> values_map;
};

struct ListedValues {
  std::vector<std::vector<Value>> properties;
};

using Values = std::variant<ListedValues, MappedValues>;

struct Expression {
  std::string expression;
};

struct Filter {
  std::string filter_expression;
};

enum class OrderingDirection { ASCENDING = 1, DESCENDING = 2 };

struct OrderBy {
  Expression expression;
  OrderingDirection direction;
};

enum class StorageView { OLD = 0, NEW = 1 };

struct ScanVerticesRequest {
  Hlc transaction_id;
  VertexId start_id;
  std::optional<std::vector<PropertyId>> props_to_return;
  std::optional<std::vector<std::string>> filter_expressions;
  std::optional<size_t> batch_limit;
  StorageView storage_view;
};

struct ScanResultRow {
  Value vertex;
  // empty is no properties returned
  std::map<PropertyId, Value> props;
};

struct ScanVerticesResponse {
  bool success;
  std::optional<VertexId> next_start_id;
  std::vector<ScanResultRow> results;
};

using VertexOrEdgeIds = std::variant<VertexId, EdgeId>;

struct GetPropertiesRequest {
  Hlc transaction_id;
  VertexOrEdgeIds vertex_or_edge_ids;
  std::vector<PropertyId> property_ids;
  std::vector<Expression> expressions;
  bool only_unique = false;
  std::optional<std::vector<OrderBy>> order_by;
  std::optional<size_t> limit;
  std::optional<Filter> filter;
};

struct GetPropertiesResponse {
  bool success;
  Values values;
};

enum class EdgeDirection : uint8_t { OUT = 1, IN = 2, BOTH = 3 };

struct VertexEdgeId {
  VertexId vertex_id;
  std::optional<EdgeId> next_id;
};

struct ExpandOneRequest {
  Hlc transaction_id;
  std::vector<VertexId> src_vertices;
  std::vector<EdgeType> edge_types;
  EdgeDirection direction;
  bool only_unique_neighbor_rows = false;
  //  The empty optional means return all of the properties, while an empty
  //  list means do not return any properties
  //  TODO(antaljanosbenjamin): All of the special values should be communicated through a single vertex object
  //                            after schema is implemented
  //  Special values are accepted:
  //  * __mg__labels
  std::optional<std::vector<PropertyId>> src_vertex_properties;
  //  TODO(antaljanosbenjamin): All of the special values should be communicated through a single vertex object
  //                            after schema is implemented
  //  Special values are accepted:
  //  * __mg__dst_id (Vertex, but without labels)
  //  * __mg__type (binary)
  std::optional<std::vector<PropertyId>> edge_properties;
  //  QUESTION(antaljanosbenjamin): Maybe also add possibility to expressions evaluated on the source vertex?
  //  List of expressions evaluated on edges
  std::vector<Expression> expressions;
  std::optional<std::vector<OrderBy>> order_by;
  std::optional<size_t> limit;
  std::optional<Filter> filter;
};

struct ExpandOneResultRow {
  // NOTE: This struct could be a single Values with columns something like this:
  // src_vertex(Vertex), vertex_prop1(Value), vertex_prop2(Value), edges(list<Value>)
  // where edges might be a list of:
  // 1. list<Value> if only a defined list of edge properties are returned
  // 2. map<binary, Value> if all of the edge properties are returned
  // The drawback of this is currently the key of the map is always interpreted as a string in Value, not as an
  // integer, which should be in case of mapped properties.
  Vertex src_vertex;
  std::optional<Values> src_vertex_properties;
  Values edges;
};

struct ExpandOneResponse {
  std::vector<ExpandOneResultRow> result;
};

// Update related messages
struct UpdateVertexProp {
  VertexId vertex;
  std::vector<std::pair<PropertyId, Value>> property_updates;
};

struct UpdateEdgeProp {
  Edge edge;
  std::vector<std::pair<PropertyId, Value>> property_updates;
};

/*
 * Vertices
 */
struct NewVertex {
  std::vector<Label> label_ids;
  PrimaryKey primary_key;
  LabelId primary_label_id;
  std::vector<std::pair<PropertyId, Value>> properties;
};

struct NewVertexLabel {
  std::string label;
  PrimaryKey primary_key;
  std::map<PropertyId, Value> properties;
};

struct CreateVerticesRequest {
  std::string label;
  Hlc transaction_id;
  std::vector<NewVertex> new_vertices;
};

struct CreateVerticesResponse {
  bool success;
};

struct DeleteVerticesRequest {
  enum class DeletionType { DELETE, DETACH_DELETE };
  Hlc transaction_id;
  std::vector<std::vector<Value>> primary_keys;
  DeletionType deletion_type;
};

struct DeleteVerticesResponse {
  bool success;
};

struct UpdateVerticesRequest {
  Hlc transaction_id;
  std::vector<UpdateVertexProp> new_properties;
};

struct UpdateVerticesResponse {
  bool success;
};

/*
 * Edges
 */
struct CreateEdgesRequest {
  Hlc transaction_id;
  std::vector<Edge> edges;
};

struct CreateEdgesResponse {
  bool success;
};

struct DeleteEdgesRequest {
  Hlc transaction_id;
  std::vector<Edge> edges;
};

struct DeleteEdgesResponse {
  bool success;
};

struct UpdateEdgesRequest {
  Hlc transaction_id;
  std::vector<UpdateEdgeProp> new_properties;
};

struct UpdateEdgesResponse {
  bool success;
};

using ReadRequests = std::variant<ExpandOneRequest, GetPropertiesRequest, ScanVerticesRequest>;
using ReadResponses = std::variant<ExpandOneResponse, GetPropertiesResponse, ScanVerticesResponse>;

using WriteRequests = std::variant<CreateVerticesRequest, DeleteVerticesRequest, UpdateVerticesRequest,
                                   CreateEdgesRequest, DeleteEdgesRequest, UpdateEdgesRequest>;
using WriteResponses = std::variant<CreateVerticesResponse, DeleteVerticesResponse, UpdateVerticesResponse,
                                    CreateEdgesResponse, DeleteEdgesResponse, UpdateEdgesResponse>;
}  // namespace requests
