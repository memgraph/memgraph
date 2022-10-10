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
#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "coordinator/hybrid_logical_clock.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/property_value.hpp"

namespace memgraph::msgs {

using coordinator::Hlc;
using storage::v3::LabelId;

struct Value;

struct Label {
  LabelId id;
  friend bool operator==(const Label &lhs, const Label &rhs) { return lhs.id == rhs.id; }
};

// TODO(kostasrim) update this with CompoundKey, same for the rest of the file.
using PrimaryKey = std::vector<Value>;
using VertexId = std::pair<Label, PrimaryKey>;

inline bool operator==(const VertexId &lhs, const VertexId &rhs) {
  return (lhs.first == rhs.first) && (lhs.second == rhs.second);
}

using Gid = size_t;
using PropertyId = memgraph::storage::v3::PropertyId;
using EdgeTypeId = memgraph::storage::v3::EdgeTypeId;

struct EdgeType {
  EdgeTypeId id;
  friend bool operator==(const EdgeType &lhs, const EdgeType &rhs) = default;
};

struct EdgeId {
  Gid gid;

  friend bool operator==(const EdgeId &lhs, const EdgeId &rhs) { return lhs.gid == rhs.gid; }
  friend bool operator<(const EdgeId &lhs, const EdgeId &rhs) { return lhs.gid < rhs.gid; }
};

struct Edge {
  VertexId src;
  VertexId dst;
  std::vector<std::pair<PropertyId, Value>> properties;
  EdgeId id;
  EdgeType type;
  friend bool operator==(const Edge &lhs, const Edge &rhs) { return lhs.id == rhs.id; }
};

struct Vertex {
  VertexId id;
  std::vector<Label> labels;
  friend bool operator==(const Vertex &lhs, const Vertex &rhs) { return lhs.id == rhs.id; }
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
  Value() : null_v{} {}

  explicit Value(const bool val) : type(Type::Bool), bool_v(val) {}
  explicit Value(const int64_t val) : type(Type::Int64), int_v(val) {}
  explicit Value(const double val) : type(Type::Double), double_v(val) {}

  explicit Value(const Vertex val) : type(Type::Vertex), vertex_v(val) {}
  explicit Value(const Edge val) : type(Type::Edge), edge_v(val) {}

  explicit Value(const std::string &val) : type(Type::String) { new (&string_v) std::string(val); }
  explicit Value(const char *val) : type(Type::String) { new (&string_v) std::string(val); }

  explicit Value(const std::vector<Value> &val) : type(Type::List) { new (&list_v) std::vector<Value>(val); }

  explicit Value(const std::map<std::string, Value> &val) : type(Type::Map) {
    new (&map_v) std::map<std::string, Value>(val);
  }

  explicit Value(std::string &&val) noexcept : type(Type::String) { new (&string_v) std::string(std::move(val)); }

  explicit Value(std::vector<Value> &&val) noexcept : type(Type::List) {
    new (&list_v) std::vector<Value>(std::move(val));
  }
  explicit Value(std::map<std::string, Value> &&val) noexcept : type(Type::Map) {
    new (&map_v) std::map<std::string, Value>(std::move(val));
  }

  ~Value() { DestroyValue(); }

  void DestroyValue() noexcept {
    switch (type) {
      case Type::Null:
      case Type::Bool:
      case Type::Int64:
      case Type::Double:
        return;

      case Type::String:
        std::destroy_at(&string_v);
        return;
      case Type::List:
        std::destroy_at(&list_v);
        return;
      case Type::Map:
        std::destroy_at(&map_v);
        return;

      case Type::Vertex:
        std::destroy_at(&vertex_v);
        return;
      case Type::Path:
        std::destroy_at(&path_v);
        return;
      case Type::Edge:
        std::destroy_at(&edge_v);
    }
  }

  Value(const Value &other) : type(other.type) {
    switch (other.type) {
      case Type::Null:
        return;
      case Type::Bool:
        this->bool_v = other.bool_v;
        return;
      case Type::Int64:
        this->int_v = other.int_v;
        return;
      case Type::Double:
        this->double_v = other.double_v;
        return;
      case Type::String:
        new (&string_v) std::string(other.string_v);
        return;
      case Type::List:
        new (&list_v) std::vector<Value>(other.list_v);
        return;
      case Type::Map:
        new (&map_v) std::map<std::string, Value>(other.map_v);
        return;
      case Type::Vertex:
        new (&vertex_v) Vertex(other.vertex_v);
        return;
      case Type::Edge:
        new (&edge_v) Edge(other.edge_v);
        return;
      case Type::Path:
        new (&path_v) Path(other.path_v);
        return;
    }
  }

  Value(Value &&other) noexcept : type(other.type) {
    switch (other.type) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int64:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(std::move(other.string_v));
        break;
      case Type::List:
        new (&list_v) std::vector<Value>(std::move(other.list_v));
        break;
      case Type::Map:
        new (&map_v) std::map<std::string, Value>(std::move(other.map_v));
        break;
      case Type::Vertex:
        new (&vertex_v) Vertex(std::move(other.vertex_v));
        break;
      case Type::Edge:
        new (&edge_v) Edge(std::move(other.edge_v));
        break;
      case Type::Path:
        new (&path_v) Path(std::move(other.path_v));
        break;
    }

    other.DestroyValue();
    other.type = Type::Null;
  }

  Value &operator=(const Value &other) {
    if (this == &other) return *this;

    DestroyValue();
    type = other.type;

    switch (other.type) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int64:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(other.string_v);
        break;
      case Type::List:
        new (&list_v) std::vector<Value>(other.list_v);
        break;
      case Type::Map:
        new (&map_v) std::map<std::string, Value>(other.map_v);
        break;
      case Type::Vertex:
        new (&vertex_v) Vertex(other.vertex_v);
        break;
      case Type::Edge:
        new (&edge_v) Edge(other.edge_v);
        break;
      case Type::Path:
        new (&path_v) Path(other.path_v);
        break;
    }

    return *this;
  }

  Value &operator=(Value &&other) noexcept {
    if (this == &other) return *this;

    DestroyValue();
    type = other.type;

    switch (other.type) {
      case Type::Null:
        break;
      case Type::Bool:
        this->bool_v = other.bool_v;
        break;
      case Type::Int64:
        this->int_v = other.int_v;
        break;
      case Type::Double:
        this->double_v = other.double_v;
        break;
      case Type::String:
        new (&string_v) std::string(std::move(other.string_v));
        break;
      case Type::List:
        new (&list_v) std::vector<Value>(std::move(other.list_v));
        break;
      case Type::Map:
        new (&map_v) std::map<std::string, Value>(std::move(other.map_v));
        break;
      case Type::Vertex:
        new (&vertex_v) Vertex(std::move(other.vertex_v));
        break;
      case Type::Edge:
        new (&edge_v) Edge(std::move(other.edge_v));
        break;
      case Type::Path:
        new (&path_v) Path(std::move(other.path_v));
        break;
    }

    other.DestroyValue();
    other.type = Type::Null;

    return *this;
  }
  enum class Type : uint8_t { Null, Bool, Int64, Double, String, List, Map, Vertex, Edge, Path };
  Type type{Type::Null};
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

  friend bool operator==(const Value &lhs, const Value &rhs) {
    if (lhs.type != rhs.type) {
      return false;
    }
    switch (lhs.type) {
      case Value::Type::Null:
        return true;
      case Value::Type::Bool:
        return lhs.bool_v == rhs.bool_v;
      case Value::Type::Int64:
        return lhs.int_v == rhs.int_v;
      case Value::Type::Double:
        return lhs.double_v == rhs.double_v;
      case Value::Type::String:
        return lhs.string_v == rhs.string_v;
      case Value::Type::List:
        return lhs.list_v == rhs.list_v;
      case Value::Type::Map:
        return lhs.map_v == rhs.map_v;
      case Value::Type::Vertex:
        return lhs.vertex_v == rhs.vertex_v;
      case Value::Type::Edge:
        return lhs.edge_v == rhs.edge_v;
      case Value::Type::Path:
        return true;
    }
  }
};

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
  StorageView storage_view{StorageView::NEW};
};

struct ScanResultRow {
  Vertex vertex;
  // empty() is no properties returned
  std::vector<std::pair<PropertyId, Value>> props;
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
  // TODO(antaljanosbenjamin): Filtering based on the id of the other end of the edge?
  Hlc transaction_id;
  std::vector<VertexId> src_vertices;
  std::vector<EdgeType> edge_types;
  EdgeDirection direction{EdgeDirection::OUT};
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
  struct EdgeWithAllProperties {
    VertexId other_end;
    EdgeType type;
    Gid gid;
    std::map<PropertyId, Value> properties;
  };

  struct EdgeWithSpecificProperties {
    VertexId other_end;
    EdgeType type;
    Gid gid;
    std::vector<Value> properties;
  };

  // NOTE: This struct could be a single Values with columns something like this:
  // src_vertex(Vertex), vertex_prop1(Value), vertex_prop2(Value), edges(list<Value>)
  // where edges might be a list of:
  // 1. list<Value> if only a defined list of edge properties are returned
  // 2. map<binary, Value> if all of the edge properties are returned
  // The drawback of this is currently the key of the map is always interpreted as a string in Value, not as an
  // integer, which should be in case of mapped properties.
  Vertex src_vertex;
  std::map<PropertyId, Value> src_vertex_properties;

  // NOTE: If the desired edges are specified in the request,
  // edges_with_specific_properties will have a value and it will
  // return the properties as a vector of property values. The order
  // of the values returned should be the same as the PropertyIds
  // were defined in the request.
  std::vector<EdgeWithAllProperties> in_edges_with_all_properties;
  std::vector<EdgeWithSpecificProperties> in_edges_with_specific_properties;
  std::vector<EdgeWithAllProperties> out_edges_with_all_properties;
  std::vector<EdgeWithSpecificProperties> out_edges_with_specific_properties;
};

struct ExpandOneResponse {
  std::vector<ExpandOneResultRow> result;
};

struct UpdateVertexProp {
  PrimaryKey primary_key;
  std::vector<std::pair<PropertyId, Value>> property_updates;
};

struct UpdateEdgeProp {
  EdgeId edge_id;
  VertexId src;
  VertexId dst;
  std::vector<std::pair<PropertyId, Value>> property_updates;
};

/*
 * Vertices
 */
struct NewVertex {
  std::vector<Label> label_ids;
  PrimaryKey primary_key;
  std::vector<std::pair<PropertyId, Value>> properties;
};

struct NewVertexLabel {
  std::string label;
  PrimaryKey primary_key;
  std::vector<std::pair<PropertyId, Value>> properties;
};

struct CreateVerticesRequest {
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
// No need for specifying direction since it has to be in one, and src and dest
// vertices clearly communicate the direction
struct NewExpand {
  EdgeId id;
  EdgeType type;
  VertexId src_vertex;
  VertexId dest_vertex;
  std::vector<std::pair<PropertyId, Value>> properties;
};

struct CreateExpandRequest {
  Hlc transaction_id;
  std::vector<NewExpand> new_expands;
};

struct CreateExpandResponse {
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

struct CommitRequest {
  Hlc transaction_id;
  Hlc commit_timestamp;
};

struct CommitResponse {
  bool success;
};

using ReadRequests = std::variant<ExpandOneRequest, GetPropertiesRequest, ScanVerticesRequest>;
using ReadResponses = std::variant<ExpandOneResponse, GetPropertiesResponse, ScanVerticesResponse>;

using WriteRequests = std::variant<CreateVerticesRequest, DeleteVerticesRequest, UpdateVerticesRequest,
                                   CreateExpandRequest, DeleteEdgesRequest, UpdateEdgesRequest, CommitRequest>;
using WriteResponses = std::variant<CreateVerticesResponse, DeleteVerticesResponse, UpdateVerticesResponse,
                                    CreateExpandResponse, DeleteEdgesResponse, UpdateEdgesResponse, CommitResponse>;

}  // namespace memgraph::msgs
