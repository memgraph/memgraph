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

#include <iostream>
#include <optional>
#include <unordered_map>
#include <variant>
#include <vector>

// TODO(kostasrim) update this with CompoundKey, same for the rest of the file.
using VertexId = size_t;
using Gid = size_t;

using Hlc = memgraph::coordinator::Hlc;

struct Label {
  size_t id;
};

struct EdgeType {
  std::string name;
};

struct EdgeId {
  VertexId id;
  Gid gid;
};

struct Vertex {
  VertexId id;
  std::vector<Label> labels;
};

struct Edge {
  VertexId src;
  VertexId dst;
  EdgeType type;
};

struct PathPart {
  Vertex dst;
  Edge edge;
};

struct Path {
  Vertex src;
  std::vector<PathPart> parts;
}

using Value = TypedValue<Vertex, Edge, Path>;

struct Null {};

struct ValuesMap {
  std::unordered_map<size_t, Value> values_map;
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

enum OrderingDirection { ASCENDING = 1, DESCENDING = 2 };

struct OrderBy {
  Expression expression;
  OrderingDirection direction;
};

enum class StorageView { OLD = 0, NEW = 1 };

struct ScanVerticesRequest {
  Hlc transaction_id;
  size_t start_id;
  std::optional<std::vector<std::string>> props_to_return;
  std::optional<std::vector<std::string>> filter_expressions;
  std::optional<size_t> batch_limit;
  StorageView storage_view;
}

struct ScanVerticesResponse {
  bool success;
  Values values;
  std::optional<std::unordered_map<size_t, std::string>> property_name_map;
  std::optional<VertexId> next_start_id;
};

using VertexOrEdgeIds = std::variant<VertexId, EdgeId>;

struct GetPropertiesRequest {
  Hlc transaction_id;
  VertexOrEdgeIds vertex_or_edge_ids;
  std::vector<std::string> property_names;
  std::vector<Expression> expressions;
  bool only_unique = false;
  std::optional<std::vector<OrderBy>> order_by;
  std::optional<size_t> limit;
  std::optional<Filter> filter
};

struct GetPropertiesResponse {
  Values values;
  std::optional<std::unordered_map<size_t, std::string>> property_name_map;
};

enum EdgeDirection { OUT = 1; IN = 2; BOTH = 3; };

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
  std::optional<std::vector<std::string>> src_vertex_properties;
  //  TODO(antaljanosbenjamin): All of the special values should be communicated through a single vertex object
  //                            after schema is implemented
  //  Special values are accepted:
  //  * __mg__dst_id (Vertex, but without labels)
  //  * __mg__type (binary)
  std::optional<std::vector<std::string>> edge_properties;
  //  QUESTION(antaljanosbenjamin): Maybe also add possibility to expressions evaluated on the source vertex?
  //  List of expressions evaluated on edges
  std::vector<Expression> expressions;
  std::optional<std::vector<OrderBy>> order_by;
  std::optional<size_t> limit;
  std::optional<Fitler> filter;
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
  // This approach might not suit the expand with per shard parrallelization,
  // because the property_name_map has to be accessed from multiple threads
  // in order to avoid duplicated keys (two threads might map the same
  // property with different numbers) and multiple passes (to unify the
  // mapping amond result returned from different shards).
  std::vector<ExpandOneResultRow> result;
  std::optional<std::unordered_map<size_t, std::string>> property_name_map;
};

struct NewVertex {
  std::vector<size_t> label_ids;
  std::map<size_t, Value> properties;
};

struct CreateVerticesRequest {
  Hlc transaction_id;
  std::unordered_map<size_t, std::string> labels_name_map;
  std::unordered_map<size_t, std::string> property_name_map;
  std::vector<NewVertex> new_vertices;
};

struct CreateVerticesResponse {
  bool success;
};

using ReadRequests = std::variant<ExpandOneRequest, GetPropertiesRequest, ScanVerticesRequest>;
using ReadResponses = std::variant<ExpandOneResponse, GetPropertiesResponse, ScanVerticesResponse>;

using WriteRequests = CreateVerticesRequest;
using WriteResponses = CreateVerticesResponse;
