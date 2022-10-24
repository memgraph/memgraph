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

#include "query/v2/accessors.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/id_types.hpp"

namespace memgraph::query::v2::accessors {
EdgeAccessor::EdgeAccessor(Edge edge) : edge(std::move(edge)) {}

EdgeTypeId EdgeAccessor::EdgeType() const { return edge.type.id; }

const std::vector<std::pair<PropertyId, Value>> &EdgeAccessor::Properties() const {
  return edge.properties;
  //    std::map<std::string, TypedValue> res;
  //    for (const auto &[name, value] : *properties) {
  //      res[name] = ValueToTypedValue(value);
  //    }
  //    return res;
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Value EdgeAccessor::GetProperty(const std::string & /*prop_name*/) const {
  // TODO(kostasrim) fix this
  return {};
}

const Edge &EdgeAccessor::GetEdge() const { return edge; }

bool EdgeAccessor::IsCycle() const { return edge.src == edge.dst; };

VertexAccessor EdgeAccessor::To() const { return VertexAccessor(Vertex{edge.dst}, {}); }

VertexAccessor EdgeAccessor::From() const { return VertexAccessor(Vertex{edge.src}, {}); }

VertexAccessor::VertexAccessor(Vertex v, std::vector<std::pair<PropertyId, Value>> props)
    : vertex(std::move(v)), properties(std::move(props)) {}

Label VertexAccessor::PrimaryLabel() const { return vertex.id.first; }

const msgs::VertexId &VertexAccessor::Id() const { return vertex.id; }

std::vector<Label> VertexAccessor::Labels() const { return vertex.labels; }

bool VertexAccessor::HasLabel(Label &label) const {
  return std::find_if(vertex.labels.begin(), vertex.labels.end(),
                      [label](const auto &l) { return l.id == label.id; }) != vertex.labels.end();
}

const std::vector<std::pair<PropertyId, Value>> &VertexAccessor::Properties() const { return properties; }

Value VertexAccessor::GetProperty(PropertyId prop_id) const {
  return std::find_if(properties.begin(), properties.end(), [&](auto &pr) { return prop_id == pr.first; })->second;
  //    return ValueToTypedValue(properties[prop_name]);
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Value VertexAccessor::GetProperty(const std::string & /*prop_name*/) const {
  // TODO(kostasrim) Add string mapping
  return {};
  //    return ValueToTypedValue(properties[prop_name]);
}

msgs::Vertex VertexAccessor::GetVertex() const { return vertex; }

}  // namespace memgraph::query::v2::accessors
