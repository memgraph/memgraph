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

namespace memgraph::query::v2::accessors {
EdgeAccessor::EdgeAccessor(Edge edge, std::map<std::string, Value> props)
    : edge(std::move(edge)), properties(std::move(props)) {}

std::string EdgeAccessor::EdgeType() const { return edge.type.name; }

std::map<std::string, Value> EdgeAccessor::Properties() const {
  return properties;
  //    std::map<std::string, TypedValue> res;
  //    for (const auto &[name, value] : *properties) {
  //      res[name] = ValueToTypedValue(value);
  //    }
  //    return res;
}

Value EdgeAccessor::GetProperty(const std::string &prop_name) const {
  MG_ASSERT(properties.contains(prop_name));
  return properties[prop_name];
}

requests::Edge EdgeAccessor::GetEdge() const { return edge; }

VertexAccessor EdgeAccessor::To() const { return VertexAccessor(Vertex{edge.dst}, {}); }

VertexAccessor EdgeAccessor::From() const { return VertexAccessor(Vertex{edge.src}, {}); }

VertexAccessor::VertexAccessor(Vertex v, std::map<requests::PropertyId, Value> props)
    : vertex(std::move(v)), properties(std::move(props)) {}

std::vector<Label> VertexAccessor::Labels() const { return vertex.labels; }

bool VertexAccessor::HasLabel(Label &label) const {
  return std::find_if(vertex.labels.begin(), vertex.labels.end(),
                      [label](const auto &l) { return l.id == label.id; }) != vertex.labels.end();
}

std::map<requests::PropertyId, Value> VertexAccessor::Properties() const {
  //    std::map<std::string, TypedValue> res;
  //    for (const auto &[name, value] : *properties) {
  //      res[name] = ValueToTypedValue(value);
  //    }
  //    return res;
  return properties;
}

Value VertexAccessor::GetProperty(requests::PropertyId prop_id) const {
  MG_ASSERT(properties.contains(prop_id));
  return properties[prop_id];
  //    return ValueToTypedValue(properties[prop_name]);
}

Value VertexAccessor::GetProperty(const std::string & /*prop_name*/) const {
  // TODO(kostasrim) Add string mapping
  auto prop_id = requests::PropertyId::FromUint(0);
  MG_ASSERT(properties.contains(prop_id));
  return properties[prop_id];
  //    return ValueToTypedValue(properties[prop_name]);
}

requests::Vertex VertexAccessor::GetVertex() const { return vertex; }

}  // namespace memgraph::query::v2::accessors
