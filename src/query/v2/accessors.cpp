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

Value EdgeAccessor::GetProperty(const std::string &prop_name) {
  MG_ASSERT(properties.contains(prop_name));
  return properties[prop_name];
}

VertexAccessor::VertexAccessor(Vertex v, std::map<std::string, Value> props)
    : vertex(std::move(v)), properties(std::move(props)) {}

std::vector<Label> VertexAccessor::Labels() const { return vertex.labels; }

bool VertexAccessor::HasLabel(Label &label) const {
  return std::find_if(vertex.labels.begin(), vertex.labels.end(),
                      [label](const auto &l) { return l.id == label.id; }) != vertex.labels.end();
}

std::map<std::string, Value> VertexAccessor::Properties() const {
  //    std::map<std::string, TypedValue> res;
  //    for (const auto &[name, value] : *properties) {
  //      res[name] = ValueToTypedValue(value);
  //    }
  //    return res;
  return properties;
}

Value VertexAccessor::GetProperty(const std::string &prop_name) {
  MG_ASSERT(properties.contains(prop_name));
  return Value(properties[prop_name]);
  //    return ValueToTypedValue(properties[prop_name]);
}

}  // namespace memgraph::query::v2::accessors
