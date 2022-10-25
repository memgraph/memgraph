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
#include "query/v2/shard_request_manager.hpp"
#include "storage/v3/id_types.hpp"

namespace memgraph::query::v2::accessors {
EdgeAccessor::EdgeAccessor(Edge edge, const msgs::ShardRequestManagerInterface *manager)
    : edge(std::move(edge)), manager_(manager) {}

EdgeTypeId EdgeAccessor::EdgeType() const { return edge.type.id; }

const std::vector<std::pair<PropertyId, Value>> &EdgeAccessor::Properties() const { return edge.properties; }

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Value EdgeAccessor::GetProperty(const std::string &prop_name) const {
  auto prop_id = manager_->NameToProperty(prop_name);
  auto it = std::find_if(edge.properties.begin(), edge.properties.end(), [&](auto &pr) { return prop_id == pr.first; });
  if (it == edge.properties.end()) {
    throw std::runtime_error("Missing property from VertexAccessor");
  }
  return it->second;
}

const Edge &EdgeAccessor::GetEdge() const { return edge; }

bool EdgeAccessor::IsCycle() const { return edge.src == edge.dst; };

VertexAccessor EdgeAccessor::To() const {
  return VertexAccessor(Vertex{edge.dst}, std::vector<std::pair<PropertyId, msgs::Value>>{}, manager_);
}

VertexAccessor EdgeAccessor::From() const {
  return VertexAccessor(Vertex{edge.src}, std::vector<std::pair<PropertyId, msgs::Value>>{}, manager_);
}

VertexAccessor::VertexAccessor(Vertex v, std::vector<std::pair<PropertyId, Value>> props,
                               const msgs::ShardRequestManagerInterface *manager)
    : vertex(std::move(v)), properties(std::move(props)), manager_(manager) {}

VertexAccessor::VertexAccessor(Vertex v, std::map<PropertyId, Value> props,
                               const msgs::ShardRequestManagerInterface *manager)
    : vertex(std::move(v)), manager_(manager) {
  properties.reserve(props.size());
  for (auto &[id, value] : props) {
    properties.emplace_back(std::make_pair(id, std::move(value)));
  }
}

Label VertexAccessor::PrimaryLabel() const { return vertex.id.first; }

const msgs::VertexId &VertexAccessor::Id() const { return vertex.id; }

std::vector<Label> VertexAccessor::Labels() const { return vertex.labels; }

bool VertexAccessor::HasLabel(Label &label) const {
  return std::find_if(vertex.labels.begin(), vertex.labels.end(),
                      [label](const auto &l) { return l.id == label.id; }) != vertex.labels.end();
}

const std::vector<std::pair<PropertyId, Value>> &VertexAccessor::Properties() const { return properties; }

Value VertexAccessor::GetProperty(PropertyId prop_id) const {
  auto it = std::find_if(properties.begin(), properties.end(), [&](auto &pr) { return prop_id == pr.first; });
  if (it == properties.end()) {
    throw std::runtime_error("Missing property from VertexAccessor");
  }
  return it->second;
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
Value VertexAccessor::GetProperty(const std::string &prop_name) const {
  return GetProperty(manager_->NameToProperty(prop_name));
}

msgs::Vertex VertexAccessor::GetVertex() const { return vertex; }

}  // namespace memgraph::query::v2::accessors
