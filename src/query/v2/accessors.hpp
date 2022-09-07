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

#include <optional>

#include "query/exceptions.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/view.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"

TypedValue ValueToTypedValue(const Value &value) {
  switch (value.type) {
    case Value::NILL:
      return {};
    case Value::BOOL:
      return {value.bool_v};
    case Value::INT64:
      return {value.int_v};
    case Value::DOUBLE:
      return {value.double_v};
    case Value::STRING:
      return {value.string_v};
    case Value::LIST:
      return {value.list_v};
    case Value::MAP:
      return {value.map_v};
    case Value::VERTEX:
    case Value::EDGE:
    case Value::PATH:
  }
  std::runtime_error("Incorrect type in conversion");
}

class VertexAccessor;

class EdgeAccessor final {
 public:
  EdgeAccessor(Edge *edge, std::map<std::string, Value> *props) : edge(edge), properties(props) {
    MG_ASSERT(edge != nullptr);
    MG_ASSERT(properties != nullptr);
  }

  std::string EdgeType() const { return edge->type.name; }

  std::map<std::string, TypedValue> Properties() const {
    std::map<std::string, TypedValue> res;
    for (const auto &[name, value] : *properties) {
      res[name] = ValueToTypedValue(value);
    }
    return res;
  }

  TypedValue GetProperty(const std::string &prop_name) const {
    MG_ASSERT(properties->contains(prop_name));
    return ValueToTypedValue(properties[prop_name]);
  }

  //  bool HasSrcAccessor const { return src == nullptr; }
  //  bool HasDstAccessor const { return dst == nullptr; }

//  VertexAccessor To() const;
//  VertexAccessor From() const;

  friend bool operator==(const EdgeAccessor &lhs, const EdgeAccessor &rhs) noexcept {
    return lhs.edge == rhs.edge && lhs.properties == rhs.properties;
  }

  friend bool operator!=(const EdgeAccessor &lhs, const EdgeAccessor &rhs) noexcept { return !(lhs == rhs); }

 private:
  Edge *edge;
  //  VertexAccessor *src {nullptr};
  //  VertexAccessor *dst {nullptr};
  std::map<std::string, Value> *properties;
};

class VertexAccessor final {
 public:
  VertexAccessor(Vertex *v, std::map<std::string, Value> *props) : vertex(v), properties(props) {
    MG_ASSERT(vertex != nullptr);
    MG_ASSERT(properties != nullptr);
  }

  std::vector<Label> Labels() const { return vertex->labels; }

  bool HasLabel(Label &label) const {
    return std::find_if(vertex->labels.begin(), vertex->labels.end(),
                        [label](const auto &l) { return l.id == label.id; }) != vertex->labels.end();
  }

  std::map<std::string, TypedValue> Properties() const {
    std::map<std::string, TypedValue> res;
    for (const auto &[name, value] : *properties) {
      res[name] = ValueToTypedValue(value);
    }
    return res;
  }

  TypedValue GetProperty(const std::string &prop_name) const {
    MG_ASSERT(properties->contains(prop_name));
    return ValueToTypedValue(properties[prop_name]);
  }

  //  auto InEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types) const
  //      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.InEdges(view)))> {
  //    auto maybe_edges = impl_.InEdges(view, edge_types);
  //    if (maybe_edges.HasError()) return maybe_edges.GetError();
  //    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  //  }
  //
  //  auto InEdges(storage::View view) const { return InEdges(view, {}); }
  //
  //  auto InEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types, const VertexAccessor &dest)
  //  const
  //      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.InEdges(view)))> {
  //    auto maybe_edges = impl_.InEdges(view, edge_types, &dest.impl_);
  //    if (maybe_edges.HasError()) return maybe_edges.GetError();
  //    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  //  }
  //
  //  auto OutEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types) const
  //      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.OutEdges(view)))> {
  //    auto maybe_edges = impl_.OutEdges(view, edge_types);
  //    if (maybe_edges.HasError()) return maybe_edges.GetError();
  //    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  //  }
  //
  //  auto OutEdges(storage::View view) const { return OutEdges(view, {}); }
  //
  //  auto OutEdges(storage::View view, const std::vector<storage::EdgeTypeId> &edge_types,
  //                const VertexAccessor &dest) const
  //      -> storage::Result<decltype(iter::imap(MakeEdgeAccessor, *impl_.OutEdges(view)))> {
  //    auto maybe_edges = impl_.OutEdges(view, edge_types, &dest.impl_);
  //    if (maybe_edges.HasError()) return maybe_edges.GetError();
  //    return iter::imap(MakeEdgeAccessor, std::move(*maybe_edges));
  //  }

  //  storage::Result<size_t> InDegree(storage::View view) const { return impl_.InDegree(view); }
  //
  //  storage::Result<size_t> OutDegree(storage::View view) const { return impl_.OutDegree(view); }
  //

  friend bool operator==(const VertexAccessor lhs, const VertexAccessor &rhs) noexcept {
    return lhs.vertex == rhs.vertex && lhs.properties == rhs.properties;
  }

  friend bool operator!=(const VertexAccessor lhs, const VertexAccessor &rhs) noexcept { return !(lhs == rhs); }

 private:
  Vertex *vertex;
  std::map<std::string, Value> *properties;
};

//inline VertexAccessor EdgeAccessor::To() const { return VertexAccessor(impl_.ToVertex()); }

//inline VertexAccessor EdgeAccessor::From() const { return VertexAccessor(impl_.FromVertex()); }
