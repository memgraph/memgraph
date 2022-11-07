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

#include <map>
#include <optional>
#include <utility>
#include <vector>

#include "query/exceptions.hpp"
#include "query/v2/requests.hpp"
#include "storage/v3/view.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"
#include "utils/memory.hpp"
#include "utils/memory_tracker.hpp"

namespace memgraph::msgs {
class ShardRequestManagerInterface;
}  // namespace memgraph::msgs

namespace memgraph::query::v2::accessors {

using Value = memgraph::msgs::Value;
using Edge = memgraph::msgs::Edge;
using Vertex = memgraph::msgs::Vertex;
using Label = memgraph::msgs::Label;
using PropertyId = memgraph::msgs::PropertyId;
using EdgeTypeId = memgraph::msgs::EdgeTypeId;

class VertexAccessor;

class EdgeAccessor final {
 public:
  explicit EdgeAccessor(Edge edge, const msgs::ShardRequestManagerInterface *manager);

  [[nodiscard]] EdgeTypeId EdgeType() const;

  [[nodiscard]] const std::vector<std::pair<PropertyId, Value>> &Properties() const;

  [[nodiscard]] Value GetProperty(const std::string &prop_name) const;

  [[nodiscard]] const Edge &GetEdge() const;

  [[nodiscard]] bool IsCycle() const;

  // Dummy function
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  [[nodiscard]] size_t CypherId() const { return 10; }

  //  bool HasSrcAccessor const { return src == nullptr; }
  //  bool HasDstAccessor const { return dst == nullptr; }

  [[nodiscard]] VertexAccessor To() const;
  [[nodiscard]] VertexAccessor From() const;

  friend bool operator==(const EdgeAccessor &lhs, const EdgeAccessor &rhs) { return lhs.edge == rhs.edge; }

  friend bool operator!=(const EdgeAccessor &lhs, const EdgeAccessor &rhs) { return !(lhs == rhs); }

 private:
  Edge edge;
  const msgs::ShardRequestManagerInterface *manager_;
};

class VertexAccessor final {
 public:
  using PropertyId = msgs::PropertyId;
  using Label = msgs::Label;
  using VertexId = msgs::VertexId;
  VertexAccessor(Vertex v, std::vector<std::pair<PropertyId, Value>> props,
                 const msgs::ShardRequestManagerInterface *manager);

  VertexAccessor(Vertex v, std::map<PropertyId, Value> &&props, const msgs::ShardRequestManagerInterface *manager);
  VertexAccessor(Vertex v, const std::map<PropertyId, Value> &props, const msgs::ShardRequestManagerInterface *manager);

  [[nodiscard]] Label PrimaryLabel() const;

  [[nodiscard]] const msgs::VertexId &Id() const;

  [[nodiscard]] std::vector<Label> Labels() const;

  [[nodiscard]] bool HasLabel(Label &label) const;

  [[nodiscard]] const std::vector<std::pair<PropertyId, Value>> &Properties() const;

  [[nodiscard]] Value GetProperty(PropertyId prop_id) const;
  [[nodiscard]] Value GetProperty(const std::string &prop_name) const;

  [[nodiscard]] msgs::Vertex GetVertex() const;

  // Dummy function
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  [[nodiscard]] size_t CypherId() const { return 10; }

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

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  [[nodiscard]] size_t InDegree() const { return 0; }

  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  [[nodiscard]] size_t OutDegree() const { return 0; }
  //

  friend bool operator==(const VertexAccessor &lhs, const VertexAccessor &rhs) {
    return lhs.vertex == rhs.vertex && lhs.properties == rhs.properties;
  }

  friend bool operator!=(const VertexAccessor &lhs, const VertexAccessor &rhs) { return !(lhs == rhs); }

 private:
  Vertex vertex;
  std::vector<std::pair<PropertyId, Value>> properties;
  const msgs::ShardRequestManagerInterface *manager_;
};

// Highly mocked interface. Won't work if used.
class Path {
 public:
  // Empty for now
  explicit Path(const VertexAccessor & /*vertex*/, utils::MemoryResource *memory = utils::NewDeleteResource())
      : mem(memory) {}

  template <typename... TOthers>
  explicit Path(const VertexAccessor &vertex, const TOthers &...others) {}

  template <typename... TOthers>
  Path(std::allocator_arg_t /*unused*/, utils::MemoryResource *memory, const VertexAccessor &vertex,
       const TOthers &...others) {}

  Path(const Path & /*other*/) {}

  Path(const Path & /*other*/, utils::MemoryResource *memory) : mem(memory) {}

  Path(Path && /*other*/) noexcept {}

  Path(Path && /*other*/, utils::MemoryResource *memory) : mem(memory) {}
  Path &operator=(const Path &path) {
    if (this == &path) {
      return *this;
    }
    return *this;
  }

  Path &operator=(Path &&path) noexcept {
    if (this == &path) {
      return *this;
    }
    return *this;
  }

  ~Path() {}

  friend bool operator==(const Path & /*lhs*/, const Path & /*rhs*/) { return true; };
  utils::MemoryResource *GetMemoryResource() { return mem; }

  auto &vertices() { return vertices_; }
  auto &edges() { return edges_; }
  const auto &vertices() const { return vertices_; }
  const auto &edges() const { return edges_; }

 private:
  std::vector<VertexAccessor> vertices_;
  std::vector<EdgeAccessor> edges_;
  utils::MemoryResource *mem = utils::NewDeleteResource();
};
}  // namespace memgraph::query::v2::accessors
