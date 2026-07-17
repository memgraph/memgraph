// Copyright 2026 Memgraph Ltd.
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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "query/db_accessor.hpp"
#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/hops_limit.hpp"
#include "query/virtual_edge.hpp"
#include "query/virtual_graph.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::query {

// A range over a VirtualGraph's nodes, yielding each node by reference. The
// VirtualGraph owns the nodes; the range only borrows its node map.
class VirtualNodeRange final {
  const VirtualGraph::node_map *nodes_;

 public:
  explicit VirtualNodeRange(const VirtualGraph::node_map &nodes) : nodes_(&nodes) {}

  class Iterator final {
    VirtualGraph::node_map::const_iterator it_;

   public:
    explicit Iterator(VirtualGraph::node_map::const_iterator it) : it_(it) {}

    const VirtualNode &operator*() const { return *it_->second; }

    Iterator &operator++() {
      ++it_;
      return *this;
    }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return it_ != other.it_; }
  };

  Iterator begin() const { return Iterator(nodes_->begin()); }

  Iterator end() const { return Iterator(nodes_->end()); }
};

// One scanned vertex: a real VertexAccessor (identity view) or a projection's
// VirtualNode. Both are concrete - a scan iterates over these directly, the
// virtual boundary having been crossed once to obtain the range. The frame
// already carries both kinds as a TypedValue, so an operator writes either.
using ScanVertex = std::variant<VertexAccessor, VirtualNode>;

// A range over the vertices yielded by one scan of a GraphView. The boundary is
// crossed once to obtain the range; iteration is then over concrete elements, so
// the real-graph read path is not regressed per row. The identity view backs it
// with the real accessor's VerticesIterable; a projection backs it with its
// VirtualGraph's nodes.
class VertexRange final {
  std::variant<VerticesIterable, VirtualNodeRange> impl_;

 public:
  explicit VertexRange(VerticesIterable iterable) : impl_(std::move(iterable)) {}

  explicit VertexRange(VirtualNodeRange iterable) : impl_(std::move(iterable)) {}

  class Iterator final {
    std::variant<VerticesIterable::Iterator, VirtualNodeRange::Iterator> it_;

   public:
    explicit Iterator(VerticesIterable::Iterator it) : it_(std::move(it)) {}

    explicit Iterator(VirtualNodeRange::Iterator it) : it_(std::move(it)) {}

    ScanVertex operator*() const {
      return std::visit([](const auto &it) -> ScanVertex { return *it; }, it_);
    }

    Iterator &operator++() {
      std::visit([](auto &it) { ++it; }, it_);
      return *this;
    }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return it_ != other.it_; }
  };

  Iterator begin() {
    return std::visit([](auto &iterable) { return Iterator(iterable.begin()); }, impl_);
  }

  Iterator end() {
    return std::visit([](auto &iterable) { return Iterator(iterable.end()); }, impl_);
  }
};

// One expanded edge: a real EdgeAccessor (identity or subgraph view) or a
// projection's VirtualEdge. Mirrors ScanVertex - an expansion yields these
// directly, the virtual boundary having been crossed once to obtain the range.
using ScanEdge = std::variant<EdgeAccessor, VirtualEdge>;

// A range over the edges yielded by one expansion of a GraphView node. As with
// VertexRange the boundary is crossed once to obtain the range; iteration is then
// over concrete elements. The identity and subgraph views back it with a real
// EdgeAccessor vector (already filtered and counted by storage); a projection
// backs it with the borrowed VirtualEdge pointers matching the requested
// direction and filters. expanded_count is the number of edges examined during
// the expansion (for hops accounting), counted before any type/destination filter
// drops one - so a projection and the real graph account the same traversal.
class EdgeRange final {
  std::variant<std::vector<EdgeAccessor>, std::vector<const VirtualEdge *>> impl_;
  int64_t expanded_count_{0};

 public:
  EdgeRange(std::vector<EdgeAccessor> edges, int64_t expanded_count)
      : impl_(std::move(edges)), expanded_count_(expanded_count) {}

  EdgeRange(std::vector<const VirtualEdge *> edges, int64_t expanded_count)
      : impl_(std::move(edges)), expanded_count_(expanded_count) {}

  [[nodiscard]] int64_t expanded_count() const { return expanded_count_; }

  class Iterator final {
    std::variant<std::vector<EdgeAccessor>::const_iterator, std::vector<const VirtualEdge *>::const_iterator> it_;

   public:
    explicit Iterator(std::vector<EdgeAccessor>::const_iterator it) : it_(it) {}

    explicit Iterator(std::vector<const VirtualEdge *>::const_iterator it) : it_(it) {}

    ScanEdge operator*() const {
      return std::visit(
          [](const auto &it) -> ScanEdge {
            // The real arm dereferences to an EdgeAccessor; the projection arm
            // dereferences to a VirtualEdge pointer, so it is deref'd once more to
            // copy the edge into the variant.
            using Elem = std::decay_t<decltype(*it)>;
            if constexpr (std::is_same_v<Elem, EdgeAccessor>) {
              return *it;
            } else {
              return **it;
            }
          },
          it_);
    }

    Iterator &operator++() {
      std::visit([](auto &it) { ++it; }, it_);
      return *this;
    }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return it_ != other.it_; }
  };

  Iterator begin() const {
    return std::visit([](const auto &edges) { return Iterator(edges.begin()); }, impl_);
  }

  Iterator end() const {
    return std::visit([](const auto &edges) { return Iterator(edges.end()); }, impl_);
  }
};

// Maps a storage edges result onto the query-layer edge/count shape, turning a
// storage error into the query error the read path reports. Mirrors the read
// path's edges-result unwrap; the operator's copy retires once expansion routes
// through this seam.
inline EdgeVertexAccessorResult UnwrapEdges(storage::Result<EdgeVertexAccessorResult> &&result) {
  if (!result) {
    switch (result.error()) {
      case storage::Error::DELETED_OBJECT:
        throw QueryRuntimeException("Trying to get relationships of a deleted node.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw QueryRuntimeException("Trying to get relationships from a node that doesn't exist.");
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::PROPERTIES_DISABLED:
        throw QueryRuntimeException("Unexpected error when accessing relationships.");
    }
  }
  return std::move(*result);
}

// Maps a storage degree result onto the query-layer count, turning a storage error into the query
// error the read path reports. Mirrors the read path's degree unwrap.
inline size_t UnwrapDegree(storage::Result<size_t> &&result) {
  if (!result) {
    switch (result.error()) {
      case storage::Error::DELETED_OBJECT:
        throw QueryRuntimeException("Trying to get degree of a deleted node.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw QueryRuntimeException("Trying to get degree of a node that doesn't exist.");
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::PROPERTIES_DISABLED:
        throw QueryRuntimeException("Unexpected error when getting node degree.");
    }
  }
  return *result;
}

// The single graph abstraction the read operators run against: scan the vertices,
// expand a node's edges, and map names to/from ids. Property reads stay on the
// element (VertexAccessor / VirtualNode); topology - scan and expand - is here, so
// the operators run over real, subgraph, and projection graphs through one seam.
//
// The real DbAccessor is the identity view (DbAccessorGraphView); a subgraph or
// projection is a view layered over it. ExecutionContext binds the ambient view,
// and a `CALL { USE ... }` scope rebinds it for the block.
class GraphView {
 public:
  GraphView(const GraphView &) = default;
  GraphView(GraphView &&) = default;
  GraphView &operator=(const GraphView &) = default;
  GraphView &operator=(GraphView &&) = default;
  virtual ~GraphView() = default;

  // Full scan of the view's vertices. No index/label overload: inside a
  // projection scope a label-filtered match is a full scan plus a filter, since
  // a projection exposes no index.
  virtual VertexRange Vertices(storage::View view) = 0;

  // Expand a scanned node's out-/in-edges through the view. edge_types filters by
  // type (empty = any); existing_dest, when set, keeps only the edge reaching that
  // already-bound endpoint. `from` must match the view's element kind - a real
  // vertex for the identity/subgraph views, a VirtualNode for a projection - which
  // holds because a node is expanded through the same view that scanned it; a
  // mismatch is a programming error.
  //
  // Topology filtering (type, destination) happens behind the seam; the range
  // yields edges only. Fine-grained READ authorization is NOT applied here - it
  // stays on the operator, which drops forbidden edges and neighbours per element -
  // so the seam stays topology-only. hops threads the traversal's budget to storage
  // for the identity and subgraph views; enforcing it over a projection lands with
  // the variable-length routing (issue 43).
  virtual EdgeRange OutEdges(const ScanVertex &from, storage::View view,
                             const std::vector<storage::EdgeTypeId> &edge_types,
                             const std::optional<ScanVertex> &existing_dest, HopsLimit *hops) = 0;

  virtual EdgeRange InEdges(const ScanVertex &from, storage::View view,
                            const std::vector<storage::EdgeTypeId> &edge_types,
                            const std::optional<ScanVertex> &existing_dest, HopsLimit *hops) = 0;

  // The (in, out) degree of a scanned node over this view: the real graph's fast per-vertex degree for
  // the identity view, the member-filtered count for a subgraph, and the projection's edge counts for a
  // projection. A node of the wrong kind for the view (a real vertex in a projection, or a synthetic
  // node with no projection bound) has no topology here and reports {0, 0}.
  virtual std::pair<int64_t, int64_t> Degree(const ScanVertex &from, storage::View view) = 0;

  // Name<->id mapping is database-global, not view-specific: every view resolves names through the same
  // DbAccessor, so these are shared non-virtual forwards, not per-adapter overrides.
  storage::LabelId NameToLabel(std::string_view name) { return names_->NameToLabel(name); }

  const std::string &LabelToName(storage::LabelId label) const { return names_->LabelToName(label); }

  storage::PropertyId NameToProperty(std::string_view name) { return names_->NameToProperty(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const { return names_->PropertyToName(prop); }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) { return names_->NameToEdgeType(name); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const { return names_->EdgeTypeToName(type); }

 protected:
  // The database-global name resolver every view shares. The identity view passes its own DbAccessor;
  // a subgraph or projection passes the accessor it was built against.
  explicit GraphView(DbAccessor *names) : names_(names) {}

  DbAccessor *names_;
};

// The identity view: the real graph seen as a GraphView. Every call forwards to
// the underlying DbAccessor, so a real-graph scan through this view produces the
// same vertices as scanning the accessor directly.
class DbAccessorGraphView final : public GraphView {
  DbAccessor *dba_;

 public:
  explicit DbAccessorGraphView(DbAccessor *dba) : GraphView(dba), dba_(dba) {}

  VertexRange Vertices(storage::View view) override { return VertexRange{dba_->Vertices(view)}; }

  EdgeRange OutEdges(const ScanVertex &from, storage::View view, const std::vector<storage::EdgeTypeId> &edge_types,
                     const std::optional<ScanVertex> &existing_dest, HopsLimit *hops) override {
    const auto &vertex = std::get<VertexAccessor>(from);
    auto result = existing_dest ? vertex.OutEdges(view, edge_types, std::get<VertexAccessor>(*existing_dest), hops)
                                : vertex.OutEdges(view, edge_types, hops);
    auto edges = UnwrapEdges(std::move(result));
    return EdgeRange{std::move(edges.edges), edges.expanded_count};
  }

  EdgeRange InEdges(const ScanVertex &from, storage::View view, const std::vector<storage::EdgeTypeId> &edge_types,
                    const std::optional<ScanVertex> &existing_dest, HopsLimit *hops) override {
    const auto &vertex = std::get<VertexAccessor>(from);
    auto result = existing_dest ? vertex.InEdges(view, edge_types, std::get<VertexAccessor>(*existing_dest), hops)
                                : vertex.InEdges(view, edge_types, hops);
    auto edges = UnwrapEdges(std::move(result));
    return EdgeRange{std::move(edges.edges), edges.expanded_count};
  }

  // The real graph's per-vertex degree (O(1) storage metadata). A synthetic node reaching the identity
  // view has no real-graph topology and reports {0, 0}.
  std::pair<int64_t, int64_t> Degree(const ScanVertex &from, storage::View view) override {
    const auto *vertex = std::get_if<VertexAccessor>(&from);
    if (vertex == nullptr) return {0, 0};
    return {static_cast<int64_t>(UnwrapDegree(vertex->InDegree(view))),
            static_cast<int64_t>(UnwrapDegree(vertex->OutDegree(view)))};
  }
};

}  // namespace memgraph::query
