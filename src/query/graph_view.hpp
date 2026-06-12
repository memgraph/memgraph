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

#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include "query/db_accessor.hpp"
#include "query/virtual_graph.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"
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

// The single graph abstraction the read operators run against: scan the vertices
// and map names to/from ids. Property reads and edge expansion stay on the
// element (VertexAccessor / VirtualNode), not here.
//
// The real DbAccessor is the identity view (DbAccessorGraphView); a subgraph or
// projection is a view layered over it. ExecutionContext binds the ambient view,
// and a `CALL { USE ... }` scope rebinds it for the block.
class GraphView {
 public:
  GraphView() = default;
  GraphView(const GraphView &) = default;
  GraphView(GraphView &&) = default;
  GraphView &operator=(const GraphView &) = default;
  GraphView &operator=(GraphView &&) = default;
  virtual ~GraphView() = default;

  // Full scan of the view's vertices. No index/label overload: inside a
  // projection scope a label-filtered match is a full scan plus a filter, since
  // a projection exposes no index.
  virtual VertexRange Vertices(storage::View view) = 0;

  virtual storage::LabelId NameToLabel(std::string_view name) = 0;
  virtual const std::string &LabelToName(storage::LabelId label) const = 0;
  virtual storage::PropertyId NameToProperty(std::string_view name) = 0;
  virtual const std::string &PropertyToName(storage::PropertyId prop) const = 0;
  virtual storage::EdgeTypeId NameToEdgeType(std::string_view name) = 0;
  virtual const std::string &EdgeTypeToName(storage::EdgeTypeId type) const = 0;
};

// The identity view: the real graph seen as a GraphView. Every call forwards to
// the underlying DbAccessor, so a real-graph scan through this view produces the
// same vertices as scanning the accessor directly.
class DbAccessorGraphView final : public GraphView {
  DbAccessor *dba_;

 public:
  explicit DbAccessorGraphView(DbAccessor *dba) : dba_(dba) {}

  VertexRange Vertices(storage::View view) override { return VertexRange{dba_->Vertices(view)}; }

  storage::LabelId NameToLabel(std::string_view name) override { return dba_->NameToLabel(name); }

  const std::string &LabelToName(storage::LabelId label) const override { return dba_->LabelToName(label); }

  storage::PropertyId NameToProperty(std::string_view name) override { return dba_->NameToProperty(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const override { return dba_->PropertyToName(prop); }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) override { return dba_->NameToEdgeType(name); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const override { return dba_->EdgeTypeToName(type); }
};

}  // namespace memgraph::query
