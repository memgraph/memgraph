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

#include <span>
#include <string>
#include <string_view>

#include "query/db_accessor.hpp"
#include "query/virtual_edge.hpp"
#include "query/virtual_graph.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/id_types.hpp"

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

// The projection seen as a graph to scan: iterate its nodes, iterate a node's
// out-edges and in-edges, and map names to/from ids. Topology comes from the
// VirtualGraph; names share the real accessor's namespace, since a projection
// mints no label/property/edge-type ids of its own.
//
// This is a standalone scan surface, verified at the unit seam. The name methods
// already match the GraphView signatures so a later slice can adopt this as the
// projection GraphView with the scan returning the common vertex range.
class VirtualGraphView final {
  const VirtualGraph *graph_;
  DbAccessor *names_;

 public:
  VirtualGraphView(const VirtualGraph *graph, DbAccessor *names) : graph_(graph), names_(names) {}

  VirtualNodeRange Nodes() const { return VirtualNodeRange{graph_->nodes()}; }

  std::span<const VirtualEdge *const> OutEdges(storage::Gid node) const { return graph_->OutEdges(node); }

  std::span<const VirtualEdge *const> InEdges(storage::Gid node) const { return graph_->InEdges(node); }

  storage::LabelId NameToLabel(std::string_view name) { return names_->NameToLabel(name); }

  const std::string &LabelToName(storage::LabelId label) const { return names_->LabelToName(label); }

  storage::PropertyId NameToProperty(std::string_view name) { return names_->NameToProperty(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const { return names_->PropertyToName(prop); }

  storage::EdgeTypeId NameToEdgeType(std::string_view name) { return names_->NameToEdgeType(name); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const { return names_->EdgeTypeToName(type); }
};

}  // namespace memgraph::query
