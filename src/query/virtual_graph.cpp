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

#include "query/virtual_graph.hpp"

namespace memgraph::query {

VirtualGraph::VirtualGraph(allocator_type alloc) : node_store_(alloc), edge_store_(alloc) {}

VirtualGraph::VirtualGraph(const VirtualGraph &other, allocator_type alloc)
    : node_store_(other.node_store_, alloc), edge_store_(other.edge_store_, alloc) {}

VirtualGraph::VirtualGraph(const VirtualGraph &other) : VirtualGraph(other, other.get_allocator()) {}

VirtualGraph::VirtualGraph(VirtualGraph &&other) noexcept = default;

VirtualGraph::VirtualGraph(VirtualGraph &&other, allocator_type alloc)
    : node_store_(std::move(other.node_store_), alloc), edge_store_(std::move(other.edge_store_), alloc) {}

void VirtualGraph::Merge(const VirtualGraph &other) {
  node_store_.MergeFrom(other.node_store_);
  for (const auto &edge : other.edge_store_.edges()) {
    edge_store_.InsertIfNew(edge);
  }
}

void VirtualGraph::Merge(VirtualGraph &&other) {
  node_store_.MergeFrom(std::move(other.node_store_));
  // edge_store_.edges() yields const references (unordered_set stores keys immutably),
  // so the inner copy can't be avoided — but we've at least saved the node copies above.
  for (const auto &edge : other.edge_store_.edges()) {
    edge_store_.InsertIfNew(edge);
  }
}

}  // namespace memgraph::query
