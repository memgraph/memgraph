// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file graph_data.hpp
/// This file contains the graph data - nodes, edges, variables.

#pragma once

#include <cstdint>
#include <type_traits>

namespace mg_graph {

/// Node representation.
///
/// Node has id and type. Id must be unique in the graph.
template <typename TSize = std::uint64_t>
struct Node {
  static_assert(std::is_unsigned_v<TSize>,
                "mg_graph::Node expects the type to be an unsigned integer type\n"
                "only (uint8_t, uint16_t, uint32_t, or uint64_t).");
  TSize id;
};

/// Edge representation.
///
/// @var id edge id
/// @var from node
/// @var to node
/// @var type edge type
/// @var variables set of variables on edge with the state
template <typename TSize = std::uint64_t>
struct Edge {
  static_assert(std::is_unsigned_v<TSize>,
                "mg_graph::Edge expects the type to be an unsigned integer type\n"
                "only (uint8_t, uint16_t, uint32_t, or uint64_t).");
  TSize id;
  TSize from;
  TSize to;
};

/// Neighbour representation.
///
/// Helper structure for storing node and edge id.
template <typename TSize = std::uint64_t>
struct Neighbour {
  static_assert(std::is_unsigned_v<TSize>,
                "mg_graph::Neighbour expects the type to be an unsigned integer type\n"
                "only (uint8_t, uint16_t, uint32_t, or uint64_t).");
  TSize node_id;
  TSize edge_id;
  Neighbour(TSize n_id, TSize e_id) : node_id(n_id), edge_id(e_id) {}
};
}  // namespace mg_graph
