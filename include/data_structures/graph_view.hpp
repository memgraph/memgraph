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

#pragma once

#include <cstdint>
#include <optional>
#include <vector>

#include "graph_data.hpp"

namespace mg_graph {

/// Graph view interface.
///
/// Interface provides methods for fetching graph data.
/// There are two methods for changing variables on the edges:
/// SetVariableState and SetVariableValue
template <typename TSize = std::uint64_t>
class GraphView {
  static_assert(std::is_unsigned_v<TSize>,
                "mg_graph::GraphView expects the type to be an unsigned integer type\n"
                "only (uint8_t, uint16_t, uint32_t, or uint64_t).");

 public:
  using TNode = Node<TSize>;
  using TEdge = Edge<TSize>;
  using TNeighbour = Neighbour<TSize>;

  /// Destroys the object.
  virtual ~GraphView() = 0;

  /// Gets all graph nodes.
  ///
  /// @return Vector of nodes
  virtual const std::vector<TNode> &Nodes() const = 0;

  /// Gets all graph edges.
  ///
  /// @return Vector of edges.
  virtual const std::vector<TEdge> &Edges() const = 0;

  /// Gets the edges between two neighbour nodes.
  ///
  /// @param[in] first node id
  /// @param[in] second node id
  ///
  /// @return     Iterator range
  virtual std::vector<TSize> GetEdgesBetweenNodes(TSize first, TSize second) const = 0;

  /// Gets all incident edges ids.
  ///
  /// @return all incident edges
  virtual const std::vector<TSize> &IncidentEdges(TSize node_id) const = 0;

  /// Gets neighbour nodes.
  ///
  /// @param[in] node_id target node id
  ///
  /// @return vector of neighbours
  virtual const std::vector<TNeighbour> &Neighbours(TSize node_id) const = 0;

  /// Gets out-neighbour nodes.
  ///
  /// @param[in] node_id target node id
  ///
  /// @return vector of neighbours
  virtual const std::vector<TNeighbour> &OutNeighbours(TSize node_id) const = 0;

  /// Gets in-neighbour nodes.
  ///
  /// @param[in] node_id target node id
  ///
  /// @return vector of neighbours
  virtual const std::vector<TNeighbour> &InNeighbours(TSize node_id) const = 0;

  /// Gets node with node id.
  ///
  /// @param[in] node_id node id
  ///
  /// @return target Node struct
  virtual const TNode &GetNode(TSize node_id) const = 0;

  /// Gets Edge with edge id.
  ///
  /// @param[in] edge_id edge id
  ///
  /// @return Edge struct
  virtual const TEdge &GetEdge(TSize edge_id) const = 0;

  ///
  ///@brief Get the Memgraph Node Id from the inner defined node ID
  ///
  ///@param node_id  node ID
  ///@return std::uint64_t
  ///
  virtual std::uint64_t GetMemgraphNodeId(TSize node_id) const = 0;

  ///
  ///@brief Get the Inner Node ID from Memgraph ID
  ///
  ///@param memgraph_id Memgraph's internal ID
  ///@return TSize
  ///@throws mg_exception::InvalidIDException if vertex does not exist
  ///
  virtual TSize GetInnerNodeId(std::uint64_t memgraph_id) const = 0;

  ///
  ///@brief Get the Inner Node ID from Memgraph ID
  ///
  ///@param memgraph_id Memgraph's internal ID
  ///@return std::optional<TSize>, which is null if vertex does not exist
  ///
  virtual std::optional<TSize> GetInnerNodeIdOpt(std::uint64_t memgraph_id) const = 0;

  ///
  ///@brief Get the Memgraph Edge Id from the inner renumbered node ID
  ///
  ///@param edge_id edge ID
  ///@return std::uint64_t
  ///
  virtual std::uint64_t GetMemgraphEdgeId(TSize edge_id) const = 0;

  ///
  ///@brief Get the Inner Edge Id from Memgraph edge Id
  ///
  ///@param memgraph_id Memgraph's internal edge ID
  ///@return TSize
  ///
  virtual TSize GetInnerEdgeId(std::uint64_t memgraph_id) const = 0;

  ///
  ///@brief Check if Memgraph's ID exists in the current graph
  ///
  ///@param memgraph_id
  ///@return true
  ///@return false
  ///
  virtual bool NodeExists(std::uint64_t memgraph_id) const = 0;

  ///@brief Checks if the current graph is empty.
  ///
  ///@return whether graph is empty
  virtual bool Empty() const = 0;

  /// Gets edge weight by edge ID.
  ///
  /// @param[in] edge_id edge ID
  ///
  /// @return double weight
  virtual double GetWeight(TSize edge_id) const = 0;

  ///
  ///@brief Return true if graph has weights on edges.
  ///
  virtual bool IsWeighted() const = 0;
};

template <typename TSize>
inline GraphView<TSize>::~GraphView() {}

}  // namespace mg_graph
