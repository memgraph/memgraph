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

/// @file mg_utils.hpp
///
/// The file contains methods that connect mg procedures and the outside code
/// Methods like mapping a graph into memory or assigning new mg results or
/// their properties are implemented.
#pragma once

#include <functional>
#include <memory>
#include <unordered_set>

#include "_mgp.hpp"
#include "mg_graph.hpp"

namespace mg_graph {

///
///@brief Creates vertex inside GraphView. First step is getting Memgraph’s UID
/// for identifying vertex inside Memgraph
/// platform.
///
///@tparam TSize Parameter for storing vertex identifiers
///@param graph Memgraph’s graph instance
///@param vertex Memgraph’s vertex instance
///
template <typename TSize>
void CreateGraphNode(mg_graph::Graph<TSize> *graph, mgp_vertex *vertex) {
  // Get Memgraph internal ID property
  auto id_val = mgp::vertex_get_id(vertex);
  auto memgraph_id = id_val.as_int;

  graph->CreateNode(memgraph_id);
}

///
///@brief Creates an edge within the graph view. Edges are defined by a trio of Memgraph IDs values: of the source
/// and destination vertices, and of the edge itself.
///
///@tparam TSize -- Vertex ID type
///@param graph -- Memgraph’s graph instance
///@param source -- Memgraph’s source vertex
///@param destination -- Memgraph’s destination vertex
///@param graph_type -- Type of stored graph (directed/undirected)
///@param weighted -- Graph weightedness
///@param weight -- Edge weight
///
template <typename TSize>
void CreateGraphEdge(mg_graph::Graph<TSize> *graph, mgp_vertex *source, mgp_vertex *destination, mgp_edge *edge,
                     const mg_graph::GraphType graph_type, bool weighted = false, double weight = 0.0) {
  // Get Memgraph-internal endpoint node IDs
  auto memgraph_id_from = mgp::vertex_get_id(source).as_int;
  auto memgraph_id_to = mgp::vertex_get_id(destination).as_int;

  // Get Memgraph-internal edge ID
  auto memgraph_edge_id = mgp::edge_get_id(edge).as_int;

  graph->CreateEdge(memgraph_id_from, memgraph_id_to, graph_type, memgraph_edge_id, weighted, weight);
}
}  // namespace mg_graph

namespace mg_utility {
double GetNumericProperty(mgp_edge *edge, const char *property_name, mgp_memory *memory, double default_weight);

/// Calls a function in its destructor (on scope exit).
///
/// Example usage:
///
/// void long_function() {
///   resource.enable();
///   // long block of code, might throw an exception
///   resource.disable(); // we want this to happen for sure, and function end
/// }
///
/// Can be nicer and safer:
///
/// void long_function() {
///   resource.enable();
///   OnScopeExit on_exit([&resource] { resource.disable(); });
///   // long block of code, might throw an exception
/// }
class OnScopeExit {
 public:
  explicit OnScopeExit(const std::function<void()> &function) : function_(function) {}
  ~OnScopeExit() { function_(); }

 private:
  std::function<void()> function_;
};

///@brief Returns Memgraph IDs of all nodes in the graph view.
///
///@param graph -- graph view
template <typename TSize>
std::unordered_set<std::uint64_t> GraphNodeIDs(mg_graph::Graph<TSize> *graph) {
  return graph->GetMemgraphNodeIDs();
}

///@brief Maps the Memgraph in-memory graph to a user-defined graph view.
/// Supports building unweighted and weighted graph views.
/// Node/relationship IDs are zero-indexed.
/// The graph view holds connection information and the local<->Memgraph ID mapping.
///
///@param graph -- Memgraph graph
///@param result -- Memgraph result object
///@param memory -- Memgraph storage object
///@param graph_type -- graph directedness
///@param weighted -- graph weightedness
///@param weight_property -- relationship property containing weight
///@param default_weight -- default relationship weight if property not set
///@param subgraph -- if set, limit the graph view to subgraph
///@param subgraph_nodes -- nodes selected for subgraph
///@param subgraph_edges -- edges selected for subgraph
///
///@return mg_graph::Graph -- graph view
template <typename TSize = std::uint64_t>
std::unique_ptr<mg_graph::Graph<TSize>> GetGraphView(mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory,
                                                     const mg_graph::GraphType graph_type, bool weighted = false,
                                                     const char *weight_property = nullptr,
                                                     double default_weight = 1.0) {
  auto graph = std::make_unique<mg_graph::Graph<TSize>>();
  graph->SetIsTransactional(mgp::graph_is_transactional(memgraph_graph));

  ///
  /// Mapping Memgraph in-memory vertices into the graph view
  ///

  {
    auto *vertices_it = mgp::graph_iter_vertices(memgraph_graph, memory);  // Safe vertex iterator creation
    mg_utility::OnScopeExit delete_vertices_it([&vertices_it] { mgp::vertices_iterator_destroy(vertices_it); });

    // Iterate through Memgraph vertices and map them to GraphView
    for (auto *vertex = mgp::vertices_iterator_get(vertices_it); vertex;
         vertex = mgp::vertices_iterator_next(vertices_it)) {
      mg_graph::CreateGraphNode(graph.get(), vertex);
    }
  }

  ///
  /// Mapping Memgraph in-memory edges into the graph view
  ///

  {
    auto *vertices_it = mgp::graph_iter_vertices(memgraph_graph, memory);  // Safe vertex iterator creation
    mg_utility::OnScopeExit delete_vertices_it([&vertices_it] { mgp::vertices_iterator_destroy(vertices_it); });

    for (auto *source = mgp::vertices_iterator_get(vertices_it); source;
         source = mgp::vertices_iterator_next(vertices_it)) {
      auto *edges_it = mgp::vertex_iter_out_edges(source, memory);  // Safe edge iterator creation
      mg_utility::OnScopeExit delete_edges_it([&edges_it] { mgp::edges_iterator_destroy(edges_it); });

      for (auto *out_edge = mgp::edges_iterator_get(edges_it); out_edge;
           out_edge = mgp::edges_iterator_next(edges_it)) {
        auto destination = mgp::edge_get_to(out_edge);

        double weight = weighted ? mg_utility::GetNumericProperty(out_edge, weight_property, memory, default_weight)
                                 : default_weight;
        mg_graph::CreateGraphEdge(graph.get(), source, destination, out_edge, graph_type, weighted, weight);
      }
    }
  }

  return graph;
}

///@brief Maps the Memgraph in-memory graph to a user-defined *weighted* graph view.
/// Kept for backward compatibility purposes.
template <typename TSize = std::uint64_t>
std::unique_ptr<mg_graph::Graph<TSize>> GetWeightedGraphView(mgp_graph *memgraph_graph, mgp_result *result,
                                                             mgp_memory *memory, const mg_graph::GraphType graph_type,
                                                             const char *weight_property, double default_weight) {
  return GetGraphView(memgraph_graph, result, memory, graph_type, true, weight_property, default_weight);
}

///@brief Maps the Memgraph in-memory graph to a user-defined subgraph view that contains the given nodes and edges.
/// Node/relationship IDs are zero-indexed.
/// The graph view holds connection information and the local<->Memgraph ID mapping.
///
///@param graph -- Memgraph graph
///@param result -- Memgraph result object
///@param memory -- Memgraph storage object
///@param graph_type -- graph directedness
///@param weighted -- graph weightedness
///@param weight_property -- relationship property containing weight
///@param default_weight -- default relationship weight if property not set
///@param subgraph -- if set, limit the graph view to subgraph
///@param subgraph_nodes -- nodes selected for subgraph
///@param subgraph_edges -- edges selected for subgraph
///
///@return mg_graph::Graph -- graph view
template <typename TSize = std::uint64_t>
std::unique_ptr<mg_graph::Graph<TSize>> GetSubgraphView(mgp_graph *memgraph_graph, mgp_result *result,
                                                        mgp_memory *memory, mgp_list *subgraph_nodes,
                                                        mgp_list *subgraph_edges, const mg_graph::GraphType graph_type,
                                                        bool weighted = false, const char *weight_property = nullptr,
                                                        double default_weight = 1.0) {
  auto graph = std::make_unique<mg_graph::Graph<TSize>>();

  ///
  /// Mapping Memgraph in-memory vertices into the graph view
  ///

  for (std::size_t i = 0; i < mgp::list_size(subgraph_nodes); i++) {
    auto vertex = mgp::value_get_vertex(mgp::list_at(subgraph_nodes, i));
    mg_graph::CreateGraphNode(graph.get(), vertex);
  }

  ///
  /// Mapping Memgraph in-memory edges into the graph view
  ///

  auto subgraph_node_ids = GraphNodeIDs(graph.get());
  for (std::size_t i = 0; i < mgp::list_size(subgraph_edges); i++) {
    auto edge = mgp::value_get_edge(mgp::list_at(subgraph_edges, i));

    auto source = mgp::edge_get_from(edge);
    auto destination = mgp::edge_get_to(edge);

    auto source_id = mgp::vertex_get_id(source).as_int;
    auto destination_id = mgp::vertex_get_id(destination).as_int;

    if (subgraph_node_ids.find(source_id) != subgraph_node_ids.end() &&
        subgraph_node_ids.find(destination_id) != subgraph_node_ids.end()) {
      double weight =
          weighted ? mg_utility::GetNumericProperty(edge, weight_property, memory, default_weight) : default_weight;
      mg_graph::CreateGraphEdge(graph.get(), source, destination, edge, graph_type, weighted, weight);
    }
  }

  return graph;
}

///@brief Maps the Memgraph in-memory graph to a user-defined *weighted* subgraph view.
/// Kept for backward compatibility purposes.
template <typename TSize = std::uint64_t>
std::unique_ptr<mg_graph::Graph<TSize>> GetWeightedSubgraphView(mgp_graph *memgraph_graph, mgp_result *result,
                                                                mgp_memory *memory, mgp_list *subgraph_nodes,
                                                                mgp_list *subgraph_edges,
                                                                const mg_graph::GraphType graph_type,
                                                                const char *weight_property, double default_weight) {
  return GetSubgraphView(memgraph_graph, result, memory, subgraph_nodes, subgraph_edges, graph_type, true,
                         weight_property, default_weight);
}

namespace {
void InsertRecord(mgp_result_record *record, const char *field_name, mgp_value *value) {
  mg_utility::OnScopeExit delete_value([&value] { mgp::value_destroy(value); });
  mgp::result_record_insert(record, field_name, value);
}
}  // namespace

/// Inserts a string of value string_value to the field field_name of
/// the record mgp_result_record record.
inline void InsertStringValueResult(mgp_result_record *record, const char *field_name, const char *string_value,
                                    mgp_memory *memory) {
  auto value = mgp::value_make_string(string_value, memory);
  InsertRecord(record, field_name, value);
}

/// Inserts an integer of value int_value to the field field_name of
/// the record mgp_result_record record.
inline void InsertIntValueResult(mgp_result_record *record, const char *field_name, const int int_value,
                                 mgp_memory *memory) {
  auto value = mgp::value_make_int(int_value, memory);
  InsertRecord(record, field_name, value);
}

/// Inserts a double of value double_value to the field field_name of
/// the record mgp_result_record record.
inline void InsertDoubleValueResult(mgp_result_record *record, const char *field_name, const double double_value,
                                    mgp_memory *memory) {
  auto value = mgp::value_make_double(double_value, memory);
  InsertRecord(record, field_name, value);
}

/// Inserts a node of value vertex_value to the field field_name of
/// the record mgp_result_record record.
inline void InsertNodeValueResult(mgp_result_record *record, const char *field_name, mgp_vertex *vertex_value,
                                  mgp_memory *memory) {
  auto value = mgp::value_make_vertex(vertex_value);
  InsertRecord(record, field_name, value);
}

// Inserts a list of value list_value to the field field_name of
// the record mgp_result_record record.
inline void InsertListValueResult(mgp_result_record *record, const char *field_name, mgp_list *list_value,
                                  mgp_memory *memory) {
  auto value = mgp::value_make_list(list_value);
  InsertRecord(record, field_name, value);
}

/// @brief Retrieves a node with the given ID to be fed into InsertNodeValueResult. If no node is found, the behavior’s
/// up to the storage mode:
/// * In transactional (ACID-compliant) storage modes one can expect vertices to not be erased -> InvalidIDException
/// * In IN_MEMORY_ANALYTICAL mode, vertices might be erased by parallel transactions -> nullptr
/// @param node_id
/// @param graph
/// @param memory
/// @return
inline mgp_vertex *GetNodeForInsertion(const int node_id, mgp_graph *graph, mgp_memory *memory) {
  auto *vertex = mgp::graph_get_vertex_by_id(graph, mgp_vertex_id{.as_int = static_cast<int64_t>(node_id)}, memory);
  if (!vertex && mgp::graph_is_transactional(graph)) {
    throw mg_exception::InvalidIDException();
  }
  return vertex;
}

/// Inserts a node with its ID node_id to create a vertex and insert
/// the node to the field field_name of the record mgp_result_record record.
/// Returns true is insert is successful, false otherwise
inline bool InsertNodeValueResult(mgp_graph *graph, mgp_result_record *record, const char *field_name,
                                  const int node_id, mgp_memory *memory) {
  auto *vertex = mgp::graph_get_vertex_by_id(graph, mgp_vertex_id{.as_int = node_id}, memory);
  if (!vertex) {
    return false;
  }
  InsertNodeValueResult(record, field_name, vertex, memory);
  return true;
}

/// Inserts a relationship of value edge_value to the field field_name of
/// the record mgp_result_record record.
inline void InsertRelationshipValueResult(mgp_result_record *record, const char *field_name, mgp_edge *edge_value,
                                          mgp_memory *memory) {
  auto value = mgp::value_make_edge(edge_value);
  InsertRecord(record, field_name, value);
}

/// Inserts a relationship with its ID edge_id to create a relationship and
/// insert the edge to the field field_name of the record mgp_result_record
/// record.
inline void InsertRelationshipValueResult(mgp_graph *graph, mgp_result_record *record, const char *field_name,
                                          const int edge_id, mgp_memory *memory);

/// Handles non-double weights for GetWeight().
/// If the weight property is an integer, mgp::value_get_double() returns 0.0.
/// To address that, this function checks the type of the edge property and
/// calls mgp::value_get_int() in case it’s an integer.
/// If the weight property is not a number, it returns the default weight.
inline double GetNumericProperty(mgp_edge *edge, const char *property_name, mgp_memory *memory, double default_weight) {
  double weight;
  auto raw_value = mgp::edge_get_property(edge, property_name, memory);
  auto type = mgp::value_get_type(raw_value);
  switch (type) {
    case MGP_VALUE_TYPE_INT:
      weight = static_cast<double>(mgp::value_get_int(raw_value));
      break;
    case MGP_VALUE_TYPE_DOUBLE:
      weight = mgp::value_get_double(raw_value);
      break;
    default:
      weight = default_weight;
  }

  mgp::value_destroy(raw_value);

  return weight;
}

/// Returns a vector of node_ids of nodes from the mgp_list node_list.
inline std::vector<std::uint64_t> GetNodeIDs(mgp_list *node_list) {
  std::vector<std::uint64_t> node_ids;
  for (std::size_t i = 0; i < mgp::list_size(node_list); i++) {
    node_ids.push_back(mgp::vertex_get_id(mgp::value_get_vertex(mgp::list_at(node_list, i))).as_int);
  }

  return node_ids;
}

/// Returns a vector of endpoints ({node_id, node_id} pairs) of edges
/// from the mgp_list edge_list.
inline std::vector<std::pair<std::uint64_t, std::uint64_t>> GetEdgeEndpointIDs(mgp_list *edge_list) {
  std::vector<std::pair<std::uint64_t, std::uint64_t>> edge_endpoint_ids;
  for (std::size_t i = 0; i < mgp::list_size(edge_list); i++) {
    auto edge = mgp::value_get_edge(mgp::list_at(edge_list, i));
    auto source_id = mgp::vertex_get_id(mgp::edge_get_from(edge));
    auto destination_id = mgp::vertex_get_id(mgp::edge_get_to(edge));
    edge_endpoint_ids.push_back(std::make_pair(source_id.as_int, destination_id.as_int));
  }

  return edge_endpoint_ids;
}

/// Return a vector of edge ids from the mgp_list edge_list
inline std::vector<std::uint64_t> GetEdgeIDs(mgp_list *edge_list) {
  auto size = mgp::list_size(edge_list);
  auto edge_ids = std::vector<std::uint64_t>(size);
  for (std::size_t i = 0; i < size; i++) {
    auto edge = mgp::value_get_edge(mgp::list_at(edge_list, i));
    auto edge_id = mgp::edge_get_id(edge).as_int;
    edge_ids[i] = edge_id;
  }
  return edge_ids;
}
}  // namespace mg_utility
