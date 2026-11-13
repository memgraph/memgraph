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

#include <cstdint>
#include <limits>
#include <numeric>
#include <unordered_map>
#include <vector>

#include <_mgp.hpp>
#include <mg_exceptions.hpp>
#include <mg_utils.hpp>

namespace {

constexpr char const *kProcedureGet = "get";

constexpr char const *kFieldVertex = "node";
constexpr char const *kFieldComponentId = "component_id";

void InsertWeaklyComponentResult(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const int component_id,
                                 const int vertex_id) {
  auto *vertex = mg_utility::GetNodeForInsertion(vertex_id, graph, memory);
  if (!vertex) return;

  auto *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertNodeValueResult(record, kFieldVertex, vertex, memory);
  mg_utility::InsertIntValueResult(record, kFieldComponentId, component_id, memory);
}

/// Finds weakly connected components of a graph.
///
/// Components are numbered in the order their first vertex is iterated.
///
/// Time complexity: O((|V|+|E|) log |V|)
// NOLINTNEXTLINE(misc-unused-parameters)
void Weak(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto const for_each_vertex = [&](auto &&callback) {
      auto *vertices_it = mgp::graph_iter_vertices(memgraph_graph, memory);
      mg_utility::OnScopeExit const delete_vertices_it([&vertices_it] { mgp::vertices_iterator_destroy(vertices_it); });
      for (auto *vertex = mgp::vertices_iterator_get(vertices_it); vertex;
           vertex = mgp::vertices_iterator_next(vertices_it)) {
        callback(vertex);
      }
    };

    std::vector<std::int64_t> memgraph_ids;
    std::unordered_map<std::int64_t, std::uint64_t> inner_ids;
    for_each_vertex([&](mgp_vertex *vertex) {
      auto const memgraph_id = mgp::vertex_get_id(vertex).as_int;
      inner_ids.emplace(memgraph_id, memgraph_ids.size());
      memgraph_ids.push_back(memgraph_id);
    });

    std::vector<std::uint64_t> parent(memgraph_ids.size());
    std::iota(parent.begin(), parent.end(), 0);
    auto const find = [&parent](std::uint64_t node) {
      while (parent[node] != node) node = parent[node] = parent[parent[node]];
      return node;
    };

    // An edge to or from a vertex created after the first pass is ignored.
    for_each_vertex([&](mgp_vertex *source) {
      auto const from = inner_ids.find(mgp::vertex_get_id(source).as_int);
      if (from == inner_ids.end()) return;
      auto *edges_it = mgp::vertex_iter_out_edges(source, memory);
      mg_utility::OnScopeExit const delete_edges_it([&edges_it] { mgp::edges_iterator_destroy(edges_it); });
      for (auto *edge = mgp::edges_iterator_get(edges_it); edge; edge = mgp::edges_iterator_next(edges_it)) {
        auto const to = inner_ids.find(mgp::vertex_get_id(mgp::edge_get_to(edge)).as_int);
        if (to != inner_ids.end()) parent[find(from->second)] = find(to->second);
      }
    });

    constexpr auto kUnnumbered = std::numeric_limits<std::uint64_t>::max();
    std::vector<std::uint64_t> root_component(memgraph_ids.size(), kUnnumbered);
    std::uint64_t component_count = 0;
    for (std::uint64_t node = 0; node < memgraph_ids.size(); ++node) {
      auto &component_id = root_component[find(node)];
      if (component_id == kUnnumbered) component_id = component_count++;
      InsertWeaklyComponentResult(
          memgraph_graph, result, memory, static_cast<int>(component_id), static_cast<int>(memgraph_ids[node]));
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

// NOLINTNEXTLINE(misc-unused-parameters)
extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory [[maybe_unused]]) {
  try {
    auto *wcc_proc = mgp::module_add_read_procedure(module, kProcedureGet, Weak);

    mgp::proc_add_result(wcc_proc, kFieldVertex, mgp::type_node());
    mgp::proc_add_result(wcc_proc, kFieldComponentId, mgp::type_int());
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
