#include "mg_procedure.h"

#include <exception>
#include <unordered_map>

#include "algorithms/algorithms.hpp"
#include "data_structures/graph.hpp"

static void communities(const mgp_list *args, const mgp_graph *graph,
                        mgp_result *result, mgp_memory *memory) {
  mgp_vertices_iterator *vertices_iterator =
      mgp_graph_iter_vertices(graph, memory);
  if (vertices_iterator == nullptr) {
    mgp_result_set_error_msg(result, "Not enough memory!");
    return;
  }

  // Normalize vertex ids
  std::unordered_map<int64_t, uint32_t> mem_to_louv_id;

  uint32_t louv_id = 0;
  for (const mgp_vertex *vertex = mgp_vertices_iterator_get(vertices_iterator);
       vertex != nullptr;
       vertex = mgp_vertices_iterator_next(vertices_iterator)) {
    mgp_vertex_id mem_id = mgp_vertex_get_id(vertex);
    mem_to_louv_id[mem_id.as_int] = louv_id;
    ++louv_id;
  }

  mgp_vertices_iterator_destroy(vertices_iterator);

  // Extract the graph structure
  // TODO(ipaljak): consider filtering nodes and edges by labels.
  comdata::Graph louvain_graph(louv_id);
  for (const auto &p : mem_to_louv_id) {
    mgp_vertex *vertex = mgp_graph_get_vertex_by_id(graph, {p.first}, memory);
    if (!vertex) {
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }

    // iterate over inbound edges. This is enough because we will eventually
    // iterate over outbound edges in another direction.
    mgp_edges_iterator *edges_iterator =
        mgp_vertex_iter_in_edges(vertex, memory);
    if (edges_iterator == nullptr) {
      mgp_vertex_destroy(vertex);
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }

    for (const mgp_edge *edge = mgp_edges_iterator_get(edges_iterator);
         edge != nullptr; edge = mgp_edges_iterator_next(edges_iterator)) {
      const mgp_vertex *next_vertex = mgp_edge_get_from(edge);
      mgp_vertex_id next_mem_id = mgp_vertex_get_id(next_vertex);
      uint32_t next_louv_id = mem_to_louv_id[next_mem_id.as_int];

      // retrieve edge weight (default to 1)
      mgp_value *weight_prop = mgp_edge_get_property(edge, "weight", memory);
      if (!weight_prop) {
        mgp_vertex_destroy(vertex);
        mgp_edges_iterator_destroy(edges_iterator);
        mgp_result_set_error_msg(result, "Not enough memory");
      }

      double weight = 1;
      if (mgp_value_is_double(weight_prop))
        weight = mgp_value_get_double(weight_prop);
      if (mgp_value_is_int(weight_prop))
        weight = static_cast<double>(mgp_value_get_int(weight_prop));

      mgp_value_destroy(weight_prop);

      try {
        louvain_graph.AddEdge(p.second, next_louv_id, weight);
      } catch (const std::exception &e) {
        mgp_vertex_destroy(vertex);
        mgp_edges_iterator_destroy(edges_iterator);
        mgp_result_set_error_msg(result, e.what());
        return;
      }
    }

    mgp_vertex_destroy(vertex);
    mgp_edges_iterator_destroy(edges_iterator);
  }

  try {
    algorithms::Louvain(&louvain_graph);
  } catch (const std::exception &e) {
    const auto msg = std::string("[Internal error] ") + e.what();
    mgp_result_set_error_msg(result, msg.c_str());
    return;
  }

  // Return node ids and their corresponding communities.
  for (const auto &p : mem_to_louv_id) {
    mgp_result_record *record = mgp_result_new_record(result);
    if (record == nullptr) {
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }

    mgp_value *mem_id_value = mgp_value_make_int(p.first, memory);
    if (mem_id_value == nullptr) {
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }

    mgp_value *com_value =
        mgp_value_make_int(louvain_graph.Community(p.second), memory);
    if (com_value == nullptr) {
      mgp_value_destroy(mem_id_value);
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }

    int mem_id_inserted =
        mgp_result_record_insert(record, "id", mem_id_value);
    int com_inserted =
        mgp_result_record_insert(record, "community", com_value);

    mgp_value_destroy(mem_id_value);
    mgp_value_destroy(com_value);

    if (!mem_id_inserted || !com_inserted) {
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }
  }
}

extern "C" int mgp_init_module(struct mgp_module *module,
                               struct mgp_memory *memory) {
  struct mgp_proc *proc =
      mgp_module_add_read_procedure(module, "communities", communities);
  if (!proc) return 1;
  if (!mgp_proc_add_result(proc, "id", mgp_type_int())) return 1;
  if (!mgp_proc_add_result(proc, "community", mgp_type_int())) return 1;
  return 0;
}

extern "C" int mgp_shutdown_module() {
  return 0;
}
