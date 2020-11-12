#include "mg_procedure.h"

#include <exception>
#include <string>
#include <unordered_map>

#include "algorithms/algorithms.hpp"
#include "data_structures/graph.hpp"

namespace {

std::optional<std::unordered_map<int64_t, uint32_t>> NormalizeVertexIds(
    const mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  std::unordered_map<int64_t, uint32_t> mem_to_louv_id;
  mgp_vertices_iterator *vertices_iterator =
      mgp_graph_iter_vertices(graph, memory);
  if (vertices_iterator == nullptr) {
    mgp_result_set_error_msg(result, "Not enough memory!");
    return std::nullopt;
  }

  uint32_t louv_id = 0;
  for (const mgp_vertex *vertex = mgp_vertices_iterator_get(vertices_iterator);
       vertex != nullptr;
       vertex = mgp_vertices_iterator_next(vertices_iterator)) {
    mgp_vertex_id mem_id = mgp_vertex_get_id(vertex);
    mem_to_louv_id[mem_id.as_int] = louv_id;
    ++louv_id;
  }

  mgp_vertices_iterator_destroy(vertices_iterator);
  return mem_to_louv_id;
}

std::optional<comdata::Graph> RunLouvain(
    const mgp_graph *graph, mgp_result *result, mgp_memory *memory,
    const std::unordered_map<int64_t, uint32_t> &mem_to_louv_id) {
  comdata::Graph louvain_graph(mem_to_louv_id.size());
  // Extract the graph structure
  // TODO(ipaljak): consider filtering nodes and edges by labels.
  for (const auto &p : mem_to_louv_id) {
    mgp_vertex *vertex = mgp_graph_get_vertex_by_id(graph, {p.first}, memory);
    if (!vertex) {
      mgp_result_set_error_msg(result, "Not enough memory!");
      return std::nullopt;
    }

    // iterate over inbound edges. This is enough because we will eventually
    // iterate over outbound edges in another direction.
    mgp_edges_iterator *edges_iterator =
        mgp_vertex_iter_in_edges(vertex, memory);
    if (edges_iterator == nullptr) {
      mgp_vertex_destroy(vertex);
      mgp_result_set_error_msg(result, "Not enough memory!");
      return std::nullopt;
    }

    for (const mgp_edge *edge = mgp_edges_iterator_get(edges_iterator);
         edge != nullptr; edge = mgp_edges_iterator_next(edges_iterator)) {
      const mgp_vertex *next_vertex = mgp_edge_get_from(edge);
      mgp_vertex_id next_mem_id = mgp_vertex_get_id(next_vertex);
      uint32_t next_louv_id;
      try {
        next_louv_id = mem_to_louv_id.at(next_mem_id.as_int);
      } catch (const std::exception &e) {
        const auto msg = std::string("[Internal error] ") + e.what();
        mgp_result_set_error_msg(result, msg.c_str());
        return std::nullopt;
      }

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
        return std::nullopt;
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
    return std::nullopt;
  }

  return louvain_graph;
}

void communities(const mgp_list *args, const mgp_graph *graph,
                 mgp_result *result, mgp_memory *memory) {
  try {
    // Normalize vertex ids
    auto mem_to_louv_id = NormalizeVertexIds(graph, result, memory);
    if (!mem_to_louv_id) return;

    // Run louvain
    auto louvain_graph = RunLouvain(graph, result, memory, *mem_to_louv_id);
    if (!louvain_graph) return;

    // Return node ids and their corresponding communities.
    for (const auto &p : *mem_to_louv_id) {
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
          mgp_value_make_int(louvain_graph->Community(p.second), memory);
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
  } catch (const std::exception &e) {
    mgp_result_set_error_msg(result, e.what());
    return;
  }
}

void modularity(const mgp_list *args, const mgp_graph *graph,
                mgp_result *result, mgp_memory *memory) {
  try {
    // Normalize vertex ids
    auto mem_to_louv_id = NormalizeVertexIds(graph, result, memory);
    if (!mem_to_louv_id) return;

    // Run louvain
    auto louvain_graph = RunLouvain(graph, result, memory, *mem_to_louv_id);
    if (!louvain_graph) return;

    // Return graph modularity after Louvain
    // TODO(ipaljak) - consider allowing the user to specify seed communities
    // and
    //                 yield modularity values both before and after running
    //                 louvain.
    mgp_result_record *record = mgp_result_new_record(result);
    if (record == nullptr) {
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }

    mgp_value *modularity_value =
        mgp_value_make_double(louvain_graph->Modularity(), memory);
    if (modularity_value == nullptr) {
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }

    int value_inserted =
        mgp_result_record_insert(record, "modularity", modularity_value);

    mgp_value_destroy(modularity_value);

    if (!value_inserted) {
      mgp_result_set_error_msg(result, "Not enough memory!");
      return;
    }
  } catch (const std::exception &e) {
    mgp_result_set_error_msg(result, e.what());
    return;
  }
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module,
                               struct mgp_memory *memory) {
  struct mgp_proc *community_proc =
      mgp_module_add_read_procedure(module, "communities", communities);
  if (!community_proc) return 1;
  if (!mgp_proc_add_result(community_proc, "id", mgp_type_int())) return 1;
  if (!mgp_proc_add_result(community_proc, "community", mgp_type_int()))
    return 1;

  struct mgp_proc *modularity_proc =
      mgp_module_add_read_procedure(module, "modularity", modularity);
  if (!modularity_proc) return 1;
  if (!mgp_proc_add_result(modularity_proc, "modularity", mgp_type_float()))
    return 1;

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
