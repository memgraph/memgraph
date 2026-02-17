// Copyright (c) 2016-2022 Memgraph Ltd. [https://memgraph.com]
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mg_cugraph_utility.hpp"

#include <unordered_map>
#include <vector>

namespace {
using vertex_t = int64_t;
using edge_t = int64_t;
using weight_t = double;
using result_t = double;

constexpr char const *kProcedurePagerank = "get";

constexpr char const *kArgumentPersonalizationVertices = "personalization_vertices";
constexpr char const *kArgumentPersonalizationValues = "personalization_values";
constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentDampingFactor = "damping_factor";
constexpr char const *kArgumentStopEpsilon = "stop_epsilon";
constexpr char const *kArgumentWeightProperty = "weight_property";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldPageRank = "pagerank";

const double kDefaultWeight = 1.0;
constexpr char const *kDefaultWeightProperty = "weight";

void InsertPersonalizedPagerankRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory,
                                      const std::uint64_t node_id, double pagerank) {
  auto *node = mgp::graph_get_vertex_by_id(graph, mgp_vertex_id{.as_int = static_cast<int64_t>(node_id)}, memory);
  if (!node) {
    if (mgp::graph_is_transactional(graph)) {
      throw mg_exception::InvalidIDException();
    }
    return;
  }

  auto *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertNodeValueResult(record, kResultFieldNode, node, memory);
  mg_utility::InsertDoubleValueResult(record, kResultFieldPageRank, pagerank, memory);
}

void PersonalizedPagerankProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto l_personalization_vertices = mgp::value_get_list(mgp::list_at(args, 0));
    auto l_personalization_values = mgp::value_get_list(mgp::list_at(args, 1));
    auto max_iterations = static_cast<std::size_t>(mgp::value_get_int(mgp::list_at(args, 2)));
    auto damping_factor = mgp::value_get_double(mgp::list_at(args, 3));
    auto stop_epsilon = mgp::value_get_double(mgp::list_at(args, 4));
    auto weight_property = mgp::value_get_string(mgp::list_at(args, 5));

    const auto n_seeds = mgp::list_size(l_personalization_vertices);
    const auto n_vals = mgp::list_size(l_personalization_values);
    if (n_seeds != n_vals) {
      throw std::runtime_error("personalization_vertices and personalization_values must have the same length.");
    }
    if (n_seeds == 0) return;

    // --- Build weighted MG graph view ---
    auto mg_graph = mg_utility::GetWeightedGraphView(graph, result, memory, mg_graph::GraphType::kDirectedGraph,
                                                     weight_property, kDefaultWeight);
    if (mg_graph->Empty()) return;

    // --- Define handle and operation stream ---
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    // PageRank requires store_transposed = true
    // Create cuGraph + edge props + renumber map (new->old index mapping).
    auto [cu_graph, edge_props, renumber_map] =
        mg_cugraph::CreateCugraphFromMemgraph<vertex_t, edge_t, weight_t, true, false>(
            *mg_graph.get(), mg_graph::GraphType::kDirectedGraph, handle);

    auto cu_graph_view = cu_graph.view();
    auto n_vertices = cu_graph_view.number_of_vertices();

    // Get edge weight view from edge properties
    auto edge_weight_view = mg_cugraph::GetEdgeWeightView<edge_t>(edge_props);

    // Build reverse mapping: old GraphView index -> new cuGraph index
    std::unordered_map<vertex_t, vertex_t> old_to_new;
    old_to_new.reserve(renumber_map.size());
    for (size_t i = 0; i < renumber_map.size(); i++) {
      old_to_new[renumber_map[i]] = static_cast<vertex_t>(i);
    }

    // --- Prepare personalization seeds (support multiple seeds, skipping isolated nodes) ---
    std::vector<vertex_t> h_personalization_vertices;
    std::vector<result_t> h_personalization_values;
    h_personalization_vertices.reserve(n_seeds);
    h_personalization_values.reserve(n_seeds);

    for (std::size_t i = 0; i < n_seeds; i++) {
      auto *v = mgp::value_get_vertex(mgp::list_at(l_personalization_vertices, i));
      auto memgraph_id = static_cast<vertex_t>(mgp::vertex_get_id(v).as_int);

      // Map Memgraph ID -> old GraphView internal ID (pre-filter)
      auto old_internal = static_cast<vertex_t>(mg_graph->GetInnerNodeId(memgraph_id));

      // Map old GraphView internal ID -> new cuGraph internal ID (post-filter/renumber)
      auto it = old_to_new.find(old_internal);
      if (it == old_to_new.end()) {
        // Seed is isolated/filtered-out in cuGraph graph; skip it.
        continue;
      }

      auto value = static_cast<result_t>(mgp::value_get_double(mgp::list_at(l_personalization_values, i)));
      h_personalization_vertices.push_back(it->second);
      h_personalization_values.push_back(value);
    }

    // If all seeds got skipped (e.g., they were isolated), return empty results
    if (h_personalization_vertices.empty()) return;

    // Copy personalization to device
    rmm::device_uvector<vertex_t> d_pers_vertices(h_personalization_vertices.size(), stream);
    rmm::device_uvector<result_t> d_pers_values(h_personalization_values.size(), stream);
    raft::update_device(d_pers_vertices.data(), h_personalization_vertices.data(), h_personalization_vertices.size(),
                        stream);
    raft::update_device(d_pers_values.data(), h_personalization_values.data(), h_personalization_values.size(), stream);

    // Create personalization tuple: spans over (vertices, values)
    auto personalization = std::make_optional(std::make_tuple(
        raft::device_span<vertex_t const>(d_pers_vertices.data(), d_pers_vertices.size()),
        raft::device_span<result_t const>(d_pers_values.data(), d_pers_values.size())));

    // --- Run modern cuGraph 25.x PageRank API with personalization ---
    auto [pageranks, metadata] = cugraph::pagerank<vertex_t, edge_t, weight_t, result_t, false>(
        handle,
        cu_graph_view,
        edge_weight_view,
        std::nullopt,  // precomputed_vertex_out_weight_sums
        personalization,
        std::nullopt,  // initial_pageranks
        static_cast<result_t>(damping_factor),
        static_cast<result_t>(stop_epsilon),
        max_iterations,
        false);  // do_expensive_check

    // Copy results to host and output
    std::vector<result_t> h_pageranks(n_vertices);
    raft::update_host(h_pageranks.data(), pageranks.data(), n_vertices, stream);
    handle.sync_stream();

    // Translate cuGraph indices back to original GraphView indices via renumber_map, then to Memgraph IDs
    for (vertex_t new_id = 0; new_id < static_cast<vertex_t>(n_vertices); ++new_id) {
      auto old_id = static_cast<vertex_t>(renumber_map[new_id]);
      auto memgraph_node_id = mg_graph->GetMemgraphNodeId(old_id);
      InsertPersonalizedPagerankRecord(graph, result, memory, memgraph_node_id, h_pageranks[new_id]);
    }

  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_max_iterations = nullptr;
  mgp_value *default_damping_factor = nullptr;
  mgp_value *default_stop_epsilon = nullptr;
  mgp_value *default_weight_property = nullptr;

  try {
    auto *proc = mgp::module_add_read_procedure(module, kProcedurePagerank, PersonalizedPagerankProc);

    default_max_iterations = mgp::value_make_int(100, memory);
    default_damping_factor = mgp::value_make_double(0.85, memory);
    default_stop_epsilon = mgp::value_make_double(1e-5, memory);
    default_weight_property = mgp::value_make_string(kDefaultWeightProperty, memory);

    // Preserve original args
    mgp::proc_add_arg(proc, kArgumentPersonalizationVertices, mgp::type_list(mgp::type_node()));
    mgp::proc_add_arg(proc, kArgumentPersonalizationValues, mgp::type_list(mgp::type_float()));
    mgp::proc_add_opt_arg(proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(proc, kArgumentDampingFactor, mgp::type_float(), default_damping_factor);
    mgp::proc_add_opt_arg(proc, kArgumentStopEpsilon, mgp::type_float(), default_stop_epsilon);
    mgp::proc_add_opt_arg(proc, kArgumentWeightProperty, mgp::type_string(), default_weight_property);

    mgp::proc_add_result(proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(proc, kResultFieldPageRank, mgp::type_float());
  } catch (const std::exception &) {
    mgp_value_destroy(default_max_iterations);
    mgp_value_destroy(default_damping_factor);
    mgp_value_destroy(default_stop_epsilon);
    mgp_value_destroy(default_weight_property);
    return 1;
  }

  mgp_value_destroy(default_max_iterations);
  mgp_value_destroy(default_damping_factor);
  mgp_value_destroy(default_stop_epsilon);
  mgp_value_destroy(default_weight_property);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
