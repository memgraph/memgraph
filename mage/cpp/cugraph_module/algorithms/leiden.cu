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

namespace {
using vertex_t = int64_t;
using edge_t = int64_t;
using weight_t = double;

constexpr char const *kProcedureLeiden = "get";

constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentResolution = "resolution";
constexpr char const *kArgumentTheta = "theta";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldPartition = "partition";

void InsertLeidenRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
                        std::int64_t partition) {
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
  mg_utility::InsertIntValueResult(record, kResultFieldPartition, partition, memory);
}

void LeidenProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto max_iterations = static_cast<std::size_t>(mgp::value_get_int(mgp::list_at(args, 0)));
    auto resolution = mgp::value_get_double(mgp::list_at(args, 1));
    auto theta = mgp::value_get_double(mgp::list_at(args, 2));

    auto mg_graph = mg_utility::GetGraphView(graph, result, memory, mg_graph::GraphType::kUndirectedGraph);
    if (mg_graph->Empty()) return;

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    // Leiden requires store_transposed = false
    auto [cu_graph, edge_props, renumber_map] = mg_cugraph::CreateCugraphFromMemgraph<vertex_t, edge_t, weight_t, false, false>(
        *mg_graph.get(), mg_graph::GraphType::kUndirectedGraph, handle);

    auto cu_graph_view = cu_graph.view();
    auto n_vertices = cu_graph_view.number_of_vertices();

    // Get edge weight view from edge properties
    auto edge_weight_view = mg_cugraph::GetEdgeWeightView<edge_t>(edge_props);

    // Allocate clustering output
    rmm::device_uvector<vertex_t> clustering_result(n_vertices, stream);

    // Create RNG state for Leiden - NOTE: Leiden takes reference, not optional
    raft::random::RngState rng_state(42);

    // Modern cuGraph 25.x Leiden API - returns pair<size_t, weight_t>
    // Signature: leiden(handle, rng_state&, graph_view, edge_weight_view, clustering*, max_level, resolution, theta)
    auto [levels, modularity] = cugraph::leiden<vertex_t, edge_t, weight_t, false>(
        handle,
        rng_state,  // Reference, not optional (unlike Louvain)
        cu_graph_view,
        edge_weight_view,
        clustering_result.data(),
        max_iterations,
        static_cast<weight_t>(resolution),
        static_cast<weight_t>(theta));

    // Copy results to host and output
    std::vector<vertex_t> h_clustering(n_vertices);
    raft::update_host(h_clustering.data(), clustering_result.data(), n_vertices, stream);
    handle.sync_stream();

    // Use renumber_map to translate cuGraph indices back to original GraphView indices
    for (vertex_t node_id = 0; node_id < static_cast<vertex_t>(n_vertices); ++node_id) {
      auto original_id = renumber_map[node_id];
      InsertLeidenRecord(graph, result, memory, mg_graph->GetMemgraphNodeId(original_id), h_clustering[node_id]);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_max_iterations;
  mgp_value *default_resolution;
  mgp_value *default_theta;
  try {
    auto *leiden_proc = mgp::module_add_read_procedure(module, kProcedureLeiden, LeidenProc);

    default_max_iterations = mgp::value_make_int(100, memory);
    default_resolution = mgp::value_make_double(1.0, memory);
    default_theta = mgp::value_make_double(1.0, memory);

    mgp::proc_add_opt_arg(leiden_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(leiden_proc, kArgumentResolution, mgp::type_float(), default_resolution);
    mgp::proc_add_opt_arg(leiden_proc, kArgumentTheta, mgp::type_float(), default_theta);

    mgp::proc_add_result(leiden_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(leiden_proc, kResultFieldPartition, mgp::type_int());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_max_iterations);
    mgp_value_destroy(default_resolution);
    mgp_value_destroy(default_theta);
    return 1;
  }

  mgp_value_destroy(default_max_iterations);
  mgp_value_destroy(default_resolution);
  mgp_value_destroy(default_theta);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
