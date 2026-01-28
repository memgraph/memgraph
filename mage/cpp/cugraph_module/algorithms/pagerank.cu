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
using result_t = double;

constexpr char const *kProcedurePagerank = "get";

constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentDampingFactor = "damping_factor";
constexpr char const *kArgumentStopEpsilon = "stop_epsilon";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldPageRank = "pagerank";

void InsertPagerankRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
                          double rank) {
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
  mg_utility::InsertDoubleValueResult(record, kResultFieldPageRank, rank, memory);
}

void PagerankProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto max_iterations = static_cast<std::size_t>(mgp::value_get_int(mgp::list_at(args, 0)));
    auto damping_factor = mgp::value_get_double(mgp::list_at(args, 1));
    auto stop_epsilon = mgp::value_get_double(mgp::list_at(args, 2));

    auto mg_graph = mg_utility::GetGraphView(graph, result, memory, mg_graph::GraphType::kDirectedGraph);
    if (mg_graph->Empty()) return;

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    // PageRank requires store_transposed = true
    auto [cu_graph, edge_props, renumber_map] = mg_cugraph::CreateCugraphFromMemgraph<vertex_t, edge_t, weight_t, true, false>(
        *mg_graph.get(), mg_graph::GraphType::kDirectedGraph, handle);

    auto cu_graph_view = cu_graph.view();
    auto n_vertices = cu_graph_view.number_of_vertices();

    // Get edge weight view from edge properties
    auto edge_weight_view = mg_cugraph::GetEdgeWeightView<edge_t>(edge_props);

    // Modern cuGraph 25.x PageRank API returns tuple<uvector, metadata>
    auto [pageranks, metadata] = cugraph::pagerank<vertex_t, edge_t, weight_t, result_t, false>(
        handle,
        cu_graph_view,
        edge_weight_view,
        std::nullopt,  // precomputed_vertex_out_weight_sums
        std::nullopt,  // personalization
        std::nullopt,  // initial_pageranks
        static_cast<result_t>(damping_factor),
        static_cast<result_t>(stop_epsilon),
        max_iterations,
        false);  // do_expensive_check

    // Copy results to host and output
    std::vector<result_t> h_pageranks(n_vertices);
    raft::update_host(h_pageranks.data(), pageranks.data(), n_vertices, stream);
    handle.sync_stream();

    // Use renumber_map to translate cuGraph indices back to original GraphView indices
    for (vertex_t node_id = 0; node_id < static_cast<vertex_t>(n_vertices); ++node_id) {
      auto original_id = renumber_map[node_id];
      InsertPagerankRecord(graph, result, memory, mg_graph->GetMemgraphNodeId(original_id), h_pageranks[node_id]);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_max_iterations;
  mgp_value *default_damping_factor;
  mgp_value *default_stop_epsilon;
  try {
    auto *pagerank_proc = mgp::module_add_read_procedure(module, kProcedurePagerank, PagerankProc);

    default_max_iterations = mgp::value_make_int(100, memory);
    default_damping_factor = mgp::value_make_double(0.85, memory);
    default_stop_epsilon = mgp::value_make_double(1e-5, memory);

    mgp::proc_add_opt_arg(pagerank_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(pagerank_proc, kArgumentDampingFactor, mgp::type_float(), default_damping_factor);
    mgp::proc_add_opt_arg(pagerank_proc, kArgumentStopEpsilon, mgp::type_float(), default_stop_epsilon);

    mgp::proc_add_result(pagerank_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(pagerank_proc, kResultFieldPageRank, mgp::type_float());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_max_iterations);
    mgp_value_destroy(default_damping_factor);
    mgp_value_destroy(default_stop_epsilon);
    return 1;
  }

  mgp_value_destroy(default_max_iterations);
  mgp_value_destroy(default_damping_factor);
  mgp_value_destroy(default_stop_epsilon);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
