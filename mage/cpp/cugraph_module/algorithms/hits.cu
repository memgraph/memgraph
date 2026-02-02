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

constexpr char const *kProcedureHits = "get";

constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentTolerance = "tolerance";
constexpr char const *kArgumentNormalized = "normalized";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldHubScore = "hubs";
constexpr char const *kResultFieldAuthoritiesScore = "authorities";

void InsertHitsRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
                      double hub, double authority) {
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
  mg_utility::InsertDoubleValueResult(record, kResultFieldHubScore, hub, memory);
  mg_utility::InsertDoubleValueResult(record, kResultFieldAuthoritiesScore, authority, memory);
}

void HitsProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto max_iterations = static_cast<std::size_t>(mgp::value_get_int(mgp::list_at(args, 0)));
    auto tolerance = mgp::value_get_double(mgp::list_at(args, 1));
    auto normalize = mgp::value_get_bool(mgp::list_at(args, 2));

    auto mg_graph = mg_utility::GetGraphView(graph, result, memory, mg_graph::GraphType::kDirectedGraph);
    if (mg_graph->Empty()) return;

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    // HITS requires store_transposed = true
    auto [cu_graph, edge_props, renumber_map] = mg_cugraph::CreateCugraphFromMemgraph<vertex_t, edge_t, weight_t, true, false>(
        *mg_graph.get(), mg_graph::GraphType::kDirectedGraph, handle);

    auto cu_graph_view = cu_graph.view();
    auto n_vertices = cu_graph_view.number_of_vertices();

    // Allocate output buffers
    rmm::device_uvector<result_t> hubs(n_vertices, stream);
    rmm::device_uvector<result_t> authorities(n_vertices, stream);

    // Modern cuGraph 25.x HITS API - returns tuple<result_t, size_t>
    auto [hub_diff, iterations] = cugraph::hits<vertex_t, edge_t, result_t, false>(
        handle,
        cu_graph_view,
        hubs.data(),
        authorities.data(),
        static_cast<result_t>(tolerance),
        max_iterations,
        false,     // has_initial_hubs_guess
        normalize,
        false);    // do_expensive_check

    // Copy results to host and output
    std::vector<result_t> h_hubs(n_vertices);
    std::vector<result_t> h_authorities(n_vertices);
    raft::update_host(h_hubs.data(), hubs.data(), n_vertices, stream);
    raft::update_host(h_authorities.data(), authorities.data(), n_vertices, stream);
    handle.sync_stream();

    // Use renumber_map to translate cuGraph indices back to original GraphView indices
    for (vertex_t node_id = 0; node_id < static_cast<vertex_t>(n_vertices); ++node_id) {
      auto original_id = renumber_map[node_id];
      InsertHitsRecord(graph, result, memory, mg_graph->GetMemgraphNodeId(original_id), h_hubs[node_id],
                       h_authorities[node_id]);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_max_iterations;
  mgp_value *default_tolerance;
  mgp_value *default_normalize;
  try {
    auto *hits_proc = mgp::module_add_read_procedure(module, kProcedureHits, HitsProc);

    default_max_iterations = mgp::value_make_int(100, memory);
    default_tolerance = mgp::value_make_double(1e-5, memory);
    default_normalize = mgp::value_make_bool(true, memory);

    mgp::proc_add_opt_arg(hits_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(hits_proc, kArgumentTolerance, mgp::type_float(), default_tolerance);
    mgp::proc_add_opt_arg(hits_proc, kArgumentNormalized, mgp::type_bool(), default_normalize);

    mgp::proc_add_result(hits_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(hits_proc, kResultFieldHubScore, mgp::type_float());
    mgp::proc_add_result(hits_proc, kResultFieldAuthoritiesScore, mgp::type_float());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_max_iterations);
    mgp_value_destroy(default_tolerance);
    mgp_value_destroy(default_normalize);
    return 1;
  }

  mgp_value_destroy(default_max_iterations);
  mgp_value_destroy(default_tolerance);
  mgp_value_destroy(default_normalize);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
