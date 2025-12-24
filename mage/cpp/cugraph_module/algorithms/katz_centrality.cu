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

constexpr char const *kProcedureKatz = "get";

constexpr char const *kArgumentAlpha = "alpha";
constexpr char const *kArgumentBeta = "beta";
constexpr char const *kArgumentEpsilon = "epsilon";
constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentNormalized = "normalized";
constexpr char const *kArgumentDirected = "directed";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldKatzCentrality = "katz_centrality";

void InsertKatzCentralityRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
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
  mg_utility::InsertDoubleValueResult(record, kResultFieldKatzCentrality, rank, memory);
}

void KatzCentralityProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto alpha_arg = static_cast<float>(mgp::value_get_double(mgp::list_at(args, 0)));
    auto beta_arg = static_cast<float>(mgp::value_get_double(mgp::list_at(args, 1)));
    auto epsilon_arg = static_cast<float>(mgp::value_get_double(mgp::list_at(args, 2)));
    auto max_iterations = mgp::value_get_int(mgp::list_at(args, 3));
    auto normalized = mgp::value_get_bool(mgp::list_at(args, 4));
    auto directed = mgp::value_get_bool(mgp::list_at(args, 5));

    // Currently doesn't support for weights
    auto graph_type = directed ? mg_graph::GraphType::kDirectedGraph : mg_graph::GraphType::kUndirectedGraph;
    auto mg_graph = mg_utility::GetGraphView(graph, result, memory, graph_type);
    if (mg_graph->Empty()) return;

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    auto cu_graph = mg_cugraph::CreateCugraphFromMemgraph(*mg_graph.get(), graph_type, handle);
    auto cu_graph_view = cu_graph.view();
    auto n_vertices = cu_graph_view.get_number_of_vertices();

    auto degrees = cu_graph_view.compute_in_degrees(handle);
    std::vector<edge_t> cu_degrees(degrees.size());
    raft::update_host(cu_degrees.data(), degrees.data(), degrees.size(), handle.get_stream());
    handle.sync_stream();
    auto max_degree = std::max_element(cu_degrees.begin(), cu_degrees.end());

    result_t alpha = result_t{alpha_arg} / static_cast<result_t>(*max_degree + 1);
    result_t beta{beta_arg};
    result_t epsilon{epsilon_arg};
    rmm::device_uvector<result_t> katz_results(n_vertices, stream);
    cugraph::katz_centrality(handle, cu_graph_view, static_cast<result_t *>(nullptr), katz_results.data(), alpha, beta,
                             epsilon, max_iterations, false, normalized, false);

    for (vertex_t node_id = 0; node_id < katz_results.size(); ++node_id) {
      auto rank = katz_results.element(node_id, stream);
      InsertKatzCentralityRecord(graph, result, memory, mg_graph->GetMemgraphNodeId(node_id), rank);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_alpha;
  mgp_value *default_beta;
  mgp_value *default_epsilon;
  mgp_value *default_normalized;
  mgp_value *default_max_iterations;
  mgp_value *default_directed;
  try {
    auto *katz_proc = mgp::module_add_read_procedure(module, kProcedureKatz, KatzCentralityProc);

    default_alpha = mgp::value_make_double(1.0, memory);
    default_beta = mgp::value_make_double(1.0, memory);
    default_epsilon = mgp::value_make_double(1e-6, memory);
    default_normalized = mgp::value_make_bool(true, memory);
    default_max_iterations = mgp::value_make_int(100, memory);
    default_directed = mgp::value_make_bool(true, memory);

    mgp::proc_add_opt_arg(katz_proc, kArgumentAlpha, mgp::type_float(), default_alpha);
    mgp::proc_add_opt_arg(katz_proc, kArgumentBeta, mgp::type_float(), default_beta);
    mgp::proc_add_opt_arg(katz_proc, kArgumentEpsilon, mgp::type_float(), default_epsilon);
    mgp::proc_add_opt_arg(katz_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(katz_proc, kArgumentNormalized, mgp::type_bool(), default_normalized);
    mgp::proc_add_opt_arg(katz_proc, kArgumentDirected, mgp::type_bool(), default_directed);

    mgp::proc_add_result(katz_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(katz_proc, kResultFieldKatzCentrality, mgp::type_float());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_alpha);
    mgp_value_destroy(default_beta);
    mgp_value_destroy(default_epsilon);
    mgp_value_destroy(default_normalized);
    mgp_value_destroy(default_max_iterations);
    mgp_value_destroy(default_directed);
    return 1;
  }

  mgp_value_destroy(default_alpha);
  mgp_value_destroy(default_beta);
  mgp_value_destroy(default_epsilon);
  mgp_value_destroy(default_normalized);
  mgp_value_destroy(default_max_iterations);
  mgp_value_destroy(default_directed);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
