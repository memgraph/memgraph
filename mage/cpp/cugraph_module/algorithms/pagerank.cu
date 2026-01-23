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
constexpr char const *kArgumentWeightProperty = "weight_property";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldPageRank = "pagerank";

const double kDefaultWeight = 1.0;
constexpr char const *kDefaultWeightProperty = "weight";

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
    auto max_iterations = mgp::value_get_int(mgp::list_at(args, 0));
    auto damping_factor = mgp::value_get_double(mgp::list_at(args, 1));
    auto stop_epsilon = mgp::value_get_double(mgp::list_at(args, 2));
    auto weight_property = mgp::value_get_string(mgp::list_at(args, 3));

    auto mg_graph = mg_utility::GetWeightedGraphView(graph, result, memory, mg_graph::GraphType::kDirectedGraph,
                                                     weight_property, kDefaultWeight);
    if (mg_graph->Empty()) return;

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    auto cu_graph = mg_cugraph::CreateCugraphFromMemgraph(*mg_graph.get(), mg_graph::GraphType::kDirectedGraph, handle);
    auto cu_graph_view = cu_graph.view();
    auto n_vertices = cu_graph_view.get_number_of_vertices();

    rmm::device_uvector<result_t> pagerank_results(n_vertices, stream);
    // IMPORTANT: store_transposed has to be true because cugraph::pagerank
    // only accepts true. It's hard to detect/debug problem because nvcc error
    // messages contain only the top call details + graph_view has many
    // template parameters.
    cugraph::pagerank<vertex_t, edge_t, weight_t, result_t, false>(handle, cu_graph_view, std::nullopt, std::nullopt,
                                                                   std::nullopt, std::nullopt, pagerank_results.data(),
                                                                   damping_factor, stop_epsilon, max_iterations);

    for (vertex_t node_id = 0; node_id < pagerank_results.size(); ++node_id) {
      auto rank = pagerank_results.element(node_id, stream);
      InsertPagerankRecord(graph, result, memory, mg_graph->GetMemgraphNodeId(node_id), rank);
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
  mgp_value *default_damping_factor;
  mgp_value *default_stop_epsilon;
  mgp_value *default_weight_property;
  try {
    auto *pagerank_proc = mgp::module_add_read_procedure(module, kProcedurePagerank, PagerankProc);

    default_max_iterations = mgp::value_make_int(100, memory);
    default_damping_factor = mgp::value_make_double(0.85, memory);
    default_stop_epsilon = mgp::value_make_double(1e-5, memory);
    default_weight_property = mgp::value_make_string(kDefaultWeightProperty, memory);

    mgp::proc_add_opt_arg(pagerank_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(pagerank_proc, kArgumentDampingFactor, mgp::type_float(), default_damping_factor);
    mgp::proc_add_opt_arg(pagerank_proc, kArgumentStopEpsilon, mgp::type_float(), default_stop_epsilon);
    mgp::proc_add_opt_arg(pagerank_proc, kArgumentWeightProperty, mgp::type_string(), default_weight_property);

    mgp::proc_add_result(pagerank_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(pagerank_proc, kResultFieldPageRank, mgp::type_float());
  } catch (const std::exception &e) {
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
