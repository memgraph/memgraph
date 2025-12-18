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

constexpr char const *kProcedureHITS = "get";

constexpr char const *kArgumentTolerance = "tolerance";
constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentNormalize = "normalized";
constexpr char const *kArgumentDirected = "directed";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldHubScore = "hubs";
constexpr char const *kResultFieldAuthoritiesScore = "authorities";

void InsertHITSRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
                      double hubs, double authorities) {
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
  mg_utility::InsertDoubleValueResult(record, kResultFieldHubScore, hubs, memory);
  mg_utility::InsertDoubleValueResult(record, kResultFieldAuthoritiesScore, authorities, memory);
}

void HITSProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto tolerance = mgp::value_get_double(mgp::list_at(args, 0));
    auto max_iterations = mgp::value_get_int(mgp::list_at(args, 1));
    auto normalize = mgp::value_get_bool(mgp::list_at(args, 2));
    auto directed = mgp::value_get_bool(mgp::list_at(args, 3));

    // Works with unweighted graph
    auto graph_type = directed ? mg_graph::GraphType::kDirectedGraph : mg_graph::GraphType::kUndirectedGraph;
    auto mg_graph = mg_utility::GetGraphView(graph, result, memory, graph_type);
    if (mg_graph->Empty()) return;

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    auto cu_graph = mg_cugraph::CreateCugraphFromMemgraph(*mg_graph.get(), graph_type, handle);
    auto cu_graph_view = cu_graph.view();

    rmm::device_uvector<result_t> hubs_result(cu_graph_view.get_number_of_local_vertices(), stream);
    rmm::device_uvector<result_t> authorities_result(cu_graph_view.get_number_of_local_vertices(), stream);
    cugraph::hits(handle, cu_graph_view, hubs_result.data(), authorities_result.data(), tolerance, max_iterations,
                  false, normalize, false);

    for (vertex_t node_id = 0; node_id < hubs_result.size(); ++node_id) {
      auto hubs = hubs_result.element(node_id, stream);
      auto authorities = authorities_result.element(node_id, stream);
      InsertHITSRecord(graph, result, memory, mg_graph->GetMemgraphNodeId(node_id), hubs, authorities);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_tolerance;
  mgp_value *default_max_iterations;
  mgp_value *default_normalize;
  mgp_value *default_directed;
  try {
    auto *hits_proc = mgp::module_add_read_procedure(module, kProcedureHITS, HITSProc);

    default_tolerance = mgp::value_make_double(1e-5, memory);
    default_max_iterations = mgp::value_make_int(100, memory);
    default_normalize = mgp::value_make_bool(true, memory);
    default_directed = mgp::value_make_bool(true, memory);

    mgp::proc_add_opt_arg(hits_proc, kArgumentTolerance, mgp::type_float(), default_tolerance);
    mgp::proc_add_opt_arg(hits_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(hits_proc, kArgumentNormalize, mgp::type_bool(), default_normalize);
    mgp::proc_add_opt_arg(hits_proc, kArgumentDirected, mgp::type_bool(), default_directed);

    mgp::proc_add_result(hits_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(hits_proc, kResultFieldHubScore, mgp::type_float());
    mgp::proc_add_result(hits_proc, kResultFieldAuthoritiesScore, mgp::type_float());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_tolerance);
    mgp_value_destroy(default_max_iterations);
    mgp_value_destroy(default_normalize);
    mgp_value_destroy(default_directed);
    return 1;
  }

  mgp_value_destroy(default_tolerance);
  mgp_value_destroy(default_max_iterations);
  mgp_value_destroy(default_normalize);
  mgp_value_destroy(default_directed);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
