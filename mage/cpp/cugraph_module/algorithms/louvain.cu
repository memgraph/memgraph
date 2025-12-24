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

constexpr char const *kProcedureLouvain = "get";

constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentResolution = "resolution";
constexpr char const *kArgumentDirected = "directed";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldPartition = "partition";

void InsertLouvainRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
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

void LouvainProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto max_iterations = mgp::value_get_int(mgp::list_at(args, 0));
    auto resolution = mgp::value_get_double(mgp::list_at(args, 1));
    auto directed = mgp::value_get_bool(mgp::list_at(args, 2));

    auto graph_type = directed ? mg_graph::GraphType::kDirectedGraph : mg_graph::GraphType::kUndirectedGraph;
    auto mg_graph = mg_utility::GetGraphView(graph, result, memory, graph_type);
    if (mg_graph->Empty()) return;

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    // IMPORTANT: Louvain cuGraph algorithm works only on non-transposed graph instances
    auto cu_graph = mg_cugraph::CreateCugraphFromMemgraph<vertex_t, edge_t, weight_t, false, false>(*mg_graph.get(),
                                                                                                    graph_type, handle);
    auto cu_graph_view = cu_graph.view();
    auto n_vertices = cu_graph_view.get_number_of_vertices();

    rmm::device_uvector<vertex_t> clustering_result(n_vertices, stream);
    cugraph::louvain(handle, cu_graph_view, clustering_result.data(), max_iterations, resolution);

    for (vertex_t node_id = 0; node_id < clustering_result.size(); ++node_id) {
      auto partition = clustering_result.element(node_id, stream);
      InsertLouvainRecord(graph, result, memory, mg_graph->GetMemgraphNodeId(node_id), partition);
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
  mgp_value *default_resolution;
  mgp_value *default_directed;
  try {
    auto *louvain_proc = mgp::module_add_read_procedure(module, kProcedureLouvain, LouvainProc);

    default_max_iterations = mgp::value_make_int(100, memory);
    default_resolution = mgp::value_make_double(1.0, memory);
    default_directed = mgp::value_make_bool(true, memory);

    mgp::proc_add_opt_arg(louvain_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(louvain_proc, kArgumentResolution, mgp::type_float(), default_resolution);
    mgp::proc_add_opt_arg(louvain_proc, kArgumentDirected, mgp::type_bool(), default_directed);

    mgp::proc_add_result(louvain_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(louvain_proc, kResultFieldPartition, mgp::type_int());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_max_iterations);
    mgp_value_destroy(default_resolution);
    mgp_value_destroy(default_directed);
    return 1;
  }

  mgp_value_destroy(default_max_iterations);
  mgp_value_destroy(default_resolution);
  mgp_value_destroy(default_directed);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
