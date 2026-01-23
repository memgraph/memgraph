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
// TODO: Check Leiden instances. Update in new cuGraph API.
using vertex_t = int32_t;
using edge_t = int32_t;
using weight_t = double;

constexpr char const *kProcedureLeiden = "get";

constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentResolution = "resolution";

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
    auto max_iterations = mgp::value_get_int(mgp::list_at(args, 0));
    auto resolution = mgp::value_get_double(mgp::list_at(args, 1));

    auto mg_graph = mg_utility::GetGraphView(graph, result, memory, mg_graph::GraphType::kUndirectedGraph);
    if (mg_graph->Empty()) return;

    auto n_vertices = mg_graph.get()->Nodes().size();

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    // IMPORTANT: Leiden cuGraph algorithm works only on legacy code
    auto cu_graph_ptr =
        mg_cugraph::CreateCugraphLegacyFromMemgraph<vertex_t, edge_t, weight_t>(*mg_graph.get(), handle);
    auto cu_graph_view = cu_graph_ptr->view();
    cu_graph_view.prop.directed = false;

    rmm::device_uvector<vertex_t> clustering_result(n_vertices, stream);
    cugraph::leiden<vertex_t, edge_t, weight_t>(handle, cu_graph_view, clustering_result.data(), max_iterations, resolution);

    for (vertex_t node_id = 0; node_id < clustering_result.size(); ++node_id) {
      auto partition = clustering_result.element(node_id, stream);
      InsertLeidenRecord(graph, result, memory, mg_graph->GetMemgraphNodeId(node_id), partition);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_max_iter;
  mgp_value *default_resolution;
  try {
    auto *leiden_proc = mgp::module_add_read_procedure(module, kProcedureLeiden, LeidenProc);

    default_max_iter = mgp::value_make_int(100, memory);
    default_resolution = mgp::value_make_double(1.0, memory);

    mgp::proc_add_opt_arg(leiden_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iter);
    mgp::proc_add_opt_arg(leiden_proc, kArgumentResolution, mgp::type_float(), default_resolution);

    mgp::proc_add_result(leiden_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(leiden_proc, kResultFieldPartition, mgp::type_int());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_max_iter);
    mgp_value_destroy(default_resolution);
    return 1;
  }

  mgp_value_destroy(default_max_iter);
  mgp_value_destroy(default_resolution);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
