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

// NOTE: spectralModularityMaximization only exists in the legacy cugraph API
// and requires legacy::GraphCSRView. There is no modern API equivalent.

#include <cugraph/legacy/graph.hpp>
#include <cugraph/algorithms.hpp>

#include "mg_cugraph_utility.hpp"

namespace {
// NOTE: Spectral clustering legacy API only supports int32_t vertex/edge types
using vertex_t = int32_t;
using edge_t = int32_t;
using weight_t = double;

constexpr char const *kProcedureSpectralClustering = "get";

constexpr char const *kArgumentNumClusters = "num_clusters";
constexpr char const *kArgumentNumEigenvectors = "num_eigenvectors";
constexpr char const *kArgumentEvTolerance = "ev_tolerance";
constexpr char const *kArgumentEvMaxIter = "ev_max_iter";
constexpr char const *kArgumentKmeanTolerance = "kmean_tolerance";
constexpr char const *kArgumentKmeanMaxIter = "kmean_max_iter";
constexpr char const *kArgumentWeightProperty = "weight_property";

constexpr char const *kResultFieldNode = "node";
constexpr char const *kResultFieldCluster = "cluster";

const double kDefaultWeight = 1.0;
constexpr char const *kDefaultWeightProperty = "weight";

void InsertSpectralClusteringResult(mgp_graph *graph, mgp_result *result, mgp_memory *memory,
                                    const std::uint64_t node_id, std::int64_t cluster) {
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
  mg_utility::InsertIntValueResult(record, kResultFieldCluster, cluster, memory);
}

void SpectralClusteringProc(mgp_list *args, mgp_graph *graph, mgp_result *result, mgp_memory *memory) {
  try {
    int num_clusters = mgp::value_get_int(mgp::list_at(args, 0));
    int num_eigenvectors = mgp::value_get_int(mgp::list_at(args, 1));
    double ev_tolerance = mgp::value_get_double(mgp::list_at(args, 2));
    int ev_maxiter = mgp::value_get_int(mgp::list_at(args, 3));
    double kmean_tolerance = mgp::value_get_double(mgp::list_at(args, 4));
    int kmean_maxiter = mgp::value_get_int(mgp::list_at(args, 5));
    auto weight_property = mgp::value_get_string(mgp::list_at(args, 6));

    auto mg_graph = mg_utility::GetWeightedGraphView(graph, result, memory, mg_graph::GraphType::kUndirectedGraph,
                                                     weight_property, kDefaultWeight);
    if (mg_graph->Empty()) return;

    auto n_vertices = mg_graph.get()->Nodes().size();

    // Define handle and operation stream
    raft::handle_t handle{};
    auto stream = handle.get_stream();

    // IMPORTANT: Spectral clustering cuGraph algorithm works only on legacy CSR graph format
    auto cu_graph_ptr =
        mg_cugraph::CreateCugraphLegacyFromMemgraph<vertex_t, edge_t, weight_t>(*mg_graph.get(), handle);
    auto cu_graph_view = cu_graph_ptr->view();
    cu_graph_view.prop.directed = false;

    rmm::device_uvector<vertex_t> clustering_result(n_vertices, stream);

    // Create RNG state for cuGraph 25.x API
    raft::random::RngState rng_state(42);

    // Call spectralModularityMaximization API - cuGraph 25.x requires handle and rng_state
    cugraph::ext_raft::spectralModularityMaximization(handle, rng_state, cu_graph_view, num_clusters, num_eigenvectors,
                                            static_cast<weight_t>(ev_tolerance), ev_maxiter,
                                            static_cast<weight_t>(kmean_tolerance), kmean_maxiter,
                                            clustering_result.data());

    // Copy results to host and output
    std::vector<vertex_t> h_clustering(n_vertices);
    raft::update_host(h_clustering.data(), clustering_result.data(), n_vertices, stream);
    handle.sync_stream();

    for (vertex_t node_id = 0; node_id < static_cast<vertex_t>(n_vertices); ++node_id) {
      InsertSpectralClusteringResult(graph, result, memory, mg_graph->GetMemgraphNodeId(node_id),
                                     h_clustering[node_id]);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_num_eigenvectors;
  mgp_value *default_ev_tolerance;
  mgp_value *default_ev_maxiter;
  mgp_value *default_kmean_tolerance;
  mgp_value *default_kmean_maxiter;
  mgp_value *default_weight_property;
  try {
    auto *spectral_proc =
        mgp::module_add_read_procedure(module, kProcedureSpectralClustering, SpectralClusteringProc);

    default_num_eigenvectors = mgp::value_make_int(2, memory);
    default_ev_tolerance = mgp::value_make_double(0.00001, memory);
    default_ev_maxiter = mgp::value_make_int(100, memory);
    default_kmean_tolerance = mgp::value_make_double(0.00001, memory);
    default_kmean_maxiter = mgp::value_make_int(20, memory);
    default_weight_property = mgp::value_make_string(kDefaultWeightProperty, memory);

    mgp::proc_add_arg(spectral_proc, kArgumentNumClusters, mgp::type_int());
    mgp::proc_add_opt_arg(spectral_proc, kArgumentNumEigenvectors, mgp::type_int(), default_num_eigenvectors);
    mgp::proc_add_opt_arg(spectral_proc, kArgumentEvTolerance, mgp::type_float(), default_ev_tolerance);
    mgp::proc_add_opt_arg(spectral_proc, kArgumentEvMaxIter, mgp::type_int(), default_ev_maxiter);
    mgp::proc_add_opt_arg(spectral_proc, kArgumentKmeanTolerance, mgp::type_float(), default_kmean_tolerance);
    mgp::proc_add_opt_arg(spectral_proc, kArgumentKmeanMaxIter, mgp::type_int(), default_kmean_maxiter);
    mgp::proc_add_opt_arg(spectral_proc, kArgumentWeightProperty, mgp::type_string(), default_weight_property);

    mgp::proc_add_result(spectral_proc, kResultFieldNode, mgp::type_node());
    mgp::proc_add_result(spectral_proc, kResultFieldCluster, mgp::type_int());
  } catch (const std::exception &e) {
    mgp_value_destroy(default_num_eigenvectors);
    mgp_value_destroy(default_ev_tolerance);
    mgp_value_destroy(default_ev_maxiter);
    mgp_value_destroy(default_kmean_tolerance);
    mgp_value_destroy(default_kmean_maxiter);
    mgp_value_destroy(default_weight_property);
    return 1;
  }

  mgp_value_destroy(default_num_eigenvectors);
  mgp_value_destroy(default_ev_tolerance);
  mgp_value_destroy(default_ev_maxiter);
  mgp_value_destroy(default_kmean_tolerance);
  mgp_value_destroy(default_kmean_maxiter);
  mgp_value_destroy(default_weight_property);
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
