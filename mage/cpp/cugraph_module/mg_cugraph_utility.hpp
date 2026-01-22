// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cugraph/algorithms.hpp>
#include <cugraph/functions.hpp>  // legacy coo_to_csr
#include <cugraph/graph_functions.hpp>
#include <cugraph/graph_generators.hpp>

#include <raft/distance/distance.hpp>
#include <raft/handle.hpp>
#include <rmm/device_uvector.hpp>

#include <mg_exceptions.hpp>
#include <mg_utils.hpp>

namespace mg_cugraph {

///
///@brief Create a cuGraph graph object from a given Memgraph graph. This method generates the graph in the
/// coordinate view with edge list being defined.
///
///@tparam TVertexT Vertex identifier type
///@tparam TEdgeT Edge identifier type
///@tparam TWeightT Weight type
///@tparam TStoreTransposed Store transposed in memory
///@tparam TMultiGPU Multi-GPU Graph
///@param mg_graph Memgraph graph object
///@param graph_type Type of the graph - directed/undirected
///@param handle Handle for GPU communication
///@return cuGraph graph object
///
template <typename TVertexT = int64_t, typename TEdgeT = int64_t, typename TWeightT = double,
          bool TStoreTransposed = true, bool TMultiGPU = false>
auto CreateCugraphFromMemgraph(const mg_graph::GraphView<> &mg_graph, const mg_graph::GraphType graph_type,
                               raft::handle_t const &handle) {
  auto mg_edges = mg_graph.Edges();
  auto mg_nodes = mg_graph.Nodes();

  if (graph_type == mg_graph::GraphType::kUndirectedGraph) {
    std::vector<mg_graph::Edge<>> undirected_edges;
    std::transform(mg_edges.begin(), mg_edges.end(), std::back_inserter(undirected_edges),
                   [](const auto &edge) -> mg_graph::Edge<> {
                     return {edge.id, edge.to, edge.from};
                   });
    mg_edges.reserve(2 * mg_edges.size());
    mg_edges.insert(mg_edges.end(), undirected_edges.begin(), undirected_edges.end());
  }

  // Flatten the data vector
  std::vector<TVertexT> mg_src;
  mg_src.reserve(mg_edges.size());
  std::vector<TVertexT> mg_dst;
  mg_dst.reserve(mg_edges.size());
  std::vector<TWeightT> mg_weight;
  mg_weight.reserve(mg_edges.size());
  std::vector<TVertexT> mg_vertices;
  mg_vertices.reserve(mg_nodes.size());

  std::transform(mg_edges.begin(), mg_edges.end(), std::back_inserter(mg_src),
                 [](const auto &edge) -> TVertexT { return edge.from; });
  std::transform(mg_edges.begin(), mg_edges.end(), std::back_inserter(mg_dst),
                 [](const auto &edge) -> TVertexT { return edge.to; });

  std::transform(
      mg_edges.begin(), mg_edges.end(), std::back_inserter(mg_weight),
      [&mg_graph](const auto &edge) -> TWeightT { return mg_graph.IsWeighted() ? mg_graph.GetWeight(edge.id) : 1.0; });
  std::transform(mg_nodes.begin(), mg_nodes.end(), std::back_inserter(mg_vertices),
                 [](const auto &node) -> TVertexT { return node.id; });

  // Synchronize the data structures to the GPU
  auto stream = handle.get_stream();
  rmm::device_uvector<TVertexT> cu_src(mg_src.size(), stream);
  raft::update_device(cu_src.data(), mg_src.data(), mg_src.size(), stream);
  rmm::device_uvector<TVertexT> cu_dst(mg_dst.size(), stream);
  raft::update_device(cu_dst.data(), mg_dst.data(), mg_dst.size(), stream);
  rmm::device_uvector<TWeightT> cu_weight(mg_weight.size(), stream);
  raft::update_device(cu_weight.data(), mg_weight.data(), mg_weight.size(), stream);
  rmm::device_uvector<TVertexT> cu_vertices(mg_vertices.size(), stream);
  raft::update_device(cu_vertices.data(), mg_vertices.data(), mg_vertices.size(), stream);

  // TODO: Deal_with/pass edge weights to CuGraph graph.
  // TODO: Allow for multigraphs
  cugraph::graph_t<TVertexT, TEdgeT, TWeightT, TStoreTransposed, TMultiGPU> cu_graph(handle);
  // NOTE: Renumbering is not required because graph coming from Memgraph is already correctly numbered.
  std::tie(cu_graph, std::ignore) =
      cugraph::create_graph_from_edgelist<TVertexT, TEdgeT, TWeightT, TStoreTransposed, TMultiGPU>(
          handle, std::move(cu_vertices), std::move(cu_src), std::move(cu_dst), std::move(cu_weight),
          cugraph::graph_properties_t{false, false}, false, false);
  stream.synchronize_no_throw();

  return std::move(cu_graph);
}

///
///@brief Create a cuGraph legacy graph object from a given Memgraph graph. This method generates the graph in the
/// Compressed Sparse Row format that defines offsets and indices.  Description is available at [Compressed Sparse
/// Row Format for Representing Graphs - Terence
/// Kelly](https://www.usenix.org/system/files/login/articles/login_winter20_16_kelly.pdf)
///
///@tparam TVertexT Vertex identifier type
///@tparam TEdgeT Edge identifier type
///@tparam TWeightT Weight type
///@param mg_graph Memgraph graph object
///@param handle Handle for GPU communication
///@return cuGraph legacy graph object
///
template <typename TVertexT = int64_t, typename TEdgeT = int64_t, typename TWeightT = double>
auto CreateCugraphLegacyFromMemgraph(const mg_graph::GraphView<> &mg_graph, raft::handle_t const &handle) {
  auto mg_nodes = mg_graph.Nodes();
  auto mg_edges = mg_graph.Edges();
  const auto n_edges = mg_edges.size();
  const auto n_vertices = mg_nodes.size();

  // Flatten the data vector
  std::vector<TVertexT> mg_deg_sum;
  std::vector<TVertexT> mg_dst;
  std::vector<TWeightT> mg_weight;

  // TODO: Check for the first index
  mg_deg_sum.push_back(0);
  for (std::int64_t v_id = 0; v_id < n_vertices; v_id++) {
    mg_deg_sum.push_back(mg_deg_sum[v_id] + mg_graph.Neighbours(v_id).size());

    auto neighbors = mg_graph.Neighbours(v_id);
    sort(neighbors.begin(), neighbors.end(), [](mg_graph::Neighbour<> &l_neighbor, mg_graph::Neighbour<> &r_neighbor) {
      return l_neighbor.node_id < r_neighbor.node_id;
    });

    for (const auto dst : neighbors) {
      mg_dst.push_back(dst.node_id);
      mg_weight.push_back(mg_graph.IsWeighted() ? mg_graph.GetWeight(dst.edge_id) : 1.0);
    }
  }

  // Synchronize the data structures to the GPU
  auto stream = handle.get_stream();
  rmm::mr::device_memory_resource *mr = rmm::mr::get_current_device_resource();

  rmm::device_buffer offsets_buffer(mg_deg_sum.data(), sizeof(TEdgeT) * (mg_deg_sum.size()), stream, mr);
  rmm::device_buffer dsts_buffer(mg_dst.data(), sizeof(TVertexT) * (mg_dst.size()), stream, mr);
  rmm::device_buffer weights_buffer(mg_weight.data(), sizeof(TWeightT) * (mg_weight.size()), stream, mr);

  cugraph::legacy::GraphSparseContents<TVertexT, TEdgeT, TWeightT> csr_contents{
      static_cast<TVertexT>(n_vertices), static_cast<TEdgeT>(n_edges),
      std::make_unique<rmm::device_buffer>(std::move(offsets_buffer)),
      std::make_unique<rmm::device_buffer>(std::move(dsts_buffer)),
      std::make_unique<rmm::device_buffer>(std::move(weights_buffer))};

  return std::make_unique<cugraph::legacy::GraphCSR<TVertexT, TEdgeT, TWeightT>>(std::move(csr_contents));
}

///
///@brief RMAT (Recursive MATrix) Generator of a graph.
///
///@tparam TVertexT Vertex identifier type
///@tparam TEdgeT Edge identifier type
///@tparam TWeightT Weight type
///@param scale Scale factor for number of vertices. |V| = 2 ** scale
///@param num_edges Number of edges generated
///@param a Probability of the first partition
///@param b Probability of the second partition
///@param c Probability of the third partition
///@param seed Random seed applied
///@param clip_and_flip Clip and flip
///@param handle Handle for GPU communication
///@return Edges in edge list format
///
template <typename TVertexT = int64_t, typename TEdgeT = int64_t, typename TWeightT = double>
auto GenerateCugraphRMAT(std::size_t scale, std::size_t num_edges, double a, double b, double c, std::uint64_t seed,
                         bool clip_and_flip, raft::handle_t const &handle) {
  // Synchronize the data structures to the GPU
  auto stream = handle.get_stream();
  rmm::device_uvector<TVertexT> cu_src(num_edges, stream);
  rmm::device_uvector<TVertexT> cu_dst(num_edges, stream);

  std::tie(cu_src, cu_dst) =
      cugraph::generate_rmat_edgelist<TVertexT>(handle, scale, num_edges, a, b, c, seed, clip_and_flip);

  std::vector<std::pair<std::uint64_t, std::uint64_t>> mg_edges;
  for (std::size_t i = 0; i < num_edges; ++i) {
    auto src = static_cast<std::uint64_t>(cu_src.element(i, stream));
    auto dst = static_cast<std::uint64_t>(cu_dst.element(i, stream));

    mg_edges.emplace_back(src, dst);
  }
  return mg_edges;
}
}  // namespace mg_cugraph
