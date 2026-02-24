import enum
from collections import defaultdict
from typing import Dict, List, Tuple

import igraph
import mgp


class MemgraphIgraph(igraph.Graph):
    def __init__(self, ctx: mgp.ProcCtx, directed: bool):
        self.ctx_graph = ctx.graph
        self.id_mappings, self.reverse_id_mappings = self._create_igraph_from_ctx(ctx=ctx, directed=directed)

    def maxflow(self, source: mgp.Vertex, target: mgp.Vertex, capacity: str) -> float:
        return (
            super()
            .maxflow(
                self.id_mappings[source.id],
                self.id_mappings[target.id],
                capacity=capacity,
            )
            .value
        )

    def pagerank(
        self,
        weights: str,
        directed: bool,
        damping: float,
        implementation: str,
    ) -> List[Tuple[mgp.Vertex, float]]:
        pagerank_values = super().pagerank(
            weights=weights,
            directed=directed,
            damping=damping,
            implementation=implementation,
        )

        return [(self.get_vertex_by_id(node_id), rank) for node_id, rank in enumerate(pagerank_values)]

    def get_all_simple_paths(self, v: mgp.Vertex, to: mgp.Vertex, cutoff: int) -> List[List[mgp.Vertex]]:
        paths = [
            self._convert_vertex_ids_to_mgp_vertices(path)
            for path in super().get_all_simple_paths(
                v=self.id_mappings[v.id], to=self.id_mappings[to.id], cutoff=cutoff
            )
        ]
        return paths

    def topological_sort(self, mode: str) -> List[mgp.Vertex]:
        sorted_vertex_ids = super().topological_sorting(mode=mode)
        return self._convert_vertex_ids_to_mgp_vertices(sorted_vertex_ids)

    def community_leiden(
        self,
        resolution_parameter,
        weights,
        n_iterations,
        beta=0.01,
        objective_function="CPM",
        initial_membership=None,
        node_weights=None,
    ) -> List[Tuple[mgp.Vertex, int]]:
        communities = super().community_leiden(
            resolution_parameter=resolution_parameter,
            weights=weights,
            n_iterations=n_iterations,
            objective_function=objective_function,
            beta=beta,
            initial_membership=initial_membership,
            node_weights=node_weights,
        )
        return [(self.get_vertex_by_id(member), i) for i, members in enumerate(communities) for member in members]

    def mincut(self, source: mgp.Vertex, target: mgp.Vertex, capacity: str) -> Tuple[List[mgp.Vertex], float]:
        cut = super().mincut(
            source=self.id_mappings[source.id],
            target=self.id_mappings[target.id],
            capacity=capacity,
        )

        partition_vertices = [
            self._convert_vertex_ids_to_mgp_vertices(vertex_ids=partition) for partition in cut.partition
        ]
        return partition_vertices, cut.value

    def spanning_tree(
        self,
        weights: str,
    ) -> List[List[mgp.Vertex]]:
        if weights:
            weights = self.es[weights]

        min_spanning_tree_edges = super().spanning_tree(weights=weights, return_tree=False)

        return self._get_min_span_tree_vertex_pairs(min_spanning_tree_edges=self.es[min_spanning_tree_edges])

    def shortest_path_length(self, source: mgp.Vertex, target: mgp.Vertex, weights: str) -> float:
        length = super().distances(
            source=self.id_mappings[source.id],
            target=self.id_mappings[target.id],
            weights=weights,
        )[0][0]

        return float(length)

    def all_shortest_path_lengths(self, weights: str) -> List[List[float]]:
        return super().distances(
            weights=weights,
        )

    def get_shortest_path(self, source: mgp.Vertex, target: mgp.Vertex, weights: str) -> List[mgp.Vertex]:
        path = super().get_shortest_paths(
            v=self.id_mappings[source.id],
            to=self.id_mappings[target.id],
            weights=weights,
        )[0]

        return self._convert_vertex_ids_to_mgp_vertices(path)

    def betweenness_centrality(
        self,
        directed: bool,
        cutoff: int,
        weights: str,
        sources: List[int] = None,
        targets: List[int] = None,
    ) -> List[Tuple[mgp.Vertex, float]]:
        cutoff_value = None if cutoff < 0 else cutoff
        betweenness_values = super().betweenness(
            directed=directed,
            cutoff=cutoff_value,
            weights=weights if weights else None,
            sources=sources,
            targets=targets,
        )
        return [(self.get_vertex_by_id(node_id), score) for node_id, score in enumerate(betweenness_values)]

    def get_vertex_by_id(self, id: int) -> mgp.Vertex:
        return self.ctx_graph.get_vertex_by_id(self.reverse_id_mappings[id])

    def _convert_vertex_ids_to_mgp_vertices(self, vertex_ids: List[int]) -> List[mgp.Vertex]:
        vertices = []
        for id in vertex_ids:
            vertices.append(self.ctx_graph.get_vertex_by_id(self.reverse_id_mappings[id]))

        return vertices

    def _get_min_span_tree_vertex_pairs(
        self,
        min_spanning_tree_edges: igraph.EdgeSeq,
    ) -> List[List[mgp.Vertex]]:
        """Function for getting vertex pairs that are connected in minimum spanning tree.

        Args:
            min_span_tree_graph (igraph.EdgeSeq): Igraph graph containing minimum spanning tree

        Returns:
            List[List[mgp.Vertex]]: List of vertex pairs that are connected in minimum spanning tree
        """

        min_span_tree = []
        for edge in min_spanning_tree_edges:
            min_span_tree.append(
                [
                    self.ctx_graph.get_vertex_by_id(self.reverse_id_mappings[edge.source]),
                    self.ctx_graph.get_vertex_by_id(self.reverse_id_mappings[edge.target]),
                ]
            )

        return min_span_tree

    def _create_igraph_from_ctx(
        self, ctx: mgp.ProcCtx, directed: bool = False
    ) -> Tuple[Dict[int, int], Dict[int, int]]:
        """Function for creating igraph.Graph from mgp.ProcCtx.

        Args:
            ctx (mgp.ProcCtx): memgraph ProcCtx object
            directed (bool, optional): Is graph directed. Defaults to False.

        Returns:
            Tuple[igraph.Graph, Dict[int, int], Dict[int, int]]: Returns Igraph.Graph object, vertex id mappings and inverted_id_mapping vertex id mappings
        """

        vertex_attrs = defaultdict(list)
        edge_list = []
        edge_attrs = defaultdict(list)
        id_mapping = {vertex.id: i for i, vertex in enumerate(ctx.graph.vertices)}
        inverted_id_mapping = {i: vertex.id for i, vertex in enumerate(ctx.graph.vertices)}
        for vertex in ctx.graph.vertices:
            for name, value in vertex.properties.items():
                vertex_attrs[name].append(value)
            for edge in vertex.out_edges:
                for name, value in edge.properties.items():
                    edge_attrs[name].append(value)
                edge_list.append((id_mapping[edge.from_vertex.id], id_mapping[edge.to_vertex.id]))

        super().__init__(
            directed=directed,
            n=len(ctx.graph.vertices),
            edges=edge_list,
            edge_attrs=edge_attrs,
            vertex_attrs=vertex_attrs,
        )

        return id_mapping, inverted_id_mapping


class PageRankImplementationOptions(enum.Enum):
    PRPACK = "prpack"
    ARPACK = "arpack"


class InvalidPageRankImplementationOption(Exception):
    pass


class TopologicalSortingModes(enum.Enum):
    IN = "in"
    OUT = "out"


class InvalidTopologicalSortingModeException(Exception):
    pass


class CommunityDetectionObjectiveFunctionOptions(enum.Enum):
    CPM = "CPM"
    MODULARITY = "modularity"


class InvalidCommunityDetectionObjectiveFunctionException(Exception):
    pass


class TopologicalSortException(Exception):
    pass
