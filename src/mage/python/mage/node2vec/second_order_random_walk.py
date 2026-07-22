from typing import List

import numpy as np
from mage.node2vec.graph import Graph

from utils import math_functions


class SecondOrderRandomWalk:
    def __init__(self, p: float, q: float, num_walks: int, walk_length: int):
        """
        Args:
            p (float): Hyperparameter for calculating transition probabilities. Determines "return" parameter
            from paper.
            q (float): Hyperparameter for calculating transition probabilities. Determines "in-out" parameter
            from paper.
            num_walks (int): Number of sampled walks for each node.
            walk_length (int): Maximum length of walk from certain node.

            If you have N nodes, there will be
            N*num_walks sampled walks, each of maximum length walk_length

        """
        self.p = p
        self.q = q
        self.num_walks = num_walks
        self.walk_length = walk_length

    def sample_node_walks(self, graph: Graph) -> List[List[int]]:
        """
        For each node we sample node walks for total of num_walks times
        Total length of list would be approximately: num_walks * walk_length * num_nodes

        Args:
            graph (Graph): Graph for which we want to sample walks

        Returns:
             List[List[int]]: List of List of node ids. Inner List represents one sampled walk.
        """
        self.set_first_pass_transition_probs(graph)
        self.set_graph_transition_probs(graph)
        walks = []
        for node in graph.nodes:
            for i in range(self.num_walks):
                walks.append(self.sample_walk(graph, node))

        return walks

    def sample_walk(self, graph: Graph, start_node_id_int: int) -> List[int]:
        """
        Sampling one walk for specific node. Max walk length is self.walk_length.
        Next node in walk is determined by transition probability depending on previous node,
        current node and current node neighbors. In-out parameter q and return parameter p determine
        probabilities.

        Args:
            graph (Graph): Graph from which we want to sample walk for specific node
            start_node_id_int (int): Starting node of walk.


        Returns:
             List[int]: List of node ids in sampled walk.
        """

        walk = [start_node_id_int]
        while len(walk) < self.walk_length:
            current_node_id = walk[-1]
            # node neighbors must be returned in sorted order - that's how they were
            # processed on edge transition probs
            node_neighbors = graph.get_neighbors(current_node_id)
            if not node_neighbors:
                break

            if len(walk) == 1:
                walk.append(
                    np.random.choice(
                        node_neighbors,
                        p=graph.get_node_first_pass_transition_probs((current_node_id)),
                    )
                )
                continue

            previous_node_id = walk[-2]

            next = np.random.choice(
                node_neighbors,
                p=graph.get_edge_transition_probs(edge=(previous_node_id, current_node_id)),
            )

            walk.append(next)
        return walk

    def set_first_pass_transition_probs(self, graph: Graph) -> None:
        """
        Calculates and sets first pass transition probs in graph.

        Args:
            graph (Graph): Graph for which to set first pass transition probs
        """
        for source_node_id in graph.nodes:
            unnormalized_probs = [
                graph.get_edge_weight(source_node_id, neighbor_id)
                for neighbor_id in graph.get_neighbors(source_node_id)
            ]

            graph.set_node_first_pass_transition_probs(source_node_id, math_functions.normalize(unnormalized_probs))

    def calculate_edge_transition_probs(self, graph: Graph, src_node_id: int, dest_node_id: int) -> List[float]:
        """
        Calculates edge transition probabilities. src_node_id and dest_node_id form transition from src_node_id
        to dest_node_id
        Take edge weight between current node and neighborhood nodes
        If neighbor of current node is also neighbor of source node - multiply edge weight with factor 1
        If neighbor of current node is not neighbor of source node - multiply edge weight with factor 1/q
        If neighbor of current node is source node  (same edge we "came from") - multiply edge weight with factor 1/p

        Args:
            graph (Graph): Graph from which we want to calculate probabilities
            src_node_id (int): Previous node in walk.
            dest_node_id (int): Current node in walk.


        Returns:
            List[float]: Returns list of normalized probabilities for transitions to new nodes connected by
            edge to dest_node_id in sorted way by neighbors id.
        """
        unnorm_trans_probs = []

        for dest_neighbor_id in graph.get_neighbors(dest_node_id):
            edge_weight = graph.get_edge_weight(dest_node_id, dest_neighbor_id)

            if dest_neighbor_id == src_node_id:
                unnorm_trans_probs.append(edge_weight / self.p)
            elif graph.has_edge(dest_neighbor_id, src_node_id):
                unnorm_trans_probs.append(edge_weight)
            else:
                unnorm_trans_probs.append(edge_weight / self.q)

        return math_functions.normalize(unnorm_trans_probs)

    def set_graph_transition_probs(self, graph: Graph) -> None:
        """
        Sets first graph transition probs in graph.

        Args:
            graph (Graph): Graph for which to set first pass transition probs
        """
        for node_from, node_to in graph.get_edges():
            graph.set_edge_transition_probs(
                (node_from, node_to),
                self.calculate_edge_transition_probs(graph, node_from, node_to),
            )
            if graph.is_directed:
                continue

            graph.set_edge_transition_probs(
                (node_to, node_from),
                self.calculate_edge_transition_probs(graph, node_to, node_from),
            )
