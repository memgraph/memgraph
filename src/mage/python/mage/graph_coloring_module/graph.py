from itertools import islice
from typing import Any, Dict, Iterator, List, Tuple


class Graph:
    """A data structure representing an undirected weighted graph.

    :param nodes: a list containing the labels of all nodes in the graph
    :param adjacency_list: a dictionary that associates each node with a list of its neighbors
    :name: a name of the graph

    """

    def __init__(
        self,
        nodes: List[Any],
        adjacency_list: Dict[Any, List[Tuple[Any, float]]],
        name: str = "",
    ):
        self._indices_to_labels = nodes
        self._labels_to_indices = dict((label, index) for index, label in enumerate(nodes))
        self._nodes_count = len(nodes)
        self._neighbors_positions = []
        self._neighbors = []
        self._weights = []
        self._name = name

        for i in range(self._nodes_count):
            self._neighbors.extend(
                [self._labels_to_indices[node[0]] for node in adjacency_list[self._indices_to_labels[i]]]
            )
            self._weights.extend([node[1] for node in adjacency_list[self._indices_to_labels[i]]])
            self._neighbors_positions.append(len(self._neighbors))

    def __str__(self):
        """Returns the name of the graph. If the name is not given
        at initialization, returns an empty string."""
        return self._name

    def __len__(self):
        """Returns the number of nodes in the graph."""
        return self._nodes_count

    def __getitem__(self, node: int) -> Iterator[int]:
        """Returns an iterator over neighbors of the given node."""
        start = self._neighbors_positions[node - 1] if node != 0 else 0
        end = self._neighbors_positions[node]
        return islice(self._neighbors, start, end)

    @property
    def nodes(self) -> Iterator[int]:
        """Returns an iterator over nodes in the graph."""
        nodes = (node for node in range(self._nodes_count))
        return nodes

    def number_of_nodes(self) -> int:
        """Returns the number of nodes in the graph."""
        return self._nodes_count

    def number_of_edges(self) -> int:
        """Returns the number of edges in the graph."""
        return len(self._neighbors) // 2

    def neighbors(self, node: int) -> Iterator[int]:
        """Returns an iterator over neighbors of node n."""
        return self.__getitem__(node)

    def weighted_neighbors(self, node: int) -> Iterator[Tuple[int, float]]:
        """Returns an iterator over neighbor and weight tuples of the node."""
        start = self._neighbors_positions[node - 1] if node != 0 else 0
        end = self._neighbors_positions[node]
        return self._neighbor_weight_tuples(start, end)

    def weight(self, node_1: int, node_2: int) -> float:
        """Returns the weight between the two given nodes."""
        weighted_neighs = self.weighted_neighbors(node_1)
        for node, weight in weighted_neighs:
            if node == node_2:
                return weight
        return 0

    def degree(self, node: int) -> int:
        """Returns the degree of the given node."""
        start = self._neighbors_positions[node - 1] if node != 0 else 0
        end = self._neighbors_positions[node]
        return start - end

    def label(self, node: int) -> Any:
        """Returns the node label."""
        return self._indices_to_labels[node]

    def _neighbor_weight_tuples(self, start: int, end: int) -> Iterator[Tuple[int, Any]]:
        return zip(
            (self._neighbors[i] for i in range(start, end)),
            (self._weights[i] for i in range(start, end)),
        )
