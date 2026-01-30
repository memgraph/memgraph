from abc import ABC, abstractmethod
from typing import Dict, List, Tuple


class Graph(ABC):
    def __init__(self, is_directed: bool):
        self._is_directed = is_directed
        self._nodes: List[int] = []
        self._preprocessed_transition_probs = {}
        self._first_pass_transition_probs = {}

    @property
    def is_directed(self):
        return self._is_directed

    @property
    def preprocessed_transition_probs(self) -> Dict[Tuple[int, int], List[float]]:
        return self._preprocessed_transition_probs

    @property
    def first_pass_transition_probs(self) -> Dict[int, List[float]]:
        return self._first_pass_transition_probs

    @property
    @abstractmethod
    def nodes(self) -> List[int]:
        pass

    @nodes.setter
    def nodes(self, value):
        self._nodes = value

    @preprocessed_transition_probs.setter
    def preprocessed_transition_probs(self, value):
        self._preprocessed_transition_probs = value

    @first_pass_transition_probs.setter
    def first_pass_transition_probs(self, value):
        self._first_pass_transition_probs = value

    @abstractmethod
    def has_edge(self, src_node_id: int, dest_node_id: int) -> bool:
        pass

    @abstractmethod
    def get_edge_weight(self, src_node_id: int, dest_node_id: int) -> float:
        pass

    @abstractmethod
    def get_neighbors(self, node_id: int) -> List[int]:
        pass

    @abstractmethod
    def get_edges(self) -> List[Tuple[int, int]]:
        pass

    @abstractmethod
    def set_edge_transition_probs(self, edge: Tuple[int, int], transition_probs: List[float]) -> None:
        pass

    @abstractmethod
    def get_edge_transition_probs(self, edge: Tuple[int, int]) -> List[float]:
        pass

    @abstractmethod
    def set_node_first_pass_transition_probs(self, source_node_id: int, normalized_probs: List[float]) -> None:
        pass

    @abstractmethod
    def get_node_first_pass_transition_probs(self, source_node_id: int) -> List[float]:
        pass


class GraphHolder(Graph):
    def __init__(self, edges_weights: Dict[Tuple[int, int], float], is_directed: bool):
        super().__init__(is_directed)
        self._edges_weights = edges_weights
        self._graph = {}
        self.init_graph()

    def nodes(self) -> List[int]:
        return list(self._graph.keys())

    def set_edge_transition_probs(self, edge: Tuple[int, int], transition_probs: List[float]) -> None:
        self._preprocessed_transition_probs[edge] = transition_probs

    def get_edge_transition_probs(self, edge: Tuple[int, int]) -> List[float]:
        return self._preprocessed_transition_probs[edge]

    def set_node_first_pass_transition_probs(self, source_node_id: int, normalized_probs: List[float]) -> None:
        self._first_pass_transition_probs[source_node_id] = normalized_probs

    def get_node_first_pass_transition_probs(self, source_node_id: int) -> List[float]:
        return self._first_pass_transition_probs[source_node_id]

    def has_edge(self, src_node_id: int, dest_node_id: int) -> bool:
        return (src_node_id, dest_node_id) in self._edges_weights or (
            not self.is_directed and (dest_node_id, src_node_id) in self._edges_weights
        )

    def get_edges(self) -> List[Tuple[int, int]]:
        edges = list(self._edges_weights.keys())
        if self._is_directed:
            return edges
        edges.extend([(edge[1], edge[0]) for edge in edges])
        return edges

    def get_edge_weight(self, src_node_id: int, dest_node_id: int) -> float:
        if not self.has_edge(src_node_id, dest_node_id):
            raise ValueError
        if (src_node_id, dest_node_id) in self._edges_weights:
            return self._edges_weights[(src_node_id, dest_node_id)]
        return self._edges_weights[(dest_node_id, src_node_id)]

    # Always return nodes in same order
    def get_neighbors(self, node_id: int) -> List[int]:
        return self._graph[node_id] if node_id in self._graph else []

    def init_graph(self) -> None:
        for node_from, node_to in self._edges_weights:
            if node_from not in self._graph:
                self._graph[node_from] = set()
            self._graph[node_from].add(node_to)
            if not self.is_directed:
                if node_to not in self._graph:
                    self._graph[node_to] = set()
                self._graph[node_to].add(node_from)

        self.nodes = list(self._graph.keys())

        for node in self._graph:
            self._graph[node] = sorted(list(self._graph[node]))
