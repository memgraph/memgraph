from typing import Dict, List, Tuple

import numpy as np


class TemporalNeighborhood:
    def __init__(self):
        super().__init__()
        self.init_temporal_neighborhood()

    def init_temporal_neighborhood(self):
        self.neighborhood: Dict[int, List[Tuple[int, int, float]]] = {}

    def update_neighborhood(
        self,
        sources: np.array,
        destinations: np.array,
        edge_idxs: np.array,
        timestamps: np.array,
    ) -> None:
        """
        Idea is that smallest new timestamp is always greater than last biggest one in dict so we don't need to
        sort arrays :)
        if it doesn't exist, create empty, else overwrite
        """
        self.neighborhood = {
            **{node: [] for node in set(sources).union(set(destinations))},
            **self.neighborhood,
        }
        for source, destination, edge_idx, timestamp in zip(sources, destinations, edge_idxs, timestamps):
            self.neighborhood[source].append((destination, edge_idx, timestamp))
            self.neighborhood[destination].append((source, edge_idx, timestamp))

    def get_neighborhood(self, node: int, timestamp: int, num_neighbors: int) -> Tuple[np.array, np.array, np.array]:
        """ """
        if node not in self.neighborhood:
            return (
                np.zeros(num_neighbors, dtype=int),
                np.zeros(num_neighbors, dtype=int),
                np.zeros(num_neighbors, dtype=int),
            )
        neighbors_tuple = self.neighborhood[node]

        neighbors, edge_idxs, timestamps = list(zip(*neighbors_tuple))
        neighbors, edge_idxs, timestamps = (
            list(neighbors),
            list(edge_idxs),
            list(timestamps),
        )

        neighbors = np.array(neighbors)
        edge_idxs = np.array(edge_idxs)
        timestamps = np.array(timestamps)

        indices = np.where(timestamps < timestamp)[0]
        indices = np.random.choice(indices, size=min(num_neighbors, len(indices)), replace=False)

        neighbors = neighbors[indices]
        edge_idxs = edge_idxs[indices]
        timestamps = timestamps[indices]

        neighbors = np.append(arr=neighbors, values=np.zeros(num_neighbors - len(neighbors), dtype=int))
        edge_idxs = np.append(arr=edge_idxs, values=np.zeros(num_neighbors - len(edge_idxs), dtype=int))
        timestamps = np.append(arr=timestamps, values=np.zeros(num_neighbors - len(timestamps), dtype=int))
        return neighbors, edge_idxs, timestamps

    def find_neighborhood(self, nodes: List[int], num_neighbors: int) -> Dict[int, List[Tuple[int, int, float]]]:
        return {node: self.neighborhood[node][:num_neighbors] for node in nodes}
