# Copyright 2025 Memgraph Ltd.
#
# Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
# License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
# this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

import math
import random
from typing import Any, List


class StreamWalkUpdater:
    """
    Sample temporal random walks for the StreamWalk algorithm.

    The probability of the walk z at time t is defined as
        p(z,t) = (beta ^ |z|) * exp(-c * (t - t1)),
        where t1 is the timestamp of the oldest edge in the walk.


    Parameters
    ----------
    half_life : int
        Half-life in seconds for time decay
    max_length : int
        Maximum length of the sampled temporal random walks
    beta : float
        Damping factor for long paths, exponential decay on the length of the walk
    cutoff: int
        Temporal cutoff in seconds to exclude very distant past
    sampled_walks: int
        Number of sampled walks for each edge update
    full_walks: bool
        Return every node of the sampled walk for representation learning (full_walks=True)
        or only the endpoints of the walk (full_walks=False)
    """

    def __init__(
        self,
        half_life: int = 7200,
        max_length: int = 3,
        beta: float = 0.9,
        cutoff: int = 604800,
        sampled_walks: int = 4,
        full_walks: bool = False,
    ):
        self.c = -math.log(0.5) / half_life
        self.beta = beta
        self.half_life = half_life
        self.sampled_walks = sampled_walks
        self.cutoff = cutoff
        self.max_length = max_length
        self.full_walks = full_walks
        # graph stores the in edges for each node
        self.graph = {}
        # last timestamp stores the last time an edge entering node arrived in the edge stream
        self.last_timestamp = {}
        # centrality is the total weight (probabilities) of all walks ending at node v at time t(uv)
        # (uv is the last edge)
        self.centrality = {}

    def process_new_edge(self, source: Any, target: Any, time: int) -> List[List[Any]]:
        self.update(source, target, time)

        if source not in self.graph:
            # source is not reachable from any node within cutoff
            return [(source, target)] * self.sampled_walks

        walks = [self.sample_single_walk(source, target, time) for _ in range(self.sampled_walks)]
        return walks

    def sample_single_walk(self, source: Any, target: Any, time: int) -> List[Any]:
        node_ = source
        time_ = self.last_timestamp.get(source, 0)
        centrality_ = self.centrality.get(source, 0)

        walk = [node_]

        while True:
            if random.uniform(0, 1) < 1 / (centrality_ + 1) or node_ not in self.graph or len(walk) >= self.max_length:
                break
            sum_ = centrality_ * random.uniform(0, 1)
            sum__ = 0
            broken = False
            for n, t, c in reversed(self.graph[node_]):
                if t < time_:
                    sum__ += (c + 1) * self.beta * math.exp(self.c * (t - time_))
                    if sum__ >= sum_:
                        broken = True
                        break
            if not broken:
                break
            node_, time_, centrality_ = n, t, c
            walk.append(node_)

        if self.full_walks:
            return [target] + walk
        return (node_, target)

    def update(self, source: Any, target: Any, time: int) -> None:
        # all walks that terminated at the target before adding the new edge are decayed
        if target in self.centrality:
            self.centrality[target] *= math.exp(self.c * (self.last_timestamp[target] - time))
        else:
            self.centrality[target] = 0

        if source in self.last_timestamp:
            self.centrality[source] *= math.exp(self.c * (self.last_timestamp[source] - time))
            self.last_timestamp[source] = time
            self.clean_in_edges(source, time)

        self.centrality[target] += (self.centrality.get(source, 0) + 1) * self.beta

        if target not in self.graph:
            self.graph[target] = []

        self.graph[target].append((source, time, self.centrality.get(source, 0)))
        self.last_timestamp[target] = time
        self.clean_in_edges(target, time)

    def clean_in_edges(self, node: Any, current_time: int) -> None:
        """
        Deletes the in edges for the given node
        that appeared more than cutoff seconds ago.
        """

        # possible improvement by binary search
        index = 0
        for source, time, centrality in self.graph[node]:
            if current_time - time < self.cutoff:
                break
            index += 1
        # drop old in edges
        self.graph[node] = self.graph[node][index:]
