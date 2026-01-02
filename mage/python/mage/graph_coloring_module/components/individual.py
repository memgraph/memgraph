from random import choice
from typing import List, Set, Tuple

from mage.graph_coloring_module.exceptions import IllegalColorException, IllegalNodeException, WrongColoringException
from mage.graph_coloring_module.graph import Graph


class Individual:
    """A class that represents an individual. The individual represents
    one possible coloring of the graph. Individual also contains data about
    conflicts, like the sum of weights of conflict edges, and set of conflict nodes.
    If a new individual is created by changing the color of some nodes of
    the current individual then this data is calculated based on the data
    of the current individual."""

    def __init__(
        self,
        no_of_colors: int,
        graph: Graph,
        chromosome: List[int] = None,
        conflicts_weight: int = None,
        conflict_nodes: Set[int] = None,
        conflicts_counter: List[int] = None,
    ):
        self._graph = graph
        self._no_of_units = len(graph)
        self._no_of_colors = no_of_colors
        if chromosome is None:
            self._chromosome = list(choice(range(0, no_of_colors)) for _ in range(len(graph)))
        else:
            self._chromosome = chromosome

        self._conflicts_weight = conflicts_weight
        self._conflicts_counter = conflicts_counter
        self._conflict_nodes = conflict_nodes

        if conflicts_weight is None or conflict_nodes is None:
            self._calculate_conflicts()

    def __getitem__(self, index: int) -> int:
        """Returns the color stored on the given index."""
        return self._chromosome[index]

    @property
    def chromosome(self) -> List[int]:
        """Returns the list representing the coloring of the graph."""
        return self._chromosome

    @property
    def conflict_nodes(self) -> Set[int]:
        """Returns a set of conflicting nodes in the coloring
        represented by the individual.."""
        return self._conflict_nodes

    @property
    def graph(self) -> Graph:
        """Returns the graph whose coloring the individual represents."""
        return self._graph

    @property
    def no_of_colors(self) -> int:
        """Returns the allowed number of colors."""
        return self._no_of_colors

    @property
    def no_of_units(self) -> int:
        """Returns the size of the chromosome."""
        return self._no_of_units

    @property
    def conflicts_weight(self) -> int:
        """Returns the sum of weights of conflicting edges
        in the coloring represented by the individual."""
        return self._conflicts_weight

    def check_coloring(self) -> bool:
        """Checks that the coloring represented by the individual is correct.
        The coloring is correct if it does not color two nodes connected with
        an edge with the same color. The function returns True if the coloring
        is correct, otherwise returns False."""
        for node in self.graph.nodes:
            for neigh in self.graph[node]:
                if self.chromosome[node] == self.chromosome[neigh]:
                    return False
        return True

    def replace_unit(self, index: int, color: int):
        """Sets the color of the node on the given index to the given color and
        returns a new individual if the given arguments are correct. If the given
        color is not allowed then the IllegalColorException exception is raised.
        If the given node does not exist then the IllegalNodeException is raised."""
        return self.replace_units([index], [color])

    def replace_units(self, indices: List[int], colors: List[int]):
        """Sets the colors of the nodes with the corresponding indices to the given
        colors and returns a new individual if the given coloring is correct. If any
        of the given colors is not allowed then the IllegalColorException exception is
        raised. If any of the given nodes does not exist then the IllegalNodeException
        is raised. If the number of given nodes is not equal to the number of given colors
        then the WrongColoringException is raised."""

        if len(indices) != len(colors):
            raise WrongColoringException("The number of given nodes must be equal to the number of given colors!")

        new_chromosome = self._chromosome[:]
        conflicts_counter = self._conflicts_counter[:]
        conflict_nodes = self._conflict_nodes.copy()
        conflict_edges = self.conflicts_weight

        for index, color in zip(indices, colors):
            if not (0 <= color < self._no_of_colors):
                raise IllegalColorException("The given color is not in the range of allowed colors!")
            if not (0 <= index < self.no_of_units):
                raise IllegalNodeException("The given node does not exist!")
            conflict_edges, conflicts_counter, conflict_nodes = self._calculate_diff(
                chromosome=new_chromosome,
                node=index,
                color=color,
                conflict_edges=conflict_edges,
                conflicts_counter=conflicts_counter,
                conflict_nodes=conflict_nodes,
            )
            new_chromosome[index] = color

        new_indv = Individual(
            no_of_colors=self.no_of_colors,
            graph=self.graph,
            chromosome=new_chromosome,
            conflicts_weight=conflict_edges,
            conflict_nodes=conflict_nodes,
            conflicts_counter=conflicts_counter,
        )

        return new_indv

    def _calculate_diff(
        self,
        chromosome: List[int],
        node: int,
        color: int,
        conflict_edges: int,
        conflicts_counter: List[int],
        conflict_nodes: Set[int],
    ) -> Tuple[int, List[int], Set[int]]:
        diff = 0
        for neigh, weight in self.graph.weighted_neighbors(node):
            if chromosome[node] == chromosome[neigh]:
                if not (color == chromosome[neigh]):
                    diff -= weight

                    conflicts_counter[neigh] -= 1
                    if conflicts_counter[neigh] == 0:
                        conflict_nodes.remove(neigh)

                    conflicts_counter[node] -= 1
                    if conflicts_counter[node] == 0:
                        conflict_nodes.remove(node)
            else:
                if color == chromosome[neigh]:
                    diff += weight

                    conflicts_counter[neigh] += 1
                    if conflicts_counter[neigh] == 1:
                        conflict_nodes.add(neigh)

                    conflicts_counter[node] += 1
                    if conflicts_counter[node] == 1:
                        conflict_nodes.add(node)

        conflict_edges = conflict_edges + diff
        return conflict_edges, conflicts_counter, conflict_nodes

    def _calculate_conflicts(self):
        self._conflict_nodes = set()
        self._conflicts_counter = [0 for _ in self.graph.nodes]
        conflicting_edges = 0

        for node in self.graph.nodes:
            for neigh, weight in self.graph.weighted_neighbors(node):
                if self.chromosome[node] == self.chromosome[neigh]:
                    self._conflicts_counter[node] += 1
                    conflicting_edges += weight
                    if self._conflicts_counter[node] == 1:
                        self._conflict_nodes.add(node)

        conflicting_edges //= 2
        self._conflicts_weight = conflicting_edges
