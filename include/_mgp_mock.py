import typing

import networkx as nx


class ImmutableObjectError(Exception):
    pass


class LogicErrorError(Exception):
    pass


class Vertex:
    __slots__ = ("_id", "_graph")

    def __init__(self, id: int, graph: nx.MultiDiGraph) -> None:
        if not isinstance(id, int):
            raise TypeError(f"Expected 'int', got '{type(id)}'")

        if not isinstance(graph, nx.MultiDiGraph):
            raise TypeError(f"Expected 'networkx.classes.multidigraph.MultiDiGraph', got '{type(graph)}'")

        if not graph.has_node(id):
            raise IndexError(f"Unable to find vertex with ID {id}.")

        self._id = id
        self._graph = graph

    @property
    def id(self) -> int:
        return self._id

    @property
    def graph(self) -> nx.MultiDiGraph:
        return self._graph

    @property
    def labels(self) -> typing.List[int]:
        return self._graph.nodes[self._id]["label"].split(":")

    def is_valid(self) -> bool:
        return self._graph.has_node(self._id)

    def add_label(self, label: str) -> None:
        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Cannot modify immutable object.")

        self._graph.nodes[self._id]["label"] += f":{label}"

    def remove_label(self, label: str) -> None:
        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Cannot modify immutable object.")

        labels = self._graph.nodes[self._id]["label"]
        if labels.startswith(f"{label}:"):
            labels = "\n" + labels  # pseudo-string starter
            self._graph.nodes[self._id]["label"] = labels.replace(f"\n{label}:", "")
        elif labels.endswith(f":{label}"):
            labels += "\n"  # pseudo-string terminator
            self._graph.nodes[self._id]["label"] = labels.replace(f":{label}\n", "")
        else:
            self._graph.nodes[self._id]["label"] = labels.replace(f":{label}:", ":")


class Edge:
    __slots__ = ("_edge", "_graph")

    I_START = 0
    I_END = 1
    I_KEY = 2

    def __init__(self, edge: typing.Tuple[int, int, int], graph: nx.MultiDiGraph) -> None:
        if not isinstance(edge, typing.Tuple):
            raise TypeError(f"Expected 'Tuple', got '{type(edge)}'")

        if not isinstance(graph, nx.MultiDiGraph):
            raise TypeError(f"Expected 'networkx.classes.multidigraph.MultiDiGraph', got '{type(graph)}'")

        if not graph.has_edge(*edge):
            raise IndexError(f"Unable to find edge with ID {edge[Edge.I_KEY]}.")

        self._edge = edge
        self._graph = graph

    @property
    def id(self) -> int:
        return self._edge[Edge.I_KEY]

    @property
    def edge(self) -> typing.Tuple[int, int, int]:
        return self._edge

    @property
    def graph(self) -> nx.MultiDiGraph:
        return self._graph

    @property
    def start_id(self) -> int:
        return self._edge[Edge.I_START]

    @property
    def end_id(self) -> int:
        return self._edge[Edge.I_END]

    def is_valid(self) -> bool:
        return self._graph.has_edge(*self._edge)

    def get_type_name(self):
        return self._graph.get_edge_data(*self._edge)["type"]


class Path:
    __slots__ = ("_vertices", "_edges", "_graph")
    __create_key = object()

    def __init__(self, create_key, vertex_id: int, graph: nx.MultiDiGraph) -> None:
        assert create_key == Path.__create_key, "Path objects must be created using Path.make_with_start"

        self._vertices = [vertex_id]
        self._edges = []
        self._graph = graph

    @classmethod
    def make_with_start(cls, vertex: Vertex, graph: nx.MultiDiGraph) -> "Path":
        if not isinstance(vertex, Vertex):
            raise TypeError(f"Expected 'Vertex', got '{type(vertex)}'")

        if not isinstance(graph, nx.MultiDiGraph):
            raise TypeError(f"Expected 'networkx.classes.multidigraph.MultiDiGraph', got '{type(graph)}'")

        if not graph.has_node(vertex._id):
            raise IndexError(f"Unable to find vertex with ID {vertex._id}.")

        return Path(cls.__create_key, vertex._id, graph)

    def is_valid(self) -> bool:
        return all(self._graph.has_node(v) for v in self._vertices) and all(
            self._graph.has_edge(*e) for e in self._edges
        )

    def expand(self, edge: Edge):
        if edge.start_id != self._vertices[-1]:
            raise LogicErrorError("Logic error.")

        self._vertices.append(edge.end_id)
        self._edges.append((edge.start_id, edge.end_id, edge.id))

    def vertex_at(self, index: int) -> Vertex:
        return Vertex(self._vertices[index], self._graph)

    def edge_at(self, index: int) -> Edge:
        return Edge(self._edges[index], self._graph)

    def size(self) -> int:
        return len(self._edges)


class Graph:
    __slots__ = ("nx", "_highest_edge_id")

    I_KEY = 2

    def __init__(self, graph: nx.MultiDiGraph) -> None:
        if not isinstance(graph, nx.MultiDiGraph):
            raise TypeError(f"Expected 'networkx.classes.multidigraph.MultiDiGraph', got '{type(graph)}'")

        self.nx = graph
        self._highest_edge_id = None

    def _new_edge_id(self):
        if self._highest_edge_id is None:
            self._highest_edge_id = max(edge[Graph.I_KEY] for edge in self.nx.edges(keys=True))

        return self._highest_edge_id + 1

    def create_edge(self, from_id: int, to_id: int, edge_type: str) -> Edge:
        edge_id = self._new_edge_id()

        self.nx.add_edge(from_id, to_id, key=edge_id, type=edge_type)
        self._highest_edge_id = edge_id

        return Edge((from_id, to_id, edge_id), self.nx)
