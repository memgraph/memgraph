import typing

import kafka
import networkx as nx
import pulsar


class ImmutableObjectError(Exception):
    pass


class LogicErrorError(Exception):
    pass


class Graph:
    __slots__ = ("nx", "_highest_edge_id", "_valid")

    I_KEY = 2

    def __init__(self, graph: nx.MultiDiGraph) -> None:
        if not isinstance(graph, nx.MultiDiGraph):
            raise TypeError(f"Expected 'networkx.classes.multidigraph.MultiDiGraph', got '{type(graph)}'")

        self.nx = graph
        self._highest_edge_id = None
        self._valid = True

    def _new_edge_id(self):
        if self._highest_edge_id is None:
            self._highest_edge_id = max(edge[Graph.I_KEY] for edge in self.nx.edges(keys=True))

        return self._highest_edge_id + 1

    def create_edge(self, from_id: int, to_id: int, edge_type: str) -> "Edge":
        edge_id = self._new_edge_id()

        self.nx.add_edge(from_id, to_id, key=edge_id, type=edge_type)
        self._highest_edge_id = edge_id

        return Edge((from_id, to_id, edge_id), self)

    def is_valid(self) -> bool:
        return self._valid

    def invalidate(self):
        self._valid = False


class Vertex:
    __slots__ = ("_id", "_graph")

    def __init__(self, id: int, graph: Graph) -> None:
        if not isinstance(id, int):
            raise TypeError(f"Expected 'int', got '{type(id)}'")

        if not isinstance(graph, Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        if not graph.nx.has_node(id):
            raise IndexError(f"Unable to find vertex with ID {id}.")

        self._id = id
        self._graph = graph

    @property
    def id(self) -> int:
        return self._id

    @property
    def graph(self) -> Graph:
        return self._graph

    @property
    def nx_graph(self) -> nx.MultiDiGraph:
        return self._graph.nx

    @property
    def labels(self) -> typing.List[int]:
        return self.nx_graph.nodes[self._id]["label"].split(":")

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    def add_label(self, label: str) -> None:
        if nx.is_frozen(self.nx_graph):
            raise ImmutableObjectError("Cannot modify immutable object.")

        self.nx_graph.nodes[self._id]["label"] += f":{label}"

    def remove_label(self, label: str) -> None:
        if nx.is_frozen(self.nx_graph):
            raise ImmutableObjectError("Cannot modify immutable object.")

        labels = self.nx_graph.nodes[self._id]["label"]
        if labels.startswith(f"{label}:"):
            labels = "\n" + labels  # pseudo-string starter
            self.nx_graph.nodes[self._id]["label"] = labels.replace(f"\n{label}:", "")
        elif labels.endswith(f":{label}"):
            labels += "\n"  # pseudo-string terminator
            self.nx_graph.nodes[self._id]["label"] = labels.replace(f":{label}\n", "")
        else:
            self.nx_graph.nodes[self._id]["label"] = labels.replace(f":{label}:", ":")


class Edge:
    __slots__ = ("_edge", "_graph")

    I_START = 0
    I_END = 1
    I_KEY = 2

    def __init__(self, edge: typing.Tuple[int, int, int], graph: Graph) -> None:
        if not isinstance(edge, typing.Tuple):
            raise TypeError(f"Expected 'Tuple', got '{type(edge)}'")

        if not isinstance(graph, Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        if not graph.nx.has_edge(*edge):
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
    def graph(self) -> Graph:
        return self._graph

    @property
    def nx_graph(self) -> nx.MultiDiGraph:
        return self._graph.nx

    @property
    def start_id(self) -> int:
        return self._edge[Edge.I_START]

    @property
    def end_id(self) -> int:
        return self._edge[Edge.I_END]

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    def get_type_name(self):
        return self.nx_graph.get_edge_data(*self._edge)["type"]


class Path:
    __slots__ = ("_vertices", "_edges", "_graph")
    __create_key = object()

    def __init__(self, create_key, vertex_id: int, graph: Graph) -> None:
        assert create_key == Path.__create_key, "Path objects must be created using Path.make_with_start"

        self._vertices = [vertex_id]
        self._edges = []
        self._graph = graph

    @classmethod
    def make_with_start(cls, vertex: Vertex, graph: Graph) -> "Path":
        if not isinstance(vertex, Vertex):
            raise TypeError(f"Expected 'Vertex', got '{type(vertex)}'")

        if not isinstance(graph, Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        if not graph.nx.has_node(vertex._id):
            raise IndexError(f"Unable to find vertex with ID {vertex._id}.")

        return Path(cls.__create_key, vertex._id, graph)

    @property
    def nx_graph(self) -> nx.MultiDiGraph:
        return self._graph.nx

    def is_valid(self) -> bool:
        return self._graph.is_valid()

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


class Message:
    __slots__ = (
        "_message",
        "_valid",
    )

    def __init__(self, message) -> None:
        if not isinstance(message, (kafka.consumer.fetcher.ConsumerRecord, pulsar.Message)):
            raise TypeError(
                f"Expected 'kafka.consumer.fetcher.ConsumerRecord' or 'pulsar.Message', got '{type(message)}'"
            )

        self._message = message
        self._valid = True

    @property
    def message(self) -> typing.Union[kafka.consumer.fetcher.ConsumerRecord, pulsar.Message]:
        return self._message

    def is_valid(self) -> bool:
        return self._valid

    def invalidate(self):
        self._valid = False


class Messages:
    __slots__ = (
        "_messages",
        "_graph",
        "_valid",
    )

    def __init__(self, messages: typing.List, graph: Graph) -> None:
        if not isinstance(messages, typing.List):
            raise TypeError("Expected 'List', got '{}'".format(type(messages)))

        if not isinstance(graph, Graph):
            raise TypeError("Expected '_mgp_mock.Graph', got '{}'".format(type(messages)))

        self._messages = messages
        self._graph = graph
        self._valid = True

    @property
    def messages(self) -> typing.List:
        return self._messages

    def is_valid(self) -> bool:
        return self._valid

    def invalidate(self):
        print("run_invalidate")
        if self._messages is not None:
            for i in range(len(self._messages)):
                self._messages[i].invalidate()
        self._messages = None

        self._valid = False
