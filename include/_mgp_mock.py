import typing
from collections import namedtuple

import kafka
import networkx as nx
import pulsar

NX_LABEL_ATTR = "labels"
NX_TYPE_ATTR = "type"


class ImmutableObjectError(Exception):
    pass


class LogicErrorError(Exception):
    pass


class Graph:
    __slots__ = ("nx", "_highest_vertex_id", "_highest_edge_id", "_valid")

    I_KEY = 2

    def __init__(self, graph: nx.MultiDiGraph) -> None:
        if not isinstance(graph, nx.MultiDiGraph):
            raise TypeError(f"Expected 'networkx.classes.multidigraph.MultiDiGraph', got '{type(graph)}'")

        self.nx = graph
        self._highest_vertex_id = None
        self._highest_edge_id = None
        self._valid = True

    @property
    def vertex_ids(self):
        return self.nx.nodes

    def vertex_is_isolate(self, vertex_id: int) -> bool:
        return nx.is_isolate(self.nx, vertex_id)

    @property
    def vertices(self):
        return (Vertex(node_id, self) for node_id in self.nx.nodes)

    def has_node(self, node_id):
        return self.nx.has_node(node_id)

    @property
    def edges(self):
        return self.nx.edges

    def is_valid(self) -> bool:
        return self._valid

    def get_vertex_by_id(self, vertex_id: int) -> "Vertex":
        return Vertex(vertex_id, self)

    def invalidate(self):
        self._valid = False

    def is_immutable(self) -> bool:
        return nx.is_frozen(self.nx)

    def make_immutable(self):
        self.nx = nx.freeze(self.nx)

    def _new_edge_id(self):
        if self._highest_edge_id is None:
            self._highest_edge_id = max(edge[Graph.I_KEY] for edge in self.nx.edges(keys=True))

        return self._highest_edge_id + 1

    def _new_vertex_id(self):
        if self._highest_vertex_id is None:
            self._highest_vertex_id = max(vertex_id for vertex_id in self.nx.nodes) + 1

        return self._highest_vertex_id + 1

    def create_vertex(self) -> "Vertex":
        vertex_id = self._new_vertex_id()

        self.nx.add_node(vertex_id)
        self._highest_vertex_id = vertex_id

        return Vertex(vertex_id, self)

    def create_edge(self, from_id: int, to_id: int, edge_type: str) -> "Edge":
        edge_id = self._new_edge_id()

        self.nx.add_edge(from_id, to_id, key=edge_id, type=edge_type)
        self._highest_edge_id = edge_id

        return Edge((from_id, to_id, edge_id), self)

    def delete_vertex(self, vertex_id: int):
        self.nx.remove_node(vertex_id)

    def delete_edge(self, from_vertex_id: int, to_vertex_id: int, edge_id: int):
        self.nx.remove_edge(from_vertex_id, to_vertex_id, edge_id)


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

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    @property
    def underlying_graph(self) -> Graph:
        return self._graph

    def underlying_graph_is_mutable(self) -> bool:
        return not nx.is_frozen(self._graph.nx)

    @property
    def labels(self) -> typing.List[int]:
        return self._graph.nx.nodes[self._id][NX_LABEL_ATTR].split(":")

    def add_label(self, label: str) -> None:
        if nx.is_frozen(self._graph.nx):
            raise ImmutableObjectError("Cannot modify immutable object.")

        self._graph.nx.nodes[self._id][NX_LABEL_ATTR] += f":{label}"

    def remove_label(self, label: str) -> None:
        if nx.is_frozen(self._graph.nx):
            raise ImmutableObjectError("Cannot modify immutable object.")

        labels = self._graph.nx.nodes[self._id][NX_LABEL_ATTR]
        if labels.startswith(f"{label}:"):
            labels = "\n" + labels  # pseudo-string starter
            self._graph.nx.nodes[self._id][NX_LABEL_ATTR] = labels.replace(f"\n{label}:", "")
        elif labels.endswith(f":{label}"):
            labels += "\n"  # pseudo-string terminator
            self._graph.nx.nodes[self._id][NX_LABEL_ATTR] = labels.replace(f":{label}\n", "")
        else:
            self._graph.nx.nodes[self._id][NX_LABEL_ATTR] = labels.replace(f":{label}:", ":")

    @property
    def id(self) -> int:
        return self._id

    @property
    def properties(self):
        return (
            (key, value)
            for key, value in self._graph.nx.nodes[self._id].items()
            if key not in (NX_LABEL_ATTR, NX_TYPE_ATTR)
        )

    def get_property(self, property_name: str):
        return self._graph.nx.nodes[self._id][property_name]

    def set_property(self, property_name: str, value: object):
        self._graph.nx.nodes[self._id][property_name] = value

    @property
    def in_edges(self) -> typing.Iterable["Edge"]:
        return [Edge(edge, self._graph) for edge in self._graph.nx.in_edges(self._id, keys=True)]

    @property
    def out_edges(self) -> typing.Iterable["Edge"]:
        return [Edge(edge, self._graph) for edge in self._graph.nx.out_edges(self._id, keys=True)]


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

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    def underlying_graph_is_mutable(self) -> bool:
        return not nx.is_frozen(self._graph.nx)

    @property
    def id(self) -> int:
        return self._edge[Edge.I_KEY]

    @property
    def edge(self) -> typing.Tuple[int, int, int]:
        return self._edge

    @property
    def start_id(self) -> int:
        return self._edge[Edge.I_START]

    @property
    def end_id(self) -> int:
        return self._edge[Edge.I_END]

    def get_type_name(self):
        return self._graph.nx.get_edge_data(*self._edge)[NX_TYPE_ATTR]

    def from_vertex(self) -> Vertex:
        return Vertex(self.start_id, self._graph)

    def to_vertex(self) -> Vertex:
        return Vertex(self.end_id, self._graph)

    @property
    def properties(self):
        return (
            (key, value)
            for key, value in self._graph.nx.edges[self._edge].items()
            if key not in (NX_LABEL_ATTR, NX_TYPE_ATTR)
        )

    def get_property(self, property_name: str):
        return self._graph.nx.edges[self._edge][property_name]

    def set_property(self, property_name: str, value: object):
        self._graph.nx.edges[self._edge][property_name] = value


class Path:
    __slots__ = ("_vertices", "_edges", "_graph")
    __create_key = object()

    def __init__(self, create_key, vertex_id: int, graph: Graph) -> None:
        assert create_key == Path.__create_key, "Path objects must be created using Path.make_with_start"

        self._vertices = [vertex_id]
        self._edges = []
        self._graph = graph

    @classmethod
    def make_with_start(cls, vertex: Vertex) -> "Path":
        if not isinstance(vertex, Vertex):
            raise TypeError(f"Expected 'Vertex', got '{type(vertex)}'")

        if not isinstance(vertex.underlying_graph, Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(vertex.underlying_graph)}'")

        if not vertex.underlying_graph.nx.has_node(vertex._id):
            raise IndexError(f"Unable to find vertex with ID {vertex._id}.")

        return Path(cls.__create_key, vertex._id, vertex.underlying_graph)

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    def underlying_graph_is_mutable(self) -> bool:
        return not nx.is_frozen(self._graph.nx)

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
    __slots__ = ("_message", "_valid")

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
    __slots__ = ("_messages", "_graph", "_valid")

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
