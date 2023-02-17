import datetime
import inspect
import sys
import typing
from collections import namedtuple
from functools import wraps

import _mgp_mock
import kafka
import networkx as nx


class InvalidContextError(Exception):
    """
    Signals using a graph element instance outside of the registered procedure.
    """

    pass


class UnknownError(Exception):
    """
    Signals unspecified failure.
    """

    pass


class InsufficientBufferError(Exception):
    """
    Signals that a buffer is not big enough.
    """

    pass


class OutOfRangeError(Exception):
    """
    Signals that an index-like parameter has a value that is outside its
    possible values.
    """

    pass


class LogicErrorError(Exception):
    """
    Signals faulty logic within the program such as violating logical
    preconditions or class invariants and may be preventable.
    """

    pass


class DeletedObjectError(Exception):
    """
    Signals accessing an already deleted object.
    """

    pass


class InvalidArgumentError(Exception):
    """
    Signals that some of the arguments have invalid values.
    """

    pass


class KeyAlreadyExistsError(Exception):
    """
    Signals that a key already exists in a container-like object.
    """

    pass


class ImmutableObjectError(Exception):
    """
    Signals modification of an immutable object.
    """

    pass


class ValueConversionError(Exception):
    """
    Signals that conversion between python and cypher values failed.
    """

    pass


class Label:
    __slots__ = ("_name",)

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def __eq__(self, other) -> bool:
        if isinstance(other, Label):
            return self._name == other.name

        if isinstance(other, str):
            return self._name == other

        return NotImplemented


# Named property value of a Vertex or an Edge.
# It would be better to use typing.NamedTuple with typed fields, but that is
# not available in Python 3.5.
Property = namedtuple("Property", ("name", "value"))


class Properties:
    __slots__ = ("_vertex_or_edge", "_len")

    def __init__(self, vertex_or_edge):
        if not isinstance(vertex_or_edge, (_mgp_mock.Vertex, _mgp_mock.Edge)):
            raise TypeError(f"Expected _mgp_mock.Vertex or _mgp_mock.Edge, got {type(vertex_or_edge)}")

        self._len = None
        self._vertex_or_edge = vertex_or_edge

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, as the underlying C API should
        # not support deepcopy. Besides, it doesn't make much sense to actually
        # copy _mgp_mock.Edge and _mgp_mock.Vertex types as they are actually references
        # to graph elements and not proper values.
        return Properties(self._vertex_or_edge)

    def get(self, property_name: str, default=None) -> object:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        try:
            return self[property_name]
        except KeyError:
            return default

    def set(self, property_name: str, value: object) -> None:
        if not self._vertex_or_edge.underlying_graph_is_mutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        self[property_name] = value

    def items(self) -> typing.Iterable[Property]:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        vertex_or_edge_props = self._vertex_or_edge.properties

        for property in vertex_or_edge_props:
            yield Property(*property)

    def keys(self) -> typing.Iterable[str]:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        for item in self.items():
            yield item.name

    def values(self) -> typing.Iterable[object]:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        for item in self.items():
            yield item.value

    def __len__(self) -> int:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        if self._len is None:
            self._len = sum(1 for _ in self.items())

        return self._len

    def __iter__(self) -> typing.Iterable[str]:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        for item in self.items():
            yield item.name

    def __getitem__(self, property_name: str) -> object:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        return self._vertex_or_edge.get_property(property_name)

    def __setitem__(self, property_name: str, value: object) -> None:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        if not self._vertex_or_edge.underlying_graph_is_mutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        self._vertex_or_edge.set_property(property_name, value)

    def __contains__(self, property_name: str) -> bool:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        try:
            _ = self[property_name]
            return True
        except KeyError:
            return False


class EdgeType:
    __slots__ = ("_name",)

    def __init__(self, name):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def __eq__(self, other) -> bool:
        if isinstance(other, EdgeType):
            return self.name == other.name

        if isinstance(other, str):
            return self.name == other

        return NotImplemented


if sys.version_info >= (3, 5, 2):
    EdgeId = typing.NewType("EdgeId", int)
else:
    EdgeId = int


class Edge:
    __slots__ = ("_edge",)

    def __init__(self, edge):
        if not isinstance(edge, _mgp_mock.Edge):
            raise TypeError(f"Expected '_mgp_mock.Edge', got '{type(edge)}'")

        self._edge = edge

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp_mock.Edge as that is actually a reference to a graph element
        # and not a proper value.
        return Edge(self._edge)

    def is_valid(self) -> bool:
        return self._edge.is_valid()

    def underlying_graph_is_mutable(self) -> bool:
        if not self.is_valid():
            raise InvalidContextError()

        return self._edge.underlying_graph_is_mutable()

    @property
    def id(self) -> EdgeId:
        if not self.is_valid():
            raise InvalidContextError()

        return self._edge.id

    @property
    def type(self) -> EdgeType:
        if not self.is_valid():
            raise InvalidContextError()
        return EdgeType(self._edge.get_type_name())

    @property
    def from_vertex(self) -> "Vertex":
        if not self.is_valid():
            raise InvalidContextError()

        return Vertex(self._edge.from_vertex())

    @property
    def to_vertex(self) -> "Vertex":
        if not self.is_valid():
            raise InvalidContextError()

        return Vertex(self._edge.to_vertex())

    @property
    def properties(self) -> Properties:
        if not self.is_valid():
            raise InvalidContextError()

        return Properties(self._edge)

    def __eq__(self, other) -> bool:
        if not self.is_valid():
            raise InvalidContextError()

        if not isinstance(other, Edge):
            return NotImplemented

        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


if sys.version_info >= (3, 5, 2):
    VertexId = typing.NewType("VertexId", int)
else:
    VertexId = int


class Vertex:
    __slots__ = ("_vertex",)

    def __init__(self, vertex):
        if not isinstance(vertex, _mgp_mock.Vertex):
            raise TypeError(f"Expected '_mgp_mock.Vertex', got '{type(vertex)}'")

        self._vertex = vertex

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp_mock.Vertex as that is actually a reference to a graph element
        # and not a proper value.
        return Vertex(self._vertex)

    def is_valid(self) -> bool:
        return self._vertex.is_valid()

    def underlying_graph_is_mutable(self) -> bool:
        if not self.is_valid():
            raise InvalidContextError()

        return self._vertex.underlying_graph_is_mutable()

    @property
    def id(self) -> VertexId:
        if not self.is_valid():
            raise InvalidContextError()

        return self._vertex.id

    @property
    def labels(self) -> typing.Tuple[Label]:
        if not self.is_valid():
            raise InvalidContextError()

        return tuple(Label(label) for label in self._vertex.labels)

    def add_label(self, label: str) -> None:
        if not self.is_valid():
            raise InvalidContextError()

        return self._vertex.add_label(label)

    def remove_label(self, label: str) -> None:
        if not self.is_valid():
            raise InvalidContextError()
        return self._vertex.remove_label(label)

    @property
    def properties(self) -> Properties:
        if not self.is_valid():
            raise InvalidContextError()

        return Properties(self._vertex)

    @property
    def in_edges(self) -> typing.Iterable[Edge]:
        if not self.is_valid():
            raise InvalidContextError()

        for edge in self._vertex.in_edges:
            yield Edge(edge)

    @property
    def out_edges(self) -> typing.Iterable[Edge]:
        if not self.is_valid():
            raise InvalidContextError()

        for edge in self._vertex.out_edges:
            yield Edge(edge)

    def __eq__(self, other) -> bool:
        if not self.is_valid():
            raise InvalidContextError()

        if not isinstance(other, Vertex):
            return NotImplemented

        return self.id == other.id

    def __hash__(self) -> int:
        return hash(self.id)


class Path:
    __slots__ = ("_path", "_vertices", "_edges")

    def __init__(self, starting_vertex_or_path: typing.Union[_mgp_mock.Path, Vertex]):
        if isinstance(starting_vertex_or_path, _mgp_mock.Path):
            self._path = starting_vertex_or_path

        elif isinstance(starting_vertex_or_path, Vertex):
            # for consistency with the Python API, the `_vertex` attribute isn’t a “public” property
            vertex = starting_vertex_or_path._vertex
            if not vertex.is_valid():
                raise InvalidContextError()

            self._path = _mgp_mock.Path.make_with_start(vertex)

        else:
            raise TypeError(f"Expected 'Vertex' or '_mgp_mock.Path', got '{type(starting_vertex_or_path)}'")

        self._vertices = None
        self._edges = None

    def __copy__(self):
        if not self.is_valid():
            raise InvalidContextError()

        assert len(self.vertices) >= 1

        path = Path(self.vertices[0])
        for e in self.edges:
            path.expand(e)

        return path

    def __deepcopy__(self, memo):
        try:
            return Path(memo[id(self._path)])
        except KeyError:
            pass
        # This is the same as the shallow copy, as the underlying C API should
        # not support deepcopy. Besides, it doesn't make much sense to actually
        # copy _mgp_mock.Edge and _mgp_mock.Vertex types as they are actually references
        # to graph elements and not proper values.
        path = self.__copy__()
        memo[id(self._path)] = path._path
        return path

    def is_valid(self) -> bool:
        return self._path.is_valid()

    def expand(self, edge: Edge):
        if not isinstance(edge, Edge):
            raise TypeError(f"Expected 'Edge', got '{type(edge)}'")

        if not self.is_valid() or not edge.is_valid():
            raise InvalidContextError()

        # for consistency with the Python API, the `_edge` attribute isn’t a “public” property
        self._path.expand(edge._edge)

        self._vertices = None
        self._edges = None

    @property
    def vertices(self) -> typing.Tuple[Vertex, ...]:
        if not self.is_valid():
            raise InvalidContextError()

        if self._vertices is None:
            num_vertices = self._path.size() + 1
            self._vertices = tuple(Vertex(self._path.vertex_at(i)) for i in range(num_vertices))

        return self._vertices

    @property
    def edges(self) -> typing.Tuple[Edge, ...]:
        if not self.is_valid():
            raise InvalidContextError()

        if self._edges is None:
            num_edges = self._path.size()
            self._edges = tuple(Edge(self._path.edge_at(i)) for i in range(num_edges))

        return self._edges


class Vertices:
    __slots__ = ("_graph", "_len")

    def __init__(self, graph):
        if not isinstance(graph, _mgp_mock.Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        self._graph = graph
        self._len = None

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp_mock.Graph as that always references the whole graph state.
        return Vertices(self._graph)

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    def __iter__(self) -> typing.Iterable[Vertex]:
        if not self.is_valid():
            raise InvalidContextError()

        for vertex in self._graph.vertices:
            yield Vertex(vertex)

    def __contains__(self, vertex: Vertex):
        return self._graph.has_node(vertex.id)

    def __len__(self):
        if not self._len:
            self._len = sum(1 for _ in self)

        return self._len


class Record:
    __slots__ = ("fields",)

    def __init__(self, **kwargs):
        self.fields = kwargs


class Graph:
    __slots__ = ("_graph",)

    def __init__(self, graph):
        if not isinstance(graph, _mgp_mock.Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        self._graph = graph

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp_mock.Graph as that always references the whole graph state.
        return Graph(self._graph)

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    def get_vertex_by_id(self, vertex_id: VertexId) -> Vertex:
        if not self.is_valid():
            raise InvalidContextError()

        return Vertex(self._graph.get_vertex_by_id(vertex_id))

    @property
    def vertices(self) -> Vertices:
        if not self.is_valid():
            raise InvalidContextError()

        return Vertices(self._graph)

    def is_mutable(self) -> bool:
        if not self.is_valid():
            raise InvalidContextError()

        return not self._graph.is_immutable()

    def create_vertex(self) -> Vertex:
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        return Vertex(self._graph.create_vertex())

    def delete_vertex(self, vertex: Vertex) -> None:
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        if not self._graph.vertex_is_isolate(vertex.id):
            raise LogicErrorError("Logic error.")

        # TODO find out whether users can ever send this method a nonexistent vertex
        self._graph.delete_vertex(vertex.id)

    def detach_delete_vertex(self, vertex: Vertex) -> None:
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        # TODO find out whether users can ever send this method a nonexistent vertex
        self._graph.delete_vertex(vertex.id)

    def create_edge(self, from_vertex: Vertex, to_vertex: Vertex, edge_type: EdgeType) -> Edge:
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        new_edge = self._graph.create_edge(from_vertex.id, to_vertex.id, edge_type.name)
        return Edge(new_edge)

    def delete_edge(self, edge: Edge) -> None:
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        self._graph.delete_edge(edge.from_vertex.id, edge.to_vertex.id, edge.id)


class AbortError(Exception):
    """Signals that the procedure was asked to abort its execution."""

    pass


class ProcCtx:
    __slots__ = ("_graph",)

    def __init__(self, graph):
        if not isinstance(graph, _mgp_mock.Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        self._graph = Graph(graph)

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    @property
    def graph(self) -> Graph:
        if not self.is_valid():
            raise InvalidContextError()

        return self._graph

    # TODO try to implement
    # def must_abort(self) -> bool:
    #     if not self.is_valid():
    #         raise InvalidContextError()

    #     return self._graph._graph.nx.must_abort()

    # def check_must_abort(self):
    #     if self.must_abort():
    #         raise AbortError


# Additional typing support

Number = typing.Union[int, float]

Map = typing.Union[dict, Edge, Vertex]

Date = datetime.date

LocalTime = datetime.time

LocalDateTime = datetime.datetime

Duration = datetime.timedelta

Any = typing.Union[bool, str, Number, Map, list, Date, LocalTime, LocalDateTime, Duration]

List = typing.List

Nullable = typing.Optional


# Procedure registration


def raise_if_does_not_meet_requirements(func: typing.Callable[..., Record]):
    if not callable(func):
        raise TypeError(f"Expected a callable object, got an instance of '{type(func)}'")
    if inspect.iscoroutinefunction(func):
        raise TypeError("Callable must not be 'async def' function")
    if sys.version_info >= (3, 6):
        if inspect.isasyncgenfunction(func):
            raise TypeError("Callable must not be 'async def' function")
    if inspect.isgeneratorfunction(func):
        raise NotImplementedError("Generator functions are not supported")


def _register_proc(func: typing.Callable[..., Record], is_write: bool):
    raise_if_does_not_meet_requirements(func)

    sig = inspect.signature(func)

    params = tuple(sig.parameters.values())
    if params and params[0].annotation is ProcCtx:

        @wraps(func)
        def wrapper(ctx, *args):
            if not is_write:
                ctx._graph._graph.nx = nx.freeze(ctx.graph._graph.nx)

            result_record = func(ctx, *args)

            # Invalidate context after execution
            ctx._graph._graph.invalidate()

            return result_record

    else:

        @wraps(func)
        def wrapper(*args):
            return func(*args)

    if sig.return_annotation is not sig.empty:
        record = sig.return_annotation

        if not isinstance(record, Record):
            raise TypeError(f"Expected '{func.__name__}' to return 'mgp.Record', got '{type(record)}'")

    return wrapper


def read_proc(func: typing.Callable[..., Record]):
    return _register_proc(func, False)


def write_proc(func: typing.Callable[..., Record]):
    return _register_proc(func, True)


class InvalidMessageError(Exception):
    """
    Signals using a message instance outside of the registered transformation.
    """

    pass


SOURCE_TYPE_KAFKA = "SOURCE_TYPE_KAFKA"
SOURCE_TYPE_PULSAR = "SOURCE_TYPE_PULSAR"


class Message:
    __slots__ = ("_message",)

    def __init__(self, message):
        if not isinstance(message, _mgp_mock.Message):
            raise TypeError(f"Expected '_mgp_mock.Message', got '{type(message)}'")

        self._message = message

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp_mock.Messages as that always references all the messages.
        return Message(self._message)

    def is_valid(self) -> bool:
        return self._message.is_valid()

    def source_type(self) -> str:
        if not self.is_valid():
            raise InvalidMessageError()

        return (
            SOURCE_TYPE_KAFKA
            if isinstance(self._message.message, kafka.consumer.fetcher.ConsumerRecord)
            else SOURCE_TYPE_PULSAR
        )

    def payload(self) -> bytes:
        if not self.is_valid():
            raise InvalidMessageError()

        return (
            self._message.message.value
            if isinstance(self._message.message, kafka.consumer.fetcher.ConsumerRecord)
            else self._message.message.data()
        )

    def topic_name(self) -> str:
        if not self.is_valid():
            raise InvalidMessageError()

        return (
            self._message.message.topic
            if isinstance(self._message.message, kafka.consumer.fetcher.ConsumerRecord)
            else self._message.message.topic_name()
        )

    def key(self) -> bytes:
        if not self.is_valid():
            raise InvalidMessageError()

        if not isinstance(self._message.message, kafka.consumer.fetcher.ConsumerRecord):
            raise InvalidArgumentError("Invalid argument.")

        return self._message.message.key

    def timestamp(self) -> int:
        if not self.is_valid():
            raise InvalidMessageError()

        if not isinstance(self._message.message, kafka.consumer.fetcher.ConsumerRecord):
            raise InvalidArgumentError("Invalid argument.")

        return self._message.message.timestamp

    def offset(self) -> int:
        if not self.is_valid():
            raise InvalidMessageError()

        if not isinstance(self._message.message, kafka.consumer.fetcher.ConsumerRecord):
            raise InvalidArgumentError("Invalid argument.")

        return self._message.message.offset


class InvalidMessagesError(Exception):
    """Signals using a messages instance outside of the registered transformation."""

    pass


class Messages:
    __slots__ = ("_messages",)

    def __init__(self, messages):
        if not isinstance(messages, _mgp_mock.Messages):
            raise TypeError("Expected '_mgp_mock.Messages', got '{}'".format(type(messages)))
        self._messages = messages

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp_mock.Messages as that always references all the messages.
        return Messages(self._messages)

    def is_valid(self) -> bool:
        return self._messages.is_valid()

    def message_at(self, id: int) -> Message:
        if not self.is_valid():
            raise InvalidMessagesError()

        return Message(self._messages.messages[id])

    def total_messages(self) -> int:
        if not self.is_valid():
            raise InvalidMessagesError()

        return len(self._messages.messages)


class TransCtx:
    __slots__ = "_graph"

    def __init__(self, graph):
        if not isinstance(graph, _mgp_mock.Graph):
            raise TypeError(f"Expected 'networkx.classes._mgp_mock.Graph', got '{type(graph)}'")

        self._graph = Graph(graph)

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    @property
    def graph(self) -> Graph:
        if not self.is_valid():
            raise InvalidContextError()

        return self._graph


def transformation(func: typing.Callable[..., Record]):
    raise_if_does_not_meet_requirements(func)

    sig = inspect.signature(func)

    params = tuple(sig.parameters.values())
    if not params or not params[0].annotation is Messages:
        if not len(params) == 2 or not params[1].annotation is Messages:
            raise NotImplementedError("Valid signatures for transformations are (TransCtx, Messages) or (Messages)")

    if params[0].annotation is TransCtx:

        @wraps(func)
        def wrapper(ctx, messages):
            result_record = func(ctx, messages)

            # Invalidate context and messages after execution
            ctx._graph._graph.invalidate()
            messages._messages.invalidate()

            return result_record

    else:

        @wraps(func)
        def wrapper(_, messages):
            result_record = func(messages)

            # Invalidate messages after execution
            messages._messages.invalidate()

            return result_record

    return wrapper


class FuncCtx:
    __slots__ = "_graph"

    def __init__(self, graph):
        if not isinstance(graph, _mgp_mock.Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        self._graph = Graph(graph)

    def is_valid(self) -> bool:
        return self._graph.is_valid()


def function(func: typing.Callable):
    raise_if_does_not_meet_requirements(func)

    sig = inspect.signature(func)

    params = tuple(sig.parameters.values())
    if params and params[0].annotation is FuncCtx:

        @wraps(func)
        def wrapper(ctx, *args):
            ctx._graph._graph.nx = nx.freeze(ctx.graph._graph.nx)

            result = func(ctx, *args)

            # Invalidate context after execution
            ctx._graph._graph.invalidate()

            return result

    else:

        @wraps(func)
        def wrapper(*args):
            return func(*args)

    return wrapper


def _wrap_exceptions():
    def wrap_function(func):
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except _mgp_mock.LogicErrorError as e:
                raise LogicErrorError(e)
            except _mgp_mock.ImmutableObjectError as e:
                raise ImmutableObjectError(e)

        return wrapped_func

    def wrap_prop_func(func):
        return None if func is None else wrap_function(func)

    def wrap_member_functions(cls: type):
        for name, obj in inspect.getmembers(cls):
            if inspect.isfunction(obj):
                setattr(cls, name, wrap_function(obj))
            elif isinstance(obj, property):
                setattr(
                    cls,
                    name,
                    property(
                        wrap_prop_func(obj.fget),
                        wrap_prop_func(obj.fset),
                        wrap_prop_func(obj.fdel),
                        obj.__doc__,
                    ),
                )

    def defined_in_this_module(obj: object):
        return getattr(obj, "__module__", "") == __name__

    module = sys.modules[__name__]
    for name, obj in inspect.getmembers(module):
        if not defined_in_this_module(obj):
            continue
        if inspect.isclass(obj):
            wrap_member_functions(obj)
        elif inspect.isfunction(obj) and not name.startswith("_"):
            setattr(module, name, wrap_function(obj))


_wrap_exceptions()
