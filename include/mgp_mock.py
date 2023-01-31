import datetime
import inspect
import sys
import typing
from collections import namedtuple
from functools import wraps


import _mgp
import networkx as nx

# TODO add ValueConversionError checks throughout
# TODO add Path


class InvalidContextError(Exception):
    """
    Signals using a graph element instance outside of the registered procedure.
    """

    pass


class UnknownError(_mgp.UnknownError):
    """
    Signals unspecified failure.
    """

    pass


class UnableToAllocateError(_mgp.UnableToAllocateError):
    """
    Signals failed memory allocation.
    """

    pass


class InsufficientBufferError(_mgp.InsufficientBufferError):
    """
    Signals that some buffer is not big enough.
    """

    pass


class OutOfRangeError(_mgp.OutOfRangeError):
    """
    Signals that an index-like parameter has a value that is outside its
    possible values.
    """

    pass


class LogicErrorError(_mgp.LogicErrorError):
    """
    Signals faulty logic within the program such as violating logical
    preconditions or class invariants and may be preventable.
    """

    pass


class DeletedObjectError(_mgp.DeletedObjectError):
    """
    Signals accessing an already deleted object.
    """

    pass


class InvalidArgumentError(_mgp.InvalidArgumentError):
    """
    Signals that some of the arguments have invalid values.
    """

    pass


class KeyAlreadyExistsError(_mgp.KeyAlreadyExistsError):
    """
    Signals that a key already exists in a container-like object.
    """

    pass


class ImmutableObjectError(_mgp.ImmutableObjectError):
    """
    Signals modification of an immutable object.
    """

    pass


class ValueConversionError(_mgp.ValueConversionError):
    """
    Signals that the conversion failed between python and cypher values.
    """

    pass


class SerializationError(_mgp.SerializationError):
    """
    Signals serialization error caused by concurrent modifications from
    different transactions.
    """

    pass


class AuthorizationError(_mgp.AuthorizationError):
    """
    Signals that the user doesn't have sufficient permissions to perform
    procedure call.
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
    __slots__ = (
        "_vertex_or_edge",
        "_len",
    )

    def __init__(self, vertex_or_edge):
        if not isinstance(vertex_or_edge, (_Vertex, _Edge)):
            raise TypeError("Expected _Vertex or _Edge, got {}".format(type(vertex_or_edge)))
        self._len = None
        self._vertex_or_edge = vertex_or_edge

    def get(self, property_name: str, default=None) -> object:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        try:
            return self[property_name]
        except KeyError:
            return default

    def set(self, property_name: str, value: object) -> None:
        if nx.is_frozen(self._vertex_or_edge.graph):
            raise ImmutableObjectError("Graph is immutable")

        self[property_name] = value

    def items(self) -> typing.Iterable[Property]:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        vertex_or_edge_props = (
            self._vertex_or_edge.graph.nodes[self._vertex_or_edge.vertex]
            if isinstance(self._vertex_or_edge, _Vertex)
            else self._vertex_or_edge.graph.edges[self._vertex_or_edge._edge[0], self._vertex_or_edge._edge[1]]
        )

        for key, value in vertex_or_edge_props.items():
            if key in ("label", "type"):
                continue

            yield Property(key, value)

            if not self._vertex_or_edge.is_valid():
                raise InvalidContextError()

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

        vertex_or_edge_props = (
            self._vertex_or_edge.graph.nodes[self._vertex_or_edge.vertex]
            if isinstance(self._vertex_or_edge, _Vertex)
            else self._vertex_or_edge.graph.edges[self._vertex_or_edge._edge[0], self._vertex_or_edge._edge[1]]
        )

        return vertex_or_edge_props[property_name]

    def __setitem__(self, property_name: str, value: object) -> None:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        if nx.is_frozen(self._vertex_or_edge.graph):
            raise ImmutableObjectError("Graph is immutable")

        if isinstance(self._vertex_or_edge, _Vertex):
            _id = self._vertex_or_edge.vertex
            self._vertex_or_edge.graph.nodes[_id][property_name] = value
            return

        edge = self._vertex_or_edge._edge
        from_id, to_id = edge
        self._vertex_or_edge.graph.edges[from_id, to_id][property_name] = value

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


class _Edge:
    __slots__ = ("_edge", "_graph")

    def __init__(self, edge, graph):
        if not isinstance(edge, typing.Tuple):
            raise TypeError(f"Expected 'Tuple', got '{type(edge)}'")
        if not isinstance(graph, nx.DiGraph):
            raise TypeError(f"Expected 'networkx.classes.digraph.DiGraph', got '{type(graph)}'")
        self._edge = edge
        self._graph = graph

    @property
    def edge(self) -> typing.Tuple[int, int]:
        return self._edge

    @property
    def graph(self) -> nx.DiGraph:
        return self._graph

    def is_valid(self) -> bool:
        return self._graph.has_edge(*self._edge)

    def get_id(self) -> EdgeId:
        # edges = [edge for edge in self._graph.edges]
        # return edges.index(self._edge)
        return int(str(self._edge[0]) + str(self._edge[1]))

    def get_type_name(self) -> EdgeType:
        return self._graph.get_edge_data(*self._edge)["type"]


class Edge:
    __slots__ = ("_edge",)

    def __init__(self, edge):
        if not isinstance(edge, _Edge):
            raise TypeError(f"Expected '_Edge', got '{type(edge)}'")
        self._edge = edge

    def is_valid(self) -> bool:
        return self._edge.is_valid()

    def underlying_graph_is_mutable(self) -> bool:
        if not self.is_valid():
            raise InvalidContextError()
        return nx.is_frozen(self._edge.graph)

    @property
    def id(self) -> EdgeId:
        if not self.is_valid():
            raise InvalidContextError()
        return self._edge.get_id()

    @property
    def type(self) -> EdgeType:
        if not self.is_valid():
            raise InvalidContextError()
        return self._edge.get_type_name()

    @property
    def from_vertex(self) -> "Vertex":
        if not self.is_valid():
            raise InvalidContextError()

        vertex = _Vertex(self._edge.edge[0], self._edge.graph)
        return Vertex(vertex)

    @property
    def to_vertex(self) -> "Vertex":
        if not self.is_valid():
            raise InvalidContextError()

        vertex = _Vertex(self._edge.edge[1], self._edge.graph)
        return Vertex(vertex)

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
        return self.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)


if sys.version_info >= (3, 5, 2):
    VertexId = typing.NewType("VertexId", int)
else:
    VertexId = int


class _Vertex:
    __slots__ = ("_vertex", "_graph")

    def __init__(self, vertex, graph):
        if not isinstance(vertex, int):
            raise TypeError(f"Expected 'int', got '{type(vertex)}'")
        if not isinstance(graph, nx.DiGraph):
            raise TypeError(f"Expected 'networkx.classes.digraph.DiGraph', got '{type(graph)}'")
        if not graph.has_node(vertex):
            raise IndexError(f"Unable to find vertex with id {vertex}.")
        self._vertex = vertex
        self._graph = graph

    @property
    def vertex(self) -> int:
        return self._vertex

    @property
    def graph(self) -> nx.DiGraph:
        return self._graph

    @property
    def labels(self) -> typing.List[int]:
        return self._graph.nodes[self._vertex]["label"].split(":")

    def is_valid(self) -> bool:
        return self._graph.has_node(self._vertex)

    # TODO remove
    # def get_id(self) -> VertexId:
    #     return self._vertex

    def add_label(self, label: str) -> None:
        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Graph is immutable")

        self._graph.nodes[self._vertex]["label"] += f":{label}"

    def remove_label(self, label: str) -> None:
        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Graph is immutable")

        labels = self._graph.nodes[self._vertex]["label"]
        if labels.endswith(f":{label}"):
            labels += "\n"
            self._graph.nodes[self._vertex]["label"] = labels.replace(f":{label}\n", "")
        else:
            self._graph.nodes[self._vertex]["label"] = labels.replace(f":{label}:", ":")


class Vertex:
    __slots__ = ("_vertex",)

    def __init__(self, vertex):
        if not isinstance(vertex, _Vertex):
            raise TypeError(f"Expected '_Vertex', got '{type(vertex)}'")
        self._vertex = vertex

    def is_valid(self) -> bool:
        return self._vertex.is_valid()

    def underlying_graph_is_mutable(self) -> bool:
        if not self.is_valid():
            raise InvalidContextError()
        return nx.is_frozen(self._vertex.graph)

    @property
    def id(self) -> VertexId:
        if not self.is_valid():
            raise InvalidContextError()
        return self._vertex.vertex

    @property
    def labels(self) -> typing.Tuple[Label]:
        if not self.is_valid():
            raise InvalidContextError()
        return tuple(Label(l) for l in self._vertex.labels)

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
        for edge in self._vertex.graph.in_edges(self.id):
            yield Edge(_Edge(edge, self._vertex.graph))
            if not self.is_valid():
                raise InvalidContextError()

    @property
    def out_edges(self) -> typing.Iterable[Edge]:
        if not self.is_valid():
            raise InvalidContextError()
        for edge in self._vertex.graph.out_edges(self.id):
            edge_ = _Edge(edge, self._vertex.graph)
            yield Edge(edge_)
            if not self.is_valid():
                raise InvalidContextError()

    def __eq__(self, other) -> bool:
        if not self.is_valid():
            raise InvalidContextError()
        if not isinstance(other, Vertex):
            return NotImplemented
        return self.id == self.id

    def __hash__(self) -> int:
        return hash(self.id)


class Vertices:
    __slots__ = ("_graph", "_len")

    def __init__(self, graph):
        if not isinstance(graph, nx.DiGraph):
            raise TypeError(f"Expected 'networkx.classes.digraph.DiGraph', got '{type(graph)}'")
        self._graph = graph
        self._len = None

    def is_valid(self) -> bool:
        return True

    def __iter__(self) -> typing.Iterable[Vertex]:
        if not self.is_valid():
            raise InvalidContextError()

        for id in self._graph.nodes:
            vertex = _Vertex(id, self._graph)
            yield Vertex(vertex)
            if not self.is_valid():
                raise InvalidContextError()

    def __contains__(self, vertex):
        return self._graph.has_node(vertex)

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
        if not isinstance(graph, nx.DiGraph):
            raise TypeError(f"Expected 'networkx.classes.digraph.DiGraph', got '{type(graph)}'")
        self._graph = graph

    def is_valid(self) -> bool:
        return True

    def get_vertex_by_id(self, vertex_id: VertexId) -> Vertex:
        if not self.is_valid():
            raise InvalidContextError()
        vertex = _Vertex(vertex_id, self._graph)
        return Vertex(vertex)

    @property
    def vertices(self) -> Vertices:
        if not self.is_valid():
            raise InvalidContextError()
        return Vertices(self._graph)

    def is_mutable(self) -> bool:
        if not self.is_valid():
            raise InvalidContextError()
        return not nx.is_frozen(self._graph)

    def create_vertex(self) -> Vertex:
        if not self.is_valid():
            raise InvalidContextError()

        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Graph is immutable")

        new_id = max(node for node in self._graph.nodes) + 1
        self._graph.add_node(new_id)

        vertex = _Vertex(new_id, self._graph)
        return Vertex(vertex)

    def delete_vertex(self, vertex: Vertex) -> None:
        if not self.is_valid():
            raise InvalidContextError()

        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Graph is immutable")

        if not nx.is_isolate(self._graph, vertex.id):
            raise LogicErrorError("Node is connected")

        # TODO find out whether users can ever send this method a nonexistent vertex
        self._graph.remove_node(vertex.id)

    def detach_delete_vertex(self, vertex: Vertex) -> None:
        if not self.is_valid():
            raise InvalidContextError()

        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Graph is immutable")

        # TODO find out whether users can ever send this method a nonexistent vertex
        self._graph.remove_node(vertex.id)

    def create_edge(self, from_vertex: Vertex, to_vertex: Vertex, edge_type: EdgeType) -> Edge:
        if not self.is_valid():
            raise InvalidContextError()

        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Graph is immutable")

        # TODO find out whether users can ever send this method nonexistent vertices
        self._graph.add_edge(from_vertex.id, to_vertex.id, type=edge_type.name)
        edge = _Edge((from_vertex.id, to_vertex.id), self._graph)
        return Edge(edge)

    def delete_edge(self, edge: Edge) -> None:
        if not self.is_valid():
            raise InvalidContextError()

        if nx.is_frozen(self._graph):
            raise ImmutableObjectError("Graph is immutable")

        self._graph.remove_edge(edge.from_vertex.id, edge.to_vertex.id)


class AbortError(Exception):
    """Signals that the procedure was asked to abort its execution."""

    pass


class ProcCtx:
    __slots__ = ("_graph",)

    def __init__(self, graph):
        if not isinstance(graph, nx.DiGraph):
            raise TypeError(f"Expected 'networkx.classes.digraph.DiGraph', got '{type(graph)}'")
        self._graph = Graph(graph)

    def is_valid(self) -> bool:
        return True

    @property
    def graph(self) -> Graph:
        if not self.is_valid():
            raise InvalidContextError()
        return self._graph

    def must_abort(self) -> bool:
        if not self.is_valid():
            raise InvalidContextError()
        return self._graph._graph.must_abort()

    def check_must_abort(self):
        if self.must_abort():
            raise AbortError


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


class UnsupportedTypingError(Exception):
    """Signals that a typing annotation is not a supported _mgp.CypherType."""

    def __init__(self, type_):
        super().__init__("Unsupported typing annotation '{}'".format(type_))


def _typing_to_cypher_type(type_):
    """Convert typing annotation to a _mgp.CypherType instance."""
    simple_types = {
        typing.Any: _mgp.type_nullable(_mgp.type_any()),
        object: _mgp.type_nullable(_mgp.type_any()),
        list: _mgp.type_list(_mgp.type_nullable(_mgp.type_any())),
        Any: _mgp.type_any(),
        bool: _mgp.type_bool(),
        str: _mgp.type_string(),
        int: _mgp.type_int(),
        float: _mgp.type_float(),
        Number: _mgp.type_number(),
        Map: _mgp.type_map(),
        Vertex: _mgp.type_node(),
        Edge: _mgp.type_relationship(),
        Date: _mgp.type_date(),
        LocalTime: _mgp.type_local_time(),
        LocalDateTime: _mgp.type_local_date_time(),
        Duration: _mgp.type_duration(),
    }
    try:
        return simple_types[type_]
    except KeyError:
        pass
    if sys.version_info >= (3, 8):
        complex_type = typing.get_origin(type_)
        type_args = typing.get_args(type_)
        if complex_type == typing.Union:
            # If we have a Union with NoneType inside, it means we are building
            # a nullable type.
            # isinstance doesn't work here because subscripted generics cannot
            # be used with class and instance checks. type comparison should be
            # fine because subclasses are not used.
            if type(None) in type_args:
                types = tuple(t for t in type_args if t is not type(None))  # noqa E721
                if len(types) == 1:
                    (type_arg,) = types
                else:
                    # We cannot do typing.Union[*types], so do the equivalent
                    # with __getitem__ which does not even need arg unpacking.
                    type_arg = typing.Union.__getitem__(types)
                return _mgp.type_nullable(_typing_to_cypher_type(type_arg))
        elif complex_type == list:
            (type_arg,) = type_args
            return _mgp.type_list(_typing_to_cypher_type(type_arg))
        raise UnsupportedTypingError(type_)
    else:
        # We cannot get to type args in any reliable way prior to 3.8, but we
        # still want to support typing.Optional and typing.List, so just parse
        # their string representations. Hopefully, that is always pretty
        # printed the same way. `typing.List[type]` is printed as such, while
        # `typing.Optional[type]` is printed as 'typing.Union[type, NoneType]'
        def parse_type_args(type_as_str):
            return tuple(
                map(
                    str.strip,
                    type_as_str[type_as_str.index("[") + 1 : -1].split(","),
                )
            )

        def fully_qualified_name(cls):
            if cls.__module__ is None or cls.__module__ == "builtins":
                return cls.__name__
            return cls.__module__ + "." + cls.__name__

        def get_simple_type(type_as_str):
            for simple_type, cypher_type in simple_types.items():
                if type_as_str == str(simple_type):
                    return cypher_type
                # Fallback to comparing to __name__ if it exits. This handles
                # the cases like when we have 'object' which is
                # `object.__name__`, but `str(object)` is "<class 'object'>"
                try:
                    if type_as_str == fully_qualified_name(simple_type):
                        return cypher_type
                except AttributeError:
                    pass

        def parse_typing(type_as_str):
            if type_as_str.startswith("typing.Union"):
                type_args_as_str = parse_type_args(type_as_str)
                none_type_as_str = type(None).__name__
                if none_type_as_str in type_args_as_str:
                    types = tuple(t for t in type_args_as_str if t != none_type_as_str)
                    if len(types) == 1:
                        (type_arg_as_str,) = types
                    else:
                        type_arg_as_str = "typing.Union[" + ", ".join(types) + "]"
                    simple_type = get_simple_type(type_arg_as_str)
                    if simple_type is not None:
                        return _mgp.type_nullable(simple_type)
                    return _mgp.type_nullable(parse_typing(type_arg_as_str))
            elif type_as_str.startswith("typing.List"):
                type_arg_as_str = parse_type_args(type_as_str)

                if len(type_arg_as_str) > 1:
                    # Nested object could be a type consisting of a list of types (e.g. mgp.Map)
                    # so we need to join the parts.
                    type_arg_as_str = ", ".join(type_arg_as_str)
                else:
                    type_arg_as_str = type_arg_as_str[0]

                simple_type = get_simple_type(type_arg_as_str)
                if simple_type is not None:
                    return _mgp.type_list(simple_type)
                return _mgp.type_list(parse_typing(type_arg_as_str))
            raise UnsupportedTypingError(type_)

        return parse_typing(str(type_))


# Procedure registration


def raise_if_does_not_meet_requirements(func: typing.Callable[..., Record]):
    if not callable(func):
        raise TypeError("Expected a callable object, got an instance of '{}'".format(type(func)))
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
    print(params)
    if params and params[0].annotation is ProcCtx:
        if len(params) == 1:

            @wraps(func)
            def wrapper(ctx):
                graph = nx.freeze(ctx.graph._graph) if not is_write else ctx.graph._graph
                return func(ProcCtx(graph))

        else:

            @wraps(func)
            def wrapper(graph, args):
                graph = nx.freeze(graph._graph) if not is_write else graph._graph
                return func(ProcCtx(graph), *args)

    else:

        @wraps(func)
        def wrapper(graph, args):
            return func(*args)

    if sig.return_annotation is not sig.empty:
        record = sig.return_annotation
        if not isinstance(record, Record):
            raise TypeError("Expected '{}' to return 'mgp.Record', got '{}'".format(func.__name__, type(record)))

    return wrapper


def read_proc(func: typing.Callable[..., Record]):
    return _register_proc(func, False)


def write_proc(func: typing.Callable[..., Record]):
    return _register_proc(func, True)


def _wrap_exceptions():
    def wrap_function(func):
        @wraps(func)
        def wrapped_func(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except _mgp.UnknownError as e:
                raise UnknownError(e)
            except _mgp.UnableToAllocateError as e:
                raise UnableToAllocateError(e)
            except _mgp.InsufficientBufferError as e:
                raise InsufficientBufferError(e)
            except _mgp.OutOfRangeError as e:
                raise OutOfRangeError(e)
            except _mgp.LogicErrorError as e:
                raise LogicErrorError(e)
            except _mgp.DeletedObjectError as e:
                raise DeletedObjectError(e)
            except _mgp.InvalidArgumentError as e:
                raise InvalidArgumentError(e)
            except _mgp.KeyAlreadyExistsError as e:
                raise KeyAlreadyExistsError(e)
            except _mgp.ImmutableObjectError as e:
                raise ImmutableObjectError(e)
            except _mgp.ValueConversionError as e:
                raise ValueConversionError(e)
            except _mgp.SerializationError as e:
                raise SerializationError(e)
            except _mgp.AuthorizationError as e:
                raise AuthorizationError(e)

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
