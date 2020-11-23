'''
This module provides the API for usage in custom openCypher procedures.
'''

# C API using `mgp_memory` is not exposed in Python, instead the usage of such
# API is hidden behind Python API. Any function requiring an instance of
# `mgp_memory` should go through a `ProcCtx` instance.
#
# `mgp_value` does not exist as such in Python, instead all `mgp_value`
# instances are marshalled to an appropriate Python object. This implies that
# `mgp_list` and `mgp_map` are mapped to `list` and `dict` respectively.
#
# Only the public API is stubbed out here. Any private details are left for the
# actual implementation. Functions have type annotations as supported by Python
# 3.5, but variable type annotations are only available with Python 3.6+

from collections import namedtuple
import functools
import inspect
import sys
import typing

import _mgp


class InvalidContextError(Exception):
    '''Signals using a graph element instance outside of the registered procedure.'''
    pass


class Label:
    '''Label of a Vertex.'''
    __slots__ = ('_name',)

    def __init__(self, name):
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
Property = namedtuple('Property', ('name', 'value'))


class Properties:
    '''A collection of properties either on a Vertex or an Edge.'''
    __slots__ = ('_vertex_or_edge', '_len',)

    def __init__(self, vertex_or_edge):
        if not isinstance(vertex_or_edge, (_mgp.Vertex, _mgp.Edge)):
            raise TypeError("Expected '_mgp.Vertex' or '_mgp.Edge', \
                            got {}".format(type(vertex_or_edge)))
        self._len = None
        self._vertex_or_edge = vertex_or_edge

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, as the underlying C API should
        # not support deepcopy. Besides, it doesn't make much sense to actually
        # copy _mgp.Edge and _mgp.Vertex types as they are actually references
        # to graph elements and not proper values.
        return Properties(self._vertex_or_edge)

    def get(self, property_name: str, default=None) -> object:
        '''Get the value of a property with the given name or return default.

        Raise InvalidContextError.
        '''
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        try:
            return self[property_name]
        except KeyError:
            return default

    def items(self) -> typing.Iterable[Property]:
        '''Raise InvalidContextError.'''
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        properties_it = self._vertex_or_edge.iter_properties()
        prop = properties_it.get()
        while prop is not None:
            yield Property(*prop)
            if not self._vertex_or_edge.is_valid():
                raise InvalidContextError()
            prop = properties_it.next()

    def keys(self) -> typing.Iterable[str]:
        '''Iterate over property names.

        Raise InvalidContextError.
        '''
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        for item in self.items():
            yield item.name

    def values(self) -> typing.Iterable[object]:
        '''Iterate over property values.

        Raise InvalidContextError.
        '''
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        for item in self.items():
            yield item.value

    def __len__(self) -> int:
        '''Raise InvalidContextError.'''
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        if self._len is None:
            self._len = sum(1 for item in self.items())
        return self._len

    def __iter__(self) -> typing.Iterable[str]:
        '''Iterate over property names.

        Raise InvalidContextError.
        '''
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        for item in self.items():
            yield item.name

    def __getitem__(self, property_name: str) -> object:
        '''Get the value of a property with the given name or raise KeyError.

        Raise InvalidContextError.'''
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        prop = self._vertex_or_edge.get_property(property_name)
        if prop is None:
            raise KeyError()
        return prop

    def __contains__(self, property_name: str) -> bool:
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()
        try:
            _ = self[property_name]
            return True
        except KeyError:
            return False


class EdgeType:
    '''Type of an Edge.'''
    __slots__ = ('_name',)

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
    EdgeId = typing.NewType('EdgeId', int)
else:
    EdgeId = int


class Edge:
    '''Edge in the graph database.

    Access to an Edge is only valid during a single execution of a procedure in
    a query. You should not globally store an instance of an Edge. Using an
    invalid Edge instance will raise InvalidContextError.
    '''
    __slots__ = ('_edge',)

    def __init__(self, edge):
        if not isinstance(edge, _mgp.Edge):
            raise TypeError("Expected '_mgp.Edge', got '{}'".format(type(edge)))
        self._edge = edge

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp.Edge as that is actually a reference to a graph element
        # and not a proper value.
        return Edge(self._edge)

    def is_valid(self) -> bool:
        '''Return True if `self` is in valid context and may be used.'''
        return self._edge.is_valid()

    @property
    def id(self) -> EdgeId:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        return self._edge.get_id()

    @property
    def type(self) -> EdgeType:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        return EdgeType(self._edge.get_type_name())

    @property
    def from_vertex(self):  # -> Vertex:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        return Vertex(self._edge.from_vertex())

    @property
    def to_vertex(self):  # -> Vertex:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        return Vertex(self._edge.to_vertex())

    @property
    def properties(self) -> Properties:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        return Properties(self._edge)

    def __eq__(self, other) -> bool:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        if not isinstance(other, Edge):
            return NotImplemented
        return self._edge == other._edge

    def __hash__(self) -> int:
        return hash(self.id)


if sys.version_info >= (3, 5, 2):
    VertexId = typing.NewType('VertexId', int)
else:
    VertexId = int


class Vertex:
    '''Vertex in the graph database.

    Access to a Vertex is only valid during a single execution of a procedure
    in a query. You should not globally store an instance of a Vertex. Using an
    invalid Vertex instance will raise InvalidContextError.
    '''
    __slots__ = ('_vertex',)

    def __init__(self, vertex):
        if not isinstance(vertex, _mgp.Vertex):
            raise TypeError("Expected '_mgp.Vertex', got '{}'".format(type(vertex)))
        self._vertex = vertex

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp.Vertex as that is actually a reference to a graph element
        # and not a proper value.
        return Vertex(self._vertex)

    def is_valid(self) -> bool:
        '''Return True if `self` is in valid context and may be used'''
        return self._vertex.is_valid()

    @property
    def id(self) -> VertexId:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        return self._vertex.get_id()

    @property
    def labels(self) -> typing.List[Label]:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        return tuple(Label(self._vertex.label_at(i))
                     for i in range(self._vertex.labels_count()))

    @property
    def properties(self) -> Properties:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        return Properties(self._vertex)

    @property
    def in_edges(self) -> typing.Iterable[Edge]:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        edges_it = self._vertex.iter_in_edges()
        edge = edges_it.get()
        while edge is not None:
            yield Edge(edge)
            if not self.is_valid():
                raise InvalidContextError()
            edge = edges_it.next()

    @property
    def out_edges(self) -> typing.Iterable[Edge]:
        '''Raise InvalidContextError.'''
        if not self.is_valid():
            raise InvalidContextError()
        edges_it = self._vertex.iter_out_edges()
        edge = edges_it.get()
        while edge is not None:
            yield Edge(edge)
            if not self.is_valid():
                raise InvalidContextError()
            edge = edges_it.next()

    def __eq__(self, other) -> bool:
        '''Raise InvalidContextError'''
        if not self.is_valid():
            raise InvalidContextError()
        if not isinstance(other, Vertex):
            return NotImplemented
        return self._vertex == other._vertex

    def __hash__(self) -> int:
        return hash(self.id)


class Path:
    '''Path containing Vertex and Edge instances.'''
    __slots__ = ('_path', '_vertices', '_edges')

    def __init__(self, starting_vertex_or_path: typing.Union[_mgp.Path, Vertex]):
        '''Initialize with a starting Vertex.

        Raise InvalidContextError if passed in Vertex is invalid.
        '''
        # We cache calls to `vertices` and `edges`, so as to avoid needless
        # allocations at the C level.
        self._vertices = None
        self._edges = None
        # Accepting _mgp.Path is just for internal usage.
        if isinstance(starting_vertex_or_path, _mgp.Path):
            self._path = starting_vertex_or_path
        elif isinstance(starting_vertex_or_path, Vertex):
            vertex = starting_vertex_or_path._vertex
            if not vertex.is_valid():
                raise InvalidContextError()
            self._path = _mgp.Path.make_with_start(vertex)
        else:
            raise TypeError("Expected '_mgp.Vertex' or '_mgp.Path', got '{}'"
                            .format(type(starting_vertex_or_path)))

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
        # copy _mgp.Edge and _mgp.Vertex types as they are actually references
        # to graph elements and not proper values.
        path = self.__copy__()
        memo[id(self._path)] = path._path
        return path

    def is_valid(self) -> bool:
        return self._path.is_valid()

    def expand(self, edge: Edge):
        '''Append an edge continuing from the last vertex on the path.

        The last vertex on the path will become the other endpoint of the given
        edge, as continued from the current last vertex.

        Raise ValueError if the current last vertex in the path is not part of
        the given edge.
        Raise InvalidContextError if using an invalid Path instance or if
        passed in edge is invalid.
        '''
        if not isinstance(edge, Edge):
            raise TypeError("Expected '_mgp.Edge', got '{}'".format(type(edge)))
        if not self.is_valid() or not edge.is_valid():
            raise InvalidContextError()
        self._path.expand(edge._edge)
        # Invalidate our cached tuples
        self._vertices = None
        self._edges = None

    @property
    def vertices(self) -> typing.Tuple[Vertex, ...]:
        '''Vertices ordered from the start to the end of the path.

        Raise InvalidContextError if using an invalid Path instance.'''
        if not self.is_valid():
            raise InvalidContextError()
        if self._vertices is None:
            num_vertices = self._path.size() + 1
            self._vertices = tuple(Vertex(self._path.vertex_at(i))
                                   for i in range(num_vertices))
        return self._vertices

    @property
    def edges(self) -> typing.Tuple[Edge, ...]:
        '''Edges ordered from the start to the end of the path.

        Raise InvalidContextError if using an invalid Path instance.'''
        if not self.is_valid():
            raise InvalidContextError()
        if self._edges is None:
            num_edges = self._path.size()
            self._edges = tuple(Edge(self._path.edge_at(i))
                                for i in range(num_edges))
        return self._edges


class Record:
    '''Represents a record of resulting field values.'''
    __slots__ = ('fields',)

    def __init__(self, **kwargs):
        '''Initialize with name=value fields in kwargs.'''
        self.fields = kwargs


class Vertices:
    '''Iterable over vertices in a graph.'''
    __slots__ = ('_graph', '_len')

    def __init__(self, graph):
        if not isinstance(graph, _mgp.Graph):
            raise TypeError("Expected '_mgp.Graph', got '{}'".format(type(graph)))
        self._graph = graph
        self._len = None

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp.Graph as that always references the whole graph state.
        return Vertices(self._graph)

    def is_valid(self) -> bool:
        '''Return True if `self` is in valid context and may be used.'''
        return self._graph.is_valid()

    def __iter__(self) -> typing.Iterable[Vertex]:
        '''Raise InvalidContextError if context is invalid.'''
        if not self.is_valid():
            raise InvalidContextError()
        vertices_it = self._graph.iter_vertices()
        vertex = vertices_it.get()
        while vertex is not None:
            yield Vertex(vertex)
            if not self.is_valid():
                raise InvalidContextError()
            vertex = vertices_it.next()

    def __contains__(self, vertex):
        try:
            _ = self._graph.get_vertex_by_id(vertex.id)
            return True
        except IndexError:
            return False

    def __len__(self):
        if not self._len:
            self._len = sum(1 for _ in self)
        return self._len


class Graph:
    '''State of the graph database in current ProcCtx.'''
    __slots__ = ('_graph',)

    def __init__(self, graph):
        if not isinstance(graph, _mgp.Graph):
            raise TypeError("Expected '_mgp.Graph', got '{}'".format(type(graph)))
        self._graph = graph

    def __deepcopy__(self, memo):
        # This is the same as the shallow copy, because we want to share the
        # underlying C struct. Besides, it doesn't make much sense to actually
        # copy _mgp.Graph as that always references the whole graph state.
        return Graph(self._graph)

    def is_valid(self) -> bool:
        '''Return True if `self` is in valid context and may be used.'''
        return self._graph.is_valid()

    def get_vertex_by_id(self, vertex_id: VertexId) -> Vertex:
        '''Return the Vertex corresponding to given vertex_id from the graph.

        Access to a Vertex is only valid during a single execution of a
        procedure in a query. You should not globally store the returned
        Vertex.

        Raise IndexError if unable to find the given vertex_id.
        Raise InvalidContextError if context is invalid.
        '''
        if not self.is_valid():
            raise InvalidContextError()
        vertex = self._graph.get_vertex_by_id(vertex_id)
        return Vertex(vertex)

    @property
    def vertices(self) -> Vertices:
        '''All vertices in the graph.

        Access to a Vertex is only valid during a single execution of a
        procedure in a query. You should not globally store the returned Vertex
        instances.

        Raise InvalidContextError if context is invalid.
        '''
        if not self.is_valid():
            raise InvalidContextError()
        return Vertices(self._graph)


class AbortError(Exception):
    '''Signals that the procedure was asked to abort its execution.'''
    pass


class ProcCtx:
    '''Context of a procedure being executed.

    Access to a ProcCtx is only valid during a single execution of a procedure
    in a query. You should not globally store a ProcCtx instance.
    '''
    __slots__ = ('_graph',)

    def __init__(self, graph):
        if not isinstance(graph, _mgp.Graph):
            raise TypeError("Expected '_mgp.Graph', got '{}'".format(type(graph)))
        self._graph = Graph(graph)

    def is_valid(self) -> bool:
        return self._graph.is_valid()

    @property
    def graph(self) -> Graph:
        '''Raise InvalidContextError if context is invalid.'''
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

Any = typing.Union[bool, str, Number, Map, Path, list]

List = typing.List

Nullable = typing.Optional


class UnsupportedTypingError(Exception):
    '''Signals a typing annotation is not supported as a _mgp.CypherType.'''

    def __init__(self, type_):
        super().__init__("Unsupported typing annotation '{}'".format(type_))


def _typing_to_cypher_type(type_):
    '''Convert typing annotation to a _mgp.CypherType instance.'''
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
        Path: _mgp.type_path()
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
            if isinstance(None, type_args):
                types = tuple(t for t in type_args if not isinstance(None, t))
                if len(types) == 1:
                    type_arg, = types
                else:
                    # We cannot do typing.Union[*types], so do the equivalent
                    # with __getitem__ which does not even need arg unpacking.
                    type_arg = typing.Union.__getitem__(types)
                return _mgp.type_nullable(_typing_to_cypher_type(type_arg))
        elif complex_type == list:
            type_arg, = type_args
            return _mgp.type_list(_typing_to_cypher_type(type_arg))
        raise UnsupportedTypingError(type_)
    else:
        # We cannot get to type args in any reliable way prior to 3.8, but we
        # still want to support typing.Optional and typing.List, so just parse
        # their string representations. Hopefully, that is always pretty
        # printed the same way. `typing.List[type]` is printed as such, while
        # `typing.Optional[type]` is printed as 'typing.Union[type, NoneType]'
        def parse_type_args(type_as_str):
            return tuple(map(str.strip,
                             type_as_str[type_as_str.index('[') + 1: -1].split(',')))

        def fully_qualified_name(cls):
            if cls.__module__ is None or cls.__module__ == 'builtins':
                return cls.__name__
            return cls.__module__ + '.' + cls.__name__

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
            if type_as_str.startswith('typing.Union'):
                type_args_as_str = parse_type_args(type_as_str)
                none_type_as_str = type(None).__name__
                if none_type_as_str in type_args_as_str:
                    types = tuple(t for t in type_args_as_str if t != none_type_as_str)
                    if len(types) == 1:
                        type_arg_as_str, = types
                    else:
                        type_arg_as_str = 'typing.Union[' + ', '.join(types) + ']'
                    simple_type = get_simple_type(type_arg_as_str)
                    if simple_type is not None:
                        return _mgp.type_nullable(simple_type)
                    return _mgp.type_nullable(parse_typing(type_arg_as_str))
            elif type_as_str.startswith('typing.List'):
                type_arg_as_str, = parse_type_args(type_as_str)
                simple_type = get_simple_type(type_arg_as_str)
                if simple_type is not None:
                    return _mgp.type_list(simple_type)
                return _mgp.type_list(parse_typing(type_arg_as_str))
            raise UnsupportedTypingError(type_)

        return parse_typing(str(type_))


# Procedure registration

class Deprecated:
    '''Annotate a resulting Record's field as deprecated.'''
    __slots__ = ('field_type',)

    def __init__(self, type_):
        self.field_type = type_


def read_proc(func: typing.Callable[..., Record]):
    '''
    Register `func` as a a read-only procedure of the current module.

    `read_proc` is meant to be used as a decorator function to register module
    procedures. The registered `func` needs to be a callable which optionally
    takes `ProcCtx` as the first argument. Other arguments of `func` will be
    bound to values passed in the cypherQuery. The full signature of `func`
    needs to be annotated with types. The return type must be
    `Record(field_name=type, ...)` and the procedure must produce either a
    complete Record or None. To mark a field as deprecated, use
    `Record(field_name=Deprecated(type), ...)`. Multiple records can be
    produced by returning an iterable of them. Registering generator functions
    is currently not supported.

    Example usage.

    ```
    import mgp

    @mgp.read_proc
    def procedure(context: mgp.ProcCtx,
                  required_arg: mgp.Nullable[mgp.Any],
                  optional_arg: mgp.Nullable[mgp.Any] = None
                  ) -> mgp.Record(result=str, args=list):
        args = [required_arg, optional_arg]
        # Multiple rows can be produced by returning an iterable of mgp.Record
        return mgp.Record(args=args, result='Hello World!')
    ```

    The example procedure above returns 2 fields: `args` and `result`.
      * `args` is a copy of arguments passed to the procedure.
      * `result` is the result of this procedure, a "Hello World!" string.
    Any errors can be reported by raising an Exception.

    The procedure can be invoked in openCypher using the following calls:
      CALL example.procedure(1, 2) YIELD args, result;
      CALL example.procedure(1) YIELD args, result;
    Naturally, you may pass in different arguments or yield less fields.
    '''
    if not callable(func):
        raise TypeError("Expected a callable object, got an instance of '{}'"
                        .format(type(func)))
    if inspect.iscoroutinefunction(func):
        raise TypeError("Callable must not be 'async def' function")
    if sys.version_info >= (3, 6):
        if inspect.isasyncgenfunction(func):
            raise TypeError("Callable must not be 'async def' function")
    if inspect.isgeneratorfunction(func):
        raise NotImplementedError("Generator functions are not supported")
    sig = inspect.signature(func)
    params = tuple(sig.parameters.values())
    if params and params[0].annotation is ProcCtx:
        @functools.wraps(func)
        def wrapper(graph, args):
            return func(ProcCtx(graph), *args)
        params = params[1:]
        mgp_proc = _mgp._MODULE.add_read_procedure(wrapper)
    else:
        @functools.wraps(func)
        def wrapper(graph, args):
            return func(*args)
        mgp_proc = _mgp._MODULE.add_read_procedure(wrapper)
    for param in params:
        name = param.name
        type_ = param.annotation
        if type_ is param.empty:
            type_ = object
        cypher_type = _typing_to_cypher_type(type_)
        if param.default is param.empty:
            mgp_proc.add_arg(name, cypher_type)
        else:
            mgp_proc.add_opt_arg(name, cypher_type, param.default)
    if sig.return_annotation is not sig.empty:
        record = sig.return_annotation
        if not isinstance(record, Record):
            raise TypeError("Expected '{}' to return 'mgp.Record', got '{}'"
                            .format(func.__name__, type(record)))
        for name, type_ in record.fields.items():
            if isinstance(type_, Deprecated):
                cypher_type = _typing_to_cypher_type(type_.field_type)
                mgp_proc.add_deprecated_result(name, cypher_type)
            else:
                mgp_proc.add_result(name, _typing_to_cypher_type(type_))
    return func
