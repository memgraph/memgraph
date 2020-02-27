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

    def __init__(self, obj):
        raise NotImplementedError()

    def get(self, property_name: str, default=None) -> object:
        '''Get the value of a property with the given name or return default.

        Raise InvalidEdgeError or InvalidVertexError.
        '''
        pass

    def items(self) -> typing.Iterable[Property]:
        '''Raise InvalidEdgeError or InvalidVertexError.'''
        pass

    def keys(self) -> typing.Iterable[str]:
        '''Iterate over property names.

        Raise InvalidEdgeError or InvalidVertexError.
        '''
        pass

    def values(self) -> typing.Iterable[object]:
        '''Iterate over property values.

        Raise InvalidEdgeError or InvalidVertexError.
        '''
        pass

    def __len__(self) -> int:
        '''Raise InvalidEdgeError or InvalidVertexError.'''
        pass

    def __iter__(self) -> typing.Iterable[str]:
        '''Iterate over property names.

        Raise InvalidEdgeError or InvalidVertexError.
        '''
        pass

    def __getitem__(self, property_name: str) -> object:
        '''Get the value of a property with the given name or raise KeyError.

        Raise InvalidEdgeError or InvalidVertexError.'''
        pass

    def __contains__(self, property_name: str) -> bool:
        pass


class EdgeType:
    '''Type of an Edge.'''
    __slots__ = ('_name',)

    def __init__(self, name):
        self._name = name

    @property
    def name(self) -> str:
        return self._name


class InvalidEdgeError(Exception):
    '''Signals using an Edge instance not part of the procedure context.'''
    pass


class Edge:
    '''Edge in the graph database.

    Access to an Edge is only valid during a single execution of a procedure in
    a query. You should not globally store an instance of an Edge. Using an
    invalid Edge instance will raise InvalidEdgeError.
    '''
    __slots__ = ('_edge',)

    def __init__(self, edge):
        if not isinstance(edge, _mgp.Edge):
            raise TypeError("Expected '_mgp.Edge', got '{}'".fmt(type(edge)))
        self._edge = edge

    def is_valid(self) -> bool:
        '''Return True if `self` is in valid context and may be used.'''
        return self._edge.is_valid()

    @property
    def type(self) -> EdgeType:
        '''Raise InvalidEdgeError.'''
        if not self.is_valid():
            raise InvalidEdgeError()
        return EdgeType(self._edge.get_type_name())

    @property
    def from_vertex(self):  # -> Vertex:
        '''Raise InvalidEdgeError.'''
        if not self.is_valid():
            raise InvalidEdgeError()
        return Vertex(self._edge.from_vertex())

    @property
    def to_vertex(self):  # -> Vertex:
        '''Raise InvalidEdgeError.'''
        if not self.is_valid():
            raise InvalidEdgeError()
        return Vertex(self._edge.to_vertex())

    @property
    def properties(self) -> Properties:
        '''Raise InvalidEdgeError.'''
        if not self.is_valid():
            raise InvalidEdgeError()
        return Properties(self._edge)

    def __eq__(self, other) -> bool:
        '''Raise InvalidEdgeError.'''
        if not self.is_valid():
            raise InvalidEdgeError()
        return self._edge == other._edge


if sys.version_info >= (3, 5, 2):
    VertexId = typing.NewType('VertexId', int)
else:
    VertexId = int


class InvalidVertexError(Exception):
    '''Signals using a Vertex instance not part of the procedure context.'''
    pass


class Vertex:
    '''Vertex in the graph database.

    Access to a Vertex is only valid during a single execution of a procedure
    in a query. You should not globally store an instance of a Vertex. Using an
    invalid Vertex instance will raise InvalidVertexError.
    '''
    __slots__ = ('_vertex',)

    def __init__(self, vertex):
        if not isinstance(vertex, _mgp.Vertex):
            raise TypeError("Expected '_mgp.Vertex', got '{}'".fmt(type(vertex)))
        self._vertex = vertex

    def is_valid(self) -> bool:
        '''Return True if `self` is in valid context and may be used'''
        return self._vertex.is_valid()

    @property
    def id(self) -> VertexId:
        '''Raise InvalidVertexError.'''
        if not self.is_valid():
            raise InvalidVertexError()
        return self._vertex.get_id()

    @property
    def labels(self) -> typing.List[Label]:
        '''Raise InvalidVertexError.'''
        if not self.is_valid():
            raise InvalidVertexError()
        return tuple(Label(self._vertex.label_at(i))
                     for i in range(self._vertex.labels_count()))

    @property
    def properties(self) -> Properties:
        '''Raise InvalidVertexError.'''
        if not self.is_valid():
            raise InvalidVertexError()
        return Properties(self._vertex)

    @property
    def in_edges(self) -> typing.Iterable[Edge]:
        '''Raise InvalidVertexError.'''
        if not self.is_valid():
            raise InvalidVertexError()
        edges_it = self._vertex.iter_in_edges()
        edge = edges_it.get()
        while edge is not None:
            yield Edge(edge)
            if not self.is_valid():
                raise InvalidVertexError()
            edge = edges_it.next()

    @property
    def out_edges(self) -> typing.Iterable[Edge]:
        '''Raise InvalidVertexError.'''
        if not self.is_valid():
            raise InvalidVertexError()
        edges_it = self._vertex.iter_out_edges()
        edge = edges_it.get()
        while edge is not None:
            yield Edge(edge)
            if not self.is_valid():
                raise InvalidVertexError()
            edge = edges_it.next()

    def __eq__(self, other) -> bool:
        '''Raise InvalidVertexError'''
        if not self.is_valid():
            raise InvalidVertexError()
        return self._vertex == other._vertex


class Path:
    '''Path containing Vertex and Edge instances.'''

    def __init__(self, starting_vertex: Vertex):
        '''Initialize with a starting Vertex.

        Raise InvalidVertexError if passed in Vertex is invalid.
        '''
        pass

    def expand(self, edge: Edge):
        '''Append an edge continuing from the last vertex on the path.

        The last vertex on the path will become the other endpoint of the given
        edge, as continued from the current last vertex.

        Raise ValueError if the current last vertex in the path is not part of
        the given edge.
        Raise InvalidEdgeError if passed in edge is invalid.
        '''
        pass

    @property
    def vertices(self) -> typing.Tuple[Vertex, ...]:
        '''Vertices ordered from the start to the end of the path.'''
        pass

    @property
    def edges(self) -> typing.Tuple[Edge, ...]:
        '''Edges ordered from the start to the end of the path.'''
        pass


class Record:
    '''Represents a record of resulting field values.'''
    __slots__ = ('fields',)

    def __init__(self, **kwargs):
        '''Initialize with name=value fields in kwargs.'''
        self.fields = kwargs


class InvalidProcCtxError(Exception):
    '''Signals using an ProcCtx instance outside of the registered procedure.'''
    pass


class Vertices:
    '''Iterable over vertices in a graph.'''
    __slots__ = ('_graph',)

    def __init__(self, graph):
        if not isinstance(graph, _mgp.Graph):
            raise TypeError("Expected '_mgp.Graph', got '{}'".fmt(type(graph)))
        self._graph = graph

    def is_valid(self) -> bool:
        '''Return True if `self` is in valid context and may be used.'''
        return self._graph.is_valid()

    def __iter__(self) -> typing.Iterable[Vertex]:
        '''Raise InvalidProcCtxError if context is invalid.'''
        if not self.is_valid():
            raise InvalidProcCtxError()
        vertices_it = self._graph.iter_vertices()
        vertex = vertices_it.get()
        while vertex is not None:
            yield Vertex(vertex)
            if not self.is_valid():
                raise InvalidProcCtxError()
            vertex = vertices_it.next()


class Graph:
    '''State of the graph database in current ProcCtx.'''
    __slots__ = ('_graph',)

    def __init__(self, graph):
        if not isinstance(graph, _mgp.Graph):
            raise TypeError("Expected '_mgp.Graph', got '{}'".format(type(graph)))
        self._graph = graph

    def is_valid(self) -> bool:
        '''Return True if `self` is in valid context and may be used.'''
        return self._graph.is_valid()

    def get_vertex_by_id(self, vertex_id: VertexId) -> Vertex:
        '''Return the Vertex corresponding to given vertex_id from the graph.

        Access to a Vertex is only valid during a single execution of a
        procedure in a query. You should not globally store the returned
        Vertex.

        Raise IndexError if unable to find the given vertex_id.
        Raise InvalidProcCtxError if context is invalid.
        '''
        if not self.is_valid():
            raise InvalidProcCtxError()
        vertex = self._graph.get_vertex_by_id(vertex_id)
        return Vertex(vertex)

    @property
    def vertices(self) -> Vertices:
        '''All vertices in the graph.

        Access to a Vertex is only valid during a single execution of a
        procedure in a query. You should not globally store the returned Vertex
        instances.

        Raise InvalidProcCtxError if context is invalid.
        '''
        if not self.is_valid():
            raise InvalidProcCtxError()
        return Vertices(self._graph)


class ProcCtx:
    '''Context of a procedure being executed.

    Access to a ProcCtx is only valid during a single execution of a procedure
    in a query. You should not globally store a ProcCtx instance.
    '''

    @property
    def graph(self) -> Graph:
        '''Raise InvalidProcCtxError if context is invalid.'''
        pass


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

        def get_simple_type(type_as_str):
            for simple_type, cypher_type in simple_types.items():
                if type_as_str == str(simple_type):
                    return cypher_type
                # Fallback to comparing to __name__ if it exits. This handles
                # the cases like when we have 'object' which is
                # `object.__name__`, but `str(object)` is "<class 'object'>"
                try:
                    if type_as_str == simple_type.__name__:
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
        params = params[1:]
        mgp_proc = _mgp._MODULE.add_read_procedure(func)
    else:
        @functools.wraps(func)
        def wrapper(*args):
            args_without_context = args[1:]
            return func(*args_without_context)
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
