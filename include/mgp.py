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
import typing


class Label:
    '''Label of a Vertex.'''

    @property
    def name(self) -> str:
        pass


class Labels:
    '''A collection of labels on a Vertex.'''

    def __len__(self) -> int:
        '''Raise InvalidVertexError.'''
        pass

    def __getitem__(self, index: int) -> Label:
        '''Raise InvalidVertexError.'''
        pass

    def __iter__(self) -> typing.Iterable[Label]:
        '''Raise InvalidVertexError.'''
        pass

    def __contains__(self, label: typing.Union[Label, str]) -> bool:
        '''Test whether a label exists, either by name or another Label.

        Raise InvalidVertexError.
        '''
        pass


# Named property value of a Vertex or an Edge.
# It would be better to use typing.NamedTuple with typed fields, but that is
# not available in Python 3.5.
Property = namedtuple('Property', ('name', 'value'))


class Properties:
    '''A collection of properties either on a Vertex or an Edge.'''

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

    @property
    def name(self) -> str:
        pass


class InvalidEdgeError(Exception):
    '''Signals using an Edge instance not part of the procedure context.'''
    pass


class Edge:
    '''Edge in the graph database.

    Access to an Edge is only valid during a single execution of a procedure in
    a query. You should not globally store an instance of an Edge. Using an
    invalid Edge instance will raise InvalidEdgeError.
    '''

    @property
    def type(self) -> EdgeType:
        '''Raise InvalidEdgeError.'''
        pass

    @property
    def from_vertex(self):  # -> Vertex:
        '''Raise InvalidEdgeError.'''
        pass

    @property
    def to_vertex(self):  # -> Vertex:
        '''Raise InvalidEdgeError.'''
        pass

    @property
    def properties(self) -> Properties:
        '''Raise InvalidEdgeError.'''
        pass

    def __eq__(self, other) -> bool:
        '''Raise InvalidEdgeError.'''
        pass


VertexId = typing.NewType('VertexId', int)


class InvalidVertexError(Exception):
    '''Signals using a Vertex instance not part of the procedure context.'''
    pass


class Vertex:
    '''Vertex in the graph database.

    Access to a Vertex is only valid during a single execution of a procedure
    in a query. You should not globally store an instance of a Vertex. Using an
    invalid Vertex instance will raise InvalidVertexError.
    '''

    @property
    def id(self) -> VertexId:
        '''Raise InvalidVertexError.'''
        pass

    @property
    def labels(self) -> Labels:
        '''Raise InvalidVertexError.'''
        pass

    @property
    def properties(self) -> Properties:
        '''Raise InvalidVertexError.'''
        pass

    @property
    def in_edges(self) -> typing.Iterable[Edge]:
        '''Raise InvalidVertexError.'''
        pass

    @property
    def out_edges(self) -> typing.Iterable[Edge]:
        '''Raise InvalidVertexError.'''
        pass

    def __eq__(self, other) -> bool:
        '''Raise InvalidVertexError.'''
        pass


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

    def __init__(self, **kwargs):
        '''Initialize with name=value fields in kwargs.'''
        pass


class InvalidProcCtxError(Exception):
    '''Signals using an ProcCtx instance outside of the registered procedure.'''
    pass


class Vertices:
    '''Iterable over vertices in a graph.'''

    def __iter__(self) -> typing.Iterable[Vertex]:
        '''Raise InvalidProcCtxError if context is invalid.'''
        pass


class Graph:
    '''State of the graph database in current ProcCtx.'''

    def get_vertex_by_id(self, vertex_id: VertexId) -> Vertex:
        '''Return the Vertex corresponding to given vertex_id from the graph.

        Access to a Vertex is only valid during a single execution of a
        procedure in a query. You should not globally store the returned
        Vertex.

        Raise IndexError if unable to find the given vertex_id.
        Raise InvalidProcCtxError if context is invalid.
        '''
        pass

    @property
    def vertices(self) -> Vertices:
        '''All vertices in the graph.

        Access to a Vertex is only valid during a single execution of a
        procedure in a query. You should not globally store the returned Vertex
        instances.

        Raise InvalidProcCtxError if context is invalid.
        '''
        pass


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

List = typing.List

Map = typing.Union[dict, Edge, Vertex]

Any = typing.Union[bool, str, Number, Map, Path, list]

Nullable = typing.Optional


# Procedure registration

class Deprecated:
    '''Annotate a resulting Record's field as deprecated.'''
    def __init__(self, type_):
        pass


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
    pass
