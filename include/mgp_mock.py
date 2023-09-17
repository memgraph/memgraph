# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
This module provides a mock Python API for easy development of custom openCypher procedures.
The API's interface is fully consistent with the Python API in mgp.py. Because of that, you
can develop procedures without setting Memgraph up for each run, and run them from Memgraph
when they're implemented in full.
"""

import datetime
import inspect
import sys
import typing
from collections import namedtuple
from copy import deepcopy
from functools import wraps

import _mgp_mock


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
    """A vertex label."""

    __slots__ = ("_name",)

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        """
        Get the name of the label.

        Returns:
            A string with the name of the label.

        Example:
            ```label.name```
        """
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
    """Collection of the properties of a vertex or an edge."""

    __slots__ = ("_vertex_or_edge", "_len")

    def __init__(self, vertex_or_edge):
        if not isinstance(vertex_or_edge, (_mgp_mock.Vertex, _mgp_mock.Edge)):
            raise TypeError(f"Expected _mgp_mock.Vertex or _mgp_mock.Edge, got {type(vertex_or_edge)}")

        self._len = None
        self._vertex_or_edge = vertex_or_edge

    def __deepcopy__(self, memo):
        # In line with the Python API, this is the same as the shallow copy.
        return Properties(self._vertex_or_edge)

    def get(self, property_name: str, default=None) -> object:
        """
        Get the value of the property with the given name, otherwise return the default value.

        Args:
            property_name: String with the property name.
            default: The value to return if there is no `property_name` property.

        Returns:
            The value associated with `property_name` or, if there’s no such property, the `default` argument.

        Raises:
            InvalidContextError: If the edge or vertex is out of context.
            DeletedObjectError: If the edge has been deleted.

        Examples:
            ```
            vertex.properties.get(property_name)
            edge.properties.get(property_name)
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        try:
            return self[property_name]
        except KeyError:
            return default

    def set(self, property_name: str, value: object) -> None:
        """
        Set the value of the given property. If `value` is `None`, the property is removed.

        Args:
            property_name: String with the property name.
            value: The new value of the `property_name` property.

        Raises:
            ImmutableObjectError: If the object is immutable.
            DeletedObjectError: If the edge has been deleted.
            ValueConversionError: If `value` is vertex, edge or path.

        Examples:
            ```
            vertex.properties.set(property_name, value)
            edge.properties.set(property_name, value)
            ```
        """
        if not self._vertex_or_edge.underlying_graph_is_mutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        self[property_name] = value

    def items(self) -> typing.Iterable[Property]:
        """
        Iterate over the properties. Doesn’t return a dynamic view of the properties, but copies the
        current properties.

        Returns:
            Iterable `Property` of names and values.

        Raises:
            InvalidContextError: If the edge or vertex is out of context.
            DeletedObjectError: If the edge or vertex has been deleted.

        Examples:
            ```
            items = vertex.properties.items()
            for it in items:
                name = it.name
                value = it.value
            ```
            ```
            items = edge.properties.items()
            for it in items:
                name = it.name
                value = it.value
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        if self._vertex_or_edge.is_deleted():
            raise DeletedObjectError("Accessing deleted object.")

        vertex_or_edge_properties = self._vertex_or_edge.properties

        for property in vertex_or_edge_properties:
            yield Property(*property)

    def keys(self) -> typing.Iterable[str]:
        """
        Iterate over property names. Doesn’t return a dynamic view of the property names, but copies the
        name of the current properties.

        Returns:
            `Iterable` of strings that represent the property names (keys).

        Raises:
            InvalidContextError: If edge or vertex is out of context.
            DeletedObjectError: If the edge or vertex has been deleted.

        Examples:
            ```
            graph.vertex.properties.keys()
            graph.edge.properties.keys()
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        for item in self.items():
            yield item.name

    def values(self) -> typing.Iterable[object]:
        """
        Iterate over property values. Doesn’t return a dynamic view of the property values, but copies the
        values of the current properties.

        Returns:
            `Iterable` of property values.

        Raises:
            InvalidContextError: If edge or vertex is out of context.
            DeletedObjectError: If the edge or vertex has been deleted.

        Examples:
            ```
            vertex.properties.values()
            edge.properties.values()
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        for item in self.items():
            yield item.value

    def __len__(self) -> int:
        """
        Get the count of the vertex or edge’s properties.

        Returns:
            The count of the stored properties.

        Raises:
            InvalidContextError: If the edge or vertex is out of context.
            DeletedObjectError: If the edge or vertex has been deleted.

        Examples:
            ```
            len(vertex.properties)
            len(edge.properties)
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        if self._len is None:
            self._len = sum(1 for _ in self.items())

        return self._len

    def __iter__(self) -> typing.Iterable[str]:
        """
        Iterate over property names.

        Returns:
            `Iterable` of strings that represent property names.

        Raises:
            InvalidContextError: If edge or vertex is out of context.
            DeletedObjectError: If the edge or vertex has been deleted.

        Examples:
            ```
            iter(vertex.properties)
            iter(edge.properties)
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        for item in self.items():
            yield item.name

    def __getitem__(self, property_name: str) -> object:
        """
        Get the value of the property with the given name, otherwise raise a KeyError.

        Args:
            property_name: String with the property name.

        Returns:
            Value of the named property.

        Raises:
            InvalidContextError: If edge or vertex is out of context.
            DeletedObjectError: If the edge or vertex has been deleted.

        Examples:
            ```
            vertex.properties[property_name]
            edge.properties[property_name]
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        if self._vertex_or_edge.is_deleted():
            raise DeletedObjectError("Accessing deleted object.")

        return self._vertex_or_edge.get_property(property_name)

    def __setitem__(self, property_name: str, value: object) -> None:
        """
        Set the value of the given property. If `value` is `None`, the property
        is removed.

        Args:
            property_name: String with the property name.
            value: Object that represents the value to be set.

        Raises:
            InvalidContextError: If the edge or vertex is out of context.
            ImmutableObjectError: If the object is immutable.
            DeletedObjectError: If the edge or vertex has been deleted.
            ValueConversionError: If `value` is vertex, edge or path.

        Examples:
            ```
            vertex.properties[property_name] = value
            edge.properties[property_name] = value
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        if not self._vertex_or_edge.underlying_graph_is_mutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        if isinstance(value, (Vertex, Edge, Path)):
            raise ValueConversionError("Value conversion failed")

        if self._vertex_or_edge.is_deleted():
            raise DeletedObjectError("Accessing deleted object.")

        self._vertex_or_edge.set_property(property_name, value)

    def __contains__(self, property_name: str) -> bool:
        """
        Check if there is a property with the given name.

        Args:
            property_name: String with the property name.

        Returns:
            Boolean value that represents whether a property with the given name exists.

        Raises:
            InvalidContextError: If edge or vertex is out of context.
            DeletedObjectError: If the edge or vertex has been deleted.

        Examples:
            ```
            if property_name in vertex.properties:
            ```
            ```
            if property_name in edge.properties:
            ```
        """
        if not self._vertex_or_edge.is_valid():
            raise InvalidContextError()

        try:
            _ = self[property_name]
            return True
        except KeyError:
            return False


class EdgeType:
    """Type of an Edge."""

    __slots__ = ("_name",)

    def __init__(self, name):
        self._name = name

    @property
    def name(self) -> str:
        """
        Get the name of an EdgeType.

        Returns:
            The string with the name of the EdgeType.

        Example:
            ```edge.type.name```
        """
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
    """Represents a graph edge.

    Access to an Edge is only valid during a single execution of a procedure in
    a query. You should not globally store an instance of an Edge. Using an
    invalid Edge instance will raise an InvalidContextError.
    """

    __slots__ = ("_edge",)

    def __init__(self, edge):
        if not isinstance(edge, _mgp_mock.Edge):
            raise TypeError(f"Expected '_mgp_mock.Edge', got '{type(edge)}'")

        self._edge = edge

    def __deepcopy__(self, memo):
        # In line with the Python API, this is the same as the shallow copy.
        return Edge(self._edge)

    def is_valid(self) -> bool:
        """
        Check if the `Edge` is in a valid context, i.e. if it may be used.

        Returns:
            A `bool` value that represents whether the edge is in a valid context.

        Examples:
            ```edge.is_valid()```
        """
        return self._edge.is_valid()

    def underlying_graph_is_mutable(self) -> bool:
        """
        Check if the underlying `Graph` is mutable.

        Returns:
            A `bool` value that represents whether the graph is mutable.

        Raises:
            InvalidContextError: If the context is not valid.

        Examples:
            ```edge.underlying_graph_is_mutable()```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return self._edge.underlying_graph_is_mutable()

    @property
    def id(self) -> EdgeId:
        """
        Get the ID of the edge.

        Returns:
            An `EdgeId` representing the edge’s ID.

        Raises:
            InvalidContextError: If edge is out of context.

        Examples:
            ```edge.id```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return self._edge.id

    @property
    def type(self) -> EdgeType:
        """
        Get the type of the `Edge`.

        Returns:
            `EdgeType` representing the edge’s type.

        Raises:
            InvalidContextError: If edge is out of context.

        Examples:
            ```edge.type```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return EdgeType(self._edge.get_type_name())

    @property
    def from_vertex(self) -> "Vertex":
        """
        Get the source (tail) vertex of the edge.

        Returns:
            `Vertex` that is the edge’s source/tail.

        Raises:
            InvalidContextError: If edge is out of context.

        Examples:
            ```edge.from_vertex```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return Vertex(self._edge.from_vertex())

    @property
    def to_vertex(self) -> "Vertex":
        """
        Get the destination (head) vertex of the edge.

        Returns:
            `Vertex` that is the edge’s destination/head.

        Raises:
            InvalidContextError: If edge is out of context.

        Examples:
            ```edge.to_vertex```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return Vertex(self._edge.to_vertex())

    @property
    def properties(self) -> Properties:
        """
        Get the edge’s properties.

        Returns:
            `Properties` containing all properties of the edge.

        Raises:
            InvalidContextError: If edge is out of context.

        Examples:
            ```edge.properties```
        """
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
    """Represents a graph vertex.

    Access to a Vertex is only valid during a single execution of a procedure
    in a query. You should not globally store an instance of a Vertex. Using an
    invalid Vertex instance will raise an InvalidContextError.
    """

    __slots__ = ("_vertex",)

    def __init__(self, vertex):
        if not isinstance(vertex, _mgp_mock.Vertex):
            raise TypeError(f"Expected '_mgp_mock.Vertex', got '{type(vertex)}'")

        self._vertex = vertex

    def __deepcopy__(self, memo):
        # In line with the Python API, this is the same as the shallow copy.
        return Vertex(self._vertex)

    def is_valid(self) -> bool:
        """
        Check if the vertex is in a valid context, i.e. if it may be used.

        Returns:
            A `bool` value that represents whether the vertex is in a valid context.

        Examples:
            ```vertex.is_valid()```
        """
        return self._vertex.is_valid()

    def underlying_graph_is_mutable(self) -> bool:
        """
        Check if the underlying graph is mutable.

        Returns:
            A `bool` value that represents whether the graph is mutable.

        Raises:
            InvalidContextError: If the context is not valid.

        Examples:
            ```edge.underlying_graph_is_mutable()```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return self._vertex.underlying_graph_is_mutable()

    @property
    def id(self) -> VertexId:
        """
        Get the ID of the vertex.

        Returns:
            A `VertexId` representing the vertex’s ID.

        Raises:
            InvalidContextError: If vertex is out of context.

        Examples:
            ```vertex.id```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return self._vertex.id

    @property
    def labels(self) -> typing.Tuple[Label]:
        """
        Get the labels of the vertex.

        Returns:
            A tuple of `Label` instances representing individual labels.

        Raises:
            InvalidContextError: If vertex is out of context.
            DeletedObjectError: If `Vertex` has been deleted.

        Examples:
            ```vertex.labels```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._vertex.is_deleted():
            raise DeletedObjectError("Accessing deleted object.")

        return tuple(Label(label) for label in self._vertex.labels)

    def add_label(self, label: str) -> None:
        """
        Add the given label to the vertex.

        Args:
            label: The label (`str`) to be added.

        Raises:
            InvalidContextError: If `Vertex` is out of context.
            ImmutableObjectError: If `Vertex` is immutable.
            DeletedObjectError: If `Vertex` has been deleted.

        Examples:
            ```vertex.add_label(label)```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._vertex.is_deleted():
            raise DeletedObjectError("Accessing deleted object.")

        return self._vertex.add_label(label)

    def remove_label(self, label: str) -> None:
        """
        Remove the given label from the vertex.

        Args:
            label: The label (`str`) to be removed.

        Raises:
            InvalidContextError: If `Vertex` is out of context.
            ImmutableObjectError: If `Vertex` is immutable.
            DeletedObjectError: If `Vertex` has been deleted.

        Examples:
            ```vertex.remove_label(label)```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._vertex.is_deleted():
            raise DeletedObjectError("Accessing deleted object.")

        return self._vertex.remove_label(label)

    @property
    def properties(self) -> Properties:
        """
        Get the properties of the vertex.

        Returns:
            The `Properties` of the vertex.

        Raises:
            InvalidContextError: If `Vertex` is out of context.

        Examples:
            ```vertex.properties```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return Properties(self._vertex)

    @property
    def in_edges(self) -> typing.Iterable[Edge]:
        """
        Iterate over the inbound edges of the vertex.
        Doesn’t return a dynamic view of the edges, but copies the current inbound edges.

        Returns:
            An `Iterable` of all `Edge` objects directed towards the vertex.

        Raises:
            InvalidContextError: If `Vertex` is out of context.
            DeletedObjectError: If `Vertex` has been deleted.

        Examples:
            ```for edge in vertex.in_edges:```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._vertex.is_deleted():
            raise DeletedObjectError("Accessing deleted object.")

        for edge in self._vertex.in_edges:
            yield Edge(edge)

    @property
    def out_edges(self) -> typing.Iterable[Edge]:
        """
        Iterate over the outbound edges of the vertex.
        Doesn’t return a dynamic view of the edges, but copies the current outbound edges.

        Returns:
            An `Iterable` of all `Edge` objects directed outwards from the vertex.

        Raises:
            InvalidContextError: If `Vertex` is out of context.
            DeletedObjectError: If `Vertex` has been deleted.

        Examples:
            ```for edge in vertex.in_edges:```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._vertex.is_deleted():
            raise DeletedObjectError("Accessing deleted object.")

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
    """Represents a path comprised of `Vertex` and `Edge` instances."""

    __slots__ = ("_path", "_vertices", "_edges")

    def __init__(self, starting_vertex_or_path: typing.Union[_mgp_mock.Path, Vertex]):
        """Initialize with a starting `Vertex`.

        Raises:
            InvalidContextError: If the given vertex is invalid.
        """
        # Accepting _mgp.Path is just for internal usage.
        if isinstance(starting_vertex_or_path, _mgp_mock.Path):
            self._path = starting_vertex_or_path

        elif isinstance(starting_vertex_or_path, Vertex):
            # For consistency with the Python API, the `_vertex` attribute isn’t a “public” property.
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
        # In line with the Python API, this is the same as the shallow copy.
        try:
            return Path(memo[id(self._path)])
        except KeyError:
            pass
        path = self.__copy__()
        memo[id(self._path)] = path._path
        return path

    def is_valid(self) -> bool:
        """
        Check if the path is in a valid context, i.e. if it may be used.

        Returns:
            A `bool` value that represents whether the path is in a valid context.

        Examples:
            ```path.is_valid()```
        """
        return self._path.is_valid()

    def expand(self, edge: Edge):
        """
        Append an edge continuing from the last vertex on the path.
        The destination (head) of the given edge will become the last vertex in the path.

        Args:
            edge: The `Edge` to be added to the path.

        Raises:
            InvalidContextError: If using an invalid `Path` instance or if the given `Edge` is invalid.
            LogicErrorError: If the current last vertex in the path is not part of the given edge.

        Examples:
            ```path.expand(edge)```
        """
        if not isinstance(edge, Edge):
            raise TypeError(f"Expected 'Edge', got '{type(edge)}'")

        if not self.is_valid() or not edge.is_valid():
            raise InvalidContextError()

        # For consistency with the Python API, the `_edge` attribute isn’t a “public” property.
        self._path.expand(edge._edge)

        self._vertices = None
        self._edges = None

    def pop(self):
        """
        Remove the last node and the last relationship from the path.

        Raises:
            InvalidContextError: If using an invalid `Path` instance
            OutOfRangeError: If the path contains no relationships.

        Examples:
            ```path.pop()```
        """
        if not self.is_valid():
            raise InvalidContextError()
        self._path.pop()

        # Invalidate cached tuples
        self._vertices = None
        self._edges = None

    @property
    def vertices(self) -> typing.Tuple[Vertex, ...]:
        """
        Get the path’s vertices in a fixed order.

        Returns:
            A `Tuple` of the path’s  vertices (`Vertex`) ordered from the start to the end of the path.

        Raises:
            InvalidContextError: If using an invalid Path instance.

        Examples:
            ```path.vertices```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._vertices is None:
            num_vertices = self._path.size() + 1
            self._vertices = tuple(Vertex(self._path.vertex_at(i)) for i in range(num_vertices))

        return self._vertices

    @property
    def edges(self) -> typing.Tuple[Edge, ...]:
        """
        Get the path’s edges in a fixed order.

        Returns:
            A `Tuple` of the path’s edges (`Edges`) ordered from the start to the end of the path.

        Raises:
            InvalidContextError: If using an invalid Path instance.

        Examples:
            ```path.vertices```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._edges is None:
            num_edges = self._path.size()
            self._edges = tuple(Edge(self._path.edge_at(i)) for i in range(num_edges))

        return self._edges


class Vertices:
    """An iterable structure of the vertices in a graph."""

    __slots__ = ("_graph", "_len")

    def __init__(self, graph):
        if not isinstance(graph, _mgp_mock.Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        self._graph = graph
        self._len = None

    def __deepcopy__(self, memo):
        # In line with the Python API, this is the same as the shallow copy.
        return Vertices(self._graph)

    def is_valid(self) -> bool:
        """
        Check if the `Vertices` object is in a valid context, i.e. if it may be used.

        Returns:
            A `bool` value that represents whether the object is in a valid context.

        Examples:
            ```vertices.is_valid()```
        """
        return self._graph.is_valid()

    def __iter__(self) -> typing.Iterable[Vertex]:
        """
        Iterate over a graph’s vertices.

        Returns:
            An `Iterable` of `Vertex` objects.

        Raises:
            InvalidContextError: If context is invalid.

        Examples:
            ```
            for vertex in graph.vertices:
            ```
            ```
            iter(graph.vertices)
            ```
        """
        if not self.is_valid():
            raise InvalidContextError()

        for vertex in self._graph.vertices:
            yield Vertex(vertex)

    def __contains__(self, vertex: Vertex):
        """
        Check if the given vertex is one of the graph vertices.

        Args:
            vertex: The `Vertex` to be checked.

        Returns:
            A Boolean value that represents whether the given vertex is one of the graph vertices.

        Raises:
            InvalidContextError: If the `Vertices` instance or the givern vertex is not in a valid context.

        Examples:
            ```if vertex in graph.vertices:```
        """
        if not self.is_valid() or not vertex.is_valid():
            raise InvalidContextError()

        return self._graph.has_node(vertex.id)

    def __len__(self):
        """
        Get the count of the graph vertices.

        Returns:
            The count of the vertices in the graph.

        Raises:
            InvalidContextError: If the `Vertices` instance is not in a valid context.

        Examples:
            ```len(graph.vertices)```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if not self._len:
            self._len = sum(1 for _ in self)

        return self._len


class Record:
    """Represents a record as returned by a query procedure.
    Records are comprised of key (field name) - value pairs."""

    __slots__ = ("fields",)

    def __init__(self, **kwargs):
        """Initialize with {name}={value} fields in kwargs."""
        self.fields = kwargs

    def __str__(self):
        return str(self.fields)


class Graph:
    """The graph that stands in for Memgraph’s graph."""

    __slots__ = ("_graph",)

    def __init__(self, graph):
        if not isinstance(graph, _mgp_mock.Graph):
            raise TypeError(f"Expected '_mgp_mock.Graph', got '{type(graph)}'")

        self._graph = graph

    def __deepcopy__(self, memo):
        # In line with the Python API, this is the same as the shallow copy.
        return Graph(self._graph)

    def is_valid(self) -> bool:
        """
        Check if the graph is in a valid context, i.e. if it may be used.

        Returns:
            A `bool` value that represents whether the graph is in a valid context.

        Examples:
            ```graph.is_valid()```
        """
        return self._graph.is_valid()

    def get_vertex_by_id(self, vertex_id: VertexId) -> Vertex:
        """
        Return the graph vertex with the given vertex_id.
        Access to a `Vertex` is only valid during a single execution of a
        procedure in a query. You should not globally store the returned
        vertex.

        Args:
            vertex_id: A Memgraph vertex ID (`Vertex ID`)

        Returns:
            The `Vertex` with the given ID.

        Raises:
            IndexError: If unable to find the given vertex_id.
            InvalidContextError: If context is invalid.

        Examples:
            ```graph.get_vertex_by_id(vertex_id)```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return Vertex(self._graph.get_vertex_by_id(vertex_id))

    @property
    def vertices(self) -> Vertices:
        """
        Get all graph vertices.

        Access to a `Vertex` is only valid during a single execution of a
        query procedure. You should not globally store the returned `Vertex`
        instances.

        Returns:
            `Vertices` in the graph.

        Raises:
            InvalidContextError: If context is invalid.

        Examples:
            Iteration over all graph vertices.

            ```
            graph = context.graph
            for vertex in graph.vertices:
            ```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return Vertices(self._graph)

    def is_mutable(self) -> bool:
        """
        Check if the graph is mutable, i.e. if it can be modified.

        Returns:
            A `bool` value that represents whether the graph is mutable.

        Raises:
            InvalidContextError: If the graph is not in a valid context.

        Examples:
            ```graph.is_mutable()```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return not self._graph.is_immutable()

    def create_vertex(self) -> Vertex:
        """
        Create an empty vertex.

        Returns:
            The created `Vertex`.

        Raises:
            InvalidContextError: If the graph is not in a valid context.
            ImmutableObjectError: If the graph is immutable.

        Examples:
            Creating an empty vertex:
            ```vertex = graph.create_vertex()```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        return Vertex(self._graph.create_vertex())

    def delete_vertex(self, vertex: Vertex) -> None:
        """
        Delete the given vertex if it’s isolated, i.e. if there are no edges connected to it.

        Args:
            vertex: The `Vertex` to be deleted.

        Raises:
            InvalidContextError: If the graph is not in a valid context.
            ImmutableObjectError: If the graph is immutable.
            LogicErrorError: If the vertex is not isolated.

        Examples:
            ```graph.delete_vertex(vertex)```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        if not self._graph.vertex_is_isolate(vertex.id):
            raise LogicErrorError("Logic error.")

        self._graph.delete_vertex(vertex.id)

    def detach_delete_vertex(self, vertex: Vertex) -> None:
        """
        Delete the given vertex together with all connected edges.

        Args:
            vertex: The `Vertex` to be deleted.

        Raises:
            InvalidContextError: If the graph is not in a valid context.
            ImmutableObjectError: If the graph is immutable.

        Examples:
            ```graph.detach_delete_vertex(vertex)```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        self._graph.delete_vertex(vertex.id)

    def create_edge(self, from_vertex: Vertex, to_vertex: Vertex, edge_type: EdgeType) -> Edge:
        """
        Create an empty edge.

        Args:
            from_vertex: The source (tail) `Vertex`.
            to_vertex: The destination (head) `Vertex`.
            edge_type: `EdgeType` specifying the new edge’s type.

        Returns:
            The created `Edge`.

        Raises:
            InvalidContextError: If the graph is not in a valid context.
            ImmutableObjectError: If the graph is immutable.
            DeletedObjectError: If `from_vertex` or `to_vertex` have been deleted.

        Examples:
            ```edge = graph.create_edge(from_vertex, vertex, edge_type)```
        """

        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        new_edge = self._graph.create_edge(from_vertex._vertex, to_vertex._vertex, edge_type.name)
        return Edge(new_edge)

    def delete_edge(self, edge: Edge) -> None:
        """
        Delete the given edge.

        Args:
            edge: The `Edge` to be deleted.

        Raises:
            InvalidContextError: If the graph is not in a valid context.
            ImmutableObjectError: If the graph is immutable.

        Examples:
            ```graph.delete_edge(edge)```
        """
        if not self.is_valid():
            raise InvalidContextError()

        if self._graph.is_immutable():
            raise ImmutableObjectError("Cannot modify immutable object.")

        self._graph.delete_edge(edge.from_vertex.id, edge.to_vertex.id, edge.id)


class AbortError(Exception):
    """Signals that the procedure was asked to abort its execution."""

    pass


class ProcCtx:
    """The context of the procedure being executed.

    Access to a `ProcCtx` is only valid during a single execution of a query procedure.
    You should not globally store a `ProcCtx` instance.
    """

    __slots__ = ("_graph",)

    def __init__(self, graph):
        if not isinstance(graph, (_mgp_mock.Graph, _mgp_mock.nx.MultiDiGraph)):
            raise TypeError(f"Expected '_mgp_mock.Graph' or 'networkx.MultiDiGraph', got '{type(graph)}'")

        self._graph = Graph(graph) if isinstance(graph, _mgp_mock.Graph) else Graph(_mgp_mock.Graph(graph))

    def is_valid(self) -> bool:
        """
        Check if the context is valid, i.e. if the contained structures may be used.

        Returns:
            A `bool` value that represents whether the context is valid.

        Examples:
            ```context.is_valid()```
        """
        return self._graph.is_valid()

    @property
    def graph(self) -> Graph:
        """
        Access the graph.

        Returns:
            A `Graph` object representing the graph.

        Raises:
            InvalidContextError: If the procedure context is not valid.

        Examples:
            ```context.graph```
        """
        if not self.is_valid():
            raise InvalidContextError()

        return self._graph


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
            result_record = None

            if is_write:
                ctx_copy = ProcCtx(deepcopy(ctx._graph._graph.nx))

                result_record = func(ctx_copy, *args)

                ctx._graph._graph = deepcopy(ctx_copy._graph._graph)

                # Invalidate context after execution
                ctx_copy._graph._graph.invalidate()
            else:
                ctx._graph._graph.make_immutable()

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
    """
    Register a function as a Memgraph read-only procedure.

    The `func` needs to be a callable and optionally take `ProcCtx` as its first argument.
    Other parameters of `func` will be bound to the passed arguments.
    The full signature of `func` needs to be annotated with types. The return type must
    be `Record(field_name=type, ...)`, and the procedure must produce either a complete
    Record or None. Multiple records can be produced by returning an iterable of them.
    Registering generator functions is currently not supported.

    Example:
    ```
    import mgp_mock

    @mgp_mock.read_proc
    def procedure(context: mgp_mock.ProcCtx,
                  required_arg: mgp_mock.Nullable[mgp_mock.Any],
                  optional_arg: mgp_mock.Nullable[mgp_mock.Any] = None
                  ) -> mgp_mock.Record(result=str, args=list):
        args = [required_arg, optional_arg]
        # Multiple rows can be produced by returning an iterable of mgp_mock.Record:
        return mgp_mock.Record(args=args, result="Hello World!")
    ```

    The above example procedure returns two fields: `args` and `result`.
      * `args` is a copy of the arguments passed to the procedure.
      * `result` is "Hello World!".

    Any errors can be reported by raising an Exception.
    """
    return _register_proc(func, False)


def write_proc(func: typing.Callable[..., Record]):
    """
    Register a function as a Memgraph write procedure.

    The `func` needs to be a callable and optionally take `ProcCtx` as its first argument.
    Other parameters of `func` will be bound to the passed arguments.
    The full signature of `func` needs to be annotated with types. The return type must
    be `Record(field_name=type, ...)`, and the procedure must produce either a complete
    Record or None. Multiple records can be produced by returning an iterable of them.
    Registering generator functions is currently not supported.

    Example:
    ```
    import mgp_mock

    @mgp_mock.write_proc
    def procedure(context: mgp_mock.ProcCtx,
                  required_arg: str,
                  optional_arg: mgp_mock.Nullable[str] = None
                  ) -> mgp_mock.Record(result=mgp_mock.Vertex):
        vertex = context.graph.create_vertex()
        vertex_properties = vertex.properties
        vertex_properties["required_arg"] = required_arg
        if optional_arg is not None:
            vertex_properties["optional_arg"] = optional_arg

        return mgp.Record(result=vertex)
    ```

    The above example procedure returns a newly created vertex that has
    up to 2 properties:
      * `required_arg` is always present and its value is the first
        argument of the procedure.
      * `optional_arg` is present if the second argument of the procedure
        is not `null`.

    Any errors can be reported by raising an Exception.
    """
    return _register_proc(func, True)


class FuncCtx:
    """The context of the function being executed.

    Access to a `FuncCtx` is only valid during a single execution of a transformation.
    You should not globally store a `FuncCtx` instance.
    The graph object within `FuncCtx` is not mutable.
    """

    __slots__ = ("_graph",)

    def __init__(self, graph):
        if not isinstance(graph, (_mgp_mock.Graph, _mgp_mock.nx.MultiDiGraph)):
            raise TypeError(f"Expected '_mgp_mock.Graph' or 'networkx.MultiDiGraph', got '{type(graph)}'")

        self._graph = Graph(graph) if isinstance(graph, _mgp_mock.Graph) else Graph(_mgp_mock.Graph(graph))

    def is_valid(self) -> bool:
        """
        Check if the context is valid, i.e. if the contained structures may be used.

        Returns:
            A `bool` value that represents whether the context is valid.

        Examples:
            ```context.is_valid()```
        """
        return self._graph.is_valid()


def function(func: typing.Callable):
    """
    Register a function as a Memgraph function.

    The `func` needs to be a callable and optionally take `ProcCtx` as its first argument.
    Other parameters of `func` will be bound to the passed arguments.
    Only the function arguments need to be annotated with types. The return type doesn’t
    need to be specified, but it has to be within `mgp_mock.Any`.
    Registering generator functions is currently not supported.

    Example:
    ```
    import mgp_mock

    @mgp_mock.function
    def procedure(context: mgp_mock.FuncCtx,
                  required_arg: str,
                  optional_arg: mgp_mock.Nullable[str] = None
                  ):
        return_args = [required_arg]
        if optional_arg is not None:
            return_args.append(optional_arg)
        # Return any result whose type is within mgp_mock.Any
        return return_args
    ```

    The above example function returns a list of the passed parameters:
      * `required_arg` is always present and its value is the first
        argument of the procedure.
      * `optional_arg` is present if the second argument of the procedure
        is not `null`.

    Any errors can be reported by raising an Exception.
    """
    raise_if_does_not_meet_requirements(func)

    sig = inspect.signature(func)

    params = tuple(sig.parameters.values())
    if params and params[0].annotation is FuncCtx:

        @wraps(func)
        def wrapper(ctx, *args):
            ctx._graph._graph.make_immutable()

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
