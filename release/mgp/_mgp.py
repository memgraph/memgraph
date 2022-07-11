from typing import Any


class MgpIterable:
    def get() -> Any:
        pass

    def next() -> Any:
        pass


class Vertex:
    def is_valid() -> bool:  # type: ignore
        pass

    def underlying_graph_is_mutable() -> bool:  # type: ignore
        pass

    def iter_properties() -> MgpIterable:  # type: ignore
        pass

    def get_property(self, property_name: str) -> "Property":  # type: ignore
        pass

    def set_property(self, property_name: str, value: Any) -> "Property":  # type: ignore
        pass

    def get_id() -> "VertexId":  # type: ignore
        pass

    def label_at(self, index: int) -> "Label":  # type: ignore
        pass

    def labels_count() -> int:  # type: ignore
        pass

    def add_label(self, label: Any):
        pass

    def remove_label(self, label: Any):
        pass

    def iter_in_edges() -> MgpIterable:  # type: ignore
        pass

    def iter_out_edges() -> MgpIterable:  # type: ignore
        pass


class Edge:
    def is_valid() -> bool:  # type: ignore
        pass

    def underlying_graph_is_mutable() -> bool:  # type: ignore
        pass

    def iter_properties() -> MgpIterable:  # type: ignore
        pass

    def get_property(self, property_name: str) -> "Property":  # type: ignore
        pass

    def set_property(self, property_name: str, valuse: Any) -> "Property":  # type: ignore
        pass

    def get_type_name() -> str:  # type: ignore
        pass

    def get_id() -> "EdgeId":  # type: ignore
        pass

    def from_vertex() -> Vertex:  # type: ignore
        pass

    def to_vertex() -> Vertex:  # type: ignore
        pass


class Path:
    def is_valid() -> bool:  # type: ignore
        pass

    @staticmethod
    def make_with_start(vertex: Vertex) -> "Path":  # type: ignore
        pass


class Graph:
    pass


class CypherType:
    pass


def type_nullable():
    pass


class UnknownError(Exception):
    pass


class UnableToAllocateError(Exception):
    pass


class InsufficientBufferError(Exception):
    pass


class OutOfRangeError(Exception):
    pass


class LogicErrorError(Exception):

    pass


class DeletedObjectError(Exception):
    pass


class InvalidArgumentError(Exception):
    pass


class KeyAlreadyExistsError(Exception):
    pass


class ImmutableObjectError(Exception):
    pass


class ValueConversionError(Exception):
    pass


class SerializationError(Exception):
    pass


def type_any():
    pass


def type_list():
    pass


def type_bool():
    pass


def type_string():
    pass


def type_int():
    pass


def type_float():
    pass


def type_number():
    pass


def type_map():
    pass


def type_node():
    pass


def type_relationship():
    pass


def type_path():
    pass


class _MODULE:
    def add_read_procedure(wrapper):
        pass

    def add_write_procedure(wrapper):
        pass

    def add_transformation(wrapper):
        pass

    def add_function(wrapper):
        pass
