# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the
# Business Source License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

"""
Tests for the PyG (PyTorch Geometric) converter module.
"""

import pytest
from mage.pyg.converter import (
    MemgraphPyGConverter,
    _is_numeric_list,
    _property_to_numeric,
    graph_to_pyg_dict,
    pyg_dict_to_graph_data,
)


# Mock classes to simulate Memgraph objects
class MockLabel:
    def __init__(self, name: str):
        self.name = name


class MockEdgeType:
    def __init__(self, name: str):
        self.name = name


class MockProperties:
    def __init__(self, props: dict):
        self._props = props

    def get(self, key: str, default=None):
        return self._props.get(key, default)

    def items(self):
        return self._props.items()


class MockVertex:
    def __init__(self, id: int, labels: list, properties: dict):
        self.id = id
        self.labels = [MockLabel(label) for label in labels]
        self.properties = MockProperties(properties)


class MockEdge:
    def __init__(
        self,
        id: int,
        from_vertex: MockVertex,
        to_vertex: MockVertex,
        edge_type: str,
        properties: dict,
    ):
        self.id = id
        self.from_vertex = from_vertex
        self.to_vertex = to_vertex
        self.type = MockEdgeType(edge_type)
        self.properties = MockProperties(properties)


class TestPropertyConversion:
    """Tests for property conversion utilities."""

    def test_property_to_numeric_int(self):
        assert _property_to_numeric(42) == 42.0

    def test_property_to_numeric_float(self):
        assert _property_to_numeric(3.14) == 3.14

    def test_property_to_numeric_bool_true(self):
        assert _property_to_numeric(True) == 1.0

    def test_property_to_numeric_bool_false(self):
        assert _property_to_numeric(False) == 0.0

    def test_property_to_numeric_string_numeric(self):
        assert _property_to_numeric("3.14") == 3.14

    def test_property_to_numeric_string_non_numeric(self):
        assert _property_to_numeric("hello") is None

    def test_property_to_numeric_none(self):
        assert _property_to_numeric(None) is None

    def test_property_to_numeric_list(self):
        # Lists should return None (handled separately)
        assert _property_to_numeric([1, 2, 3]) is None


class TestIsNumericList:
    """Tests for numeric list detection."""

    def test_is_numeric_list_int_list(self):
        assert _is_numeric_list([1, 2, 3]) is True

    def test_is_numeric_list_float_list(self):
        assert _is_numeric_list([1.0, 2.5, 3.14]) is True

    def test_is_numeric_list_mixed(self):
        assert _is_numeric_list([1, 2.5, 3]) is True

    def test_is_numeric_list_tuple(self):
        assert _is_numeric_list((1, 2, 3)) is True

    def test_is_numeric_list_with_string(self):
        assert _is_numeric_list([1, "two", 3]) is False

    def test_is_numeric_list_empty(self):
        assert _is_numeric_list([]) is True

    def test_is_numeric_list_not_list(self):
        assert _is_numeric_list("not a list") is False


class TestMemgraphPyGConverterExport:
    """Tests for exporting Memgraph data to PyG format."""

    @pytest.fixture
    def simple_graph(self):
        """Create a simple graph with 3 nodes and 2 edges."""
        v0 = MockVertex(
            0,
            ["Person"],
            {"name": "Alice", "age": 30, "features": [1.0, 2.0]},
        )
        v1 = MockVertex(
            1,
            ["Person"],
            {"name": "Bob", "age": 25, "features": [3.0, 4.0]},
        )
        v2 = MockVertex(
            2,
            ["Person", "Admin"],
            {"name": "Charlie", "age": 35, "features": [5.0, 6.0]},
        )

        e0 = MockEdge(0, v0, v1, "KNOWS", {"weight": 0.5, "since": 2020})
        e1 = MockEdge(1, v1, v2, "KNOWS", {"weight": 0.8, "since": 2021})

        return [v0, v1, v2], [e0, e1]

    def test_basic_export(self, simple_graph):
        vertices, edges = simple_graph
        converter = MemgraphPyGConverter()

        result = converter.export_to_pyg_dict(
            vertices=vertices,
            edges=edges,
        )

        assert result["num_nodes"] == 3
        assert result["edge_index"] == [[0, 1], [1, 2]]
        assert "node_id_mapping" in result
        assert result["node_id_mapping"] == {0: 0, 1: 1, 2: 2}

    def test_export_with_node_features(self, simple_graph):
        vertices, edges = simple_graph
        converter = MemgraphPyGConverter()

        result = converter.export_to_pyg_dict(
            vertices=vertices,
            edges=edges,
            node_property_names=["age"],
        )

        assert "x" in result
        assert result["x"] == [[30.0], [25.0], [35.0]]

    def test_export_with_list_node_features(self, simple_graph):
        vertices, edges = simple_graph
        converter = MemgraphPyGConverter()

        result = converter.export_to_pyg_dict(
            vertices=vertices,
            edges=edges,
            node_property_names=["features"],
        )

        assert "x" in result
        assert result["x"] == [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]

    def test_export_with_edge_features(self, simple_graph):
        vertices, edges = simple_graph
        converter = MemgraphPyGConverter()

        result = converter.export_to_pyg_dict(
            vertices=vertices,
            edges=edges,
            edge_property_names=["weight"],
        )

        assert "edge_attr" in result
        assert result["edge_attr"] == [[0.5], [0.8]]

    def test_export_with_labels(self, simple_graph):
        vertices, edges = simple_graph
        converter = MemgraphPyGConverter()

        result = converter.export_to_pyg_dict(
            vertices=vertices,
            edges=edges,
            include_labels=True,
        )

        assert "labels" in result
        assert result["labels"] == [["Person"], ["Person"], ["Person", "Admin"]]

    def test_export_with_edge_types(self, simple_graph):
        vertices, edges = simple_graph
        converter = MemgraphPyGConverter()

        result = converter.export_to_pyg_dict(
            vertices=vertices,
            edges=edges,
        )

        assert "edge_types" in result
        assert result["edge_types"] == ["KNOWS", "KNOWS"]

    def test_export_with_node_label_property(self, simple_graph):
        vertices, edges = simple_graph
        converter = MemgraphPyGConverter()

        result = converter.export_to_pyg_dict(
            vertices=vertices,
            edges=edges,
            node_label_property="age",
        )

        assert "y" in result
        assert result["y"] == [30, 25, 35]


class TestMemgraphPyGConverterImport:
    """Tests for preparing PyG data for import into Memgraph."""

    def test_basic_import_preparation(self):
        pyg_dict = {
            "edge_index": [[0, 1], [1, 2]],
            "num_nodes": 3,
        }

        nodes_data, edges_data = MemgraphPyGConverter.prepare_import_data(
            pyg_dict=pyg_dict,
            default_node_label="Node",
            default_edge_type="EDGE",
        )

        assert len(nodes_data) == 3
        assert len(edges_data) == 2

        # Check node indices
        assert nodes_data[0]["idx"] == 0
        assert nodes_data[1]["idx"] == 1
        assert nodes_data[2]["idx"] == 2

        # Check edge source/target
        assert edges_data[0]["source_idx"] == 0
        assert edges_data[0]["target_idx"] == 1
        assert edges_data[1]["source_idx"] == 1
        assert edges_data[1]["target_idx"] == 2

    def test_import_with_node_features(self):
        pyg_dict = {
            "edge_index": [[0], [1]],
            "num_nodes": 2,
            "x": [[1.0, 2.0], [3.0, 4.0]],
        }

        nodes_data, _ = MemgraphPyGConverter.prepare_import_data(
            pyg_dict=pyg_dict,
            node_property_names=["feat1", "feat2"],
        )

        assert nodes_data[0]["properties"]["feat1"] == 1.0
        assert nodes_data[0]["properties"]["feat2"] == 2.0
        assert nodes_data[1]["properties"]["feat1"] == 3.0
        assert nodes_data[1]["properties"]["feat2"] == 4.0

    def test_import_with_edge_features(self):
        pyg_dict = {
            "edge_index": [[0], [1]],
            "num_nodes": 2,
            "edge_attr": [[0.5, 1.0]],
        }

        _, edges_data = MemgraphPyGConverter.prepare_import_data(
            pyg_dict=pyg_dict,
            edge_property_names=["weight", "strength"],
        )

        assert edges_data[0]["properties"]["weight"] == 0.5
        assert edges_data[0]["properties"]["strength"] == 1.0

    def test_import_with_labels(self):
        pyg_dict = {
            "edge_index": [[], []],
            "num_nodes": 2,
            "labels": [["Person", "User"], ["Person"]],
        }

        nodes_data, _ = MemgraphPyGConverter.prepare_import_data(pyg_dict=pyg_dict)

        assert nodes_data[0]["labels"] == ["Person", "User"]
        assert nodes_data[1]["labels"] == ["Person"]

    def test_import_with_edge_types(self):
        pyg_dict = {
            "edge_index": [[0, 1], [1, 0]],
            "num_nodes": 2,
            "edge_types": ["KNOWS", "FOLLOWS"],
        }

        _, edges_data = MemgraphPyGConverter.prepare_import_data(pyg_dict=pyg_dict)

        assert edges_data[0]["type"] == "KNOWS"
        assert edges_data[1]["type"] == "FOLLOWS"

    def test_import_with_y_labels(self):
        pyg_dict = {
            "edge_index": [[], []],
            "num_nodes": 3,
            "y": [0, 1, 1],
        }

        nodes_data, _ = MemgraphPyGConverter.prepare_import_data(pyg_dict=pyg_dict)

        assert nodes_data[0]["properties"]["y"] == 0
        assert nodes_data[1]["properties"]["y"] == 1
        assert nodes_data[2]["properties"]["y"] == 1


class TestConvenienceFunctions:
    """Tests for convenience wrapper functions."""

    @pytest.fixture
    def simple_graph(self):
        v0 = MockVertex(0, ["Node"], {"val": 1.0})
        v1 = MockVertex(1, ["Node"], {"val": 2.0})
        e0 = MockEdge(0, v0, v1, "REL", {"w": 0.5})
        return [v0, v1], [e0]

    def test_graph_to_pyg_dict(self, simple_graph):
        vertices, edges = simple_graph

        result = graph_to_pyg_dict(
            vertices=vertices,
            edges=edges,
            node_property_names=["val"],
        )

        assert result["num_nodes"] == 2
        assert result["x"] == [[1.0], [2.0]]

    def test_pyg_dict_to_graph_data(self):
        pyg_dict = {
            "edge_index": [[0], [1]],
            "num_nodes": 2,
            "x": [[1.0], [2.0]],
        }

        nodes_data, edges_data = pyg_dict_to_graph_data(
            pyg_dict=pyg_dict,
            default_node_label="TestNode",
        )

        assert len(nodes_data) == 2
        assert len(edges_data) == 1
        assert nodes_data[0]["labels"] == ["TestNode"]


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    def test_empty_graph(self):
        converter = MemgraphPyGConverter()
        result = converter.export_to_pyg_dict(vertices=[], edges=[])

        assert result["num_nodes"] == 0
        assert result["edge_index"] == [[], []]

    def test_disconnected_vertices(self):
        v0 = MockVertex(0, ["Node"], {})
        v1 = MockVertex(1, ["Node"], {})
        v2 = MockVertex(2, ["Node"], {})

        converter = MemgraphPyGConverter()
        result = converter.export_to_pyg_dict(
            vertices=[v0, v1, v2],
            edges=[],
        )

        assert result["num_nodes"] == 3
        assert result["edge_index"] == [[], []]

    def test_missing_property(self):
        v0 = MockVertex(0, ["Node"], {"a": 1.0})
        v1 = MockVertex(1, ["Node"], {})  # Missing property 'a'

        converter = MemgraphPyGConverter()
        result = converter.export_to_pyg_dict(
            vertices=[v0, v1],
            edges=[],
            node_property_names=["a"],
        )

        # Missing properties should default to 0.0
        assert result["x"] == [[1.0], [0.0]]

    def test_non_numeric_property(self):
        v0 = MockVertex(0, ["Node"], {"name": "Alice"})

        converter = MemgraphPyGConverter()
        result = converter.export_to_pyg_dict(
            vertices=[v0],
            edges=[],
            node_property_names=["name"],
        )

        # Non-numeric properties should default to 0.0
        assert result["x"] == [[0.0]]

    def test_self_loop(self):
        v0 = MockVertex(0, ["Node"], {})
        e0 = MockEdge(0, v0, v0, "SELF", {})  # Self-loop

        converter = MemgraphPyGConverter()
        result = converter.export_to_pyg_dict(
            vertices=[v0],
            edges=[e0],
        )

        assert result["edge_index"] == [[0], [0]]

    def test_multiple_edges_between_nodes(self):
        v0 = MockVertex(0, ["Node"], {})
        v1 = MockVertex(1, ["Node"], {})
        e0 = MockEdge(0, v0, v1, "REL1", {})
        e1 = MockEdge(1, v0, v1, "REL2", {})

        converter = MemgraphPyGConverter()
        result = converter.export_to_pyg_dict(
            vertices=[v0, v1],
            edges=[e0, e1],
        )

        assert result["edge_index"] == [[0, 0], [1, 1]]
        assert result["edge_types"] == ["REL1", "REL2"]
