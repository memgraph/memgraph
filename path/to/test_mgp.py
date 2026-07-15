# Complete code
import unittest
from mgp import Vertex, Edge
from mgp.serialization import to_json

class TestSerialization(unittest.TestCase):
    def test_serialize_vertex(self):
        vertex = Vertex(1, "Person", {"name": "John", "age": 30})
        json_str = to_json(vertex)
        expected_json = '{"id": 1, "label": "Person", "properties": {"name": "John", "age": 30}}'
        self.assertEqual(json_str, expected_json)

    def test_serialize_edge(self):
        edge = Edge(1, "Friend", 1, 2, {"since": "2022-01-01"})
        json_str = to_json(edge)
        expected_json = '{"id": 1, "label": "Friend", "start_vertex_id": 1, "end_vertex_id": 2, "properties": {"since": "2022-01-01"}}'
        self.assertEqual(json_str, expected_json)

    def test_serialize_list(self):
        vertex = Vertex(1, "Person", {"name": "John", "age": 30})
        edge = Edge(1, "Friend", 1, 2, {"since": "2022-01-01"})
        json_str = to_json([vertex, edge])
        expected_json = '[{"id": 1, "label": "Person", "properties": {"name": "John", "age": 30}}, {"id": 1, "label": "Friend", "start_vertex_id": 1, "end_vertex_id": 2, "properties": {"since": "2022-01-01"}}]'
        self.assertEqual(json_str, expected_json)

if __name__ == "__main__":
    unittest.main()