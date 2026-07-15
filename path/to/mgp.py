# Complete code
import json
from serialization import to_json

class Vertex:
    def __init__(self, id, label, properties=None):
        self.id = id
        self.label = label
        self.properties = properties if properties else {}

    def __repr__(self):
        return f"Vertex({self.id}, {self.label}, {self.properties})"

class Edge:
    def __init__(self, id, label, start_vertex_id, end_vertex_id, properties=None):
        self.id = id
        self.label = label
        self.start_vertex_id = start_vertex_id
        self.end_vertex_id = end_vertex_id
        self.properties = properties if properties else {}

    def __repr__(self):
        return f"Edge({self.id}, {self.label}, {self.start_vertex_id}, {self.end_vertex_id}, {self.properties})"