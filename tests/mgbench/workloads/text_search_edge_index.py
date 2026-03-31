# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import random

from benchmark_context import BenchmarkContext
from constants import GraphVendors
from workloads.base import Workload


class TextSearchEdgeIndex(Workload):
    NAME = "text_search_edge_index"
    VARIANTS = ["default", "small", "large"]
    DEFAULT_VARIANT = "default"
    SIZES = {
        "default": {"vertices": 10000, "edges": 10000},
        "small": {"vertices": 1000, "edges": 1000},
        "large": {"vertices": 10000, "edges": 100000},
    }
    PROPERTIES_ON_EDGES = True

    CATEGORIES = ["science", "technology", "engineering", "mathematics", "art", "history", "literature", "philosophy"]
    ADJECTIVES = ["advanced", "modern", "classical", "innovative", "fundamental", "applied", "theoretical", "practical"]
    NOUNS = ["research", "analysis", "method", "theory", "system", "framework", "model", "algorithm"]

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)
        self._nodes_count = self._size["vertices"]
        self._edges_count = self._size["edges"]
        self._next_edge_id = self._edges_count

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Node(id);", {}),
            ("CREATE TEXT EDGE INDEX index ON :Edge;", {}),
        ]

    def dataset_generator(self):
        queries = []
        for i in range(0, self._nodes_count):
            queries.append(("CREATE (:Node {id:%i});" % i, {}))
        for i in range(0, self._edges_count):
            a = self._get_random_node()
            b = self._get_random_node()
            queries.append(
                (
                    "MATCH (a:Node {id:$A_id}), (b:Node {id:$B_id}) CREATE (a)-[:Edge {id: $id, title: $title, category: $category, content: $content}]->(b);",
                    {
                        "A_id": a,
                        "B_id": b,
                        "id": i,
                        "title": self._get_random_title(),
                        "category": self._get_random_category(),
                        "content": self._get_random_content(),
                    },
                )
            )
        return queries

    def _get_random_node(self):
        return random.randint(0, self._nodes_count - 1)

    def _get_random_edge(self):
        return random.randint(0, self._edges_count - 1)

    def _get_random_category(self):
        return random.choice(TextSearchEdgeIndex.CATEGORIES)

    def _get_random_title(self):
        adj = random.choice(TextSearchEdgeIndex.ADJECTIVES)
        noun = random.choice(TextSearchEdgeIndex.NOUNS)
        cat = random.choice(TextSearchEdgeIndex.CATEGORIES)
        return f"{adj} {noun} in {cat}"

    def _get_random_content(self):
        parts = []
        for _ in range(random.randint(3, 5)):
            adj = random.choice(TextSearchEdgeIndex.ADJECTIVES)
            noun = random.choice(TextSearchEdgeIndex.NOUNS)
            parts.append(f"{adj} {noun}")
        return "The " + " provides a ".join(parts)

    def _get_random_search_query(self):
        cat = random.choice(TextSearchEdgeIndex.CATEGORIES)
        return f"data.category:{cat}"

    def _get_random_keyword(self):
        return random.choice(TextSearchEdgeIndex.NOUNS + TextSearchEdgeIndex.ADJECTIVES)

    def _get_random_regex(self):
        word = random.choice(TextSearchEdgeIndex.NOUNS)
        return f".*{word}.*"

    def benchmark__text__running_traversals(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (a:Node {id:$A_id})-[*bfs..4]->(b:Node {id:$B_id}) RETURN a.id, b.id;",
                    {"A_id": self._get_random_node(), "B_id": self._get_random_node()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__search_edges(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "CALL text_search.search_edges('index', $query) YIELD edge, score RETURN id(edge), score;",
                    {"query": self._get_random_search_query()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__search_all_edges(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "CALL text_search.search_all_edges('index', $query) YIELD edge, score RETURN id(edge), score;",
                    {"query": self._get_random_keyword()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__regex_search_edges(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "CALL text_search.regex_search_edges('index', $query) YIELD edge, score RETURN id(edge), score;",
                    {"query": self._get_random_regex()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__insert_edge(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                i = self._next_edge_id
                self._next_edge_id += 1
                return (
                    "MATCH (a:Node {id:$A_id}), (b:Node {id:$B_id}) CREATE (a)-[:Edge {id: $id, title: $title, category: $category, content: $content}]->(b);",
                    {
                        "A_id": self._get_random_node(),
                        "B_id": self._get_random_node(),
                        "id": i,
                        "title": self._get_random_title(),
                        "category": self._get_random_category(),
                        "content": self._get_random_content(),
                    },
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__delete_edge(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH ()-[e:Edge {id: $id}]->() DELETE e;",
                    {"id": self._get_random_edge()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")
