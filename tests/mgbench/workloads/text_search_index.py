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


class TextSearchIndex(Workload):
    NAME = "text_search_index"
    VARIANTS = ["default", "small", "large"]
    DEFAULT_VARIANT = "default"
    SIZES = {
        "default": {"vertices": 10000, "edges": 1000},
        "small": {"vertices": 1000, "edges": 1000},
        "large": {"vertices": 100000, "edges": 1000},
    }

    CATEGORIES = ["science", "technology", "engineering", "mathematics", "art", "history", "literature", "philosophy"]
    ADJECTIVES = ["advanced", "modern", "classical", "innovative", "fundamental", "applied", "theoretical", "practical"]
    NOUNS = ["research", "analysis", "method", "theory", "system", "framework", "model", "algorithm"]

    def __init__(self, variant: str = None, benchmark_context: BenchmarkContext = None):
        super().__init__(variant, benchmark_context=benchmark_context)
        self._nodes_count = self._size["vertices"]
        self._edges_count = self._size["edges"]
        self._next_node_id = self._nodes_count
        random.seed(10)

    def indexes_generator(self):
        return [
            ("CREATE INDEX ON :Node(id);", {}),
            ("CREATE TEXT INDEX index ON :Node;", {}),
        ]

    def dataset_generator(self):
        queries = []
        for i in range(0, self._nodes_count):
            queries.append(
                (
                    "CREATE (:Node {id: $id, title: $title, category: $category, content: $content});",
                    {
                        "id": i,
                        "title": self._get_random_title(),
                        "category": self._get_random_category(),
                        "content": self._get_random_content(),
                    },
                )
            )
        for i in range(0, self._edges_count):
            a = self._get_random_node()
            b = self._get_random_node()
            queries.append(
                ("MATCH (a:Node {id:$A_id}), (b:Node {id:$B_id}) CREATE (a)-[:Edge]->(b);", {"A_id": a, "B_id": b})
            )
        return queries

    def _get_random_node(self):
        return random.randint(0, self._nodes_count - 1)

    def _get_random_category(self):
        return random.choice(TextSearchIndex.CATEGORIES)

    def _get_random_title(self):
        adj = random.choice(TextSearchIndex.ADJECTIVES)
        noun = random.choice(TextSearchIndex.NOUNS)
        cat = random.choice(TextSearchIndex.CATEGORIES)
        return f"{adj} {noun} in {cat}"

    def _get_random_content(self):
        parts = []
        for _ in range(random.randint(3, 5)):
            adj = random.choice(TextSearchIndex.ADJECTIVES)
            noun = random.choice(TextSearchIndex.NOUNS)
            parts.append(f"{adj} {noun}")
        return "The " + " provides a ".join(parts)

    def _get_random_search_query(self):
        cat = random.choice(TextSearchIndex.CATEGORIES)
        return f"data.category:{cat}"

    def _get_random_keyword(self):
        return random.choice(TextSearchIndex.NOUNS + TextSearchIndex.ADJECTIVES)

    def _get_random_regex(self):
        word = random.choice(TextSearchIndex.NOUNS)
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

    def benchmark__text__search(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "CALL text_search.search('index', $query) YIELD node, score RETURN node.id, score;",
                    {"query": self._get_random_search_query()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__search_all(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "CALL text_search.search_all('index', $query) YIELD node, score RETURN node.id, score;",
                    {"query": self._get_random_keyword()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__regex_search(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "CALL text_search.regex_search('index', $query) YIELD node, score RETURN node.id, score;",
                    {"query": self._get_random_regex()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__insert_node(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                i = self._next_node_id
                self._next_node_id += 1
                return (
                    "CREATE (:Node {id: $id, title: $title, category: $category, content: $content});",
                    {
                        "id": i,
                        "title": self._get_random_title(),
                        "category": self._get_random_category(),
                        "content": self._get_random_content(),
                    },
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")

    def benchmark__text__delete_node(self):
        match self._vendor:
            case GraphVendors.MEMGRAPH:
                return (
                    "MATCH (n:Node {id: $id}) DETACH DELETE n;",
                    {"id": self._get_random_node()},
                )
            case _:
                raise Exception(f"Unknown vendor {self._vendor}")
