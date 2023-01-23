# Copyright 2022 Memgraph Ltd.
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

import helpers


# Base dataset class used as a template to create each individual dataset. All
# common logic is handled here.
class Dataset:
    # Name of the dataset.
    NAME = "Base dataset"
    # List of all variants of the dataset that exist.
    VARIANTS = ["default"]
    # One of the available variants that should be used as the default variant.
    DEFAULT_VARIANT = "default"
    # List of query files that should be used to import the dataset.
    DEFAULT_VENDOR = "memgraph"
    FILES = {
        "default": "/foo/bar",
    }
    INDEX = None
    INDEX_FILES = {"default": ""}
    # List of query file URLs that should be used to import the dataset.
    URLS = None
    # Number of vertices/edges for each variant.
    SIZES = {
        "default": {"vertices": 0, "edges": 0},
    }
    # Indicates whether the dataset has properties on edges.
    PROPERTIES_ON_EDGES = False

    def __init__(self, variant=None, vendor=None):
        """
        Accepts a `variant` variable that indicates which variant
        of the dataset should be executed.
        """
        if variant is None:
            variant = self.DEFAULT_VARIANT
        if variant not in self.VARIANTS:
            raise ValueError("Invalid test variant!")
        if (self.FILES and variant not in self.FILES) and (self.URLS and variant not in self.URLS):
            raise ValueError("The variant doesn't have a defined URL or " "file path!")
        if variant not in self.SIZES:
            raise ValueError("The variant doesn't have a defined dataset " "size!")
        if vendor is None:
            vendor = self.DEFAULT_VENDOR
        if vendor not in self.INDEX_FILES:
            raise ValueError("Vendor does not have INDEX for dataset!")
        self._variant = variant
        self._vendor = vendor
        if self.FILES is not None:
            self._file = self.FILES.get(variant, None)
        else:
            self._file = None
        if self.URLS is not None:
            self._url = self.URLS.get(variant, None)
        else:
            self._url = None

        if self.INDEX_FILES is not None:
            self._index = self.INDEX_FILES.get(vendor, None)
        else:
            self._index = None

        self._size = self.SIZES[variant]
        if "vertices" not in self._size or "edges" not in self._size:
            raise ValueError("The size defined for this variant doesn't " "have the number of vertices and/or edges!")
        self._num_vertices = self._size["vertices"]
        self._num_edges = self._size["edges"]

    def prepare(self, directory):
        if self._file is not None:
            print("Using dataset file:", self._file)
        else:
            # TODO: add support for JSON datasets
            cached_input, exists = directory.get_file("dataset.cypher")
            if not exists:
                print("Downloading dataset file:", self._url)
                downloaded_file = helpers.download_file(self._url, directory.get_path())
                print("Unpacking and caching file:", downloaded_file)
                helpers.unpack_and_move_file(downloaded_file, cached_input)
            print("Using cached dataset file:", cached_input)
            self._file = cached_input

        cached_index, exists = directory.get_file(self._vendor + ".cypher")
        if not exists:
            print("Downloading index file:", self._index)
            downloaded_file = helpers.download_file(self._index, directory.get_path())
            print("Unpacking and caching file:", downloaded_file)
            helpers.unpack_and_move_file(downloaded_file, cached_index)
        print("Using cached index file:", cached_index)
        self._index = cached_index

    def get_variant(self):
        """Returns the current variant of the dataset."""
        return self._variant

    def get_index(self):
        """Get index file, defined by vendor"""
        return self._index

    def get_file(self):
        """
        Returns path to the file that contains dataset creation queries.
        """
        return self._file

    def get_size(self):
        """Returns number of vertices/edges for the current variant."""
        return self._size

    # All tests should be query generator functions that output all of the
    # queries that should be executed by the runner. The functions should be
    # named `benchmark__GROUPNAME__TESTNAME` and should not accept any
    # arguments.


class Pokec(Dataset):
    NAME = "pokec"
    VARIANTS = ["small", "medium", "large"]
    DEFAULT_VARIANT = "small"
    FILES = None

    URLS = {
        "small": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/pokec_small.setup.cypher",
        "medium": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/pokec_medium.setup.cypher",
        "large": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/pokec_large.setup.cypher.gz",
        "small": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_small_import.cypher",
        "medium": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_medium_import.cypher",
        "large": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_large.setup.cypher.gz",
    }
    SIZES = {
        "small": {"vertices": 10000, "edges": 121716},
        "medium": {"vertices": 100000, "edges": 1768515},
        "large": {"vertices": 1632803, "edges": 30622564},
    }
    INDEX = None
    INDEX_FILES = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/memgraph.cypher",
        "neo4j": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/neo4j.cypher",
    }

    PROPERTIES_ON_EDGES = False

    # Helpers used to generate the queries

    def _get_random_vertex(self):
        # All vertices in the Pokec dataset have an ID in the range
        # [1, _num_vertices].
        return random.randint(1, self._num_vertices)

    def _get_random_from_to(self):
        vertex_from = self._get_random_vertex()
        vertex_to = vertex_from
        while vertex_to == vertex_from:
            vertex_to = self._get_random_vertex()
        return (vertex_from, vertex_to)

    # Arango benchmarks

    def benchmark__arango__single_vertex_read(self):
        return ("MATCH (n:User {id : $id}) RETURN n", {"id": self._get_random_vertex()})

    def benchmark__arango__single_vertex_write(self):
        return (
            "CREATE (n:UserTemp {id : $id}) RETURN n",
            {"id": random.randint(1, self._num_vertices * 10)},
        )

    def benchmark__arango__single_edge_write(self):
        vertex_from, vertex_to = self._get_random_from_to()
        return (
            "MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m " "CREATE (n)-[e:Temp]->(m) RETURN e",
            {"from": vertex_from, "to": vertex_to},
        )

    def benchmark__arango__aggregate(self):
        return ("MATCH (n:User) RETURN n.age, COUNT(*)", {})

    def benchmark__arango__aggregate_with_filter(self):
        return ("MATCH (n:User) WHERE n.age >= 18 RETURN n.age, COUNT(*)", {})

    def benchmark__arango__expansion_1(self):
        return (
            "MATCH (s:User {id: $id})-->(n:User) " "RETURN n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__expansion_1_with_filter(self):
        return (
            "MATCH (s:User {id: $id})-->(n:User) " "WHERE n.age >= 18 " "RETURN n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__expansion_2(self):
        return (
            "MATCH (s:User {id: $id})-->()-->(n:User) " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__expansion_2_with_filter(self):
        return (
            "MATCH (s:User {id: $id})-->()-->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__expansion_3(self):
        return (
            "MATCH (s:User {id: $id})-->()-->()-->(n:User) " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__expansion_3_with_filter(self):
        return (
            "MATCH (s:User {id: $id})-->()-->()-->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__expansion_4(self):
        return (
            "MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__expansion_4_with_filter(self):
        return (
            "MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__neighbours_2(self):
        return (
            "MATCH (s:User {id: $id})-[*1..2]->(n:User) " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__neighbours_2_with_filter(self):
        return (
            "MATCH (s:User {id: $id})-[*1..2]->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__neighbours_2_with_data(self):
        return (
            "MATCH (s:User {id: $id})-[*1..2]->(n:User) " "RETURN DISTINCT n.id, n",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__neighbours_2_with_data_and_filter(self):
        return (
            "MATCH (s:User {id: $id})-[*1..2]->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id, n",
            {"id": self._get_random_vertex()},
        )

    def benchmark__arango__shortest_path(self):
        vertex_from, vertex_to = self._get_random_from_to()
        return (
            "MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m "
            "MATCH p=(n)-[*bfs..15]->(m) "
            "RETURN extract(n in nodes(p) | n.id) AS path",
            {"from": vertex_from, "to": vertex_to},
        )

    def benchmark__arango__shortest_path_with_filter(self):
        vertex_from, vertex_to = self._get_random_from_to()
        return (
            "MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m "
            "MATCH p=(n)-[*bfs..15 (e, n | n.age >= 18)]->(m) "
            "RETURN extract(n in nodes(p) | n.id) AS path",
            {"from": vertex_from, "to": vertex_to},
        )

    # Our benchmark queries

    def benchmark__create__edge(self):
        vertex_from, vertex_to = self._get_random_from_to()
        return (
            "MATCH (a:User {id: $from}), (b:User {id: $to}) " "CREATE (a)-[:TempEdge]->(b)",
            {"from": vertex_from, "to": vertex_to},
        )

    def benchmark__create__pattern(self):
        return ("CREATE ()-[:TempEdge]->()", {})

    def benchmark__create__vertex(self):
        return ("CREATE ()", {})

    def benchmark__create__vertex_big(self):
        return (
            "CREATE (:L1:L2:L3:L4:L5:L6:L7 {p1: true, p2: 42, "
            'p3: "Here is some text that is not extremely short", '
            'p4:"Short text", p5: 234.434, p6: 11.11, p7: false})',
            {},
        )

    def benchmark__aggregation__count(self):
        return ("MATCH (n) RETURN count(n), count(n.age)", {})

    def benchmark__aggregation__min_max_avg(self):
        return ("MATCH (n) RETURN min(n.age), max(n.age), avg(n.age)", {})

    def benchmark__match__pattern_cycle(self):
        return (
            "MATCH (n:User {id: $id})-[e1]->(m)-[e2]->(n) " "RETURN e1, m, e2",
            {"id": self._get_random_vertex()},
        )

    def benchmark__match__pattern_long(self):
        return (
            "MATCH (n1:User {id: $id})-[e1]->(n2)-[e2]->" "(n3)-[e3]->(n4)<-[e4]-(n5) " "RETURN n5 LIMIT 1",
            {"id": self._get_random_vertex()},
        )

    def benchmark__match__pattern_short(self):
        return (
            "MATCH (n:User {id: $id})-[e]->(m) " "RETURN m LIMIT 1",
            {"id": self._get_random_vertex()},
        )

    def benchmark__match__vertex_on_label_property(self):
        return (
            "MATCH (n:User) WITH n WHERE n.id = $id RETURN n",
            {"id": self._get_random_vertex()},
        )

    def benchmark__match__vertex_on_label_property_index(self):
        return ("MATCH (n:User {id: $id}) RETURN n", {"id": self._get_random_vertex()})

    def benchmark__match__vertex_on_property(self):
        return ("MATCH (n {id: $id}) RETURN n", {"id": self._get_random_vertex()})

    # Basic benchmark queries

    def benchmark__basic__single_vertex_read_read(self):
        return ("MATCH (n:User {id : $id}) RETURN n", {"id": self._get_random_vertex()})

    def benchmark__basic__single_vertex_write_write(self):
        return (
            "CREATE (n:UserTemp {id : $id}) RETURN n",
            {"id": random.randint(1, self._num_vertices * 10)},
        )

    def benchmark__basic__single_vertex_property_update_update(self):
        return (
            "MATCH (n {id: $id}) SET n.property = -1",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__single_edge_write_write(self):
        vertex_from, vertex_to = self._get_random_from_to()
        return (
            "MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m " "CREATE (n)-[e:Temp]->(m) RETURN e",
            {"from": vertex_from, "to": vertex_to},
        )

    def benchmark__basic__aggregate_aggregate(self):
        return ("MATCH (n:User) RETURN n.age, COUNT(*)", {})

    def benchmark__basic__aggregate_count_aggregate(self):
        return ("MATCH (n) RETURN count(n), count(n.age)", {})

    def benchmark__basic__aggregate_with_filter_aggregate(self):
        return ("MATCH (n:User) WHERE n.age >= 18 RETURN n.age, COUNT(*)", {})

    def benchmark__basic__min_max_avg_aggregate(self):
        return ("MATCH (n) RETURN min(n.age), max(n.age), avg(n.age)", {})

    def benchmark__basic__expansion_1_analytical(self):
        return (
            "MATCH (s:User {id: $id})-->(n:User) " "RETURN n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__expansion_1_with_filter_analytical(self):
        return (
            "MATCH (s:User {id: $id})-->(n:User) " "WHERE n.age >= 18 " "RETURN n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__expansion_2_analytical(self):
        return (
            "MATCH (s:User {id: $id})-->()-->(n:User) " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__expansion_2_with_filter_analytical(self):
        return (
            "MATCH (s:User {id: $id})-->()-->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__expansion_3_analytical(self):
        return (
            "MATCH (s:User {id: $id})-->()-->()-->(n:User) " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__expansion_3_with_filter_analytical(self):
        return (
            "MATCH (s:User {id: $id})-->()-->()-->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__expansion_4_analytical(self):
        return (
            "MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__expansion_4_with_filter_analytical(self):
        return (
            "MATCH (s:User {id: $id})-->()-->()-->()-->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__neighbours_2_analytical(self):
        return (
            "MATCH (s:User {id: $id})-[*1..2]->(n:User) " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__neighbours_2_with_filter_analytical(self):
        return (
            "MATCH (s:User {id: $id})-[*1..2]->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__neighbours_2_with_data_analytical(self):
        return (
            "MATCH (s:User {id: $id})-[*1..2]->(n:User) " "RETURN DISTINCT n.id, n",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__neighbours_2_with_data_and_filter_analytical(self):
        return (
            "MATCH (s:User {id: $id})-[*1..2]->(n:User) " "WHERE n.age >= 18 " "RETURN DISTINCT n.id, n",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__pattern_cycle_analytical(self):
        return (
            "MATCH (n:User {id: $id})-[e1]->(m)-[e2]->(n) " "RETURN e1, m, e2",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__pattern_long_analytical(self):
        return (
            "MATCH (n1:User {id: $id})-[e1]->(n2)-[e2]->" "(n3)-[e3]->(n4)<-[e4]-(n5) " "RETURN n5 LIMIT 1",
            {"id": self._get_random_vertex()},
        )

    def benchmark__basic__pattern_short_analytical(self):
        return (
            "MATCH (n:User {id: $id})-[e]->(m) " "RETURN m LIMIT 1",
            {"id": self._get_random_vertex()},
        )


class AccessControl(Dataset):

    # Explaination of datasets:
    #   - empty_only_index: contains index; contains no data
    #   - small/medium/large: contains index; contains data (respectively small/medium/large dataset)
    #
    # See dataset_creator.py to understand the datamodel and generate a dataset

    NAME = "accesscontrol"
    VARIANTS = ["empty_only_index", "small", "medium", "large"]
    DEFAULT_VARIANT = "empty_only_index"
    URLS = {
        "empty_only_index": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/accesscontrol/accesscontrol_empty_only_index.setup.cypher.gz",
        "small": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/accesscontrol/accesscontrol_small.setup.cypher.gz",
        "medium": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/accesscontrol/accesscontrol_medium.setup.cypher.gz",
        "large": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/accesscontrol/accesscontrol_large.setup.cypher.gz",
    }

    INDEX_FILES = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/accesscontrol/accesscontrol_empty_only_index.setup.cypher.gz",
    }

    SIZES = {
        "empty_only_index": {
            "vertices": 0,
            "edges": -1,  # not used
            "uuid_ranges": {
                "File": {"first_uuid": 0, "last_uuid": 0},
                "Permission": {"first_uuid": 0, "last_uuid": 0},
                "Identity": {"first_uuid": 0, "last_uuid": 0},
            },
        },
        "small": {
            "vertices": 30,
            "edges": -1,  # not used
            "uuid_ranges": {
                "File": {"first_uuid": 1, "last_uuid": 10},
                "Identity": {"first_uuid": 11, "last_uuid": 20},
                "Permission": {"first_uuid": 21, "last_uuid": 120},  # 120=10*10+20
            },
        },
        "medium": {
            "vertices": 3000,
            "edges": -1,  # not used
            "uuid_ranges": {
                "File": {"first_uuid": 1, "last_uuid": 1000},
                "Identity": {"first_uuid": 1001, "last_uuid": 2000},
                "Permission": {"first_uuid": 2001, "last_uuid": 1002000},  # 1002000=1000*1000+2000
            },
        },
        "large": {
            "vertices": 30000,
            "edges": -1,  # not used
            "uuid_ranges": {
                "File": {"first_uuid": 1, "last_uuid": 10000},
                "Identity": {"first_uuid": 10001, "last_uuid": 20000},
                "Permission": {"first_uuid": 20001, "last_uuid": 100020000},  # 100020000=10000*10000+20000
            },
        },
    }

    def _get_random_uuid(self, type):
        assert type in ["File", "Permission", "Identity"]

        first_uuid = self.get_size()["uuid_ranges"][type]["first_uuid"]
        last_uuid = self.get_size()["uuid_ranges"][type]["last_uuid"]

        random_value = random.randint(first_uuid, last_uuid)
        return random_value

    def __init__(self, variant=None, vendor=None):
        super().__init__(variant)
        self.next_value_idx = self.get_size()["vertices"] + 1

    def benchmark__create__vertex(self):
        self.next_value_idx += 1
        query = (f"CREATE (:File {{uuid: {self.next_value_idx}}});", {})
        return query

    def benchmark__create__edges(self):
        permission_uuid = self._get_random_uuid("Permission")
        file_uuid = self._get_random_uuid("File")

        query = (
            "MATCH (permission:Permission {uuid: $permission_uuid}), (file:File {uuid: $file_uuid}) "
            "CREATE (permission)-[:IS_FOR_FILE]->(file)",
            {"permission_uuid": permission_uuid, "file_uuid": file_uuid},
        )

        return query

    def benchmark__match__match_all_vertices(self):
        self.next_value_idx += 1
        query = ("MATCH (n) RETURN *", {})
        return query

    def benchmark__match__match_on_labelled_vertices(self):
        self.next_value_idx += 1
        query = ("MATCH (n:File) RETURN *", {})
        return query

    def benchmark__match__match_all_vertices_with_edges(self):
        self.next_value_idx += 1
        query = (
            "MATCH (permission:Permission)-[e:IS_FOR_FILE]->(file:File) RETURN *",
            {},
        )
        return query


class TestDataset(Dataset):
    NAME = "test"
    VARIANTS = ["small"]
    DEFAULT_VARIANT = "small"
    # TODO(gitbuda): ~/Downloads/ doesn't work for some reason, fix! :)
    # TODO(gitbuda): If ~ is wrong, client fails just with SIGABRT -> hard to figure out -> enable more details from client.
    FILES = {
        "small": "/home/buda/Downloads/mgbench/test_dataset_small.cypherl",
    }
    SIZES = {
        "small": {"vertices": 10, "edges": 9},
    }
    # TODO(gitbuda): What's the purpose of the INDEX inside the Dataset?
    # TODO(gitbuda): It's not possible to also inject local index file, it's much easier to add your local file compare to uploading it somewhere or running local HTTP server (easy with e.g. python, but...)
    INDEX_FILES = {
        "memgraph": "http://localhost:8080/test_dataset_index.cypherl",
    }

    def benchmark__basic__test(self):
        return ("MATCH (n:Label {id: 0})-[*]->(m) RETURN n, m;", {})
