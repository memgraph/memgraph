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

    def benchmark__arango__aggregate_with_distinct(self):
        return ("MATCH (n:User) RETURN COUNT(DISTINCT n.age)", {})

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

    def benchmark__arango__allshortest_paths(self):
        vertex_from, vertex_to = self._get_random_from_to()
        return (
            "MATCH (n:User {id: $from}), (m:User {id: $to}) WITH n, m "
            "MATCH p=(n)-[*allshortest 2 (r, n | 1) total_weight]->(m) "
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

    def benchmark__update__vertex_on_property(self):
        return (
            "MATCH (n {id: $id}) SET n.property = -1",
            {"id": self._get_random_vertex()},
        )

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


class LDBC(Dataset):
    NAME = "ldbc"
    VARIANTS = ["small"]
    DEFAULT_VARIANT = "small"
    FILES = {"small": "/home/maple/repos/test/memgraph/tests/mgbench/out/out.cypher"}

    URLS = {
        "small": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/small_ldbc_import.gz",
    }
    SIZES = {
        "small": {"vertices": 1, "edges": 1},
    }
    INDEX = None
    INDEX_FILES = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/ldbc/benchmark/memgraph.cypher",
        "neo4j": "",
    }

    PROPERTIES_ON_EDGES = False

    def benchmark__ldbc__sample_query(self):
        return "MATCH(n:Tag) RETURN n;"

    def benchmark__ldbc__interactive_complex_query_1(self):
        return (
            """
                MATCH (p:Person {id: $personId}), (friend:Person {firstName: $firstName})
                WHERE NOT p=friend
                WITH p, friend
                MATCH path = shortestPath((p)-[:KNOWS*1..3]-(friend))
                WITH min(length(path)) AS distance, friend
            ORDER BY
                distance ASC,
                friend.lastName ASC,
                toInteger(friend.id) ASC
            LIMIT 20

            MATCH (friend)-[:IS_LOCATED_IN]->(friendCity:City)
            OPTIONAL MATCH (friend)-[studyAt:STUDY_AT]->(uni:University)-[:IS_LOCATED_IN]->(uniCity:City)
            WITH friend, collect(
                CASE uni.name
                    WHEN null THEN null
                    ELSE [uni.name, studyAt.classYear, uniCity.name]
                END ) AS unis, friendCity, distance

            OPTIONAL MATCH (friend)-[workAt:WORK_AT]->(company:Company)-[:IS_LOCATED_IN]->(companyCountry:Country)
            WITH friend, collect(
                CASE company.name
                    WHEN null THEN null
                    ELSE [company.name, workAt.workFrom, companyCountry.name]
                END ) AS companies, unis, friendCity, distance

            RETURN
                friend.id AS friendId,
                friend.lastName AS friendLastName,
                distance AS distanceFromPerson,
                friend.birthday AS friendBirthday,
                friend.creationDate AS friendCreationDate,
                friend.gender AS friendGender,
                friend.browserUsed AS friendBrowserUsed,
                friend.locationIP AS friendLocationIp,
                friend.email AS friendEmails,
                friend.speaks AS friendLanguages,
                friendCity.name AS friendCityName,
                unis AS friendUniversities,
                companies AS friendCompanies
            ORDER BY
                distanceFromPerson ASC,
                friendLastName ASC,
                toInteger(friendId) ASC
            LIMIT 20
            """,
            {"personId": "4398046511333", "firstName": "Jose"},
        )

    def benchmark__ldbc__interactive_complex_query_2(self):
        return (
            """
            MATCH (:Person {id: $personId })-[:KNOWS]-(friend:Person)<-[:HAS_CREATOR]-(message:Message)
            WHERE message.creationDate <= $maxDate
            RETURN
                friend.id AS personId,
                friend.firstName AS personFirstName,
                friend.lastName AS personLastName,
                message.id AS postOrCommentId,
                coalesce(message.content,message.imageFile) AS postOrCommentContent,
                message.creationDate AS postOrCommentCreationDate
            ORDER BY
                postOrCommentCreationDate DESC,
                toInteger(postOrCommentId) ASC
            LIMIT 20
            """,
            {"personId": "10995116278009", "maxDate": "1287230400000"},
        )

    def benchmark__ldbc__interactive_complex_query_3(self):
        return (
            """
            MATCH (countryX:Country {name: $countryXName }),
                (countryY:Country {name: $countryYName }),
                (person:Person {id: $personId })
            WITH person, countryX, countryY
            LIMIT 1
            MATCH (city:City)-[:IS_PART_OF]->(country:Country)
            WHERE country IN [countryX, countryY]
            WITH person, countryX, countryY, collect(city) AS cities
            MATCH (person)-[:KNOWS*1..2]-(friend)-[:IS_LOCATED_IN]->(city)
            WHERE NOT person=friend AND NOT city IN cities
            WITH DISTINCT friend, countryX, countryY
            MATCH (friend)<-[:HAS_CREATOR]-(message),
                (message)-[:IS_LOCATED_IN]->(country)
            WHERE $endDate > message.creationDate >= $startDate AND
                country IN [countryX, countryY]
            WITH friend,
                CASE WHEN country=countryX THEN 1 ELSE 0 END AS messageX,
                CASE WHEN country=countryY THEN 1 ELSE 0 END AS messageY
            WITH friend, sum(messageX) AS xCount, sum(messageY) AS yCount
            WHERE xCount>0 AND yCount>0
            RETURN friend.id AS friendId,
                friend.firstName AS friendFirstName,
                friend.lastName AS friendLastName,
                xCount,
                yCount,
                xCount + yCount AS xyCount
            ORDER BY xyCount DESC, friendId ASC
            LIMIT 20
            """,
            {
                "personId": 6597069766734,
                "countryXName": "Angola",
                "countryYName": "Colombia",
                "startDate": 1275393600000,
                "endDate": 1277812800000,
            },
        )

    def benchmark__ldbc__interactive_complex_query_4(self):
        return (
            """
            MATCH (person:Person {id: $personId })-[:KNOWS]-(friend:Person),
                (friend)<-[:HAS_CREATOR]-(post:Post)-[:HAS_TAG]->(tag)
            WITH DISTINCT tag, post
            WITH tag,
                CASE
                WHEN $endDate > post.creationDate >= $startDate THEN 1
                ELSE 0
                END AS valid,
                CASE
                WHEN $startDate > post.creationDate THEN 1
                ELSE 0
                END AS inValid
            WITH tag, sum(valid) AS postCount, sum(inValid) AS inValidPostCount
            WHERE postCount>0 AND inValidPostCount=0
            RETURN tag.name AS tagName, postCount
            ORDER BY postCount DESC, tagName ASC
            LIMIT 10
            """,
            {
                "personId": "4398046511333",
                "startDate": "1275350400000",
                "endDate": "1277856000000",
            },
        )

    def benchmark__ldbc__interactive_complex_query_5(self):
        return (
            """
            MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(otherPerson)
            WHERE
                person <> otherPerson
            WITH DISTINCT otherPerson
            MATCH (otherPerson)<-[membership:HAS_MEMBER]-(forum)
            WHERE
                membership.creationDate > $minDate
            WITH
                forum,
                collect(otherPerson) AS otherPersons
            OPTIONAL MATCH (otherPerson2)<-[:HAS_CREATOR]-(post)<-[:CONTAINER_OF]-(forum)
            WHERE
                otherPerson2 IN otherPersons
            WITH
                forum,
                count(post) AS postCount
            RETURN
                forum.title AS forumName,
                postCount
            ORDER BY
                postCount DESC,
                forum.id ASC
            LIMIT 20
            """,
            {
                "personId": "6597069766734",
                "minDate": "1288612800000",
            },
        )

    def benchmark__ldbc__interactive_complex_query_6(self):
        return (
            """
            MATCH (knownTag:Tag { name: $tagName })
            WITH knownTag.id as knownTagId

            MATCH (person:Person { id: $personId })-[:KNOWS*1..2]-(friend)
            WHERE NOT person=friend
            WITH
                knownTagId,
                collect(distinct friend) as friends
            UNWIND friends as f
                MATCH (f)<-[:HAS_CREATOR]-(post:Post),
                    (post)-[:HAS_TAG]->(t:Tag{id: knownTagId}),
                    (post)-[:HAS_TAG]->(tag:Tag)
                WHERE NOT t = tag
                WITH
                    tag.name as tagName,
                    count(post) as postCount
            RETURN
                tagName,
                postCount
            ORDER BY
                postCount DESC,
                tagName ASC
            LIMIT 10
            """,
            {"personId": "4398046511333", "tagName": "Carl_Gustaf_Emil_Mannerheim"},
        )
