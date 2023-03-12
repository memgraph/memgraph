import random

from .dataset import Dataset


class Pokec(Dataset):
    NAME = "pokec"
    VARIANTS = ["small", "medium", "large"]
    DEFAULT_VARIANT = "small"
    FILES = None

    URL_CYPHER = {
        "small": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_small_import.cypher",
        "medium": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_medium_import.cypher",
        "large": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/pokec_large.setup.cypher.gz",
    }
    SIZES = {
        "small": {"vertices": 10000, "edges": 121716},
        "medium": {"vertices": 100000, "edges": 1768515},
        "large": {"vertices": 1632803, "edges": 30622564},
    }

    URL_INDEX_FILES = {
        "memgraph": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/memgraph.cypher",
        "neo4j": "https://s3.eu-west-1.amazonaws.com/deps.memgraph.io/dataset/pokec/benchmark/neo4j.cypher",
    }

    PROPERTIES_ON_EDGES = False

    def __init__(self, variant=None, vendor=None):
        super().__init__(variant, vendor)

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
        return ("MATCH (n:User {id: $id}) RETURN n", {"id": self._get_random_vertex()})

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
            "MATCH (n:User {id: $id}) SET n.property = -1",
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
