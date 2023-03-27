import random

from workloads.base import Workload


class Demo(Workload):

    NAME = "demo"

    def indexes_generator(self):
        indexes = []
        if "neo4j" in self.benchmark_context.vendor_name:
            indexes.extend(
                [
                    ("CREATE INDEX FOR (n:NodeA) ON (n.id);", {}),
                    ("CREATE INDEX FOR (n:NodeB) ON (n.id);", {}),
                ]
            )
        else:
            indexes.extend(
                [
                    ("CREATE INDEX ON :NodeA(id);", {}),
                    ("CREATE INDEX ON :NodeB(id);", {}),
                ]
            )
        return indexes

    def dataset_generator(self):

        queries = []
        for i in range(0, 100):
            queries.append(("CREATE (:NodeA {id: $id});", {"id": i}))
            queries.append(("CREATE (:NodeB {id: $id});", {"id": i}))
        for i in range(0, 300):
            a = random.randint(0, 99)
            b = random.randint(0, 99)
            queries.append(
                (("MATCH(a:NodeA {id: $A_id}),(b:NodeB{id: $B_id}) CREATE (a)-[:EDGE]->(b)"), {"A_id": a, "B_id": b})
            )

        return queries

    def benchmark__test__get_nodes(self):
        return ("MATCH (n) RETURN n;", {})

    def benchmark__test__get_node_by_id(self):
        return ("MATCH (n:NodeA{id: $id}) RETURN n;", {"id": random.randint(0, 99)})
