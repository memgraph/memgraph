import random

from workloads.base import Workload


class Demo(Workload):

    NAME = "demo"

    def dataset_generator(self):

        queries = [("MATCH (n) DETACH DELETE n;", {})]
        for i in range(0, 100):
            queries.append(("CREATE (:NodeA{{ id:{}}});".format(i), {}))
            queries.append(("CREATE (:NodeB{{ id:{}}});".format(i), {}))

        for i in range(0, 100):
            a = random.randint(0, 99)
            b = random.randint(0, 99)
            queries.append(("MATCH(a:NodeA{{ id: {}}}),(b:NodeB{{id: {}}}) CREATE (a)-[:EDGE]->(b)".format(a, b), {}))

        return queries

    def benchmark__test__sample_query1(self):
        return ("MATCH (n) RETURN n", {})

    def benchmark__test__sample_query2(self):
        return ("MATCH (n) RETURN n", {})
