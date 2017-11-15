import random
import common

random.seed(0)

for i in range(common.VERTEX_COUNT * common.QUERIES_PER_VERTEX):
    a = int(random.random() * common.VERTEX_COUNT)
    b = int(random.random() * common.VERTEX_COUNT)
    print("MATCH (a: Node {id: %d}), (b: Node {id: %d}) CREATE (a)-[:Friend]->(b);" % (a, b))
