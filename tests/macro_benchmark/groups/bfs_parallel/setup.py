import random
import common

random.seed(0)

for i in range(common.VERTEX_COUNT):
    print("CREATE (n: Node {id: %d});" % i)

print(";")
print("CREATE INDEX ON :Node(id);")
print(";")

# create a tree to be sure there is a path between each two nodes
for i in range(1, common.VERTEX_COUNT):
    dad = int(random.random() * i)
    print("MATCH (a: Node {id: %d}), (b: Node {id: %d}) CREATE (a)-[:Friend]->(b);" % (dad, i))
    print("MATCH (a: Node {id: %d}), (b: Node {id: %d}) CREATE (a)-[:Friend]->(b);" % (i, dad))

# add random edges
for i in range(common.VERTEX_COUNT * common.VERTEX_COUNT // common.SPARSE_FACTOR):
    a = int(random.random() * common.VERTEX_COUNT)
    b = int(random.random() * common.VERTEX_COUNT)
    print("MATCH (a: Node {id: %d}), (b: Node {id: %d}) CREATE (a)-[:Friend]->(b);" % (a, b))

