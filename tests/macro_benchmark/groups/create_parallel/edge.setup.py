import common

for i in range(common.VERTEX_COUNT):
    print("CREATE (n: Node {id: %d});" % i)
print("CREATE INDEX ON :Node(id);")
