from setup import ID, rint, VERTEX_COUNT
print("MATCH (n)-[r]->(m) WHERE n.%s = %d AND m.%s = %d RETURN *" % (
    ID, rint(VERTEX_COUNT), ID, rint(VERTEX_COUNT)))
