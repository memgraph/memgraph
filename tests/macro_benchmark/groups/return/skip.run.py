from setup import VERTEX_COUNT

print("MATCH (n) RETURN n SKIP %d" % (VERTEX_COUNT // 2))
