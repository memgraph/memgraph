from setup import LABEL_INDEX, ID, VERTEX_COUNT, rint

print("UNWIND range(0, 10000) AS i "
      "MATCH (n:%s {%s: %d}) RETURN n SKIP 1000000" % (
        LABEL_INDEX, ID, rint(VERTEX_COUNT)))
