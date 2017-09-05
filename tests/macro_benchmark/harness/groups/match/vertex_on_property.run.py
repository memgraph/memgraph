from setup import PROP_PREFIX, MAX_PROPS, rint, MAX_PROP_VALUE

print("UNWIND range(0, 50) AS i MATCH (n {%s%d: %d}) RETURN n SKIP 10000" % (
    PROP_PREFIX, rint(MAX_PROPS), rint(MAX_PROP_VALUE)))
