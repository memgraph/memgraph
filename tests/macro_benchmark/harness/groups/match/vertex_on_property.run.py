from setup import PROP_PREFIX, MAX_PROPS, rint
print("MATCH (n {%s%d: %d}) RETURN n" % (
    PROP_PREFIX, rint(MAX_PROPS), rint(10)))
