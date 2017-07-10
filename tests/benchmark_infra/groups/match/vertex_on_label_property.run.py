from setup import LABEL_PREFIX, PROP_PREFIX, MAX_PROPS, LABEL_COUNT, rint
print("MATCH (n:%s%d {%s%d: %d}) RETURN n" % (
    LABEL_PREFIX, rint(LABEL_COUNT), PROP_PREFIX, rint(MAX_PROPS), rint(10)))
