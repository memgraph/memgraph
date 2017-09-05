from setup import LABEL_PREFIX, PROP_PREFIX, MAX_PROPS, MAX_PROP_VALUE, LABEL_COUNT, rint

for i in range(LABEL_COUNT):
    print("UNWIND range(0, 50) AS i MATCH (n:%s%d {%s%d: %d}) RETURN n SKIP 10000;" % (
        LABEL_PREFIX, i, PROP_PREFIX, rint(MAX_PROPS), rint(MAX_PROP_VALUE)))
