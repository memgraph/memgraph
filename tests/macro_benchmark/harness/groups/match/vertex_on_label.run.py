from setup import LABEL_COUNT, LABEL_PREFIX

for i in range(LABEL_COUNT):
    print("UNWIND range(0, 30) AS i MATCH (n:%s%d) "
          "RETURN n SKIP 1000000;" % (LABEL_PREFIX, i))
