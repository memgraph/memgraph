from setup import LABEL_COUNT, rint
print("MATCH (n:Label%d) RETURN n" % rint(LABEL_COUNT))
