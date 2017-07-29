VERTEX_COUNT = 100000
BATCH_SIZE = 50

query = []
while VERTEX_COUNT > 0:
  new_vertices = min(BATCH_SIZE, VERTEX_COUNT)
  query.append('CREATE (:L1:L2:L3:L4:L5:L6:L7 '
               '{p1: true, p2: 42, '
               'p3: "Here is some text that is not extremely short", '
               'p4:"Short text", p5: 234.434, p6: 11.11, p7: false})'
               * new_vertices)
  query.append(";")
  VERTEX_COUNT -= new_vertices

print(" ".join(query))
