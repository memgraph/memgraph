VERTEX_COUNT = 100000
BATCH_SIZE = 50

query = []
while VERTEX_COUNT > 0:
  new_vertices = min(BATCH_SIZE, VERTEX_COUNT)
  query.append('CREATE ()' * new_vertices)
  query.append(";")
  VERTEX_COUNT -= new_vertices

print(" ".join(query))
