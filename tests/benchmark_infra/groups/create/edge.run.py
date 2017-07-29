EDGE_COUNT = 100000
BATCH_SIZE = 50

query = []
while EDGE_COUNT > 0:
  query.append("MATCH (a), (b)")
  new_edges = min(BATCH_SIZE, EDGE_COUNT)
  query.append('CREATE (a)-[:Type]->(b) ' * new_edges)
  query.append(";")
  EDGE_COUNT -= new_edges

print(" ".join(query))
