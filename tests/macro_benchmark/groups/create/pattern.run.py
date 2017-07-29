PATTERN_COUNT = 100000
BATCH_SIZE = 50

query = []
while PATTERN_COUNT > 0:
  new_patterns = min(BATCH_SIZE, PATTERN_COUNT)
  query.append('CREATE ()-[:Type]->() ' * new_patterns)
  query.append(";")
  PATTERN_COUNT -= new_patterns

print(" ".join(query))
