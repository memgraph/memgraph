import random
import common

# Here we set seed to 1, instead of 0, because seed in setup is 0 and we want to
# be sure here that we will generate different numbers.
random.seed(1)

for i in range(common.BFS_ITERS):
    a = int(random.random() * common.VERTEX_COUNT)
    b = int(random.random() * common.VERTEX_COUNT)
    print("MATCH (from: Node {id: %d}) WITH from "
          "MATCH (to: Node {id: %d}) WITH to     "
          "MATCH path = (from)-[*bfs..%d (e, n | true)]->(to) WITH path "
          "LIMIT 10 RETURN 0;"
          % (a, b, common.PATH_LENGTH))
