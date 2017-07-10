BATCH_SIZE = 50
VERTEX_COUNT = 500

for i in range(VERTEX_COUNT):
    print("CREATE (n%d {x: %d})" % (i, i))
    # batch CREATEs because we can't execute all at once
    if i != 0 and i % BATCH_SIZE == 0:
        print(";")
