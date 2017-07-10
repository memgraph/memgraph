BATCH_SIZE = 50
VERTEX_COUNT = 500
UNIQUE_VALUES = 50


def main():
    for i in range(VERTEX_COUNT):
        print("CREATE (n%d {x: %d, id: %d})" % (i, i % UNIQUE_VALUES, i))
        # batch CREATEs because we can't execute all at once
        if i != 0 and i % BATCH_SIZE == 0:
            print(";")

if __name__ == '__main__':
    main()
