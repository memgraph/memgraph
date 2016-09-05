import time
from neo4j.v1 import GraphDatabase, basic_auth, types
from concurrent.futures import ProcessPoolExecutor

# create session
driver = GraphDatabase.driver("bolt://localhost",
                              auth=basic_auth("neo4j", "neo4j"),
                              encrypted=0)
session = driver.session()

queries_no = 10

queries = ["CREATE (n {prop: 10}) RETURN n"] * queries_no

def create_query(index):
    '''
    Task (process or thread)
    Runs create query agains the database.

    :param index: int -> number of task
    :returns: (int, float) -> (task index, elapsed time)
    '''
    start = time.time()
    for query in queries:
        for record in session.run(query):
            pass
    end = time.time()
    return time


with ProcessPoolExecutor(processes=4) as executor:
    results = []
    print(results)

# print(1.0 * queries_no / (end - start))
