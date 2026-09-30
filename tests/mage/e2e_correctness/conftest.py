def pytest_addoption(parser):
    parser.addoption("--memgraph-port", type=int, action="store")
    parser.addoption("--neo4j-port", type=int, action="store")
    parser.addoption("--neo4j-container", type=str, action="store")
