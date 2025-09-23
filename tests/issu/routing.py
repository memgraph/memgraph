from neo4j import GraphDatabase


class Neo4jService:
    def __init__(self, uri, user="", password=""):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def read_greeting(self):
        with self.driver.session() as session:
            session.execute_read(self._create_and_return_greeting)
            print("Read txn passed!")

    @staticmethod
    def _create_and_return_greeting(tx):
        tx.run("MATCH (n:Greeting) RETURN n.message AS message")


def greetings_from_uri(uri):
    service = Neo4jService(uri)
    service.read_greeting()
    service.close()


def main():
    uri = "neo4j://localhost:7687"

    try:
        greetings_from_uri(uri)
    except Exception as error:
        print(f"An error occurred: {error}")
        exit(-1)
    print("Finished reading route")


if __name__ == "__main__":
    main()
