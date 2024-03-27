from neo4j import GraphDatabase


class Neo4jService:
    def __init__(self, uri, user="", password=""):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_greeting(self):
        with self.driver.session() as session:
            session.execute_write(self._create_and_return_greeting)
            print("Write txn passed!")

    @staticmethod
    def _create_and_return_greeting(tx):
        tx.run("CREATE (n:Greeting {message: 'Hello from Python'}) RETURN n.message AS message")


def create_greetings_from_uri(uri):
    service = Neo4jService(uri)
    service.create_greeting()
    service.close()


def main():
    print("Started writing route")
    uris = ["neo4j://localhost:7690", "neo4j://localhost:7691", "neo4j://localhost:7692"]

    try:
        for uri in uris:
            create_greetings_from_uri(uri)
    except Exception as error:
        print(f"An error occurred: {error}")
        exit(-1)
    print("Finished writing route")


if __name__ == "__main__":
    main()
