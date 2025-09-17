from neo4j import GraphDatabase


class Neo4jService:
    def __init__(self, uri, user="", password=""):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        with self.driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n").consume()
            session.run("CREATE DATABASE db1").consume()
            session.run("CREATE DATABASE db2").consume()

    def close(self):
        self.driver.close()

    def create_greeting(self):
        # Use memgraph db
        with self.driver.session() as session:
            session.execute_write(self._create_and_return_greeting)

        # Use memgraph db
        with self.driver.session(database="memgraph") as session:
            session.execute_write(self._create_and_return_greeting)

        for i in range(3):
            with self.driver.session(database="db1") as session:
                session.execute_write(self._create_and_return_greeting)

        for i in range(1):
            with self.driver.session(database="db2") as session:
                session.execute_write(self._create_and_return_greeting)

        print("Write txns passed!")

        with self.driver.session(database="memgraph") as session:
            assert session.execute_read(self._get_count) == 2
            session.run("MATCH (n) DETACH DELETE n")

        print("Tested default db")

        with self.driver.session(database="db1") as session:
            assert session.execute_read(self._get_count) == 3
            session.run("MATCH (n) DETACH DELETE n")

        print("Tested db1")

        with self.driver.session(database="db2") as session:
            assert session.execute_read(self._get_count) == 1
            session.run("MATCH (n) DETACH DELETE n")

        print("Tested db2")

    @staticmethod
    def _create_and_return_greeting(tx):
        tx.run("CREATE (n:Greeting {message: 'Hello from Python'}) RETURN n.message AS message")

    @staticmethod
    def _get_count(tx):
        rec = tx.run("MATCH (n) RETURN count(n) AS c").single()
        return rec["c"]


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
