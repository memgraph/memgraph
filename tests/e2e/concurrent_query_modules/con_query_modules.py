import sys

import pytest
from client import *

query = "MATCH (my_person:Person)-[:FOLLOW]->(follow_person:Person) MATCH (follow_person)-[: LIKE]->(post:Post) WHERE post.indexedAt IS NOT NULL AND NOT exists((post)-[:ROOT]->(:Post)) WITH localDateTime() - post.indexedAt as duration, post, follow_person WHERE duration.day < 5 WITH (duration.day * 24) + duration.hour as hour_age, post, follow_person ORDER BY post.indexedAt DESC LIMIT 500 MATCH(: Person) - [l: LIKE] -> (post) WITH count(l) as likes, hour_age, post, follow_person CALL libmodule_test.hacker_news(likes, 123, 4.1) YIELD score RETURN ID(post), post.uri, hour_age, likes, score, follow_person ORDER BY score DESC, hour_age ASC, post.indexedAt DESC LIMIT 100;"


def test_concurrent_module_access(client):
    client.initialize_to_execute(query, 200)
    client.initialize_to_execute(query, 200)
    client.initialize_to_execute(query, 200)

    success = client.execute_queries()
    assert success


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
