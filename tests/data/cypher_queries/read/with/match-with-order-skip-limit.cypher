# MATCH (user)-[:FRIEND]-(friend) WITH user, count(friend) AS friends ORDER BY friends DESC SKIP 1 LIMIT 3 RETURN user
