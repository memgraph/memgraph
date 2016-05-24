# MATCH (user)-[:FRIEND]-(friend) WHERE user.name = "test" WITH user, count(friend) AS friends WHERE friends > 10 RETURN user
