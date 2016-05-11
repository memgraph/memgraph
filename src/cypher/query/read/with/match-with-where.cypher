MATCH (user)-[:FRIEND]-(friend) WHERE user.name = {name} WITH user, count(friend) AS friends WHERE friends > 10 RETURN user
