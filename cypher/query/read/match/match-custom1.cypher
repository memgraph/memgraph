MATCH (user:User { name: 'Dominik', age: 8 + 4})-[has:HAS|IS|CAN { duration: 'PERMANENT'}]->(item:Item)--(shop) RETURN shop
