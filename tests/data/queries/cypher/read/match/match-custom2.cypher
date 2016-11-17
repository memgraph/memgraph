MATCH (user:User { name: 'Dominik', age: 24})-[has:HAS]->(item:Item) WHERE item.name = 'XPS 13' AND item.price = 11999.99 RETURN user, has, item
