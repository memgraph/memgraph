CREATE (a:N {lat: 0, lon: 0, name: 'a'}), (b:N {lat: 1, lon: 0, name: 'b'}), (c:N {lat: 2, lon: 0, name: 'c'}), (b)-[:E {distance: 4}]->(a), (b)-[:E {distance: 6}]->(c)
