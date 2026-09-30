CREATE (alice:Person {name: 'Alice', age: 30, city: 'New York'}), (toyota:Car {model: 'Toyota', year: 2020, color: 'red'}), (alice)-[:OWNS {since: 2020, status: 'active'}]->(toyota);
