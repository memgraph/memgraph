# Extend and Compile Filter Expressions

Memgraph evaluates filter expression by traversing the abstract syntax tree of
the given filter. Filtering is a general operation in query execution.

Some simple examples are:
```
MATCH (n:Person {name: "John"}) WHERE n.age > 20 AND n.age < 40 RETURN n;
MATCH (a {id: 723})-[*bfs..10 (e, n | e.x > 12 AND n.y < 3)]-() RETURN *;
```

More real-world example looks like this (Ethereum network analysis):
```
MATCH (a: Address {addr: ''})-[]->(t: Transaction)-[]->(b: Address)
RETURN DISTINCT b.addr
UNION
MATCH (a: Address {addr: ''})-[]->(t: Transaction)-[]->(b1: Address)-[]->(t2: Transaction)-[]->(b: Address)
WHERE t2.timestamp > t.timestamp
RETURN DISTINCT b.addr
UNION
MATCH (a: Address {addr: ''})-[]->(t: Transaction)-[]->(b1: Address)-[]->(t2: Transaction)-[]->(b2: Address)-[]->(t3: Transaction)-[]->(b: Address)
WHERE t2.timestamp > t.timestamp AND t3.timestamp > t2.timestamp
return distinct b.addr
UNION
MATCH (a: Address {addr: ''})-[]->(t: Transaction)-[]->(b1: Address)-[]->(t2: Transaction)-[]->(b2: Address)-[]->(t3: Transaction)-[]->(b3: Address)-[]->(t4: Transaction)-[]->(b: Address)
WHERE t2.timestamp > t.timestamp AND t3.timestamp > t2.timestamp AND t4.timestamp > t3.timestamp
RETURN DISTINCT b.addr
UNION
MATCH (a: Address {addr: ''})-[]->(t: Transaction)-[]->(b1: Address)-[]->(t2: Transaction)-[]->(b2: Address)-[]->(t3: Transaction)-[]->(b3: Address)-[]->(t4: Transaction)-[]->(b4: Address)-[]->(t5: Transaction)-[]->(b: Address)
WHERE t2.timestamp > t.timestamp AND t3.timestamp > t2.timestamp AND t4.timestamp > t3.timestamp AND t5.timestamp > t4.timestamp
RETURN DISTINCT b.addr;
```

Filtering may take a significant portion of query execution, which means it has
to be fast. Furthermore, filtering has to be arbitrarily complex. The most
compelling case to improve from the expressivity perspective is variable-length
expansions like DFS/BFS/WeightedShortestPath. They contain an expression that
gets a limited set of inputs (only currently visited node/edge). A noticeable
improvement would be to pass the current path as well.

The first step towards improvement might be to expose an API under which a
developer can implement its filtering logic (it's OK to support only C++ in the
beginning). Later on, we can introduce an automatic compilation of filtering
expressions.

The goals of this feature are:

* Improve query execution times by improving expression evaluation.
* Extend current filtering abilities, DFS/BFS/WeightedShortestPath are all
  great places to start.

