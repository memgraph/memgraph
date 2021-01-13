# Extend Variable-length Filter Expressions

Variable-length filtering (DFS/BFS/WeightedShortestPath) can to be arbitrarily
complex. At this point, the filtering expression only gets currently visited
node and edge:

```
MATCH (a {id: 723})-[*bfs..10 (e, n | e.x > 12 AND n.y < 3)]-() RETURN *;
```

If a user had the whole path available, he would write more complex filtering
logic.
