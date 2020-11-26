# A-star Variable-length Expand

Like DFS/BFS/WeightedShortestPath, it should be possible to support the A-star
algorithm in the format of variable length expansion.

Syntactically, the query should look like the following one:
```
MATCH (start)-[
          *aStar{{hops}} {{heuristic_expression} {{weight_expression}} {{aggregated_weight_variable}} {{filtering_expression}}
      ]-(end)
RETURN {{aggregated_weight_variable}};
```

It would be convenient to add geospatial data support before because A-star
works well with geospatial data (heuristic function might exist).
