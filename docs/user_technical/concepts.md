## Concepts

### Weighted Shortest Path

Weighted shortest path problem is the problem of finding a path between two
nodes in a graph such that the sum of the weights of edges connecting nodes on
the path is minimized.
More about the *weighted shortest path* problem can be found
[here](https://en.wikipedia.org/wiki/Shortest_path_problem).

## Implementation

Our implementation of the *weighted shortest path* algorithm uses a modified
version of Dijkstra's algorithm that can handle length restriction. The length
restriction parameter is optional, and when it's not set it could increase the
complexity of the algorithm.

A sample query that finds a shortest path between two nodes can look like this:

      MATCH (a {id: 723})-[le *wShortest 10 (e, n | e.weight) total_weight]-(b {id: 882}) RETURN *

This query has an upper bound length restriction set to `10`. This means that no
path that traverses more than `10` edges will be considered as a valid result.


#### Upper Bound Implications

Since the upper bound parameter is optional, we can have different results based
on this parameter.

Lets take a look at the following graph and queries.

```
      5            5
      /-----[1]-----\
     /               \
    /                 \      2
  [0]                 [4]---------[5]
    \                 /
     \               /
      \--[2]---[3]--/
      3      3     3
```

      MATCH (a {id: 0})-[le *wShortest 3 (e, n | e.weight) total_weight]-(b {id: 5}) RETURN *

      MATCH (a {id: 0})-[le *wShortest   (e, n | e.weight) total_weight]-(b {id: 5}) RETURN *


The first query will try to find the weighted shortest path between nodes `0`
and `5` with the restriction on the path length set to `3`, and the second query
will try to find the weighted shortest path with no restriction on the path
length.

The expected result for the first query is `0 -> 1 -> 4 -> 5` with total cost of
`12`, while the expected result for the second query is `0 -> 2 -> 3 -> 4 -> 5`
with total cost of `11`. Obviously, the second query can find the true shortest
path because it has no restrictions on the length.

To handle cases when the length restriction is set, *weighted shortest path*
algorithm uses both vertex and distance as the state. This causes the search
space to increase by the factor of the given upper bound. On the other hand, not
setting the upper bound parameter, the search space might contain the whole
graph.

Because of this, one should always try to narrow down the upper bound limit to
be as precise as possible in order to have a more performant query.

