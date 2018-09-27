## Graph Algorithms

### Filtering Variable Length Paths

OpenCypher supports only simple filtering when matching variable length paths.
For example:

```opencypher
MATCH (n)-[edge_list:Type * {x: 42}]-(m)
```

This will produce only those paths whose edges have the required `Type` and `x`
property value. Edges that compose the produced paths are stored in a symbol
named `edge_list`. Naturally, the user could have specified any other symbol
name.

Memgraph extends openCypher with a syntax for arbitrary filter expressions
during path matching. The next example filters edges which have property `x`
between `0` and `10`.

```opencypher
MATCH (n)-[edge_list * (edge, node | 0 < edge.x < 10)]-(m)
```

Here we introduce a lambda function with parentheses, where the first two
arguments, `edge` and `node`, correspond to each edge and node during path
matching. `node` is the destination node we are moving to across the current
`edge`. The last `node` value will be the same value as `m`. Following the
pipe (`|`) character is an arbitrary expression which must produce a boolean
value.  If `True`, matching continues, otherwise the path is discarded.

The previous example can be written using the `all` function:

```opencypher
MATCH (n)-[edge_list *]-(m) WHERE all(edge IN edge_list WHERE 0 < edge.x < 10)
```

However, filtering using a lambda function is more efficient because paths
may be discarded earlier in the traversal. Furthermore, it provides more
flexibility for deciding what kind of paths are matched due to more expressive
filtering capabilities. Therefore, filtering through lambda functions should
be preferred whenever possible.

### Breadth First Search

A typical graph use-case is searching for the shortest path between nodes.
The openCypher standard does not define this feature, so Memgraph provides
a custom implementation, based on the edge expansion syntax.

Finding the shortest path between nodes can be done using breadth-first
expansion:

```opencypher
MATCH (a {id: 723})-[edge_list:Type *bfs..10]-(b {id: 882}) RETURN *
```

The above query will find all paths of length up to 10 between nodes `a` and `b`.
The edge type and maximum path length are used in the same way like in variable
length expansion.

To find only the shortest path, simply append `LIMIT 1` to the `RETURN` clause.

```opencypher
MATCH (a {id: 723})-[edge_list:Type *bfs..10]-(b {id: 882}) RETURN * LIMIT 1
```

Breadth-first expansion allows an arbitrary expression filter that determines
if an expansion is allowed. Following is an example in which expansion is
allowed only over edges whose `x` property is greater than `12` and nodes `y`
whose property is less than `3`:

```opencypher
MATCH (a {id: 723})-[*bfs..10 (e, n | e.x > 12 AND n.y < 3)]-() RETURN *
```

The filter is defined as a lambda function over `e` and `n`, which denote the edge
and node being expanded over in the breadth first search. Note that if the user
omits the edge list symbol (`edge_list` in previous examples) it will not be included
in the result.

There are a few benefits of the breadth-first expansion approach, as opposed to
a specialized `shortestPath` function. For one, it is possible to inject
expressions that filter on nodes and edges along the path itself, not just the final
destination node. Furthermore, it's possible to find multiple paths to multiple destination
nodes regardless of their length. Also, it is possible to simply go through a node's
neighbourhood in breadth-first manner.

Currently, it isn't possible to get all shortest paths to a single node using
Memgraph's breadth-first expansion.

### Weighted Shortest Path

Another standard use-case in a graph is searching for the weighted shortest
path between nodes. The openCypher standard does not define this feature, so
Memgraph provides a custom implementation, based on the edge expansion syntax.

Finding the weighted shortest path between nodes is done using the weighted
shortest path expansion:

```opencypher
MATCH (a {id: 723})-[
        edge_list *wShortest 10 (e, n | e.weight) total_weight
    ]-(b {id: 882})
RETURN *
```

The above query will find the shortest path of length up to 10 nodes between
nodes `a`  and `b`. The length restriction parameter is optional.

Weighted Shortest Path expansion allows an arbitrary expression that determines
the weight for the current expansion. Total weight of a path is calculated as
the sum of all weights on the path between two nodes. Following is an example in
which the weight between nodes is defined as the product of edge weights
(instead of sum), assuming all weights are greater than '1':

```opencypher
MATCH (a {id: 723})-[
        edge_list *wShortest 10 (e, n | log(e.weight)) total_weight
    ]-(b {id: 882})
RETURN exp(total_weight)
```

Weighted Shortest Path expansions also allows an arbitrary expression filter
that determines if an expansion is allowed. Following is an example in which
expansion is allowed only over edges whose `x` property is greater than `12`
and nodes `y` whose property is less than `3`:

```opencypher
MATCH (a {id: 723})-[
        edge_list *wShortest 10 (e, n | e.weight) total_weight (e, n | e.x > 12 AND n.y < 3)
    ]-(b {id: 882})
RETURN exp(total_weight)
```

Both weight and filter expression are defined as lambda functions over `e` and
`n`, which denote the edge and the node being expanded over in the weighted
shortest path search.
