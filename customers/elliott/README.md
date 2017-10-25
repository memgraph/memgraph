DISCLAIMER: this is just an initial test, graph might not resemble
the graph in the use case at all and the data might be completely
irrelevant.

We tried generating a few sample graphs from the vague description
given in the use case doc. Then we tried writing queries that would
solve the problem of updating nodes when a leaf value changes,
assuming all the internal nodes compute only the sum function.

We start by creating an index on `id` property to improve initial lookup
performance:

    CREATE INDEX ON :Leaf(id)

Set values of all leafs to 1:

    MATCH (u:Leaf) SET u.value = 1

Now we initialize the values of all other nodes in the graph:

    MATCH (u) WHERE NOT u:Leaf SET u.value = 0

    MATCH (u) WITH u
    ORDER BY u.topological_index DESC
    MATCH (u)-->(v) SET u.value = u.value + v.value

Change the value of a leaf:

    MATCH (u:Leaf {id: "9"}) SET u.value = 10

We have to reset all the updated nodes to a neutral element:

    MATCH (u:Leaf {id: "18"})<-[* bfs]-(v)
    WHERE NOT v:Leaf SET v.value = 0

Finally, we recalculate their values in topological order:

    MATCH (u:Leaf {id: "18"})<-[* bfs]-(v)
    WITH v ORDER BY v.topological_index DESC
    MATCH (v)-->(w) SET v.value = v.value + w.value

There are a few assumptions made worth pointing out.

* We are able to efficiently maintain topological order
  of vertices in the graph.

* It is possible to accumulate the value of the function.  Formally: 
  $$f(x_1, x_2, ..., x_n) = g(...(g(g(x_1, x_2), x_3), ...), x_n).$$

* There is a neutral element for the operation. However, this
  assumption can be dropped by introducing an artificial neutral element.

Number of operations required is proportional to sum of degrees of affected
nodes.

We generated graph with $10^5$ nodes ($20\ 000$ nodes in each layer),  varied the
degree distribution in node layers and measured time for the query to execute:

| # |  Root-Category-Group degree | Group-CustomGroup-Leaf degree |   Time    |
|:-:|:---------------------------:|:-----------------------------:|:---------:|
| 1 |            [1, 10]          |           [20, 40]            |   ~1.1s   |
| 2 |            [1, 10]          |          [50, 100]            |   ~2.5s   |
| 3 |           [10, 50]          |          [50, 100]            |   ~3.3s   |

Due to the structure of the graph, update of a leaf required update of almost
all the nodes in the graph so we don't show times required for initial graph
update and update after leaf change separately.

However, there is not enough info on the use case to make the test more
sophisticated.
