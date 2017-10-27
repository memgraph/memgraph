We tried generating a few sample graphs from the description given at
the in-person meetings.  Then, we tried writing queries that would solve
the problem of updating nodes when a leaf value changes, assuming all the
internal nodes compute only the sum function.

We started by creating an index on `id` property to improve initial lookup
performance:

    CREATE INDEX ON :Leaf(id)

Afther that, we set values of all leafs to 1:

    MATCH (u:Leaf) SET u.value = 1

We then initialized the values of all other nodes in the graph:

    MATCH (u) WHERE NOT u:Leaf SET u.value = 0

    MATCH (u) WITH u
    ORDER BY u.topological_index DESC
    MATCH (u)-->(v) SET u.value = u.value + v.value

Leaf value change and update of affected values in the graph can
be done using three queries. To change the value of a leaf:

    MATCH (u:Leaf {id: "18"}) SET u.value = 10

Then we had to reset all the affected nodes to the neutral element:

    MATCH (u:Leaf {id: "18"})<-[* bfs]-(v)
    WHERE NOT v:Leaf SET v.value = 0

Finally, we recalculated their values in topological order:

    MATCH (u:Leaf {id: "18"})<-[* bfs]-(v)
    WITH v ORDER BY v.topological_index DESC
    MATCH (v)-->(w) SET v.value = v.value + w.value

There are a few assumptions necessary for the approach above to work.

* We are able to maintain topological order of vertices during graph
  structure changes.  

* It is possible to accumulate the value of the function.  Formally: 
  $$f(x_1, x_2, ..., x_n) = g(...(g(g(x_1, x_2), x_3), ...), x_n)$$

* There is a neutral element for the operation. However, this
  assumption can be dropped by introducing an artificial neutral element.

Above assumptions could be changed, relaxed or dropped, depending on the
specifics of the use case.

Number of operations required is proportional to the sum of degrees of affected
nodes. 
