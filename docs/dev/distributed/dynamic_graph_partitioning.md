## Dynamic Graph Partitioning

Memgraph supports dynamic graph partitioning similar to the Spinner algorithm, 
mentioned in this paper: [https://arxiv.org/pdf/1404.3861.pdf].

Dgp is useful because it tries to group `local` date on the same worker, i.e. 
it tries to keep closely connected data on one worker. It tries to avoid jumps 
across workers when querying/traversing the distributed graph.

### Our implementation

It works independently on each worker but it is always running the migration 
on only one worker at the same time. It achieves that by sharing a token 
between workers, and the token ownership is transferred to the next worker 
when the current worker finishes its migration step. 

The reason that we want workers to work in  disjoint time slots is it avoid 
serialization errors caused by creating/removing edges of vertices during 
migrations, which might cause an update of some vertex from two or more 
different transactions.

### Migrations

For each vertex and workerid (label in the context of Dgp algorithm) we define  
a score function. Score function takes into account labels of surrounding 
endpoints of vertex edges (in/out) and the capacity of the worker with said 
label. Score function loosely looks like this 
```
locality(v, l) = 
count endpoints of edges of vertex `v` with label `l` / degree of `v`

capacity(l) = 
number of vertices on worker `l` divided by the worker capacity 
(usually equal to the average number of vertices per worker)

score(v, l) = locality(v, l) - capacity(l)
```
We also define two flags alongside ```dynamic_graph_partitioner_enabled```, 
   ```dgp_improvement_threshold``` and ```dgp_max_batch_size```.

These two flags are used during the migration phase.
When deciding if we need to migrate some vertex `v` from worker `l1` to worker 
`l2` we examine the difference in scores, i.e.
if score(v, l1) - dgp_improvement_threshold / 100 < score(v, l2) then we 
migrate the vertex. 

Max batch size flag limits the number of vertices we can transfer in one batch 
(one migration step). 
Setting this value to a too large value will probably cause
a lot of interference with client queries, and having it a small value
will slow down convergence of the algorithm.
