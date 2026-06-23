// ============================================================================
// feat/projection - multi-hop collapse + write-back
//
// Pattern: find multi-hop paths in the real graph, derive() collapses each path
// to ONE virtual edge between its endpoints (a smaller overlay graph), compute a
// graph metric over that overlay inside a read-only USE scope, then write the
// metric back onto the REAL nodes outside the scope.
//
// Verified end to end on the feat/projection build.
// ============================================================================

// Real graph: a chain A -SEG-> X -SEG-> B -SEG-> Y -SEG-> C  (4 real edges).
MATCH (n) DETACH DELETE n;
CREATE
  (a:Stop {name:'A'})-[:SEG]->(x:Stop {name:'X'}),
  (x)-[:SEG]->(b:Stop {name:'B'}),
  (b)-[:SEG]->(y:Stop {name:'Y'}),
  (y)-[:SEG]->(c:Stop {name:'C'});


// ----------------------------------------------------------------------------
// 1. Collapse: each 2-hop path becomes a single REACHES edge between endpoints.
//    4 real SEG edges -> 3 overlay REACHES edges (A->B, X->Y, B->C). The
//    intermediate hop of each path is dropped; derive() keeps only .front()/.back().
// ----------------------------------------------------------------------------
MATCH p=(:Stop)-[:SEG*2..2]->(:Stop)
WITH derive(p, {virtualEdgeType:'REACHES'}) AS g
RETURN size(g.nodes) AS overlay_nodes, size(g.edges) AS overlay_edges;
// expect: 5 nodes, 3 edges

MATCH p=(:Stop)-[:SEG*2..2]->(:Stop)
WITH derive(p, {virtualEdgeType:'REACHES'}) AS g
UNWIND g.edges AS e
RETURN startNode(e).name AS from, type(e) AS t, endNode(e).name AS to ORDER BY from, to;
// expect: A->B, B->C, X->Y


// ----------------------------------------------------------------------------
// 2. Compute over the collapsed overlay + write back to the real nodes.
//    outDegree(n) inside USE counts REACHES edges (the collapsed reach), NOT the
//    node's real SEG degree. SET runs OUTSIDE the read-only scope; reach2 is an
//    undeclared key, so it routes to the real origin node and persists.
// ----------------------------------------------------------------------------
MATCH p=(:Stop)-[:SEG*2..2]->(:Stop)
WITH derive(p, {virtualEdgeType:'REACHES'}) AS g
CALL { USE g MATCH (n) RETURN n AS node, outDegree(n) AS reach }
SET node.reach2 = reach
RETURN node.name AS name, reach ORDER BY name;
// expect: A=1, B=1, C=0, X=1, Y=0

// Separate statement reads the persisted value straight off the real nodes.
MATCH (s:Stop) RETURN s.name AS name, s.reach2 AS reach2 ORDER BY name;
// expect: identical to above - the write persisted to the real store


// ----------------------------------------------------------------------------
// Per-property write routing (propertyPolicy):
//   - undeclared key  -> origin   (persists; this is reach2 above)
//   - 'origin' key     -> origin   (persists)
//   - 'overlay' key    -> overlay  (compute-only, never persisted)
// A SET to an 'overlay'-declared key mutates the view but leaves the real node
// untouched - Memgraph as a compute layer.
// ----------------------------------------------------------------------------
MATCH p=(:Stop)-[:SEG*2..2]->(:Stop)
WITH derive(p, {virtualEdgeType:'REACHES', propertyPolicy: {scratch: 'overlay'}}) AS g
UNWIND g.nodes AS n
SET n.scratch = 123
RETURN n.name AS name, n.scratch AS in_query_value ORDER BY name;
// expect: scratch = 123 in-query (overlay holds it)

MATCH (s:Stop) RETURN s.name AS name, s.scratch AS persisted ORDER BY name;
// expect: persisted = null everywhere - the overlay write did NOT touch the store
