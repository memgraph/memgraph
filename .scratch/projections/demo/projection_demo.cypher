// ============================================================================
// feat/projection demo
//
// A projection is a VALUE you build (virtualGraph / project / derive). USE binds
// it as the AMBIENT graph for a CALL { ... } block, so ordinary Cypher runs
// against the view instead of the real graph. v1 is read-only, single-level,
// full-scan.
//
// The runner splits this file on the @@ sentinels. Do not remove them.
// ============================================================================


// @@MAIN

// ----------------------------------------------------------------------------
// Setup: a small org graph. :Person(id) is indexed on purpose (see Act 6).
// ----------------------------------------------------------------------------
CREATE INDEX ON :Person(id);
CREATE
  (a:Person {id: 1, name: 'Ana',  team: 'A', score: 10}),
  (b:Person {id: 2, name: 'Ben',  team: 'A', score: 20}),
  (c:Person {id: 3, name: 'Cara', team: 'B', score: 30}),
  (d:Team   {id: 9, name: 'Platform'}),
  (a)-[:KNOWS]->(b),
  (b)-[:KNOWS]->(c),
  (a)-[:KNOWS]->(c),
  (a)-[:MEMBER_OF]->(d),
  (b)-[:MEMBER_OF]->(d);


// ============================================================================
// Act 1 - Three ways to construct a projection value
// ============================================================================

// (1) virtualGraph(): assemble a graph from explicit node and edge lists.
WITH [virtualNode(1, 'N', {x: 10}), virtualNode(2, 'N', {x: 20})] AS nodes,
     [virtualEdge('R', 1, 2)] AS edges
RETURN virtualGraph(nodes, edges) AS g;

// (2) project(): a view over a real-graph subgraph captured by a MATCH path.
MATCH p=(:Person)-[:KNOWS]->(:Person)
RETURN project(p) AS sg;

// (3) derive(): an overlay over a matched path with a synthetic edge type.
MATCH p=(:Person {id:1})-[:KNOWS]->(:Person {id:2})
RETURN derive(p, {virtualEdgeType: 'LINK'}) AS g;


// ============================================================================
// Act 2 - The headline: USE binds the projection as the ambient graph
// ============================================================================

// MATCH inside the block scans the PROJECTION's nodes, not the real graph.
// expect: 10, 20
WITH [virtualNode(1, 'N', {x: 10}), virtualNode(2, 'N', {x: 20})] AS nodes, [] AS edges
WITH virtualGraph(nodes, edges) AS g
CALL { USE g MATCH (n) RETURN n.x AS x }
RETURN x ORDER BY x;

// Scoped: outside the block sees the real graph, inside sees the view.
// expect: real_count = full real graph count, and x = 10
MATCH (r) WITH count(r) AS real_count
WITH real_count, virtualGraph([virtualNode(1, 'N', {x: 10})], []) AS g
CALL { USE g MATCH (n) RETURN n.x AS x }
RETURN real_count, x;

// Expansion + type filter + label filter + property predicate, all in-scope.
// expect: 1, 'R', 2
WITH [virtualNode(1,'A',{x:1}), virtualNode(2,'B',{x:2}), virtualNode(3,'A',{x:3})] AS nodes,
     [virtualEdge('R',1,2), virtualEdge('S',1,3)] AS edges
WITH virtualGraph(nodes, edges) AS g
CALL { USE g MATCH (a:A)-[r:R]->(b) WHERE b.x > 1 RETURN a.x AS ax, type(r) AS t, b.x AS bx }
RETURN ax, t, bx;


// ============================================================================
// Act 3 - A subgraph view of the real graph (project)
// ============================================================================

// USE scans only the subgraph members, and expansion respects membership:
// the MEMBER_OF edges and the Team node are NOT in the KNOWS projection.
// expect: only KNOWS edges among Ana/Ben/Cara, no MEMBER_OF, no Platform
MATCH p=(:Person)-[:KNOWS]->(:Person)
WITH project(p) AS sg
CALL { USE sg MATCH (x)-[r]->(y) RETURN x.name AS xn, type(r) AS t, y.name AS yn }
RETURN xn, t, yn ORDER BY xn, yn;


// ============================================================================
// Act 4 - derive: read-through, overlay, and hidden properties
// ============================================================================

// Read-through: the view exposes the origin's real property values.
// expect: Ben, 20
MATCH p=(:Person {id:1})-[:KNOWS]->(:Person {id:2})
WITH derive(p, {virtualEdgeType:'LINK'}) AS g
CALL { USE g MATCH (n) WHERE n.score > 15 RETURN n.name AS name, n.score AS s }
RETURN name, s;

// Overlay shadows an origin value, and hidden makes a key invisible to reads + predicates.
// expect: 999, null
MATCH p=(:Person {id:1})-[:KNOWS]->(:Person {id:2})
WITH derive(p, {virtualEdgeType:'LINK',
                propertyPolicy: {score: 'overlay', team: 'hidden'},
                sourceNodeProperties: {score: 999}}) AS g
CALL { USE g MATCH (n) WHERE n.id = 1 RETURN n.score AS overlaid, n.team AS hidden }
RETURN overlaid, hidden;


// ============================================================================
// Act 5 - Topology differs from the real graph (the point of a projection)
// ============================================================================

// Ana has real out-degree 3 (two KNOWS + one MEMBER_OF), but the projection
// built from a single edge gives her projection out-degree 1.
// expect: 1, 1, 0
MATCH p=(:Person {id:1})-[:KNOWS]->(:Person {id:2})
WITH derive(p, {virtualEdgeType:'LINK'}) AS g
CALL { USE g MATCH (n) WHERE n.id = 1 RETURN degree(n) AS d, outDegree(n) AS od, inDegree(n) AS ind }
RETURN d, od, ind;

// The same node's real-graph out-degree, for contrast.
// expect: 3
MATCH (a:Person {id:1}) RETURN outDegree(a) AS real_out_degree;


// ============================================================================
// Act 6 - Full-scan guarantee
// ============================================================================

// A label/property match in-scope IGNORES the real :Person(id) index and reads
// the projection. This is correctness, not a missed optimization: an index scan
// would silently read the real graph.
// expect: 1, from the projection
WITH virtualGraph([virtualNode(1,'Person',{id:1}), virtualNode(2,'Person',{id:2})], []) AS g
CALL { USE g MATCH (n:Person {id: 1}) RETURN n.id AS hit }
RETURN count(hit) AS hits;


// @@OPTIONAL_PROC
// ============================================================================
// Act 7 - A procedure called in-scope picks up the ambient view.
// Runs only with --with-procedures (installs the read.* test module).
// ============================================================================

// read.subgraph_get_vertices() takes no graph argument - the ambient USE view is
// injected, so the procedure iterates the projection's members.
MATCH p=(:Person)-[:KNOWS]->(:Person)
WITH project(p) AS sg
CALL { USE sg CALL read.subgraph_get_vertices() YIELD node RETURN node.name AS name }
RETURN name ORDER BY name;


// @@EXPECTED_FAILURE
// ============================================================================
// Act 8 - Guardrails (these statements are MEANT to raise SemanticException)
// ============================================================================

// Read-only boundary: a write clause inside USE is rejected.
WITH virtualGraph([virtualNode(1,'N',{})], []) AS g
CALL { USE g CREATE (x) } RETURN 1;

// Single-level nesting: a USE inside a USE is rejected in v1.
WITH 1 AS g, 2 AS h
CALL { USE g CALL { USE h MATCH (n) RETURN n } RETURN n } RETURN n;
