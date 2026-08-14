// Small weighted DAG used by the KSHORTEST feature tests.
//
// Topology (all edges :CONN, directed S -> ... -> T):
//
//        1          1          2
//    S ------> A ------> B ------> T
//    |          \                 ^
//    | 3         5 \               |
//    +-----------> B?  (see below) |
//
// Explicit edges and weights:
//   S -> A : 1
//   S -> B : 3
//   S -> T : 10
//   A -> B : 1
//   A -> T : 5
//   B -> T : 2
//
// All four distinct S -> T paths, by TOTAL WEIGHT (least-cost first):
//   S-A-B-T : 1+1+2 = 4
//   S-B-T   : 3+2   = 5
//   S-A-T   : 1+5   = 6
//   S-T     : 10    = 10
//
// The same paths by HOP COUNT (fewest hops first):
//   S-T       : 1 hop
//   S-A-T     : 2 hops
//   S-B-T     : 2 hops
//   S-A-B-T   : 3 hops
//
// The two orderings are reversed at the extremes, which is what makes this
// graph useful: it shows the difference between today's hop-count KSHORTEST
// and the weighted least-cost KSHORTEST we want.
//
// The `h` property on each node is a (perfect, consistent) heuristic estimate
// of the remaining distance to T. It is used by the A*-style scenario to drive
// the search via reduced edge costs:  w'(u->v) = w(u,v) - h(u) + h(v).
CREATE
  (s:Node {name: 'S', h: 4}),
  (a:Node {name: 'A', h: 3}),
  (b:Node {name: 'B', h: 2}),
  (t:Node {name: 'T', h: 0}),
  (s)-[:CONN {weight: 1}]->(a),
  (s)-[:CONN {weight: 3}]->(b),
  (s)-[:CONN {weight: 10}]->(t),
  (a)-[:CONN {weight: 1}]->(b),
  (a)-[:CONN {weight: 5}]->(t),
  (b)-[:CONN {weight: 2}]->(t);
