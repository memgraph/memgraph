Feature: K Shortest Paths

  # This feature file documents both the CURRENT behavior of KSHORTEST and the
  # behavior we WANT to achieve.
  #
  #   * Scenarios under "Current behavior" pass on today's build. KSHORTEST
  #     today ranks paths by HOP COUNT and accepts no lambda.
  #
  #   * Scenarios under "Desired behavior" encode the target feature: a weight
  #     lambda + `total_weight` so KSHORTEST returns the K LEAST-COST paths
  #     (weighted), and the A*-style variant where a heuristic is folded into
  #     the weight lambda via reduced edge costs. These currently FAIL (the
  #     parser rejects a lambda on KSHORTEST) and are the spec for the work.
  #
  # All scenarios share the weighted DAG in graphs/kshortest.cypher.

  ########################################################################
  # Current behavior: KSHORTEST ranks by hop count, no lambda.
  ########################################################################

  Scenario: KSHORTEST by hop count returns the single fewest-hop path
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 1]->(t)
          RETURN [n IN nodes(p) | n.name] AS path, size(r) AS hops
          """
      Then the result should be:
          | path         | hops |
          | ['S', 'T']   | 1    |

  Scenario: KSHORTEST by hop count enumerates every path, fewest hops first
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 10]->(t)
          RETURN [n IN nodes(p) | n.name] AS path, size(r) AS hops
          """
      Then the result should be:
          | path                        | hops |
          | ['S', 'T']                  | 1    |
          | ['S', 'A', 'T']             | 2    |
          | ['S', 'B', 'T']             | 2    |
          | ['S', 'A', 'B', 'T']        | 3    |

  ########################################################################
  # Desired behavior: weighted KSHORTEST returns the K least-cost paths.
  # (Currently fails: "KSHORTEST expansion does not support filter lambda.")
  ########################################################################

  Scenario: Weighted KSHORTEST returns the single least-cost path
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 1 (e, n | e.weight) total_weight]->(t)
          RETURN [n IN nodes(p) | n.name] AS path, total_weight
          """
      Then the result should be:
          | path                   | total_weight |
          | ['S', 'A', 'B', 'T']   | 4            |

  Scenario: Weighted KSHORTEST returns the two least-cost paths, cheapest first
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 2 (e, n | e.weight) total_weight]->(t)
          RETURN [n IN nodes(p) | n.name] AS path, total_weight
          ORDER BY total_weight
          """
      Then the result should be, in order:
          | path                   | total_weight |
          | ['S', 'A', 'B', 'T']   | 4            |
          | ['S', 'B', 'T']        | 5            |

  Scenario: Weighted KSHORTEST enumerates all paths in increasing cost order
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 10 (e, n | e.weight) total_weight]->(t)
          RETURN [n IN nodes(p) | n.name] AS path, total_weight
          ORDER BY total_weight
          """
      Then the result should be, in order:
          | path                   | total_weight |
          | ['S', 'A', 'B', 'T']   | 4            |
          | ['S', 'B', 'T']        | 5            |
          | ['S', 'A', 'T']        | 6            |
          | ['S', 'T']             | 10           |

  Scenario: Weighted KSHORTEST stops at K even when more paths exist
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 3 (e, n | e.weight) total_weight]->(t)
          RETURN count(p) AS returned_paths
          """
      Then the result should be:
          | returned_paths |
          | 3              |

  ########################################################################
  # Desired behavior: A*-guided KSHORTEST.
  #
  # A* is Dijkstra on reduced edge costs w'(u->v) = w(u,v) - h(u) + h(v),
  # where h is a consistent heuristic (here the node `h` property). The
  # heuristic only reorders exploration; the K paths and their REAL weights
  # are identical to the plain weighted run above. We recover the real weight
  # with reduce() so the heuristic offset does not leak into the result.
  ########################################################################

  Scenario: A*-guided KSHORTEST yields the same K least-cost paths
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 2 (e, n | e.weight - startNode(e).h + n.h) reduced]->(t)
          RETURN [n IN nodes(p) | n.name] AS path,
                 reduce(acc = 0, e IN r | acc + e.weight) AS real_weight
          ORDER BY real_weight
          """
      Then the result should be, in order:
          | path                   | real_weight |
          | ['S', 'A', 'B', 'T']   | 4           |
          | ['S', 'B', 'T']        | 5           |
