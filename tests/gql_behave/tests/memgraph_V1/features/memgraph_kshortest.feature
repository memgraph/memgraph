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
  #     the weight lambda via reduced edge costs. The parser now accepts these,
  #     but the cursor still ranks by hop count and does not populate
  #     total_weight, so they FAIL at the result level until the cursor is made
  #     weight-aware. They are the spec for the remaining work.
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
  # (Parses now; fails until the cursor honors the weight lambda + total_weight.)
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
  # Desired behavior: weighted KSHORTEST with an optional filter lambda,
  # mirroring WSHORTEST's two-lambda form:
  #   *KSHORTEST | K (weight_lambda) total_weight (filter_lambda)
  # The filter lambda prunes edges/nodes during expansion. Here it excludes
  # node B, leaving only the two paths that avoid it (S-A-T and S-T).
  ########################################################################

  Scenario: Weighted KSHORTEST with a filter lambda excludes pruned paths
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 10 (e, n | e.weight) total_weight (e, n | n.name <> 'B')]->(t)
          RETURN [n IN nodes(p) | n.name] AS path, total_weight
          ORDER BY total_weight
          """
      Then the result should be, in order:
          | path              | total_weight |
          | ['S', 'A', 'T']   | 6            |
          | ['S', 'T']        | 10           |

  ########################################################################
  # Desired behavior: A*-guided KSHORTEST via a first-class HEURISTIC lambda.
  #
  # HEURISTIC (e, n | ...) estimates the remaining cost from n to the target and
  # only reorders the search (priority f = g + h). The K paths and the reported
  # total_weight are the TRUE least-cost results (no reduced-cost offset). The node
  # `h` property is a consistent, admissible estimate of the distance to T.
  ########################################################################

  Scenario: A*-guided KSHORTEST yields the same K least-cost paths with true costs
      Given graph "kshortest"
      When executing query:
          """
          MATCH (s:Node {name: 'S'}), (t:Node {name: 'T'})
          WITH s, t
          MATCH p = (s)-[r *KSHORTEST | 2 (e, n | e.weight) total_weight HEURISTIC (e, n | n.h)]->(t)
          RETURN [n IN nodes(p) | n.name] AS path, total_weight
          ORDER BY total_weight
          """
      Then the result should be, in order:
          | path                   | total_weight |
          | ['S', 'A', 'B', 'T']   | 4            |
          | ['S', 'B', 'T']        | 5            |
