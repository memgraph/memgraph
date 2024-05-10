Feature: Queries related to all Stackoverflow questions related to WSP

  Scenario: Dijkstra with pre calculated cost

    Given graph "wsp1"
    When executing query:
      """
      MATCH (a {id: 1})-[
        edges *wShortest
        (e, n | (e.distance * 0.25) + (e.noise_level * 0.25) + (e.pollution_level * 0.5))
        total_weight
      ]-(b {id: 4})
      RETURN startNode(head(edges)).id + reduce(acc = "", edge IN edges | acc + " -> " + endNode(edge).id) AS path, total_weight;
      """
    Then the result should be:
      | path        | total_weight |
      |'1 -> 2 -> 4'| 4.75         |

  Scenario: Using more than one property Dijkstra

    Given graph "wsp2"
    When executing query:
      """
      MATCH (a {id: 1})-[edges *wShortest (e, n | e.weight1 + e.weight2) total_weight]-(b {id: 4})
      RETURN startNode(head(edges)).id + reduce(acc = "", edge IN edges | acc + " -> " + endNode(edge).id) AS path, total_weight;
      """
    Then the result should be:
      | path        | total_weight |
      |'1 -> 3 -> 4'| 17           |
