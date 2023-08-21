Feature: Creating map values

  Scenario: Creating a map with multiple properties from a vertex
    Given an empty graph
    And having executed
      """
      CREATE (:Employee {name: "Andy", surname: "Walker", age: 24, id: 1234});
      """
    When executing query:
      """
      MATCH (e:Employee) RETURN {name: e.name, surname: e.surname, age: e.age} AS public_data;
      """
    Then the result should be:
      | public_data                                |
      | {age: 24, name: 'Andy', surname: 'Walker'} |

  Scenario: Creating instances of a map with multiple properties from a vertex
    Given an empty graph
    And having executed
      """
      CREATE (:Employee {name: "Andy", surname: "Walker", age: 24, id: 1234}), (:Person), (:Person);
      """
    When executing query:
      """
      MATCH (e:Employee), (n:Person) RETURN {name: e.name, surname: e.surname, age: e.age} AS public_data;
      """
    Then the result should be:
      | public_data                                |
      | {age: 24, name: 'Andy', surname: 'Walker'} |
      | {age: 24, name: 'Andy', surname: 'Walker'} |

  Scenario: Creating a map with multiple properties from each vertex
    Given an empty graph
    And having executed
      """
      CREATE (:Cat {name: "Luigi", age: 11}), (:Dog {name: "Don", age: 10}), (:Owner {name: "Ivan"});
      """
    When executing query:
      """
      MATCH (m:Cat), (n:Dog), (o:Owner) SET o += {catName: m.name, catAge: m.age, dogName: n.name, dogAge: n.age} RETURN o;
      """
    Then the result should be:
      | o                                                                                 |
      | (:Owner {catAge: 11, catName: 'Luigi', dogAge: 10, dogName: 'Don', name: 'Ivan'}) |

  Scenario: Creating distinct maps with multiple properties, each from one vertex
    Given an empty graph
    And having executed
      """
      FOREACH (i in range(1, 5) | CREATE (:Node {prop1: i, prop2: 2 * i}));
      """
    When executing query:
      """
      MATCH (n) RETURN {prop1: n.prop1, prop2: n.prop2} AS prop_data;
      """
    Then the result should be:
      | prop_data             |
      | {prop1: 1, prop2: 2}  |
      | {prop1: 2, prop2: 4}  |
      | {prop1: 3, prop2: 6}  |
      | {prop1: 4, prop2: 8}  |
      | {prop1: 5, prop2: 10} |

  Scenario: Creating a map with multiple properties from a vertex; one property is null
    Given an empty graph
    And having executed
      """
      CREATE (:Employee {name: "Andy", surname: "Walker", age: 24, id: 1234});
      """
    When executing query:
      """
      MATCH (e:Employee) RETURN {name: e.name, surname: e.surname, age: e.age, null_prop: e.nonexistent} AS public_data;
      """
    Then the result should be:
      | public_data                                                 |
      | {age: 24, name: 'Andy', null_prop: null, surname: 'Walker'} |

  Scenario: Creating a map with multiple properties from an edge
    Given an empty graph
    And having executed
      """
      CREATE (m)-[:ROUTE {km: 466, cross_border: true}]->(n);
      """
    When executing query:
      """
      MATCH ()-[r:ROUTE]->() RETURN {km: r.km, cross_border: r.cross_border} AS route_data;
      """
    Then the result should be:
      | route_data                    |
      | {cross_border: true, km: 466} |

  Scenario: Creating instances of a map with multiple properties from an edge
    Given an empty graph
    And having executed
      """
      CREATE (m)-[:ROUTE {km: 466, cross_border: true}]->(n), (:City), (:City);
      """
    When executing query:
      """
      MATCH (:City), ()-[r:ROUTE]->() RETURN {km: r.km, cross_border: r.cross_border} AS route_data;
      """
    Then the result should be:
      | route_data                    |
      | {cross_border: true, km: 466} |
      | {cross_border: true, km: 466} |

  Scenario: Creating a map with multiple properties from each edge
    Given an empty graph
    And having executed
      """
      CREATE (m)-[:HIGHWAY {km: 466, cross_border: true}]->(n), (m)-[:FLIGHT {km: 350, daily: true}]->(n);
      """
    When executing query:
      """
      MATCH ()-[h:HIGHWAY]->(), ()-[f:FLIGHT]->()
      RETURN {km_hwy: h.km, cross_border: h.cross_border, km_air: f.km, daily_flight: f.daily} AS routes_data;
      """
    Then the result should be:
      | routes_data                                                        |
      | {cross_border: true, daily_flight: true, km_air: 350, km_hwy: 466} |

  Scenario: Creating distinct maps with multiple properties, each from one edge
    Given an empty graph
    And having executed
      """
      MERGE (m:City) MERGE (n:Country) FOREACH (i in range(1, 5) | CREATE (m)-[:IN {prop1: i, prop2: 2 * i}]->(n));
      """
    When executing query:
      """
      MATCH (m)-[r]->(n) RETURN {prop1: r.prop1, prop2: r.prop2} AS prop_data;
      """
    Then the result should be:
      | prop_data             |
      | {prop1: 1, prop2: 2}  |
      | {prop1: 2, prop2: 4}  |
      | {prop1: 3, prop2: 6}  |
      | {prop1: 4, prop2: 8}  |
      | {prop1: 5, prop2: 10} |

  Scenario: Creating a map with multiple properties from an edge; one property is null
    Given an empty graph
    And having executed
      """
      CREATE (m)-[:ROUTE {km: 466, cross_border: true}]->(n);
      """
    When executing query:
      """
      MATCH ()-[r:ROUTE]->() RETURN {km: r.km, cross_border: r.cross_border, null_prop: r.nonexistent} AS route_data;
      """
    Then the result should be:
      | route_data                                     |
      | {cross_border: true, km: 466, null_prop: null} |

  Scenario: Creating a map with multiple properties from both a vertex and an edge
    Given an empty graph
    And having executed
      """
      CREATE (m:City {name: "Split", highway_connected: true})-[:ROUTE {km: 466, cross_border: true}]->(n:City {name: "Ljubljana"});
      """
    When executing query:
      """
      MATCH (m:City {name: "Split"})-[r:ROUTE]->()
      RETURN {km: r.km, cross_border: r.cross_border, start_city: m.name, highway_connected: m.highway_connected} AS route_data;
      """
    Then the result should be:
      | route_data                                                                  |
      | {cross_border: true, highway_connected: true, km: 466, start_city: 'Split'} |
