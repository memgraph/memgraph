Feature: Map projection

  Scenario: Returning an empty map projection
    When executing query:
      """
      WITH {} AS map
      RETURN map {} AS result
      """
    Then the result should be:
      | result |
      | {}     |

  Scenario: Returning a map projection with each type of map projection element
    Given an empty graph
    And having executed
      """
      CREATE (n:Actor {name: "Morgan", lastName: "Freeman"})
      """
    When executing query:
      """
      WITH 85 as age
      MATCH (actor:Actor)
      RETURN actor {.*, .name, age, oscars: 1} AS result
      """
    Then the result should be:
      | result                                                    |
      | {age: 85, lastName: 'Freeman', name: 'Morgan', oscars: 1} |

  Scenario: Projecting from a null value
    When executing query:
      """
      WITH "value" AS var
      OPTIONAL MATCH (n:Nonexistent)
      RETURN n {.*} AS result0, n {.prop} AS result1, n {prop: "value"} AS result2, n {var} AS result3;
      """
    Then the result should be:
      | result0 | result1 | result2 | result3 |
      | null    | null    | null    | null    |

  Scenario: Projecting a nonexistent property
    When executing query:
      """
      WITH {name: "Morgan", lastName: "Freeman"} as actor
      RETURN actor.age;
      """
    Then the result should be:
      | actor.age |
      | null      |

  Scenario: Storing a map projection as a property
    Given an empty graph
    And having executed
      """
      WITH {name: "Morgan", lastName: "Freeman"} as person
      WITH person {.*, wonOscars: true} as actor
      CREATE (n:Movie {lead: actor});
      """
    When executing query:
      """
      MATCH (movie:Movie)
      RETURN movie.lead
      """
    Then the result should be:
      | movie.lead                                             |
      | {lastName: 'Freeman', name: 'Morgan', wonOscars: true} |

  Scenario: Looking up the properties of a map projection
    When executing query:
      """
      WITH {name: "Morgan", lastName: "Freeman"} as actor, {oscars: 1} as awards
      WITH actor {.*, awards: awards} AS actor
      RETURN actor.name, actor.awards.oscars;
      """
    Then the result should be:
      | actor.name | actor.awards.oscars |
      | 'Morgan'   | 1                   |

  Scenario: Indexing a map projection
    When executing query:
      """
      WITH {name: "Morgan", lastName: "Freeman"} as actor, {oscars: 1} as awards
      WITH actor {.*, awards: awards} AS actor
      RETURN actor["name"], actor["awards"]["oscars"]
      """
    Then the result should be:
      | actor["name"] | actor["awards"]["oscars"] |
      | 'Morgan'      | 1                         |
