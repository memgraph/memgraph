Feature: WHERE exists

  Scenario: Test exists with empty edge and node specifiers
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]-()) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists with empty edge and node specifiers return 2 entries
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two), (:One {prop: 3})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]-()) RETURN n.prop ORDER BY n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |
          | 3      |

  Scenario: Test exists with edge specifier
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE]-()) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists with wrong edge specifier
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE2]-()) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with correct edge direction
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE]->()) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists with wrong edge direction
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)<-[:TYPE]-()) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with destination node label
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]->(:Two)) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists with wrong destination node label
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]->(:Three)) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with destination node property
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]->({prop: 2})) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists with wrong destination node property
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]->({prop: 3})) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with edge property
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE {prop: 1}]->()) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists with wrong edge property
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE {prop: 2}]->()) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with both edge property and node label property
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE {prop: 1}]->(:Two {prop: 2})) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists with correct edge property and wrong node label property
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE {prop: 1}]->(:Two {prop: 3})) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with wrong edge property and correct node label property
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE {prop: 2}]->(:Two {prop:2})) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with wrong edge property and wrong node label property
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE {prop: 2}]->(:Two {prop:3})) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists AND exists
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE]->()) AND exists((n)-[]->(:Two)) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists OR exists first condition
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE]->()) OR exists((n)-[]->(:Three)) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists OR exists second condition
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE2]->()) OR exists((n)-[]->(:Two)) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists OR exists fail
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE2]->()) OR exists((n)-[]->(:Three)) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test NOT exists
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})
          """
      When executing query:
          """
          MATCH (n:One) WHERE NOT exists((n)-[:TYPE2]->()) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test multi-hop first in sequence
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})-[:TYPE {prop:2}]->(:Three {prop: 3})
          """
      When executing query:
          """
          MATCH (n) WHERE exists((n)-[]->()-[]->()) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test multi-hop in middle sequence
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})-[:TYPE {prop:2}]->(:Three {prop: 3})
          """
      When executing query:
          """
          MATCH (n) WHERE exists(()-[]->(n)-[]->()) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 2      |

  Scenario: Test multi-hop at the end of the sequence
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})-[:TYPE {prop:2}]->(:Three {prop: 3})
          """
      When executing query:
          """
          MATCH (n) WHERE exists(()-[]->()-[]->(n)) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 3      |

  Scenario: Test multi-hop not exists
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})-[:TYPE {prop:2}]->(:Three {prop: 3})
          """
      When executing query:
          """
          MATCH (n) WHERE exists(()-[]->(n)<-[]-()) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test multi-hop with filters
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})-[:TYPE {prop:2}]->(:Three {prop: 3})
          """
      When executing query:
          """
          MATCH (n) WHERE exists(({prop: 1})-[:TYPE]->(n)-[{prop:2}]->(:Three)) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 2      |

  Scenario: Test multi-hop with wrong filters
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})-[:TYPE {prop:2}]->(:Three {prop: 3})
          """
      When executing query:
          """
          MATCH (n) WHERE exists(({prop: 1})-[:TYPE]->(n)-[:TYPE2]->(:Three)) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with different edge type
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two)
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[:TYPE2]->()) RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists with correct edge type multiple edges
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two {prop: 10}), (:One {prop: 2})-[:TYPE]->(:Two {prop: 11});
          """
      When executing query:
          """
          MATCH (n:Two) WHERE exists((n)<-[:TYPE]-()) RETURN n.prop ORDER BY n.prop;
          """
      Then the result should be:
          | n.prop |
          | 10     |
          | 11     |

  Scenario: Test exists in the WHERE of a WITH
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two {prop:2})
          CREATE (:Two {prop:3})
          """
      When executing query:
          """
          MATCH (n:Two) WITH n WHERE exists((n)<-[:TYPE]-()) RETURN n.prop AS prop;
          """
      Then the result should be:
          | prop |
          | 2    |

  Scenario: Test exists is not null
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]-()) is not null RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists is null
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]-()) is null RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists equal to true
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]-()) = true RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists equal to false
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]-()) = false RETURN n.prop;
          """
      Then the result should be empty

  Scenario: Test exists in list
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]-()) in [true] RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test BFS hop
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})-[:TYPE {prop:2}]->(:Three {prop: 3})
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[*bfs]->(:Three)) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |

  Scenario: Test exists not in list
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n:One) WHERE exists((n)-[]-()) in [false] RETURN n.prop;
          """
      Then the result should be empty

	Scenario: Test exists on multihop patterns without results
		Given an empty graph
		And having executed:
				"""
				MATCH (n) DETACH DELETE n;
				"""
		When executing query:
				"""
				MATCH ()-[]-(m)-[]->(a) WHERE m.prop=1 and a.prop=3 and exists(()-[]->(m)) RETURN m, a;
				"""
  	Then the result should be empty

  Scenario: Test exists does not work in SetProperty clauses
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n:Two) SET n.prop = exists((n)<-[:TYPE]-()) RETURN n.prop;
          """
      Then an error should be raised

  Scenario: Test exists in RETURN clauses
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two {prop:2})
          CREATE (:Three {prop:9})
          """
      When executing query:
          """
          MATCH (n) RETURN n.prop AS prop, exists((n)-[]-()) AS h ORDER BY prop;
          """
      Then the result should be:
          | prop | h     |
          | 1    | true  |
          | 2    | true  |
          | 9    | false |

  Scenario: Test basic EXISTS subquery with pattern
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person) WHERE EXISTS { (person)-[:HAS_DOG]->(:Dog) } RETURN person.name AS name;
          """
      Then the result should be:
          | name   |
          | 'John' |

  Scenario: Test EXISTS subquery with WHERE clause
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'John'})
          CREATE (:Person {name: 'Alice'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person) WHERE EXISTS { MATCH (person)-[:HAS_DOG]->(dog:Dog) WHERE person.name = dog.name } RETURN person.name AS name;
          """
      Then the result should be:
          | name   |
          | 'John' |

  Scenario: Test EXISTS subquery with WITH clause
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Ozzy'})
          CREATE (:Person {name: 'Alice'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          WITH 'Peter' as name MATCH (person:Person {name: name}) WHERE EXISTS { WITH "Ozzy" AS name MATCH (person)-[:HAS_DOG]->(d:Dog) WHERE d.name = name } RETURN person.name AS name;
          """
      Then the result should be empty

  Scenario: Test EXISTS subquery with nested WITH
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Ozzy'})
          CREATE (:Person {name: 'Alice'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person) WHERE EXISTS { WITH 'Ozzy' AS dogName MATCH (person)-[:HAS_DOG]->(d:Dog) WHERE d.name = dogName } RETURN person.name AS name;
          """
      Then the result should be:
          | name   |
          | 'John' |

  Scenario: Test EXISTS subquery with RETURN clause
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person) WHERE EXISTS { MATCH (person)-[:HAS_DOG]->(:Dog) RETURN person.name } RETURN person.name AS name;
          """
      Then the result should be:
          | name   |
          | 'John' |

  Scenario: Test nested EXISTS subqueries
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})-[:HAS_TOY]->(:Toy {name: 'Banana'})
          CREATE (:Person {name: 'Alice'})-[:HAS_DOG]->(:Dog {name: 'Max'})-[:HAS_TOY]->(:Toy {name: 'Ball'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            WHERE EXISTS {
              MATCH (dog)-[:HAS_TOY]->(toy:Toy)
              WHERE toy.name = 'Banana'
            }
          }
          RETURN person.name AS name;
          """
      Then the result should be:
          | name   |
          | 'John' |

  Scenario: Test EXISTS subquery with multiple patterns
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})-[:HAS_TOY]->(:Toy {name: 'Ball'})
          CREATE (:Person {name: 'Alice'})-[:HAS_DOG]->(:Dog {name: 'Max'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)-[:HAS_TOY]->(:Toy)
          }
          RETURN person.name AS name;
          """
      Then the result should be:
          | name   |
          | 'John' |

  Scenario: Test not EXISTS subquery with multiple patterns
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})-[:HAS_TOY]->(:Toy {name: 'Ball'})
          CREATE (:Person {name: 'Alice'})-[:HAS_DOG]->(:Dog {name: 'Max'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE NOT EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)-[:HAS_TOY]->(:Toy)
          }
          RETURN person.name AS name;
          """
      Then the result should be:
          | name    |
          | 'Alice' |

  Scenario: Test EXISTS subquery with variable from outer scope
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John', age: 30})-[:HAS_DOG]->(:Dog {name: 'Rex', age: 5})
          CREATE (:Person {name: 'Alice', age: 25})-[:HAS_DOG]->(:Dog {name: 'Max', age: 3})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            WHERE dog.age < person.age
          }
          RETURN person.name AS name;
          """
      Then the result should be:
          | name    |
          | 'John'  |
          | 'Alice' |

  Scenario: Test EXISTS subquery with multiple conditions
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex', age: 5})
          CREATE (:Person {name: 'Alice'})-[:HAS_DOG]->(:Dog {name: 'Max', age: 3})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            WHERE dog.age > 4 AND dog.name = 'Rex'
          }
          RETURN person.name AS name;
          """
      Then the result should be:
          | name   |
          | 'John' |

  Scenario: Test RETURN EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (person:Person) RETURN person.name AS name, EXISTS { (person)-[:HAS_DOG]->(:Dog) } AS has_dog ORDER BY name;
          """
      Then the result should be:
          | name   | has_dog |
          | 'Bob'  | false   |
          | 'John' | true    |

  Scenario: Test invalid periodic commit inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            USING PERIODIC COMMIT 1
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            RETURN dog
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  Scenario: Test invalid parallel execution inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            USING PARALLEL EXECUTION
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            RETURN dog
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  Scenario: Test invalid SET inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            SET dog.name = 'NewName'
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  Scenario: Test invalid CREATE inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            CREATE (person)-[:HAS_DOG]->(:Dog {name: 'Rex'})
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  Scenario: Test invalid DELETE inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            DELETE dog
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  Scenario: Test invalid DETACH DELETE inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            DETACH DELETE dog
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  Scenario: Test invalid REMOVE inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_DOG]->(dog:Dog)
            REMOVE dog.name
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  # A UNION's later branches are each their own SingleQuery, so the clause rules above have to be checked on every
  # one of them, not just the first. Both were accepted before. The earlier branches match nothing on purpose: the
  # fold stops at the first row, so only then is the writing branch pulled - measured, it created the node.

  Scenario: Test invalid CREATE in a UNION branch inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_CAT]->(cat:Cat)
            RETURN cat AS d
            UNION
            MATCH (other:Dog)
            CREATE (:Marker)
            RETURN other AS d
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  Scenario: Test invalid CREATE in the last of three UNION branches inside EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
            MATCH (person)-[:HAS_CAT]->(cat:Cat)
            RETURN cat AS d
            UNION
            MATCH (second:Cat)
            RETURN second AS d
            UNION
            MATCH (third:Dog)
            CREATE (:Marker)
            RETURN third AS d
          }
          RETURN person.name AS name;
          """
      Then an error should be raised

  Scenario: Test EXISTS with UNION in RETURN
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          CREATE (:Person {name: 'Alice'})-[:HAS_CAT]->(:Cat {name: 'Whiskers'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (person:Person)
          RETURN
              person.name AS name,
              EXISTS {
                  MATCH (person)-[:HAS_DOG]->(:Dog)
                  UNION
                  MATCH (person)-[:HAS_CAT]->(:Cat)
              } AS hasPet
          ORDER BY name;
          """
      Then the result should be:
          | name    | hasPet |
          | 'Alice' | true   |
          | 'Bob'   | false  |
          | 'John'  | true   |

  Scenario: Test valid EXISTS with UNION in WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          CREATE (:Person {name: 'Alice'})-[:HAS_CAT]->(:Cat {name: 'Whiskers'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (person:Person)
          WHERE EXISTS {
              MATCH (person)-[:HAS_DOG]->(:Dog)
              UNION
              MATCH (person)-[:HAS_CAT]->(:Cat)
          }
          RETURN person.name AS name;
          """
      Then the result should be:
          | name    |
          | 'John'  |
          | 'Alice' |

   Scenario: EXISTS simple match finds node
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 2});
          """
      When executing query:
          """
          MATCH (n:Node)
          WHERE EXISTS {
              MATCH (n)
          }
          RETURN n.id as id;
          """
      Then the result should be:
          | id    |
          | 2     |

  Scenario: EXISTS simple match finds no node
      Given an empty graph
      When executing query:
          """
          MATCH (n:Node)
          WHERE EXISTS {
              MATCH (n)
          }
          RETURN n.id as id;
          """
      Then the result should be empty

  Scenario: Test EXISTS subquery in a WITH projection
      Given an empty graph
      And having executed:
          """
          CREATE (r:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (p:Person)
          WITH p, EXISTS { MATCH (p)-[:ACTED_IN]->(m:Movie) WHERE m.title STARTS WITH 'J' } AS hasActed
          RETURN p.name AS name, hasActed ORDER BY name;
          """
      Then the result should be:
          | name           | hasActed |
          | 'Bob'          | false    |
          | 'Regina King'  | true     |

  Scenario: Test EXISTS subquery in an ORDER BY
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (p:Person)
          WITH p ORDER BY EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) } DESC, p.name
          RETURN p.name AS name;
          """
      Then the result should be, in order:
          | name           |
          | 'Regina King'  |
          | 'Bob'          |

  # The four aggregating scenarios below match on awards rather than on people, so a group holds more than one row -
  # two for Regina King and three for Bob. A branch spliced after the Aggregate, or evaluated once per group instead
  # of once per input row, would still answer these correctly if every group held a single row.

  Scenario: Test EXISTS subquery alongside an aggregation
      Given an empty graph
      And having executed:
          """
          CREATE (r:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (b:Person {name: 'Bob'})
          CREATE (r)-[:AWARDED]->(:Award {name: 'Emmy'})
          CREATE (r)-[:AWARDED]->(:Award {name: 'Oscar'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Golden Raspberry'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Razzie'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Teen Choice'})
          """
      When executing query:
          """
          MATCH (p:Person)-[:AWARDED]->(a:Award)
          RETURN p.name AS name, count(a) AS c, EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) } AS h
          ORDER BY name;
          """
      Then the result should be:
          | name           | c | h     |
          | 'Bob'          | 3 | false |
          | 'Regina King'  | 2 | true  |

  Scenario: Test EXISTS subquery in the WHERE of an aggregating WITH
      Given an empty graph
      And having executed:
          """
          CREATE (r:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (b:Person {name: 'Bob'})
          CREATE (r)-[:AWARDED]->(:Award {name: 'Emmy'})
          CREATE (r)-[:AWARDED]->(:Award {name: 'Oscar'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Golden Raspberry'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Razzie'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Teen Choice'})
          """
      When executing query:
          """
          MATCH (p:Person)-[:AWARDED]->(a:Award)
          WITH p, count(a) AS c WHERE EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) }
          RETURN p.name AS name, c;
          """
      Then the result should be:
          | name           | c |
          | 'Regina King'  | 2 |

  Scenario: Test EXISTS subquery in the ORDER BY of an aggregating WITH
      Given an empty graph
      And having executed:
          """
          CREATE (r:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (b:Person {name: 'Bob'})
          CREATE (r)-[:AWARDED]->(:Award {name: 'Emmy'})
          CREATE (r)-[:AWARDED]->(:Award {name: 'Oscar'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Golden Raspberry'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Razzie'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Teen Choice'})
          """
      When executing query:
          """
          MATCH (p:Person)-[:AWARDED]->(a:Award)
          WITH p, count(a) AS c ORDER BY EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) } DESC
          RETURN p.name AS name, c;
          """
      Then the result should be, in order:
          | name           | c |
          | 'Regina King'  | 2 |
          | 'Bob'          | 3 |

  # Here the EXISTS correlates to the award rather than to the person, so its value varies *within* a group. That is
  # what pins the branch to the input row: one evaluation per group would collapse each list to a single repeated
  # value, and an evaluation after the Aggregate would have no award to correlate to at all.
  Scenario: Test EXISTS subquery inside an aggregate argument
      Given an empty graph
      And having executed:
          """
          CREATE (r:Person {name: 'Regina King'})
          CREATE (b:Person {name: 'Bob'})
          CREATE (o:Org {name: 'Academy'})
          CREATE (r)-[:AWARDED]->(e:Award {name: 'Emmy'})
          CREATE (r)-[:AWARDED]->(s:Award {name: 'Oscar'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Golden Raspberry'})
          CREATE (b)-[:AWARDED]->(:Award {name: 'Razzie'})
          CREATE (s)-[:GIVEN_BY]->(o)
          """
      When executing query:
          """
          MATCH (p:Person)-[:AWARDED]->(a:Award)
          WITH p, a ORDER BY p.name, a.name
          RETURN p.name AS name, collect(EXISTS { MATCH (a)-[:GIVEN_BY]->(:Org) }) AS h
          ORDER BY name;
          """
      Then the result should be:
          | name           | h              |
          | 'Bob'          | [false, false] |
          | 'Regina King'  | [false, true]  |

  Scenario: Test EXISTS subquery on a null variable
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          """
      When executing query:
          """
          MATCH (p:Person)
          OPTIONAL MATCH (p)-[:DIRECTED]->(q)
          RETURN p.name AS name, EXISTS { MATCH (q)-[:ACTED_IN]->() } AS h;
          """
      Then the result should be:
          | name           | h     |
          | 'Regina King'  | false |

  Scenario: Test EXISTS subquery in a RETURN projection after a write
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (p:Person {name: 'Bob'})
          CREATE (p)-[:ACTED_IN]->(:Movie {title: 'Juno'})
          RETURN p.name AS name,
                 EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) } AS subquery_form,
                 exists((p)-[:ACTED_IN]->(:Movie)) AS pattern_form;
          """
      Then the result should be:
          | name  | subquery_form | pattern_form |
          | 'Bob' | true          | true         |

  # A subquery expression nested in the body has to inherit the branch's view. Planned on demand from the body's own
  # WITH, it starts a query part with no write history of its own, so without the branch flag it reads View::OLD and
  # disagrees with the body's MATCH about the write - silently, in the RETURN position, where no Accumulate advances
  # the command.
  Scenario: Test EXISTS subquery nested in an EXISTS body in a RETURN projection after a write
      Given an empty graph
      And having executed:
          """
          CREATE (:Root)
          """
      When executing query:
          """
          MATCH (r:Root) CREATE (:New)
          RETURN EXISTS { MATCH (r) WITH r, EXISTS { MATCH (x:New) } AS i WHERE i } AS nested,
                 EXISTS { MATCH (x:New) } AS flat;
          """
      Then the result should be:
          | nested | flat |
          | true   | true |

  # The same, one AST node over: a body MATCH's WHERE reaches MakeExistsFilter, which chose the view separately.
  Scenario: Test EXISTS subquery in an EXISTS body's WHERE in a RETURN projection after a write
      Given an empty graph
      And having executed:
          """
          CREATE (:Root)
          """
      When executing query:
          """
          MATCH (r:Root) CREATE (:New)
          RETURN EXISTS { MATCH (r) WHERE EXISTS { MATCH (x:New) } } AS h;
          """
      Then the result should be:
          | h    |
          | true |

  # The comprehension half of the same rule: a pattern comprehension in the body is planned through the same
  # on-demand path and must see the write too.
  Scenario: Test pattern comprehension in an EXISTS body in a RETURN projection after a write
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (p:P) CREATE (p)-[:Z]->(:New)
          RETURN p.id AS id,
                 EXISTS { MATCH (r:P) WITH r, [(r)-[:Z]->(x) | x] AS l WHERE size(l) > 0 } AS h
          ORDER BY id;
          """
      Then the result should be:
          | id | h    |
          | 1  | true |
          | 2  | true |

  # The EXISTS matches only nodes this query just created, so it comes back all-false if the branch cannot see the
  # write. The `n.rank + 1` correlation keeps the two rows distinguishable, so an all-true answer is detected too.
  #
  # It has to be a RETURN, not a WITH: GenWith advances the command in its Accumulate, which drains its input before
  # the branch above it ever pulls, so a WITH sees the write through View::OLD anyway and the scenario would pass with
  # the view rule reverted.
  Scenario: Test EXISTS subquery in a projection reading what a CREATE just wrote
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {rank: 1})
          CREATE (:Person {rank: 2})
          """
      When executing query:
          """
          MATCH (n:Person) CREATE (:Tag {v: n.rank + 1})
          RETURN n.rank AS rank, EXISTS { MATCH (t:Tag) WHERE t.v = n.rank } AS e ORDER BY rank;
          """
      Then the result should be:
          | rank | e     |
          | 1    | false |
          | 2    | true  |

  Scenario: Test EXISTS subquery correlating to a non-projected variable after a write
      Given an empty graph
      And having executed:
          """
          CREATE (:P {k:1})-[:R]->(:Q)
          CREATE (:P {k:2})
          CREATE (:P {k:3})-[:R]->(:Q)
          """
      When executing query:
          """
          MATCH (n:P) SET n.t = 1
          WITH EXISTS { MATCH (x:P) WHERE x.k = n.k + 1 } AS e
          RETURN e ORDER BY e;
          """
      Then the result should be, in order:
          | e     |
          | false |
          | true  |
          | true  |

  Scenario: EXISTS subquery body that is only a RETURN
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          """
      When executing query:
          """
          MATCH (n:Node)
          WHERE EXISTS {
              RETURN 1
          }
          RETURN n.id as id;
          """
      Then the result should be:
          | id    |
          | 1     |

  Scenario: EXISTS subquery body that is a UNION of RETURN-only bodies
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          """
      When executing query:
          """
          MATCH (n:Node)
          WHERE EXISTS {
              RETURN 1 AS c
              UNION
              RETURN 2 AS c
          }
          RETURN n.id as id;
          """
      Then the result should be:
          | id    |
          | 1     |

  Scenario: EXISTS subquery in a projection whose body is a UNION of RETURN-only bodies
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          """
      When executing query:
          """
          MATCH (n:Node)
          RETURN EXISTS {
              RETURN 1 AS c
              UNION ALL
              RETURN 2 AS c
          } AS h;
          """
      Then the result should be:
          | h    |
          | true |

  Scenario: EXISTS subquery body that is a UNION of one matching and one empty branch
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          """
      When executing query:
          """
          MATCH (n:Node)
          RETURN EXISTS {
              MATCH (x:Missing) RETURN x AS c
              UNION
              RETURN 2 AS c
          } AS h;
          """
      Then the result should be:
          | h    |
          | true |

  Scenario: EXISTS subquery body that is a UNION of a branch with rows and a RETURN-only one
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          CREATE (:Node {id: 2})
          """
      When executing query:
          """
          MATCH (n:Node)
          RETURN EXISTS {
              MATCH (x:Node) RETURN x AS c
              UNION
              RETURN 2 AS c
          } AS h;
          """
      Then the result should be:
          | h    |
          | true |
          | true |

  Scenario: EXISTS subquery body that is a UNION of two branches matching nothing
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          """
      When executing query:
          """
          MATCH (n:Node)
          RETURN EXISTS {
              MATCH (x:Missing) RETURN x AS c
              UNION
              MATCH (y:AlsoMissing) RETURN y AS c
          } AS h;
          """
      Then the result should be:
          | h     |
          | false |

  Scenario: EXISTS subquery body that is only a RETURN in a projection
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          """
      When executing query:
          """
          MATCH (n:Node)
          RETURN EXISTS {
              RETURN 1
          } AS h;
          """
      Then the result should be:
          | h    |
          | true |

  Scenario: EXISTS subquery body that is only a RETURN in a WITH projection
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          """
      When executing query:
          """
          MATCH (n:Node)
          WITH n, EXISTS {
              RETURN 1
          } AS e
          RETURN e;
          """
      Then the result should be:
          | e    |
          | true |

  Scenario: EXISTS subquery body that is only a RETURN in an ORDER BY
      Given an empty graph
      And having executed:
          """
          CREATE (:Node {id: 1})
          """
      When executing query:
          """
          MATCH (n:Node)
          WITH n ORDER BY EXISTS {
              RETURN 1
          }
          RETURN n.id AS id;
          """
      Then the result should be:
          | id    |
          | 1     |

  # CASE holds no position of its own; it carries whichever position it sits in. The fixture keeps one P with an
  # outgoing edge and one without, so an all-true or all-false answer would be visible.

  Scenario: Test EXISTS subquery in a CASE condition
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id, CASE WHEN EXISTS { MATCH (a)-[:R]->() } THEN 'yes' ELSE 'no' END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | 'yes' |
          | 2  | 'no'  |

  Scenario: Test EXISTS subquery in a CASE inside a WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P) WHERE CASE WHEN TRUE THEN EXISTS { MATCH (a)-[:R]->() } ELSE false END
          RETURN a.id AS id;
          """
      Then the result should be:
          | id |
          | 1  |

  Scenario: Test EXISTS subquery in both arms of a CASE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id,
                 CASE WHEN a.id = 1 THEN EXISTS { MATCH (a)-[:R]->() } ELSE EXISTS { MATCH (:Nope) } END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | true  |
          | 2  | false |

  Scenario: Test EXISTS subquery inside a nested CASE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id,
                 CASE WHEN a.id <= 2
                      THEN CASE WHEN EXISTS { MATCH (a)-[:R]->() } THEN 'yes' ELSE 'no' END
                      ELSE 'skip' END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | 'yes' |
          | 2  | 'no'  |

  Scenario: Test EXISTS subquery in an arm of a simple CASE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id, CASE a.id WHEN 1 THEN EXISTS { MATCH (a)-[:R]->() } ELSE EXISTS { MATCH (:Nope) } END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | true  |
          | 2  | false |

  Scenario: Test EXISTS subquery in a CASE inside an aggregate argument
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN sum(CASE WHEN EXISTS { MATCH (a)-[:R]->() } THEN 1 ELSE 0 END) AS c;
          """
      Then the result should be:
          | c |
          | 1 |

  Scenario: Test EXISTS subquery in a CASE beside an aggregation
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id, count(*) AS c, CASE WHEN EXISTS { MATCH (a)-[:R]->() } THEN 'any' ELSE 'none' END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | c | h      |
          | 1  | 1 | 'any'  |
          | 2  | 1 | 'none' |

  Scenario: Test EXISTS subquery in a CASE inside ORDER BY
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id
          ORDER BY CASE WHEN EXISTS { MATCH (a)-[:R]->() } THEN 1 ELSE 0 END;
          """
      Then the result should be, in order:
          | id |
          | 2  |
          | 1  |

  Scenario: Test EXISTS subquery with a RETURN-only body inside a CASE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id, CASE WHEN a.id = 1 THEN EXISTS { RETURN 1 } ELSE EXISTS { MATCH (:Nope) } END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | true  |
          | 2  | false |

  # A simple CASE compares one test expression against each alternative, so an EXISTS in the test position is reached
  # once per arm. The pattern form names its variables at parse time, so an arm past the first used to redeclare them.

  Scenario: Test EXISTS subquery as the test of a simple CASE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id, CASE EXISTS { MATCH (a)-[:R]->() } WHEN true THEN 'yes' WHEN false THEN 'no' ELSE '?' END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | 'yes' |
          | 2  | 'no'  |

  Scenario: Test pattern EXISTS as the test of a simple CASE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id, CASE exists((a)-[:R]->()) WHEN true THEN 'yes' WHEN false THEN 'no' ELSE '?' END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | 'yes' |
          | 2  | 'no'  |

  Scenario: Test pattern EXISTS as the test of a simple CASE in a WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          WHERE CASE exists((a)-[:R]->()) WHEN true THEN true WHEN false THEN false END
          RETURN a.id AS id;
          """
      Then the result should be:
          | id |
          | 1  |

  Scenario: Test pattern EXISTS as the test of a simple CASE with four alternatives
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id,
                 CASE exists((a)-[:R]->()) WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN true THEN 'yes' WHEN false THEN 'no' ELSE '?' END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | 'yes' |
          | 2  | 'no'  |

  # The pattern form takes its own planner path - rooted at an Once over the bound symbols rather than planned
  # recursively - so each position it can now reach through a CASE is pinned separately from the subquery form.

  Scenario: Test pattern EXISTS in a CASE in a WITH projection
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          WITH a.id AS id, CASE WHEN exists((a)-[:R]->()) THEN 'yes' ELSE 'no' END AS h
          RETURN id, h ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | 'yes' |
          | 2  | 'no'  |

  Scenario: Test pattern EXISTS in a CASE in a WITH's WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          WITH a WHERE CASE WHEN exists((a)-[:R]->()) THEN true ELSE false END
          RETURN a.id AS id;
          """
      Then the result should be:
          | id |
          | 1  |

  Scenario: Test pattern EXISTS in a CASE in an ORDER BY
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id
          ORDER BY CASE WHEN exists((a)-[:R]->()) THEN 1 ELSE 0 END;
          """
      Then the result should be, in order:
          | id |
          | 2  |
          | 1  |

  Scenario: Test pattern EXISTS in a CASE inside an aggregate argument
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN sum(CASE WHEN exists((a)-[:R]->()) THEN 1 ELSE 0 END) AS c;
          """
      Then the result should be:
          | c |
          | 1 |

  Scenario: Test both EXISTS forms in one CASE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id,
                 CASE exists((a)-[:R]->()) WHEN true THEN EXISTS { MATCH (a)-[:R]->() } ELSE false END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | true  |
          | 2  | false |

  Scenario: Test an aggregation in an EXISTS body inside a CASE
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          CREATE (:P {id: 2})
          """
      When executing query:
          """
          MATCH (a:P)
          RETURN a.id AS id,
                 CASE WHEN EXISTS { MATCH (a)-[:R]->(x) WITH count(x) AS c WHERE c > 0 RETURN c } THEN 'yes' ELSE 'no' END AS h
          ORDER BY id;
          """
      Then the result should be, in order:
          | id | h     |
          | 1  | 'yes' |
          | 2  | 'no'  |

  # An EXISTS inside an EXISTS pattern's property map has no splice point of its own. The refusal is the position
  # message; before the pattern form had a scope it reached the planner and surfaced an internal error instead.

  Scenario: Test EXISTS inside an EXISTS pattern's property map is refused
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          """
      When executing query:
          """
          MATCH (a:P) RETURN exists((a)-[:R]->({p: EXISTS { MATCH (:X) }})) AS h;
          """
      Then an error should be raised

  Scenario: Test a pattern EXISTS inside an EXISTS pattern's property map is refused
      Given an empty graph
      And having executed:
          """
          CREATE (:P {id: 1})-[:R]->(:X)
          """
      When executing query:
          """
          MATCH (a:P) RETURN exists((a)-[:R]->({p: exists((a)-[:R]->())})) AS h;
          """
      Then an error should be raised

  # The body's RETURN decides how many rows reach the fold, so SKIP, LIMIT and aggregation on it change the answer.
  # DISTINCT alone cannot - it never empties a non-empty table - so it appears only composed with a SKIP.

  Scenario: Test EXISTS subquery whose body RETURN aggregates
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN count(f) } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h    |
          | 'Alice' | true |
          | 'Bob'   | true |
          | 'Carol' | true |

  # Invariance pin: the WITH spelling was always planned, so this agrees either way. It guards the two from diverging.
  Scenario: Test EXISTS subquery whose body RETURN aggregates agrees with the WITH spelling
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) WITH count(f) AS c RETURN c } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h    |
          | 'Alice' | true |
          | 'Bob'   | true |
          | 'Carol' | true |

  # The counterpart to the scenario above, and the reason it is worth pinning: an un-aggregated column becomes a
  # grouping key, and a grouped aggregate emits no row at all on empty input, so Carol flips to false. The body's
  # column list therefore decides the row count. Neither of these two answers depends on this change - both hold with
  # the body's RETURN discarded - so this pair guards a future rewrite that prunes an unread projection column from
  # silently turning the grouped answer into the ungrouped one. Both measured against the reference engine.
  Scenario: Test EXISTS subquery whose body RETURN aggregates with a grouping key
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN 1, count(f) } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | true  |
          | 'Bob'   | true  |
          | 'Carol' | false |

  Scenario: Test EXISTS subquery whose body RETURN has a LIMIT
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN f LIMIT 0 } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | false |
          | 'Bob'   | false |
          | 'Carol' | false |

  Scenario: Test EXISTS subquery whose body RETURN has a SKIP
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN f SKIP 1 } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | true  |
          | 'Bob'   | false |
          | 'Carol' | false |

  # Alice is the discriminating row: her doubled KNOWS edge to F1 makes 3 rows but 2 distinct, so the SKIP clears
  # the table only if DISTINCT was planned. It is the one fixture here that needs a parallel edge.
  # Dave, with 3 distinct, keeps a row either way, so an all-false table cannot pass by accident.
  Scenario: Test EXISTS subquery whose body RETURN is DISTINCT
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}), (d:Person {name: 'Dave'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'}), (f3:Friend {name: 'F3'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          CREATE (d)-[:KNOWS]->(f1)
          CREATE (d)-[:KNOWS]->(f2)
          CREATE (d)-[:KNOWS]->(f3)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN DISTINCT f SKIP 2 } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | false |
          | 'Bob'   | false |
          | 'Carol' | false |
          | 'Dave'  | true  |

  Scenario: Test EXISTS subquery in a WHERE whose body RETURN aggregates
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN count(f) }
          RETURN p.name AS name
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    |
          | 'Alice' |
          | 'Bob'   |
          | 'Carol' |

  # The grouping key reaches the WHERE fold too, and drops the row the ungrouped spelling above keeps.
  Scenario: Test EXISTS subquery in a WHERE whose body RETURN aggregates with a grouping key
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN 1, count(f) }
          RETURN p.name AS name
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    |
          | 'Alice' |
          | 'Bob'   |

  Scenario: Test EXISTS subquery in a WHERE whose body RETURN has a SKIP
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN f SKIP 1 }
          RETURN p.name AS name
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    |
          | 'Alice' |

  # ORDER BY cannot change the answer - a sort permutes the table and the fold reads only whether it is empty. These
  # pin that invariance, and that the sort still plans: composed with a SKIP, and ordering on a correlated outer symbol.

  Scenario: Test EXISTS subquery whose body RETURN has an ORDER BY and a SKIP
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN f ORDER BY f.name SKIP 1 } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | true  |
          | 'Bob'   | false |
          | 'Carol' | false |

  Scenario: Test EXISTS subquery whose body RETURN orders on a correlated outer symbol
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) RETURN f ORDER BY p.name DESC } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | true  |
          | 'Bob'   | true  |
          | 'Carol' | false |

  # Nesting reaches a second RETURN through the inner body's WHERE. Only Alice knows a friend who likes anything.
  # The first scenario is the baseline; the LIMIT 0 pair below empties one table each and says which RETURN is planned.

  Scenario: Test nested EXISTS subqueries with a RETURN in both bodies
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (b)-[:KNOWS]->(f2)
          CREATE (f1)-[:LIKES]->(:Movie {name: 'M1'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name,
                 EXISTS { MATCH (p)-[:KNOWS]->(f) WHERE EXISTS { MATCH (f)-[:LIKES]->(m) RETURN m } RETURN f } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | true  |
          | 'Bob'   | false |
          | 'Carol' | false |

  Scenario: Test nested EXISTS subqueries where the inner body RETURN has a LIMIT
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (b)-[:KNOWS]->(f2)
          CREATE (f1)-[:LIKES]->(:Movie {name: 'M1'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name,
                 EXISTS { MATCH (p)-[:KNOWS]->(f) WHERE EXISTS { MATCH (f)-[:LIKES]->(m) RETURN m LIMIT 0 } RETURN f } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | false |
          | 'Bob'   | false |
          | 'Carol' | false |

  Scenario: Test nested EXISTS subqueries where the outer body RETURN has a LIMIT
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (b)-[:KNOWS]->(f2)
          CREATE (f1)-[:LIKES]->(:Movie {name: 'M1'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name,
                 EXISTS { MATCH (p)-[:KNOWS]->(f) WHERE EXISTS { MATCH (f)-[:LIKES]->(m) RETURN m } RETURN f LIMIT 0 } AS h
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | h     |
          | 'Alice' | false |
          | 'Bob'   | false |
          | 'Carol' | false |
