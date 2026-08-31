Feature: Subquery expressions

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
  # silently turning the grouped answer into the ungrouped one.
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

  # COUNT { ... } counts the rows of the body's result table - not matches, not distinct values. The fixture
  # discriminates all three: Alice has 3 KNOWS rows to only 2 distinct friends, Bob has 1, Carol has 0.
  Scenario: Test COUNT subquery over a plain MATCH body
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 3 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |
  # Columns are irrelevant to COUNT - only the row count is.
  Scenario: Test COUNT subquery whose body RETURNs a column
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) RETURN f } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 3 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |
  # Alice's 3 rows collapse to her 2 distinct friends.
  Scenario: Test COUNT subquery whose body RETURN is DISTINCT
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) RETURN DISTINCT f } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |
  # An ungrouped aggregate emits exactly one row even on empty input, so Carol counts 1 too.
  Scenario: Test COUNT subquery whose body RETURN aggregates
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) RETURN count(f) } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 1 |
          | 'Bob'   | 1 |
          | 'Carol' | 1 |
  Scenario: Test COUNT subquery whose body RETURN has a LIMIT
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) RETURN f LIMIT 1 } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 1 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |
  Scenario: Test COUNT subquery whose body RETURN has a SKIP
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) RETURN f SKIP 1 } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 0 |
          | 'Carol' | 0 |
  Scenario: Test COUNT subquery whose body has a WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) WHERE f.name = 'F1' } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |
  Scenario: Test COUNT subquery whose body has a WITH with a LIMIT
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) WITH f LIMIT 1 RETURN f } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 1 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |
  # Nothing correlates, so every outer row gets the same count.
  Scenario: Test COUNT subquery with an uncorrelated body
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (f:Friend) } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 2 |
          | 'Carol' | 2 |
  # UNION ALL keeps both branches' rows, so the count doubles.
  Scenario: Test COUNT subquery whose body is a UNION ALL
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) RETURN f AS x UNION ALL MATCH (p)-[:KNOWS]->(g) RETURN g AS x } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 6 |
          | 'Bob'   | 2 |
          | 'Carol' | 0 |
  # The pattern form, which takes the same splice point as the subquery form.
  Scenario: Test COUNT subquery with a bare anonymous pattern body
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { (p)-[:KNOWS]->() } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 3 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |

  # The MATCH a bare body omits is synthesised, so the body takes everything a MATCH's pattern list takes: a
  # variable it declares itself, a trailing WHERE, a named path, a comma-separated list, and a lone node.
  Scenario: Test COUNT subquery with a bare pattern that declares its own variable
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
          RETURN p.name AS name, COUNT { (p)-[:KNOWS]->(f:Friend) } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |

  Scenario: Test COUNT subquery with a bare pattern and a trailing WHERE
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
          RETURN p.name AS name, COUNT { (p)-[:KNOWS]->(f:Friend) WHERE f.name = 'F2' } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 1 |
          | 'Bob'   | 0 |
          | 'Carol' | 0 |

  Scenario: Test COUNT subquery with a bare pattern that names its path
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
          RETURN p.name AS name, COUNT { path = (p)-[:KNOWS]->(f:Friend) } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |

  # Two patterns in one body, so relationship uniqueness applies across them: Alice's two edges make two ordered
  # pairs, Bob's single edge makes none.
  Scenario: Test COUNT subquery with a bare comma-separated pattern list
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
          RETURN p.name AS name, COUNT { (p)-[:KNOWS]->(f:Friend), (p)-[:KNOWS]->(g:Friend) } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 0 |
          | 'Carol' | 0 |

  Scenario: Test EXISTS subquery with a bare pattern holding only a node
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { (p) } AS e
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | e    |
          | 'Alice' | true |
          | 'Bob'   | true |

  # COUNT takes the same positions as EXISTS, through the same gate. A MATCH's WHERE is the deferred fold, so a
  # disjunct the evaluator never reaches skips the branch's whole drain; everything else is the forced fold.

  Scenario: Test COUNT subquery in a WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE COUNT { MATCH (p)-[:KNOWS]->(f) } > 1
          RETURN p.name AS name
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    |
          | 'Alice' |

  # Two identical elements, so the answer depends on the second invocation: the closure re-Resets the branch per call,
  # and a stale slot would answer 0 on the second element and drop every row.
  Scenario: Test COUNT subquery re-evaluated per element of a lambda in a WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE all(x IN [0, 0] WHERE COUNT { MATCH (p)-[:KNOWS]->(f) } > 0)
          RETURN p.name AS name
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    |
          | 'Alice' |
          | 'Bob'   |

  Scenario: Test COUNT subquery in a CASE
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, CASE WHEN COUNT { MATCH (p)-[:KNOWS]->(f) } > 1 THEN 'many' ELSE 'few' END AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c      |
          | 'Alice' | 'many' |
          | 'Bob'   | 'few'  |
          | 'Carol' | 'few'  |

  Scenario: Test COUNT subquery inside an aggregate argument
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN sum(COUNT { MATCH (p)-[:KNOWS]->(f) }) AS s;
          """
      Then the result should be:
          | s |
          | 4 |

  Scenario: Test COUNT subquery in an ORDER BY
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name
          ORDER BY COUNT { MATCH (p)-[:KNOWS]->(f) } ASC, name;
          """
      Then the result should be, in order:
          | name    |
          | 'Carol' |
          | 'Bob'   |
          | 'Alice' |

  Scenario: Test COUNT subquery in a WITH projection read by a later WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WITH p, COUNT { MATCH (p)-[:KNOWS]->(f) } AS c
          WHERE c > 1
          RETURN p.name AS name, c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 3 |

  # The conjunct ordering is pinned as a plan shape by the unit tests; these pin the point of it. `1/0` in the body
  # raises only if the branch actually drains, so a false expectation here means the ordering stopped working.
  Scenario: Test COUNT subquery skipped when a cheaper conjunct already failed
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE p.name = 'Nobody' AND COUNT { MATCH (p)-[:KNOWS]->(f) WHERE 1/0 > 0 } > 1
          RETURN p.name AS name;
          """
      Then the result should be empty

  Scenario: Test COUNT subquery conjuncts evaluated in authoring order
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE COUNT { MATCH (p)-[:KNOWS]->(f) WHERE f.name = 'ZZZ' } > 0
            AND COUNT { MATCH (p)-[:KNOWS]->(g) WHERE 1/0 > 0 } > 1
          RETURN p.name AS name;
          """
      Then the result should be empty

  # The same two conjuncts swapped: the expensive one is now written first, so it does run and does raise. Without
  # this the scenario above would pass even if neither conjunct were ever evaluated.
  Scenario: Test COUNT subquery conjunct written first is the one evaluated
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE COUNT { MATCH (p)-[:KNOWS]->(g) WHERE 1/0 > 0 } > 1
            AND COUNT { MATCH (p)-[:KNOWS]->(f) WHERE f.name = 'ZZZ' } > 0
          RETURN p.name AS name;
          """
      Then an error should be raised

  # Spliced above the WITH's Produce, so the count is recomputed per row. A branch frozen at one value would answer
  # every row with Alice's 3 and let Bob through.
  Scenario: Test COUNT subquery in the WHERE of a WITH
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          WITH p
          WHERE COUNT { MATCH (p)-[:KNOWS]->(f) } > 1
          RETURN p.name AS name
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    |
          | 'Alice' |

  # A simple CASE has one test-expression node with an arm per WHEN, so the spliced branch is reached repeatedly. A
  # count picks out which arm was taken, where a bool could only say whether any was.
  Scenario: Test COUNT subquery as the test expression of a simple CASE
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name,
                 CASE COUNT { MATCH (p)-[:KNOWS]->(f) } WHEN 3 THEN 'three' WHEN 1 THEN 'one' ELSE 'other' END AS k
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | k       |
          | 'Alice' | 'three' |
          | 'Bob'   | 'one'   |
          | 'Carol' | 'other' |

  Scenario: Test COUNT subquery nested in another COUNT subquery
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COUNT { MATCH (p)-[:KNOWS]->(f) WHERE COUNT { MATCH (f)<-[:KNOWS]-() } > 1 } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |

  # Nesting COUNT in COUNT leaves both folds the same, so a nested subquery inheriting its enclosing fold would pass.
  # These two spell the folds differently: outer forced fold on the main chain, inner deferred fold under a Filter.
  Scenario: Test EXISTS subquery whose body filters on a nested COUNT subquery
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name,
                 EXISTS { MATCH (p)-[:KNOWS]->(f) WHERE COUNT { MATCH (f)<-[:KNOWS]-() } > 1 } AS e
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | e     |
          | 'Alice' | true  |
          | 'Bob'   | true  |
          | 'Carol' | false |

  Scenario: Test COUNT subquery whose body filters on a nested EXISTS subquery
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name,
                 COUNT { MATCH (p)-[:KNOWS]->(f) WHERE EXISTS { MATCH (f)<-[:KNOWS]-(:Person {name: 'Bob'}) } } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | c |
          | 'Alice' | 2 |
          | 'Bob'   | 1 |
          | 'Carol' | 0 |

  Scenario: Test COUNT subquery beside an EXISTS subquery
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (f1:Friend {name: 'F1'}), (f2:Friend {name: 'F2'})
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f1)
          CREATE (a)-[:KNOWS]->(f2)
          CREATE (b)-[:KNOWS]->(f1)
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, EXISTS { MATCH (p)-[:KNOWS]->(f) } AS e, COUNT { MATCH (p)-[:KNOWS]->(f) } AS c
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | e     | c |
          | 'Alice' | true  | 3 |
          | 'Bob'   | true  | 1 |
          | 'Carol' | false | 0 |

  Scenario: Test COUNT subquery on a null variable
      Given an empty graph
      When executing query:
          """
          OPTIONAL MATCH (z:Nope)
          RETURN COUNT { MATCH (z)-[:KNOWS]->(f) } AS c;
          """
      Then the result should be:
          | c |
          | 0 |

  Scenario: Test COUNT subquery does not disturb the count aggregation
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
          CREATE (a)-[:KNOWS]->(:Friend {name: 'F1'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN count(*) AS star, count(p.name) AS named, COUNT { MATCH (p)-[:KNOWS]->(f) } AS sub
          ORDER BY sub;
          """
      Then the result should be, in order:
          | star | named | sub |
          | 1    | 1     | 0   |
          | 1    | 1     | 1   |


  # COLLECT { ... } is the third fold on the same node: the body's one column, per row, in the body's order.

  Scenario: Test COLLECT subquery collects its body's column per outer row
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'})
          CREATE (a)-[:KNOWS]->(:Friend {name: 'F1'})
          CREATE (a)-[:KNOWS]->(:Friend {name: 'F2'})
          CREATE (b)-[:KNOWS]->(:Friend {name: 'F3'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, COLLECT { MATCH (p)-[:KNOWS]->(f) RETURN f.name AS fn ORDER BY fn } AS friends
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | friends      |
          | 'Alice' | ['F1', 'F2'] |
          | 'Bob'   | ['F3']       |
          | 'Carol' | []           |

  # No rows is an empty list, not null - so size() is 0 and the value is not null.
  Scenario: Test COLLECT subquery with no rows is an empty list
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'Alice'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN COLLECT { MATCH (n:Nope) RETURN n } AS r, size(COLLECT { MATCH (n:Nope) RETURN n }) AS sz,
                 COLLECT { MATCH (n:Nope) RETURN n } IS NULL AS isnull;
          """
      Then the result should be:
          | r  | sz | isnull |
          | [] | 0  | false  |

  # The collected order is the body's row order, so an ORDER BY in the body decides it.
  Scenario: Test COLLECT subquery keeps its body's ORDER BY
      Given an empty graph
      And having executed:
          """
          CREATE (:Item {x: 3}), (:Item {x: 1}), (:Item {x: 2})
          """
      When executing query:
          """
          RETURN COLLECT { MATCH (i:Item) RETURN i.x AS v ORDER BY v DESC } AS down,
                 COLLECT { MATCH (i:Item) RETURN i.x AS v ORDER BY v ASC } AS up,
                 COLLECT { MATCH (i:Item) RETURN i.x AS v ORDER BY v DESC SKIP 1 LIMIT 1 } AS middle;
          """
      Then the result should be:
          | down      | up        | middle |
          | [3, 2, 1] | [1, 2, 3] | [2]    |

  Scenario: Test COLLECT subquery honours DISTINCT in its body
      Given an empty graph
      And having executed:
          """
          CREATE (:Item {x: 1}), (:Item {x: 1}), (:Item {x: 2})
          """
      When executing query:
          """
          RETURN COLLECT { MATCH (i:Item) RETURN DISTINCT i.x AS v ORDER BY v } AS distinct_xs,
                 COLLECT { MATCH (i:Item) RETURN i.x AS v ORDER BY v } AS all_xs;
          """
      Then the result should be:
          | distinct_xs | all_xs    |
          | [1, 2]      | [1, 1, 2] |

  # UNION deduplicates across branches, UNION ALL does not - one column per branch either way.
  Scenario: Test COLLECT subquery whose body is a UNION
      Given an empty graph
      And having executed:
          """
          CREATE (:Item {x: 1}), (:Item {x: 2})
          """
      When executing query:
          """
          RETURN COLLECT { MATCH (i:Item) RETURN i.x AS v UNION MATCH (j:Item {x: 1}) RETURN j.x AS v } AS deduped,
                 COLLECT { MATCH (i:Item) RETURN i.x AS v UNION ALL MATCH (j:Item {x: 1}) RETURN j.x AS v } AS kept;
          """
      Then the result should be:
          | deduped | kept         |
          | [1, 2]  | [1, 2, 1]    |

  Scenario: Test COLLECT subquery in a WHERE
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'})
          CREATE (a)-[:KNOWS]->(:Friend {name: 'F1'})
          CREATE (a)-[:KNOWS]->(:Friend {name: 'F2'})
          CREATE (b)-[:KNOWS]->(:Friend {name: 'F3'})
          """
      When executing query:
          """
          MATCH (p:Person)
          WHERE size(COLLECT { MATCH (p)-[:KNOWS]->(f) RETURN f }) > 1
          RETURN p.name AS name;
          """
      Then the result should be:
          | name    |
          | 'Alice' |

  Scenario: Test COLLECT subquery nested in another COLLECT subquery
      Given an empty graph
      And having executed:
          """
          CREATE (:Item {x: 1}), (:Item {x: 2})
          """
      When executing query:
          """
          RETURN COLLECT { MATCH (i:Item) RETURN COLLECT { MATCH (j:Item) RETURN j.x AS v ORDER BY v } AS inner } AS r;
          """
      Then the result should be:
          | r                |
          | [[1, 2], [1, 2]] |

  Scenario: Test COLLECT subquery beside the collect aggregation
      Given an empty graph
      And having executed:
          """
          CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(:Friend {name: 'F1'})
          CREATE (b:Person {name: 'Bob'})-[:KNOWS]->(:Friend {name: 'F2'})
          CREATE (c:Person {name: 'Carol'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, collect(p.name) AS aggregated,
                 COLLECT { MATCH (p)-[:KNOWS]->(f) RETURN f.name AS fn } AS subquery
          ORDER BY name;
          """
      Then the result should be, in order:
          | name    | aggregated | subquery |
          | 'Alice' | ['Alice']  | ['F1']   |
          | 'Bob'   | ['Bob']    | ['F2']   |
          | 'Carol' | ['Carol']  | []       |

  Scenario: Test COLLECT subquery body may reuse an outer name in a pattern
      Given an empty graph
      And having executed:
          """
          CREATE (p:Person {name: 'Peter'})-[:HAS_DOG]->(:Dog {name: 'Ozzy'})
          """
      When executing query:
          """
          MATCH (person:Person)
          RETURN COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name AS dn } AS r;
          """
      Then the result should be:
          | r        |
          | ['Ozzy'] |
