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

  Scenario: Test exists equal to true
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

  Scenario: Test EXISTS subquery alongside an aggregation
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (p:Person)
          RETURN p.name AS name, count(*) AS c, EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) } AS h
          ORDER BY name;
          """
      Then the result should be:
          | name           | c | h     |
          | 'Bob'          | 1 | false |
          | 'Regina King'  | 1 | true  |

  Scenario: Test EXISTS subquery in the WHERE of an aggregating WITH
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (p:Person)
          WITH p, count(*) AS c WHERE EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) }
          RETURN p.name AS name, c;
          """
      Then the result should be:
          | name           | c |
          | 'Regina King'  | 1 |

  Scenario: Test EXISTS subquery in the ORDER BY of an aggregating WITH
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (p:Person)
          WITH p, count(*) AS c ORDER BY EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) } DESC
          RETURN p.name AS name, c;
          """
      Then the result should be, in order:
          | name           | c |
          | 'Regina King'  | 1 |
          | 'Bob'          | 1 |

  Scenario: Test EXISTS subquery inside an aggregate argument
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'Regina King'})-[:ACTED_IN]->(:Movie {title: 'Jerry Maguire'})
          CREATE (:Person {name: 'Bob'})
          """
      When executing query:
          """
          MATCH (p:Person)
          WITH p ORDER BY p.name
          RETURN collect(EXISTS { MATCH (p)-[:ACTED_IN]->(:Movie) }) AS h;
          """
      Then the result should be:
          | h              |
          | [false, true]  |

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
