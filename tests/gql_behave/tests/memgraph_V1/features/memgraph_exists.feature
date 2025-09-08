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

  Scenario: Test exists does not work in WITH clauses
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n:Two) WITH n WHERE exists((n)<-[:TYPE]-()) RETURN n.prop;
          """
      Then an error should be raised

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

  Scenario: Test exists does not work in RETURN clauses
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE]->(:Two);
          """
      When executing query:
          """
          MATCH (n) RETURN exists((n)-[]-());
          """
      Then an error should be raised

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

  Scenario: Test invalid RETURN EXISTS
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          """
      When executing query:
          """
          MATCH (person:Person) RETURN EXISTS { (person)-[:HAS_DOG]->(:Dog) } AS has_dog;
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

  Scenario: Test invalid EXISTS with UNION in RETURN
      Given an empty graph
      And having executed:
          """
          CREATE (:Person {name: 'John'})-[:HAS_DOG]->(:Dog {name: 'Rex'})
          CREATE (:Person {name: 'Alice'})-[:HAS_CAT]->(:Cat {name: 'Whiskers'})
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
              } AS hasPet;
          """
      Then an error should be raised

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
