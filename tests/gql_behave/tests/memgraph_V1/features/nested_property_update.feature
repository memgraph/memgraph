Feature: Nested property Update

    Scenario: Set nested property in a map on a node
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.age = 21
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                       |
            | {id: 1, details: {age: 21, name: 'Gareth'}} |

    Scenario: Set nested property in a map on a node to null
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.age = null
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                         |
            | {id: 1, details: {age: null, name: 'Gareth'}} |

    Scenario: Set nonexistent nested property in a map on a node
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.gender = 'M'
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                                    |
            | {id: 1, details: {age: 20, gender: 'M', name: 'Gareth'}} |

    Scenario: Set nonexistent nested property in a map on a node to null
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.gender = null
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                                    |
            | {id: 1, details: {age: 20, gender: null, name: 'Gareth'}} |

    Scenario: Deep nesting of properties inside a node
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.nested.class = 1
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                                           |
            | {id: 1, details: {age: 20, name: 'Gareth', nested: {class: 1}}} |

    Scenario: Deep nesting of nonexistent properties inside a node
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details2.nested.class = 1
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                                                       |
            | {id: 1, details: {age: 20, name: 'Gareth'}, details2: {nested: {class: 1}}} |

    Scenario: Overwrite nested map to primitive
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details = 2
            RETURN properties(n) as props
            """
        Then the result should be:
            | props               |
            | {id: 1, details: 2} |

    Scenario: Append nested map with primitive throws error
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.age += 1
            RETURN properties(n) as props
            """
        Then an error should be raised

    Scenario: Append nested map with additional key value pair
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details += {class: 1}
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                                 |
            | {id: 1, details: {age: 20, class: 1, name: 'Gareth'}} |

    Scenario: Append nested map with additional key value pairs
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details += {class: 1, class2: 2}
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                                            |
            | {id: 1, details: {age: 20, class: 1, class2: 2, name: 'Gareth'}} |

    Scenario: Append nested map property primitive with a key value pair throws error
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.age += {class: 1}
            RETURN properties(n) as props
            """
        Then an error should be raised

    Scenario: Append nonexistent map with key value pair
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details2 += {class: 1}
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                                             |
            | {id: 1, details: {age: 20, name: 'Gareth'}, details2: {class: 1}} |

    Scenario: Append deep nested map with key value pair
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, details2: {dummy: 1}, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.details2 += {class: 1}
            RETURN properties(n) as props
            """
        Then the result should be:
            | props                                                                       |
            | {id: 1, details: {age: 20, details2: {dummy: 1, class: 1}, name: 'Gareth'}} |


    Scenario: Append nonexistent map with key value pair error
        Given an empty graph
        And having executed
            """
            CREATE (n {id: 1, details: {age: 20, name: 'Gareth'}})
            """
        When executing query:
            """
            MATCH (n)
            SET n.details.age.dummy += {dummy: 1}
            RETURN properties(n) as props
            """
        Then an error should be raised

    Scenario: Set nested property in a map on an edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.age = 21
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                       |
            | {id: 1, details: {age: 21, name: 'Gareth'}} |

    Scenario: Set nested property in a map on an edge to null
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.age = null
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                         |
            | {id: 1, details: {age: null, name: 'Gareth'}} |

    Scenario: Set nonexistent nested property in a map on an edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.gender = 'M'
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                                    |
            | {id: 1, details: {age: 20, gender: 'M', name: 'Gareth'}} |

    Scenario: Set nonexistent nested property in a map on an edge to null
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.gender = null
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                                     |
            | {id: 1, details: {age: 20, gender: null, name: 'Gareth'}} |

    Scenario: Deep nesting of properties inside an edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.nested.class = 1
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                                           |
            | {id: 1, details: {age: 20, name: 'Gareth', nested: {class: 1}}} |

    Scenario: Overwrite nested map to primitive in an edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details = 2
            RETURN properties(r) as props
            """
        Then the result should be:
            | props               |
            | {id: 1, details: 2} |

    Scenario: Append nested map with primitive in an edge throws error
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.age += 1
            RETURN properties(r) as props
            """
        Then an error should be raised

    Scenario: Append nested map with additional key value pair in an edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details += {class: 1}
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                                 |
            | {id: 1, details: {age: 20, class: 1, name: 'Gareth'}} |

    Scenario: Append nested map with additional key value pairs in an edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details += {class: 1, class2: 2}
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                                            |
            | {id: 1, details: {age: 20, class: 1, class2: 2, name: 'Gareth'}} |

    Scenario: Append nested map property primitive with a key value pair in an edge throws error
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.age += {class: 1}
            RETURN properties(r) as props
            """
        Then an error should be raised

    Scenario: Append nonexistent map with key value pair in an edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details2 += {class: 1}
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                                             |
            | {id: 1, details: {age: 20, name: 'Gareth'}, details2: {class: 1}} |

    Scenario: Append nonexistent map with key value pair in an edge error
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.age.dummy += {dummy: 1}
            RETURN properties(r) as props
            """
        Then an error should be raised

    Scenario: Append deep nested map with key value pair on edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, details2: {dummy: 1}, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details.details2 += {class: 1}
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                                                       |
            | {id: 1, details: {age: 20, details2: {dummy: 1, class: 1}, name: 'Gareth'}} |

    Scenario: Deep nesting of nonexistent properties inside an edge
        Given an empty graph
        And having executed
            """
            CREATE ()-[:NEXT {id: 1, details: {age: 20, name: 'Gareth'}}]->()
            """
        When executing query:
            """
            MATCH ()-[r]->()
            SET r.details2.nested.class = 1
            RETURN properties(r) as props
            """
        Then the result should be:
            | props                                                                       |
            | {id: 1, details: {age: 20, name: 'Gareth'}, details2: {nested: {class: 1}}} |
