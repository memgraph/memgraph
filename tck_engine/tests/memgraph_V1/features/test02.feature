Feature: Test02

    Scenario: Match create return test
        Given an empty graph
        And having executed
            """
            CREATE (:x_1), (:z2_), (:qw34)
            """
        When executing query:
            """
            MATCH (a:x_1), (b:z2_), (c:qw34)
            CREATE (a)-[x:X]->(b) CREATE (b)<-[y:Y]-(c)
            RETURN x, y
            """
        Then the result should be:
            |  x   |  y   |
            | [:X] | [:Y] |

    Scenario: Multiple matches in one query
        Given an empty graph
        And having executed
            """
            CREATE (:x{age: 5}), (:y{age: 4}), (:z), (:x), (:y)
            """
        When executing query:
            """
            MATCH (a:x), (b:y), (c:z)
            WHERE a.age=5
            MATCH (b{age: 4})
            RETURN a, b, c
            """
        Then the result should be:
            |  a           |  b           | c    |
            | (:x{age: 5}) | (:y{age: 4}) | (:z) |

    Scenario: Match set one property return test
        Given an empty graph
        And having executed
            """
            CREATE (:q)-[:X]->()
            """
        When executing query:
            """
            MATCH (a:q)-[b]-(c)
            SET a.name='Sinisa'
            RETURN a, b, c
            """
        Then the result should be:
            |  a                   |  b   | c  |
            | (:q{name: 'Sinisa'}) | [:X] | () |

    Scenario: Match set properties from node to node return test
        Given an empty graph
        And having executed
            """
            CREATE (:q{name: 'Sinisa', x: 'y'})-[:X]->({name: 'V',  desc: 'Traktor'})
            """
        When executing query:
            """
            MATCH (a:q)-[b]-(c)
            SET c=a
            RETURN a, b, c
            """
        Then the result should be:
            |  a                           |  b   | c                          |
            | (:q{name: 'Sinisa', x: 'y'}) | [:X] | ({name: 'Sinisa', x: 'y'}) |

    Scenario: Match set properties from node to relationship return test
        Given an empty graph
        And having executed
            """
            CREATE (:q{x: 'y'})-[:X]->({y: 't'})
            """
        When executing query:
            """
            MATCH (a:q)-[b]-(c)
            SET b=a
            RETURN a, b, c
            """
        Then the result should be:
            |  a           |  b           | c          |
            | (:q{x: 'y'}) | [:X{x: 'y'}] | ({y: 't'}) |

    Scenario: Match set properties from relationship to node return test
        Given an empty graph
        And having executed
            """
            CREATE (:q)-[:X{x: 'y'}]->({y: 't'})
            """
        When executing query:
            """
            MATCH (a:q)-[b]-(c)
            SET a=b
            RETURN a, b, c
            """
        Then the result should be:
            |  a           |  b           | c          |
            | (:q{x: 'y'}) | [:X{x: 'y'}] | ({y: 't'}) |

    Scenario: Match, set properties from relationship to relationship, return test
        Given an empty graph
        When executing query:
            """
            CREATE ()-[b:X{x: 'y'}]->()-[a:Y]->()
            SET a=b
            RETURN a, b
            """
        Then the result should be:
            |  a           |  b           |
            | [:Y{x: 'y'}] | [:X{x: 'y'}] |

    Scenario: Create, set adding properties, return test
        Given an empty graph
        When executing query:
            """
            CREATE ()-[b:X{x: 'y', y: 'z'}]->()-[a:Y{x: 'z'}]->()
            SET a += b
            RETURN a, b
            """
        Then the result should be:
            |  a                   |  b                   |
            | [:Y{x: 'y', y: 'z'}] | [:X{x: 'y', y: 'z'}] |

    Scenario: Create node and add labels using set, return test
        Given an empty graph
        When executing query:
            """
            CREATE (a)
            SET a :sinisa:vu
            RETURN a
            """
        Then the result should be:
            |  a           |
            | (:sinisa:vu) |



    Scenario: Create node and delete it
        Given an empty graph
        And having executed:
           """
           CREATE (n)
           DELETE (n)
           """
        When executing query:
            """
            MATCH (n)
            RETURN n
            """
        Then the result should be empty

    Scenario: Create node with relationships and delete it, check for relationships
        Given an empty graph
        And having executed:
            """
            CREATE (n)-[:X]->()
            CREATE (n)-[:Y]->()
            DETACH DELETE (n)
            """
        When executing query:
            """
            MATCH ()-[n]->()
            RETURN n
            """
        Then the result should be empty

    Scenario: Create node with relationships and delete it, check for nodes
        Given an empty graph
        And having executed:
            """
            CREATE (n:l{a: 1})-[:X]->()
            CREATE (n)-[:Y]->()
            DETACH DELETE (n)
            """
        When executing query:
            """
            MATCH (n)
            RETURN n
            """
        Then the result should be:
            | n |
            |( )|
            |( )|

    Scenario: Create node with relationships and delete it (without parentheses), check for nodes
        Given an empty graph
        And having executed:
            """
            CREATE (n:l{a: 1})-[:X]->()
            CREATE (n)-[:Y]->()
            DETACH DELETE n
            """
        When executing query:
            """
            MATCH (n)
            RETURN n
            """
        Then the result should be:
            | n |
            |( )|
            |( )|



    Scenario: Test equal operator
        When executing query:
        """
        CREATE (a)
        RETURN 1=1 and 1.0=1.0 and 'abc'='abc' and false=false and a.age is null as n
        """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test not equal operator
        When executing query:
        """
        CREATE (a{age: 1})
        RETURN not 1<>1 and 1.0<>1.1 and 'abcd'<>'abc' and false<>true and a.age is not null as n
        """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test greater operator
        When executing query:
            """
            RETURN 2>1 and not 1.0>1.1 and 'abcd'>'abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test less operator
        When executing query:
            """
            RETURN not 2<1 and 1.0<1.1 and not 'abcd'<'abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test greater equal operator
        When executing query:
            """
            RETURN 2>=2 and not 1.0>=1.1 and 'abcd'>='abc' as n
            """
        Then the result should be:
            |   n  |
            | true |

     Scenario: Test less equal operator
        When executing query:
            """
            RETURN 2<=2 and 1.0<=1.1 and not 'abcd'<='abc' as n
            """
        Then the result should be:
            |   n  |
            | true |




    Scenario: Test plus operator
        When executing query:
            """
            RETURN 3+2=1.09+3.91 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test minus operator
        When executing query:
            """
            RETURN 3-2=1.09-0.09 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test multiply operator
        When executing query:
            """
            RETURN 3*2=1.5*4 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test divide operator1
        When executing query:
            """
            RETURN 3/2<>7.5/5 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test divide operator2
        When executing query:
            """
            RETURN 3.0/2=7.5/5 as n
            """
        Then the result should be:
            |   n  |
            | true |

    Scenario: Test mod operator
        When executing query:
            """
            RETURN 3%2=1 as n
            """
        Then the result should be:
            |   n  |
            | true |

	    #    Scenario: Test exponential operator
	    #        When executing query:
	    #            """
	    #            RETURN 3^2=81^0.5 as n
	    #            """
	    #        Then the result should be:
	    #            |   n  |
	    #            | true |
	    #
	    #    Scenario: Test one big mathematical equation
	    #        When executing query:
	    #            """
	    #            RETURN (3+2*4-3/2%2*10)/5.0^2.0=0.04 as n
	    #            """
	    #        Then the result should be:
	    #            |   n  |
	    #            | true |

	 Scenario: Test one big logical equation
	    When executing query:
		"""
		RETURN not true or true and false or not ((true xor false or true) and true or false xor true ) as n
		"""
	    Then the result should be:
		|   n   |
		| false |
