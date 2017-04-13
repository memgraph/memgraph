Feature: test03

  Scenario: Match multiple patterns 01
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (a)-[:X]->(b), (c)-[:X]->(a)
      """
    When executing query:
      """
      MATCH (a)-[]->(), (b) CREATE (a)-[r:R]->(b) RETURN a, b, r
      """
    Then the result should be:
      | a    | b    | r    |
      | (:C) | (:A) | [:R] |
      | (:C) | (:B) | [:R] |
      | (:C) | (:C) | [:R] |
      | (:A) | (:A) | [:R] |
      | (:A) | (:B) | [:R] |
      | (:A) | (:C) | [:R] |

  Scenario: Match multiple patterns 02
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (e:E), (f:F), (a)-[:X]->(b), (b)-[:X]->(c), (d)-[:X]->(e), (e)-[:X]->(f)
      """
    When executing query:
      """
      MATCH (a:B)--(b), (c:E)--(d) CREATE (b)-[r:R]->(d) return b, d, r
      """
    Then the result should be:
      | b    | d    | r    |
      | (:A) | (:D) | [:R] |
      | (:A) | (:F) | [:R] |
      | (:C) | (:D) | [:R] |
      | (:C) | (:F) | [:R] |
    
  Scenario: Match multiple patterns 03
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d)
      """
    When executing query:
      """
      MATCH (a:B)--(b), (c:B)--(d) RETURN b, d 
      """
    Then the result should be:
      | b    | d    |
      | (:A) | (:C) |
      | (:C) | (:A) |

  Scenario: Match multiple patterns 04
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d)
      """
    When executing query:
      """
      MATCH (a:A)--(b), (c:A)--(d) RETURN a, b, c, d
      """
    Then the result should be empty




  Scenario: Multiple match 01
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d)
      """
    When executing query:
      """
      MATCH (a:B)--(b) MATCH (c:B)--(d) RETURN b, d 
      """
    Then the result should be:
      | b    | d    |
      | (:A) | (:A) |
      | (:A) | (:C) |
      | (:C) | (:A) |
      | (:C) | (:C) |

  Scenario: Multiple match 02
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d)
      """
    When executing query:
      """
      MATCH (a:A)--(b) MATCH (a)--(c) RETURN a, b, c
      """
    Then the result should be:
      | a    | b    | c    |
      | (:A) | (:B) | (:B) |

  Scenario: Multiple match 03
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (a)-[:X]->(b), (c)-[:X]->(a)
      """
    When executing query:
      """
      MATCH (a)-[]->() MATCH (b) CREATE (a)-[r:R]->(b) RETURN a, b, r
      """
    Then the result should be:
      | a    | b    | r    |
      | (:C) | (:A) | [:R] |
      | (:C) | (:B) | [:R] |
      | (:C) | (:C) | [:R] |
      | (:A) | (:A) | [:R] |
      | (:A) | (:B) | [:R] |
      | (:A) | (:C) | [:R] |

  Scenario: Multiple match 04
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (e:E), (f:F), (a)-[:X]->(b), (b)-[:X]->(c), (d)-[:X]->(e), (e)-[:X]->(f)
      """
    When executing query:
      """
      MATCH (a:B)--(b) MATCH (c:E)--(d) CREATE (b)-[r:R]->(d) return b, d, r
      """
    Then the result should be:
      | b    | d    | r    |
      | (:A) | (:D) | [:R] |
      | (:A) | (:F) | [:R] |
      | (:C) | (:D) | [:R] |
      | (:C) | (:F) | [:R] |
  
  Scenario: Multiple match 05
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C)
      """
    When executing query:
      """
      MATCH(a) MATCH(a) RETURN a 
      """
    Then the result should be:
      | a    |
      | (:A) |
      | (:B) |
      | (:C) |

  Scenario: Multiple match 06
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (a)-[:R]->(b), (b)-[:R]->(c)
      """
    When executing query:
      """
      MATCH (a)-[]->() MATCH (a:B) MATCH (b:C) RETURN a, b
      """
    Then the result should be:
      | a    | b    |
      | (:B) | (:C) |

  Scenario: Multiple match 07
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C), (a)-[:R]->(b), (b)-[:R]->(c)
      """
    When executing query:
      """
      MATCH (a)-[]->() MATCH (a:B) MATCH (a:C) RETURN a
      """
    Then the result should be empty




  Scenario: Remove 01
    Given an empty graph
    And having executed
      """
      CREATE (a:A:B:C)
      """
    When executing query:
      """
      MATCH (n) REMOVE n:A:B:C RETURN n
      """
    Then the result should be:
      | n   |
      | ()  |

  Scenario: Remove 02
    Given an empty graph
    And having executed
      """
      CREATE (a:A:B:C)
      """
    When executing query:
      """
      MATCH (n) REMOVE n:B:C RETURN n
      """
    Then the result should be:
      | n     |
      | (:A)  |

  Scenario: Remove 03
    Given an empty graph
    And having executed
      """
      CREATE (a{a: 1, b: 1.0, c: 's', d: false})
      """
    When executing query:
      """
      MATCH (n) REMOVE n:A:B, n.a REMOVE n.b, n.c, n.d RETURN n
      """
    Then the result should be:
      | n   |
      | ()  |

  Scenario: Remove 04
    Given an empty graph
    And having executed
      """
      CREATE (a:A:B{a: 1, b: 's', c: 1.0, d: true})
      """
    When executing query:
      """
      MATCH (n) REMOVE n:B, n.a, n.d RETURN n

      """
    Then the result should be:
      | n                    |
      | (:A{b: 's', c: 1.0}) |




  Scenario: Multiple create 01:
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B) CREATE (c:C), (a)-[:R]->(b) CREATE (b)-[:R]->(c)
      """ 
    When executing query:
      """
      MATCH (a)-[]->() MATCH (a:B) MATCH (b:C) RETURN a, b
      """
    Then the result should be:
      | a    | b    |
      | (:B) | (:C) |

  Scenario: Multiple create 02:
    Given an empty graph
    When executing query:
      """
      CREATE (a:A) CREATE (a:B)
      """
    Then an error should be raised

  Scenario: Multiple create 02:
    Given an empty graph
    When executing query:
      """
      CREATE (a:A) CREATE (a:B)
      """
    Then an error should be raised



  
  Scenario: Count test 01:
    Given an empty graph
    And having executed
      """
      CREATE (a:A), (b:B), (c:C)
      """ 
    When executing query:
      """
      MATCH (a) RETURN COUNT(a) AS n
      """
    Then the result should be:
      | n |
      | 3 |

  Scenario: Count test 02:
    Given an empty graph 
    When executing query:
      """
      RETURN COUNT(123) AS n
      """
    Then the result should be:
      | n |
      | 1 | 
  
  Scenario: Count test 03:
    Given an empty graph
    When executing query:
      """
      RETURN COUNT(true) AS n
      """
    Then the result should be:
      | n |
      | 1 |

  Scenario: Count test 04:
    Given an empty graph 
    When executing query:
      """
      RETURN COUNT('abcd') AS n
      """
    Then the result should be:
      | n |
      | 1 |

  Scenario: Count test 05:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0}), (b{x: 0}), (c{x: 0}), (d{x: 1}), (e{x: 1})
      """ 
    When executing query:
      """
      MATCH (a) RETURN COUNT(a) AS n, a.x
      """
    Then the result should be:
      | n | a.x |
      | 3 | 0   |
      | 2 | 1   |

  Scenario: Count test 06:
    Given an empty graph
    And having executed
      """
      CREATE (), (), (), (), ()
      """ 
    When executing query:
      """
      MATCH (n) RETURN COUNT(*) AS n
      """
    Then the result should be:
      | n |
      | 5 |




  Scenario: Sum test 01:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
      """ 
    When executing query:
      """
      MATCH (a) RETURN SUM(a.x) AS n
      """
    Then an error should be raised

  Scenario: Sum test 02:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b), (c{x: 5}), (d{x: null})
      """ 
    When executing query:
      """
      MATCH (a) RETURN SUM(a.x) AS n
      """
    Then the result should be:
      | n |
      | 6 |

  Scenario: Sum test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
      """ 
    When executing query:
      """
      MATCH (a) RETURN SUM(a.y) AS n, a.x
      """
    Then the result should be:
      | n | a.x |
      | 4 | 0   |
      | 4 | 1   | 




  Scenario: Avg test 01:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
      """ 
    When executing query:
      """
      MATCH (a) RETURN AVG(a.x) AS n
      """
    Then an error should be raised

  Scenario: Avg test 02:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1.25}), (b), (c{x: 4.75}), (d{x: null})
      """ 
    When executing query:
      """
      MATCH (a) RETURN AVG(a.x) AS n
      """
    Then the result should be:
      | n   |
      | 3.0 |

  Scenario: Avg test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
      """ 
    When executing query:
      """
      MATCH (a) RETURN AVG(a.y) AS n, a.x
      """
    Then the result should be:
      | n   | a.x |
      | 2.0 | 0   |
      | 4.0 | 1   |  




  Scenario: Sqrt test 01:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
      """ 
    When executing query:
      """
      MATCH (a) RETURN SQRT(a.x) AS n
      """
    Then an error should be raised

  Scenario: Sqrt test 02:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
      """ 
    When executing query:
      """
      MATCH (a) RETURN SQRT(a.x) AS n
      """
    Then the result should be:
      | n    |
      | 1.0  |
      | null |
      | 3.0  |
      | null |




  Scenario: Min test 01:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
      """ 
    When executing query:
      """
      MATCH (a) RETURN MIN(a.x) AS n
      """
    Then an error should be raised

  Scenario: Min test 02:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
      """ 
    When executing query:
      """
      MATCH (a) RETURN MIN(a.x) AS n
      """
    Then the result should be:
      | n  |
      | 1  |

  Scenario: Min test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
      """ 
    When executing query:
      """
      MATCH (a) RETURN MIN(a.y) AS n, a.x
      """
    Then the result should be:
      | n | a.x |
      | 1 | 0   |
      | 4 | 1   |




  Scenario: Max test 01:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b{x: 7}), (c{x: 5}), (d{x: 'x'})
      """ 
    When executing query:
      """
      MATCH (a) RETURN MAX(a.x) AS n
      """
    Then an error should be raised

  Scenario: Max test 02:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b), (c{x: 9}), (d{x: null})
      """ 
    When executing query:
      """
      MATCH (a) RETURN MAX(a.x) AS n
      """
    Then the result should be:
      | n  |
      | 9  |

  Scenario: Max test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0, y:3}), (b{x: 0, y:1}), (c{x: 0}), (d{x: 1, y:4}), (e{x: 1})
      """ 
    When executing query:
      """
      MATCH (a) RETURN Max(a.y) AS n, a.x
      """
    Then the result should be:
      | n | a.x |
      | 3 | 0   |
      | 4 | 1   |




  Scenario: ToBoolean test 01:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b{x: 0}))
      """ 
    When executing query:
      """
      MATCH (a) RETURN TOBOOLEAN(a.x) AS n
      """
    Then an error should be raised

  Scenario: ToBoolean test 02:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 'TrUe'}), (b{x: 'not bool'}), (c{x: faLsE}), (d{x: null}), (e{x: 'fALse'}), (f{x: tRuE})
      """ 
    When executing query:
      """
      MATCH (a) RETURN TOBOOLEAN(a.x) AS n
      """
    Then the result should be:
      | n     |
      | true  |
      | null  |
      | false |
      | null  |
      | false |
      | true  |




  Scenario: ToInteger test 01:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: true}))
      """ 
    When executing query:
      """
      MATCH (a) RETURN TOINTEGER(a.x) AS n
      """
    Then an error should be raised

  Scenario: ToInteger test 02:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b{x: 'not int'}), (c{x: '-12'}), (d{x: null}), (e{x: '1.2'}), (f{x: '1.9'})
      """ 
    When executing query:
      """
      MATCH (a) RETURN TOINTEGER(a.x) AS n
      """
    Then the result should be:
      | n    |
      | 1    |
      | null |
      | -12  |
      | null |
      | 1    |
      | 1    |




  Scenario: ToFloat test 01:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: true}))
      """ 
    When executing query:
      """
      MATCH (a) RETURN TOFLOAT(a.x) AS n
      """
    Then an error should be raised

  Scenario: ToFloat test 02:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (b{x: 'not float'}), (c{x: '-12'}), (d{x: null}), (e{x: '1.2'}), (f{x: 1.9})
      """ 
    When executing query:
      """
      MATCH (a) RETURN TOFLOAT(a.x) AS n
      """
    Then the result should be:
      | n     |
      | 1.0   |
      | null  |
      | -12.0 |
      | null  |
      | 1.2   |
      | 1.9   |




  Scenario: Abs test 01:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: true})
      """ 
    When executing query:
      """
      MATCH (a) RETURN ABS(a.x) AS n
      """
    Then an error should be raised

  Scenario: Abs test 02:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: '1.0'})
      """ 
    When executing query:
      """
      MATCH (a) RETURN ABS(a.x) AS n
      """
    Then an error should be raised

  Scenario: Abs test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (c{x: -12}), (d{x: null}), (e{x: -2.3}), (f{x: 1.9})
      """ 
    When executing query:
      """
      MATCH (a) RETURN ABS(a.x) AS n
      """
    Then the result should be:
      | n    |
      | 1    |
      | 12   |
      | null |
      | 2.3  |
      | 1.9  |




  Scenario: Exp test 01:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: true}))
      """ 
    When executing query:
      """
      MATCH (a) RETURN EXP(a.x) AS n
      """
    Then an error should be raised

  Scenario: Exp test 02:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: '1.0'})),
      """ 
    When executing query:
      """
      MATCH (a) RETURN EXP(a.x) AS n
      """
    Then an error should be raised

  Scenario: Exp test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 1}), (c{x: -12}), (d{x: null}), (e{x: -2.3}), (f{x: 1.9})
      """ 
    When executing query:
      """
      MATCH (a) RETURN EXP(a.x) AS n
      """
    Then the result should be:
      | n                     |
      | 2.718281828459045     |
      | .00000614421235332821 |
      | null                  |
      | 0.10025884372280375   |
      | 6.6858944422792685    |




  Scenario: Log test 01:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: true}))
      """ 
    When executing query:
      """
      MATCH (a) RETURN LOG(a.x) AS n
      """
    Then an error should be raised

  Scenario: Log test 02:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: '1.0'})),
      """ 
    When executing query:
      """
      MATCH (a) RETURN LOG(a.x) AS n
      """
    Then an error should be raised

  Scenario: Log test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
      """ 
    When executing query:
      """
      MATCH (a) RETURN LOG(a.x) AS n
      """
    Then the result should be:
      | n                   |
      | -2.0955709236097197 |
      | nan                 |
      | null                |
      | 3.295836866004329   |




  Scenario: Log10 test 01:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: true}))
      """ 
    When executing query:
      """
      MATCH (a) RETURN LOG10(a.x) AS n
      """
    Then an error should be raised

  Scenario: Log10 test 02:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: '1.0'})),
      """ 
    When executing query:
      """
      MATCH (a) RETURN LOG10(a.x) AS n
      """
    Then an error should be raised

  Scenario: Log10 test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
      """ 
    When executing query:
      """
      MATCH (a) RETURN LOG10(a.x) AS n
      """
    Then the result should be:
      | n                   |
      | -0.9100948885606021 |
      | nan                 |
      | null                |
      | 1.4313637641589874  |




  Scenario: Sin test 01:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: true}))
      """ 
    When executing query:
      """
      MATCH (a) RETURN SIN(a.x) AS n
      """
    Then an error should be raised

  Scenario: Sin test 02:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: '1.0'})),
      """ 
    When executing query:
      """
      MATCH (a) RETURN SIN(a.x) AS n
      """
    Then an error should be raised

  Scenario: Sin test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
      """ 
    When executing query:
      """
      MATCH (a) RETURN SIN(a.x) AS n
      """
    Then the result should be:
      | n                   |
      | 0.12269009002431533 |
      | 0.5365729180004349  |
      | null                |
      | 0.956375928404503   |




  Scenario: Cos test 01:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: true}))
      """ 
    When executing query:
      """
      MATCH (a) RETURN COS(a.x) AS n
      """
    Then an error should be raised

  Scenario: Cos test 02:
    Given an empty graph
    And having executed
      """
      CREATE (b{x: '1.0'})),
      """ 
    When executing query:
      """
      MATCH (a) RETURN COS(a.x) AS n
      """
    Then an error should be raised

  Scenario: Cos test 03:
    Given an empty graph
    And having executed
      """
      CREATE (a{x: 0.123}), (c{x: -12}), (d{x: null}), (e{x: 27})
      """ 
    When executing query:
      """
      MATCH (a) RETURN COS(a.x) AS n
      """
    Then the result should be:
      | n                   |
      | 0.9924450321351935  |
      | 0.8438539587324921  |
      | null                |
      | -0.2921388087338362 |




  Scenario: With test 01:
    Given an empty graph
    And having executed:
      """
      CREATE (a:A), (b:B), (c:C), (d:D), (e:E), (a)-[:R]->(b), (b)-[:R]->(c),
      (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
      """
    When executing query:
      """
      MATCH (:A)--(a)-->() WITH a, COUNT(*) AS n WHERE n > 1 RETURN a
      """
    Then the result should be:
      | a    |
      | (:B) |

  Scenario: With 02
    Given an empty graph
    And having executed
      """
      CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (a)-[:R]->(b), (b)-[:R]->(c), (c)-[:R]->(d), (d)-[:R]->(a)
      """
    When executing query:
      """
      MATCH (a)--(b)
      WITH a, MAX(b.x) AS s
      RETURN a, s
      """
    Then the result should be:
      | a          |  s  |
      | (:A{x: 1}) |  4  |  
      | (:B{x: 2}) |  3  |
      | (:C{x: 3}) |  4  |
      | (:D{x: 4}) |  3  |

  Scenario: With 03
    Given an empty graph
    And having executed
      """
      CREATE (a:A{x: 1}), (b:B{x: 2}), (a)-[:R]->(b), (a)-[:R]->(b), (b)-[:R]->(a), (b)-[:R]->(a)
      """
    When executing query:
      """
      MATCH (b)--(a)--(c)
      WITH a, (SUM(b.x)+SUM(c.x)) AS s
      RETURN a, s
      """
    Then the result should be:
      | a          | s  |
      | (:A{x: 1}) | 48 |  
      | (:B{x: 2}) | 24 |

  Scenario: With test 04:
    Given an empty graph
    And having executed:
      """
      CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c),
      (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
      """
    When executing query:
      """
      MATCH (c)--(a:B)--(b)--(d)
      WITH a, b, SUM(c.x)+SUM(d.x) AS n RETURN a, b, n
      """
    Then the result should be:
      | a          | b          | n   | 
      | (:B{x: 2}) | (:A{x: 1}) | 13  |
      | (:B{x: 2}) | (:C{x: 3}) | 22  |
      | (:B{x: 2}) | (:D{x: 4}) | 14  |

  Scenario: With test 05:
    Given an empty graph
    And having executed:
      """
      CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c),
      (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
      """
    When executing query:
      """
      MATCH (c)--(a:B)--(b)--(d)
      WITH a, b, AVG(c.x + d.x) AS n RETURN a, b, n
      """
    Then the result should be:
      | a          | b          | n   | 
      | (:B{x: 2}) | (:A{x: 1}) | 6.5 |
      | (:B{x: 2}) | (:C{x: 3}) | 5.5 |
      | (:B{x: 2}) | (:D{x: 4}) | 7.0 |

  Scenario: With test 06:
    Given an empty graph
    And having executed:
      """
      CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c),
      (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
      """
    When executing query:
      """
      MATCH (c)--(a:B)--(b)--(d)
      WITH a, b, AVG(c.x + d.x) AS n RETURN MAX(n) AS n
      """
    Then the result should be:
      | n   | 
      | 7.0 |

  Scenario: With test 07:
    Given an empty graph
    And having executed:
      """
      CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c),
      (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
      """
    When executing query:
      """
      MATCH (c)--(a:B)--(b)--(d)
      WITH a, b, AVG(c.x + d.x) AS n
      WITH a, MAX(n) AS n RETURN a, n
      """
    Then the result should be:
      | a          | n   | 
      | (:B{x: 2}) | 7.0 |

  Scenario: With test 07:
    Given an empty graph
    When executing query:
      """
      CREATE (a), (b) WITH a, b CREATE (a)-[r:R]->(b) RETURN r
      """
    Then the result should be:
      | r    |
      | [:R] |

  Scenario: With test 08:
    Given an empty graph
    When executing query:
      """
      CREATE (a), (b) WITH a, b SET a:X SET b:Y WITH a, b MATCH(x:X) RETURN x
      """
    Then the result should be:
      | x    |
      | (:X) |

   Scenario: With test 09:
    Given an empty graph
    When executing query:
      """
      CREATE (a), (b), (a)-[:R]->(b) WITH a, b SET a:X SET b:Y
      WITH a MATCH(x:X)--(b) RETURN x, x AS y
      """
    Then the result should be:
      | x    | y    |
      | (:X) | (:X) |

  Scenario: With test 10:
    Given an empty graph
    And having executed:
      """
      CREATE (a:A{x: 1}), (b:B{x: 2}), (c:C{x: 3}), (d:D{x: 4}), (e:E{x: 5}), (a)-[:R]->(b), (b)-[:R]->(c),
      (b)-[:R]->(d), (c)-[:R]->(a), (c)-[:R]->(e), (d)-[:R]->(e)
      """
    When executing query:
      """
      MATCH (c)--(a:B)--(b)--(d) WITH a, b, AVG(c.x + d.x) AS av WITH AVG(av) AS avg 
      MATCH (c)--(a:B)--(b)--(d) WITH a, b, avg, AVG(c.x + d.x) AS av WHERE av>avg RETURN av
      """
    Then the result should be:
      | av  |
      | 6.5 |
      | 7.0 |

  Scenario: With test 11:
    Given an empty graph
    When executing query:
      """
      CREATE(a:A), (b:B), (c:C), (a)-[:T]->(b) WITH a DETACH DELETE a WITH a MATCH()-[r:T]->() RETURN r
      """
    Then the result should be empty

  Scenario: Exception test scenario:
    Given an empty graph
    When executing query:
      """
      CREATE(a:A) CREATE(a:B)
      """
    Then an error should be raised

