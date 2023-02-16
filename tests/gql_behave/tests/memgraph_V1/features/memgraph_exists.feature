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
          MATCH (n:One) WHERE exists((n)-[]-()) RETURN n.prop;
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
          MATCH (n:One) WHERE exists((n)-[:TYPE2]->()) AND exists((n)-[]->(:Three)) RETURN n.prop;
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


  Scenario: Test node-only hop
      Given an empty graph
      And having executed:
          """
          CREATE (:One {prop:1})-[:TYPE {prop: 1}]->(:Two {prop: 2})-[:TYPE {prop:2}]->(:Three {prop: 3})
          """
      When executing query:
          """
          MATCH (n) WHERE exists((n)) RETURN n.prop;
          """
      Then the result should be:
          | n.prop |
          | 1      |
          | 2      |
          | 3      |
