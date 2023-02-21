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
          MATCH (n:Two) WHERE exists((n)<-[:TYPE]-()) RETURN n.prop;
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
