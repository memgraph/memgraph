Feature: Case

    Scenario: Simple CASE:
        Given an empty graph
        When executing query:
            """
            UNWIND range(1, 3) as x RETURN CASE x WHEN 2 THEN "two" END
            """
        Then the result should be:
            |  CASE x WHEN 2 THEN "two" END |
            |          null                 |
            |          'two'                |
            |          null                 |

    Scenario: Simple CASE with ELSE:
        Given an empty graph
        When executing query:
            """
            UNWIND range(1, 3) as x RETURN CASE x WHEN 2 THEN "two" ELSE "nottwo" END as z
            """
        Then the result should be:
            |    z     |
            | 'nottwo' |
            |  'two'   |
            | 'nottwo' |

    Scenario: Generic CASE:
        Given an empty graph
        When executing query:
            """
            UNWIND range(1, 3) as x RETURN CASE WHEN x > 1 THEN "greater" END as z
            """
        Then the result should be:
            |    z      |
            |   null    |
            | 'greater' |
            | 'greater' |

    Scenario: Generic CASE multiple matched whens:
        Given an empty graph
        When executing query:
            """
            UNWIND range(1, 3) as x RETURN CASE WHEN x > 10 THEN 10 WHEN x > 1 THEN 1 WHEN x > 0 THEN 0 WHEN x > "mirko" THEN 1000 END as z
            """
        Then the result should be:
            | z |
            | 0 |
            | 1 |
            | 1 |

    Scenario: Simple CASE in collect:
        Given an empty graph
        When executing query:
            """
            UNWIND range(1, 3) as x RETURN collect(CASE x WHEN 2 THEN "two" ELSE "nottwo" END) as z
            """
        Then the result should be:
            |           z                 |
            | ['nottwo', 'two', 'nottwo'] |

    Scenario: Simple CASE nullcheck does not have match:
        Given an empty graph
        When executing query:
            """
            WITH 2 AS name RETURN CASE name WHEN 3 THEN 'something went wrong' WHEN null THEN "doesn't work" ELSE 'works' END
            """
        Then the result should be:
            |  CASE name WHEN 3 THEN 'something went wrong' WHEN null THEN "doesn't work" ELSE 'works' END |
            |  'works'                                                                                     |

    Scenario: Simple CASE nullcheck does have match:
        Given an empty graph
        When executing query:
            """
            WITH 2 AS name RETURN CASE name WHEN 2 THEN 'works' WHEN null THEN "doesn't work" ELSE 'something went wrong' END
            """
        Then the result should be:
            |  CASE name WHEN 2 THEN 'works' WHEN null THEN "doesn't work" ELSE 'something went wrong' END |
            |  'works'                                                                                     |

    Scenario: Generic CASE nullcheck does have match:
        Given an empty graph
        When executing query:
            """
            WITH 2 AS name RETURN CASE WHEN name is NULL THEN "doesn't work" WHEN name = 2 THEN "works" ELSE "something went wrong" END
            """
        Then the result should be:
            |  CASE WHEN name is NULL THEN "doesn't work" WHEN name = 2 THEN "works" ELSE "something went wrong" END |
            |  'works'                                                                                               |

    Scenario: Generic CASE expression is null:
        Given an empty graph
        When executing query:
            """
            WITH null AS name RETURN CASE name WHEN null THEN "doesn't work" WHEN 2 THEN "doesn't work" ELSE 'works' END
            """
        Then the result should be:
            |  CASE name WHEN null THEN "doesn't work" WHEN 2 THEN "doesn't work" ELSE 'works' END |
            |  'works'                                                                             |
