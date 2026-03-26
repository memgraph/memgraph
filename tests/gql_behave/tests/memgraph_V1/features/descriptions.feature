Feature: Server-side descriptions

    Scenario: Set label description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person "A person node"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type    | label      | start_node_labels | end_node_labels | property | description     |
            | 'label' | ['Person'] | null              | null            | null     | 'A person node' |

    Scenario: Set edge type description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON EDGE TYPE :KNOWS "Knows relationship"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type        | label   | start_node_labels | end_node_labels | property | description          |
            | 'edge type' | 'KNOWS' | null              | null            | null     | 'Knows relationship' |

    Scenario: Set label-scoped property description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL PROPERTY :Person(age) "Age of the person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type             | label      | start_node_labels | end_node_labels | property | description         |
            | 'label property' | ['Person'] | null              | null            | 'age'    | 'Age of the person' |

    Scenario: Show all descriptions
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person "A person node"
            """
        Then the result should be empty
        When executing query:
            """
            SET DESCRIPTION ON LABEL PROPERTY :Person(name) "Name of the person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type             | label      | start_node_labels | end_node_labels | property | description          |
            | 'label'          | ['Person'] | null              | null            | null     | 'A person node'      |
            | 'label property' | ['Person'] | null              | null            | 'name'   | 'Name of the person' |

    Scenario: Delete description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person "A person node"
            """
        Then the result should be empty
        When executing query:
            """
            DELETE DESCRIPTION ON LABEL :Person
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be empty

    Scenario: Update description with SET
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person "First description"
            """
        Then the result should be empty
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person "Updated description"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type    | label      | start_node_labels | end_node_labels | property | description           |
            | 'label' | ['Person'] | null              | null            | null     | 'Updated description' |

    Scenario: Multi-label description is stored as a single combo entry
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person:Student "A student person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type    | label                 | start_node_labels | end_node_labels | property | description        |
            | 'label' | ['Person', 'Student'] | null              | null            | null     | 'A student person' |

    Scenario: Label-scoped property descriptions are independent per label
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL PROPERTY :Person(age) "Age of the person"
            """
        Then the result should be empty
        When executing query:
            """
            SET DESCRIPTION ON LABEL PROPERTY :Student(age) "Age of the student"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type             | label       | start_node_labels | end_node_labels | property | description          |
            | 'label property' | ['Person']  | null              | null            | 'age'    | 'Age of the person'  |
            | 'label property' | ['Student'] | null              | null            | 'age'    | 'Age of the student' |

    Scenario: Multi-label property description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL PROPERTY :Person:Student(age) "Age of a student person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type             | label                 | start_node_labels | end_node_labels | property | description               |
            | 'label property' | ['Person', 'Student'] | null              | null            | 'age'    | 'Age of a student person' |
        When executing query:
            """
            DELETE DESCRIPTION ON LABEL PROPERTY :Person:Student(age)
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be empty

    Scenario: Set database description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON DATABASE memgraph "The main graph database"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type       | label      | start_node_labels | end_node_labels | property | description               |
            | 'database' | 'memgraph' | null              | null            | null     | 'The main graph database' |

    Scenario: Set edge-type-scoped property description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON EDGE TYPE PROPERTY :KNOWS(since) "Year the relationship started"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type                 | label   | start_node_labels | end_node_labels | property | description                     |
            | 'edge type property' | 'KNOWS' | null              | null            | 'since'  | 'Year the relationship started' |

    Scenario: Setting description on wrong database throws error
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON DATABASE other_db "Some description"
            """
        Then an error should be raised

    Scenario: Deleting description on wrong database throws error
        Given an empty graph
        When executing query:
            """
            DELETE DESCRIPTION ON DATABASE other_db
            """
        Then an error should be raised

    Scenario: Set global property description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON PROPERTY age "Age in years"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type       | label | start_node_labels | end_node_labels | property | description    |
            | 'property' | null  | null              | null            | 'age'    | 'Age in years' |
        When executing query:
            """
            DELETE DESCRIPTION ON PROPERTY age
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be empty

    Scenario: Set edge type pattern description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON EDGE TYPE (:City)-[:IS]->(:Location) "city is a location"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type        | label | start_node_labels | end_node_labels  | property | description          |
            | 'edge type' | 'IS'  | ['City']          | ['Location']     | null     | 'city is a location' |

    Scenario: Delete edge type pattern description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON EDGE TYPE (:City)-[:IS]->(:Location) "city is a location"
            """
        Then the result should be empty
        When executing query:
            """
            DELETE DESCRIPTION ON EDGE TYPE (:City)-[:IS]->(:Location)
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be empty

    Scenario: Edge type pattern description in SHOW DESCRIPTIONS
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON EDGE TYPE (:Person)-[:KNOWS]->(:Person) "person knows person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type        | label   | start_node_labels | end_node_labels | property | description           |
            | 'edge type' | 'KNOWS' | ['Person']        | ['Person']      | null     | 'person knows person' |

    Scenario: Multi-label edge type pattern description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON EDGE TYPE (:Person:Employee)-[:MENTORS]->(:Person:Student) "Employee mentors student"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type        | label     | start_node_labels        | end_node_labels            | property   | description                |
            | 'edge type' | 'MENTORS' | [Person, 'Employee']     | ['Person', 'Student']      | null       | 'Employee mentors student' |
        When executing query:
            """
            DELETE DESCRIPTION ON EDGE TYPE (:Person:Employee)-[:MENTORS]->(:Person:Student)
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be empty

    Scenario: Set and delete edge type pattern property description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON EDGE TYPE PROPERTY (:Person)-[:KNOWS]->(:Person)(since) "Year they met"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | type                 | label   | start_node_labels | end_node_labels | property | description     |
            | 'edge type property' | 'KNOWS' | ['Person']        | ['Person']      | 'since'  | 'Year they met' |
        When executing query:
            """
            DELETE DESCRIPTION ON EDGE TYPE PROPERTY (:Person)-[:KNOWS]->(:Person)(since)
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be empty
