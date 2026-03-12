Feature: Server-side descriptions

    Scenario: Set and show label description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person "A person node"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTION ON LABEL :Person
            """
        Then the result should be:
            | description     |
            | 'A person node' |

    Scenario: Set and show edge type description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON EDGE TYPE :KNOWS "Knows relationship"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTION ON EDGE TYPE :KNOWS
            """
        Then the result should be:
            | description          |
            | 'Knows relationship' |

    Scenario: Set and show label-scoped property description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON PROPERTY :Person(age) "Age of the person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTION ON PROPERTY :Person(age)
            """
        Then the result should be:
            | description         |
            | 'Age of the person' |

    Scenario: Show all descriptions
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person "A person node"
            """
        Then the result should be empty
        When executing query:
            """
            SET DESCRIPTION ON PROPERTY :Person(name) "Name of the person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | kind       | name          | description         |
            | 'LABEL'    | 'Person'      | 'A person node'     |
            | 'PROPERTY' | 'Person(name)'| 'Name of the person'|

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
            SHOW DESCRIPTION ON LABEL :Person
            """
        Then the result should be:
            | description           |
            | 'Updated description' |

    Scenario: Multi-label description is stored as a single combo entry
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON LABEL :Person:Student "A student person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTION ON LABEL :Person:Student
            """
        Then the result should be:
            | description        |
            | 'A student person' |
        When executing query:
            """
            SHOW DESCRIPTION ON LABEL :Person
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | kind    | name             | description        |
            | 'LABEL' | 'Person:Student' | 'A student person' |

    Scenario: Label-scoped property descriptions are independent per label
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON PROPERTY :Person(age) "Age of the person"
            """
        Then the result should be empty
        When executing query:
            """
            SET DESCRIPTION ON PROPERTY :Student(age) "Age of the student"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTION ON PROPERTY :Person(age)
            """
        Then the result should be:
            | description         |
            | 'Age of the person' |
        When executing query:
            """
            SHOW DESCRIPTION ON PROPERTY :Student(age)
            """
        Then the result should be:
            | description          |
            | 'Age of the student' |
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | kind       | name           | description          |
            | 'PROPERTY' | 'Person(age)'  | 'Age of the person'  |
            | 'PROPERTY' | 'Student(age)' | 'Age of the student' |

    Scenario: Multi-label property description
        Given an empty graph
        When executing query:
            """
            SET DESCRIPTION ON PROPERTY :Person:Student(age) "Age of a student person"
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTION ON PROPERTY :Person:Student(age)
            """
        Then the result should be:
            | description               |
            | 'Age of a student person' |
        When executing query:
            """
            SHOW DESCRIPTION ON PROPERTY :Person(age)
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be:
            | kind       | name                   | description               |
            | 'PROPERTY' | 'Person:Student(age)'  | 'Age of a student person' |
        When executing query:
            """
            DELETE DESCRIPTION ON PROPERTY :Person:Student(age)
            """
        Then the result should be empty
        When executing query:
            """
            SHOW DESCRIPTIONS
            """
        Then the result should be empty

    Scenario: Show description on label with no description returns empty
        Given an empty graph
        When executing query:
            """
            SHOW DESCRIPTION ON LABEL :Unknown
            """
        Then the result should be empty
