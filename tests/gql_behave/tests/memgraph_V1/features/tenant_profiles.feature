Feature: Tenant profiles

    Scenario: Create, show, and drop tenant profile
        Given an empty graph
        When executing query:
            """
            CREATE TENANT PROFILE test_prof LIMIT memory_limit 256 MB
            """
        Then the result should be empty
        When executing query:
            """
            SHOW TENANT PROFILES
            """
        Then the result should be:
            | profile     | memory_limit | databases |
            | 'test_prof' | '256.00MiB'  | ''        |
        When executing query:
            """
            DROP TENANT PROFILE test_prof
            """
        Then the result should be empty
        When executing query:
            """
            SHOW TENANT PROFILES
            """
        Then the result should be empty

    Scenario: Show single tenant profile
        Given an empty graph
        When executing query:
            """
            CREATE TENANT PROFILE single_prof LIMIT memory_limit 100 MB
            """
        Then the result should be empty
        When executing query:
            """
            SHOW TENANT PROFILE single_prof
            """
        Then the result should be:
            | profile        | memory_limit | databases |
            | 'single_prof'  | '100.00MiB'  | ''        |
        When executing query:
            """
            DROP TENANT PROFILE single_prof
            """
        Then the result should be empty

    Scenario: Alter tenant profile
        Given an empty graph
        When executing query:
            """
            CREATE TENANT PROFILE alter_prof LIMIT memory_limit 100 MB
            """
        Then the result should be empty
        When executing query:
            """
            ALTER TENANT PROFILE alter_prof SET memory_limit 500 MB
            """
        Then the result should be empty
        When executing query:
            """
            SHOW TENANT PROFILE alter_prof
            """
        Then the result should be:
            | profile      | memory_limit | databases |
            | 'alter_prof' | '500.00MiB'  | ''        |
        When executing query:
            """
            DROP TENANT PROFILE alter_prof
            """
        Then the result should be empty

    Scenario: Create duplicate profile fails
        Given an empty graph
        When executing query:
            """
            CREATE TENANT PROFILE dup_prof LIMIT memory_limit 100 MB
            """
        Then the result should be empty
        When executing query:
            """
            CREATE TENANT PROFILE dup_prof LIMIT memory_limit 200 MB
            """
        Then an error should be raised

    Scenario: Cleanup after duplicate profile test
        Given an empty graph
        When executing query:
            """
            DROP TENANT PROFILE dup_prof
            """
        Then the result should be empty

    Scenario: Alter nonexistent profile fails
        Given an empty graph
        When executing query:
            """
            ALTER TENANT PROFILE ghost SET memory_limit 100 MB
            """
        Then an error should be raised

    Scenario: Show nonexistent profile fails
        Given an empty graph
        When executing query:
            """
            SHOW TENANT PROFILE ghost
            """
        Then an error should be raised

    Scenario: Set and remove tenant profile on default database
        Given an empty graph
        When executing query:
            """
            CREATE TENANT PROFILE db_prof LIMIT memory_limit 200 MB
            """
        Then the result should be empty
        When executing query:
            """
            SET TENANT PROFILE ON DATABASE memgraph TO db_prof
            """
        Then the result should be empty
        When executing query:
            """
            REMOVE TENANT PROFILE FROM DATABASE memgraph
            """
        Then the result should be empty
        When executing query:
            """
            DROP TENANT PROFILE db_prof
            """
        Then the result should be empty

    Scenario: Drop fails when database is attached
        Given an empty graph
        When executing query:
            """
            CREATE TENANT PROFILE attached LIMIT memory_limit 100 MB
            """
        Then the result should be empty
        When executing query:
            """
            SET TENANT PROFILE ON DATABASE memgraph TO attached
            """
        Then the result should be empty
        When executing query:
            """
            DROP TENANT PROFILE attached
            """
        Then an error should be raised

    Scenario: Cleanup after drop-with-attached test
        Given an empty graph
        When executing query:
            """
            REMOVE TENANT PROFILE FROM DATABASE memgraph
            """
        Then the result should be empty
        When executing query:
            """
            DROP TENANT PROFILE attached
            """
        Then the result should be empty

    Scenario: Unknown limit key fails
        Given an empty graph
        When executing query:
            """
            CREATE TENANT PROFILE bad_key LIMIT typo_limit 100 MB
            """
        Then an error should be raised
