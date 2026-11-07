# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

Feature: Database name validation

    CREATE DATABASE and RENAME DATABASE reject a name only if it is empty, longer
    than 63 bytes, not valid UTF-8, contains a control character, or has leading
    or trailing whitespace. Every other name is accepted. SHOW DATABASES must
    never list a rejected name.

    Scenario: CREATE DATABASE rejects empty and space-only names
        When executing query:
            """
            CREATE DATABASE ``
            """
        Then an error should be raised
        When executing query:
            """
            CREATE DATABASE ` `
            """
        Then an error should be raised

    Scenario: CREATE DATABASE rejects leading and trailing whitespace
        When executing query:
            """
            CREATE DATABASE ` lead`
            """
        Then an error should be raised
        When executing query:
            """
            CREATE DATABASE `trail `
            """
        Then an error should be raised
        When executing query:
            """
            SHOW DATABASES
            """
        Then the database list should not contain lead
        And the database list should not contain trail

    Scenario Outline: CREATE DATABASE rejects a name over 63 bytes and lists no row
        When executing query:
            """
            CREATE DATABASE `<identifier>`
            """
        Then an error should be raised
        When executing query:
            """
            SHOW DATABASES
            """
        Then the database list should not contain <identifier>

        Examples:
            | identifier |
            | xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx |
            | жжжжжжжжжжжжжжжжжжжжжжжжжжжжжжжж |

    Scenario Outline: RENAME DATABASE rejects an invalid target and keeps the source
        When executing query:
            """
            CREATE DATABASE <source>
            """
        Then no error should be raised
        When executing query:
            """
            RENAME DATABASE <source> TO `<target>`
            """
        Then an error should be raised
        When executing query:
            """
            SHOW DATABASES
            """
        Then the database list should not contain <target>
        And the database list should contain <source>
        When executing query:
            """
            DROP DATABASE <source>
            """
        And executing query:
            """
            SHOW DATABASES
            """
        Then the database list should not contain <source>

        Examples:
            | source          | target |
            | rename_src_long | xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx |
            | rename_src_cyr  | жжжжжжжжжжжжжжжжжжжжжжжжжжжжжжжж |

    Scenario: RENAME DATABASE rejects a target with leading whitespace and keeps the source
        When executing query:
            """
            CREATE DATABASE rename_src_space
            """
        Then no error should be raised
        When executing query:
            """
            RENAME DATABASE rename_src_space TO ` lead`
            """
        Then an error should be raised
        When executing query:
            """
            SHOW DATABASES
            """
        Then the database list should contain rename_src_space
        When executing query:
            """
            DROP DATABASE rename_src_space
            """
        And executing query:
            """
            SHOW DATABASES
            """
        Then the database list should not contain rename_src_space

    Scenario Outline: CREATE DATABASE accepts names the old rule rejected
        When executing query:
            """
            CREATE DATABASE `<identifier>`
            """
        Then no error should be raised
        When executing query:
            """
            SHOW DATABASES
            """
        Then the database list should contain <name>
        When executing query:
            """
            DROP DATABASE `<identifier>`
            """
        Then no error should be raised

        Examples:
            | identifier | name |
            | .. | .. |
            | a/b | a/b |
            | a@b | a@b |
            | a:b | a:b |
            | x*y | x*y |
            | a``b | a`b |
            | my db | my db |
            | база | база |
            | xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx | xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx |
            | жжжжжжжжжжжжжжжжжжжжжжжжжжжжжжж | жжжжжжжжжжжжжжжжжжжжжжжжжжжжжжж |

    Scenario Outline: RENAME DATABASE accepts names the old rule rejected
        When executing query:
            """
            CREATE DATABASE <source>
            """
        Then no error should be raised
        When executing query:
            """
            RENAME DATABASE <source> TO `<target>`
            """
        Then no error should be raised
        When executing query:
            """
            SHOW DATABASES
            """
        Then the database list should contain <target>
        And the database list should not contain <source>
        When executing query:
            """
            DROP DATABASE `<target>`
            """
        Then no error should be raised

        Examples:
            | source          | target |
            | rename_ok_dots  | .. |
            | rename_ok_slash | a/b |
            | rename_ok_space | my db |
            | rename_ok_cyr   | база |

    Scenario: Valid database names are still accepted by CREATE, RENAME and SHOW DATABASES
        When executing query:
            """
            CREATE DATABASE valid_name_src
            """
        And executing query:
            """
            RENAME DATABASE valid_name_src TO valid_name_dst
            """
        When executing query:
            """
            SHOW DATABASES
            """
        Then the database list should contain valid_name_dst
        And the database list should not contain valid_name_src
        When executing query:
            """
            DROP DATABASE valid_name_dst
            """
        Then no error should be raised
