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

Feature: ProcedureCall
    Behaviour tests for Memgraph extensions to the CALL procedure clause

    Background:
        Given an empty graph

    Scenario: CALL YIELD WHERE filters results
        When executing query:
            """
            CALL mg.procedures() YIELD name WHERE name = 'mg.procedures'
            RETURN name
            """
        Then the result should be:
            | name            |
            | 'mg.procedures' |
        And no side effects

    Scenario: CALL YIELD WHERE filters out all results
        When executing query:
            """
            CALL mg.procedures() YIELD name WHERE name = 'this.does.not.exist'
            RETURN name
            """
        Then the result should be empty
        And no side effects

    Scenario: CALL YIELD WHERE with aliased field
        When executing query:
            """
            CALL mg.procedures() YIELD name AS proc_name WHERE proc_name = 'mg.procedures'
            RETURN proc_name
            """
        Then the result should be:
            | proc_name       |
            | 'mg.procedures' |
        And no side effects

    Scenario: CALL YIELD star WHERE filters results
        When executing query:
            """
            CALL mg.procedures() YIELD * WHERE name = 'mg.procedures'
            RETURN name
            """
        Then the result should be:
            | name            |
            | 'mg.procedures' |
        And no side effects
