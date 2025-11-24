# Copyright 2023 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import sys

import pytest
from common import memgraph
from gqlalchemy import GQLAlchemyError


def test_cant_add_2_nodes_with_same_property_and_unique_constraint(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")

    memgraph.execute("CREATE (:Node {prop: 1})")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE (:Node {prop: 1})")


def test_unique_constraint_fails_when_adding_label(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")

    memgraph.execute("CREATE (:Node {prop: 1})")
    memgraph.execute("CREATE (:Node2 {prop: 1})")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n:Node2) SET n:Node;")


def test_unique_constraint_fails_when_setting_property(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")

    memgraph.execute("CREATE (:Node {prop: 1})")
    memgraph.execute("CREATE (:Node:Node2 {prop: 2})")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n:Node2) SET n.prop = 1;")


def test_unique_constraint_fails_when_updating_property(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")

    memgraph.execute("CREATE (:Node {prop: 1})")
    memgraph.execute("CREATE (:Node {prop: 2})")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) WHERE n.prop = 2 SET n.prop = 1;")


def test_unique_constraint_passes_when_setting_property_to_null(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")

    memgraph.execute("CREATE (:Node {prop: 1})")
    memgraph.execute("CREATE (:Node {prop: 2})")

    memgraph.execute("MATCH (n) SET n.prop = null;")


def test_unique_constraint_passes_when_removing_property(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")

    memgraph.execute("CREATE (:Node {prop: 1})")
    memgraph.execute("CREATE (:Node {prop: 2})")

    memgraph.execute("MATCH (n) REMOVE n.prop;")


def test_unique_constraint_fails_when_setting_properties(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")

    memgraph.execute("CREATE (:Node {prop: 1})")
    memgraph.execute("CREATE (:Node {prop: 2})")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) SET n += {prop: 1, prop2: 1};")


def test_unique_constraint_fails_when_setting_properties_alternative(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")

    memgraph.execute("CREATE (:Node {prop: 1})")
    memgraph.execute("CREATE (:Node {prop: 2})")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) SET n = {prop: 1, prop2: 1};")


def test_existence_constraint_fails_when_adding_label_with_nonexistent_property(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    memgraph.execute("CREATE ();")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) SET n:Node")


def test_existence_constraint_fails_when_creating_node_with_nonexistent_property(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE (:Node);")


def test_existence_constraint_passes_when_updating_node_property(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    memgraph.execute("CREATE (:Node {prop: 1});")
    memgraph.execute("MATCH (n) SET n.prop = 2;")


def test_existence_constraint_fails_when_updating_node_property_to_null(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    memgraph.execute("CREATE (:Node {prop: 1});")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) SET n.prop = null;")


def test_existence_constraint_fails_when_removing_node_property(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    memgraph.execute("CREATE (:Node {prop: 1});")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) REMOVE n.prop;")


def test_existence_constraint_fails_when_setting_properties(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    memgraph.execute("CREATE (:Node {prop: 1});")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) SET n += {prop: null, prop2: 2};")


def test_existence_constraint_fails_when_setting_properties_alternative(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    memgraph.execute("CREATE (:Node {prop: 1});")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) SET n = {prop: null, prop2: 2};")


def test_existence_constraint_fails_when_creating_node_null_property(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE (:Node {prop: null});")


# TODO: Remove drop constraint after adding type constraints to gqlalchemy


def test_type_constraint_passes(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")

    memgraph.execute("CREATE (n:Node {prop:1}) RETURN n;")
    memgraph.execute("MATCH (n:Node) SET n.prop = 10;")

    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_violation_on_creation(memgraph):
    memgraph.execute("CREATE (n:Node {prop:'str'}) RETURN n;")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_violation_on_new_node(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE (n:Node {prop:'prop'}) RETURN n;")

    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_violation_on_property_change(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")

    memgraph.execute("CREATE (n:Node {prop:1}) RETURN n;")
    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n:Node) SET n.prop = 'prop';")

    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_violation_on_new_prop(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")

    memgraph.execute("CREATE (n:Node {prop1:1}) RETURN n;")
    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n:Node) SET n.prop = 'prop';")

    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_violation_on_label_change(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")

    memgraph.execute("CREATE (n:Node1 {prop1:1}) RETURN n;")
    with pytest.raises(GQLAlchemyError):
        memgraph.execute("MATCH (n) REMOVE n:Node1 SET n:Node SET n.prop = 'prop';")

    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_pass_on_label_change(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")

    memgraph.execute("CREATE (n:Node1 {prop1:1}) RETURN n;")
    memgraph.execute("MATCH (n) REMOVE n:Node1 SET n:Node SET n.prop = 5;")

    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_drop_wrong_type(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED STRING;")

    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_with_triggers_success(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")
    memgraph.execute(
        "CREATE TRIGGER test_trigger ON () CREATE BEFORE COMMIT EXECUTE UNWIND createdVertices AS vertex SET vertex.prop = 42;"
    )
    memgraph.execute("CREATE (n:label {prop:1});")

    properties = list(memgraph.execute_and_fetch("MATCH (n) RETURN n;"))[0]["n"]._properties
    assert properties["prop"] == 42

    memgraph.execute("DROP TRIGGER test_trigger;")
    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_with_triggers_fail_modify(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")
    memgraph.execute(
        "CREATE TRIGGER test_trigger ON () CREATE BEFORE COMMIT EXECUTE UNWIND createdVertices AS vertex SET vertex.prop = 'string';"
    )

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE (n:Node {prop:1});")

    memgraph.execute("DROP TRIGGER test_trigger;")
    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_type_constraint_with_triggers_fail_create(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")
    memgraph.execute("CREATE TRIGGER test_trigger ON () CREATE BEFORE COMMIT EXECUTE CREATE (n:Node {prop: 'string'})")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE (n:Node {prop:1});")

    memgraph.execute("DROP TRIGGER test_trigger;")
    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT n.prop IS TYPED INTEGER;")


def test_delta_release_on_unique_constraint_error(memgraph):
    memgraph.execute("create constraint on (n:Label) ASSERT n.id IS UNIQUE;")
    memgraph.execute("foreach (i in range(1, 100000) | CREATE (:Label {id: i}));")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("foreach (i in range(1, 10) | CREATE (:Label {id: i}));")

    memgraph.execute("FREE MEMORY;")

    results = list(memgraph.execute_and_fetch("SHOW STORAGE INFO;"))
    unreleased_delta_objects_actual = [x for x in results if x["storage info"] == "unreleased_delta_objects"][0][
        "value"
    ]

    assert unreleased_delta_objects_actual == 0


def test_cant_change_to_analytical_mode_with_constraints(memgraph):
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")
    with pytest.raises(GQLAlchemyError):
        memgraph.execute("STORAGE MODE IN_MEMORY_ANALYTICAL;")

    memgraph.execute("DROP CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")
    memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")
    with pytest.raises(GQLAlchemyError):
        memgraph.execute("STORAGE MODE IN_MEMORY_ANALYTICAL;")


def test_cant_create_constraints_in_analytical_mode(memgraph):
    memgraph.execute("STORAGE MODE IN_MEMORY_ANALYTICAL;")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT EXISTS (n.prop);")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.prop IS UNIQUE;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
