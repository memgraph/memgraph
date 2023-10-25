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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
