# Copyright 2025 Memgraph Ltd.
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

from common import get_results_length, memgraph


def test_drop_all_constraints(memgraph):
    memgraph.execute("CREATE (:Person {name: 'John', age: 30, email: 'john@example.com'})")
    memgraph.execute("CREATE (:Person {name: 'Jane', age: 25, email: 'jane@example.com'})")
    memgraph.execute("CREATE (:Company {name: 'Acme Corp', founded: 2020})")
    
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name)")
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE")
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT n.age IS TYPED INTEGER")
    memgraph.execute("CREATE CONSTRAINT ON (n:Company) ASSERT EXISTS (n.name)")
    memgraph.execute("CREATE CONSTRAINT ON (n:Company) ASSERT n.founded IS TYPED INTEGER")
    
    constraint_info = list(memgraph.execute_and_fetch("SHOW CONSTRAINT INFO"))
    assert len(constraint_info) == 5, f"Expected exactly 5 constraints, but got {len(constraint_info)}"
    
    memgraph.execute("DROP ALL CONSTRAINTS")

    constraint_info_after = list(memgraph.execute_and_fetch("SHOW CONSTRAINT INFO"))
    assert len(constraint_info_after) == 0, f"Expected 0 constraints after DROP ALL CONSTRAINTS, but got {len(constraint_info_after)}: {constraint_info_after}"

    person_count = get_results_length(memgraph, "MATCH (n:Person) RETURN n")
    company_count = get_results_length(memgraph, "MATCH (n:Company) RETURN n")
    
    assert person_count == 2, f"Expected 2 Person nodes, but got {person_count}"
    assert company_count == 1, f"Expected 1 Company node, but got {company_count}"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
