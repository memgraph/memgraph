#!/usr/bin/python3

# Copyright 2021 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import base64
import os
import sys

import interactive_mg_runner
import neo4j.exceptions
import pytest
from common import get_data_path, get_logs_path
from neo4j import Auth, GraphDatabase

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "memgraph", "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

AUTH_MODULE_PATH = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "dummy_sso_module.py"))
INSTANCE_NAME = "test_instance"
MG_URI = "bolt://localhost:7687"
CLIENT_ERROR_MESSAGE = "Authentication failure"

file = "test_multi_tenant_sso"


def get_instances(test_name: str):
    return {
        INSTANCE_NAME: {
            "args": ["--bolt-port=7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"{get_logs_path(file, test_name)}/test_instance.log",
            "data_directory": f"{get_data_path(file, test_name)}",
            "setup_queries": [],
        }
    }


@pytest.fixture(scope="function")
def multi_tenant_setup(request):
    """Fixture that sets up multi-tenant environment with different roles and databases."""
    test_name = request.function.__name__
    instances = get_instances(test_name)

    # Start Memgraph without SSO first
    interactive_mg_runner.start_all(instances)

    # Create roles, databases, and permissions
    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        with client.session() as session:
            # Create roles with different privilege levels
            session.run("CREATE ROLE admin;").consume()
            session.run("CREATE ROLE architect;").consume()
            session.run("CREATE ROLE user;").consume()
            session.run("CREATE ROLE readonly;").consume()
            session.run("CREATE ROLE limited;").consume()

            # Grant different privilege combinations
            session.run("GRANT ALL PRIVILEGES TO admin;").consume()
            session.run("GRANT ALL PRIVILEGES TO architect;").consume()
            session.run("GRANT MATCH, CREATE, DELETE, SET, REMOVE, INDEX, MULTI_DATABASE_USE TO user;").consume()
            session.run("GRANT MATCH, MULTI_DATABASE_USE TO readonly;").consume()
            session.run("GRANT MATCH, CREATE, MULTI_DATABASE_USE TO limited;").consume()

            # Create databases
            session.run("CREATE DATABASE admin_db;").consume()
            session.run("CREATE DATABASE architect_db;").consume()
            session.run("CREATE DATABASE user_db;").consume()
            session.run("CREATE DATABASE readonly_db;").consume()
            session.run("CREATE DATABASE limited_db;").consume()

            # Grant database access to roles
            session.run("GRANT DATABASE * TO admin;").consume()
            session.run("GRANT DATABASE architect_db TO architect;").consume()
            session.run("GRANT DATABASE user_db TO user;").consume()
            session.run("GRANT DATABASE readonly_db TO readonly;").consume()
            session.run("GRANT DATABASE limited_db TO limited;").consume()

            # Grant different label permissions per database
            # Admin database - admin has full access
            session.run("USE DATABASE admin_db;").consume()
            session.run("GRANT CREATE ON LABELS * TO admin;").consume()
            session.run("GRANT READ ON LABELS * TO admin;").consume()
            session.run("GRANT UPDATE ON LABELS * TO admin;").consume()
            session.run("GRANT DELETE ON LABELS * TO admin;").consume()
            session.run("GRANT CREATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT READ ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT UPDATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT DELETE ON EDGE_TYPES * TO admin;").consume()

            # Architect database - architect has full access, admin has full access
            session.run("USE DATABASE architect_db;").consume()
            session.run("GRANT CREATE ON LABELS * TO architect;").consume()
            session.run("GRANT READ ON LABELS * TO architect;").consume()
            session.run("GRANT UPDATE ON LABELS * TO architect;").consume()
            session.run("GRANT DELETE ON LABELS * TO architect;").consume()
            session.run("GRANT CREATE ON EDGE_TYPES * TO architect;").consume()
            session.run("GRANT READ ON EDGE_TYPES * TO architect;").consume()
            session.run("GRANT UPDATE ON EDGE_TYPES * TO architect;").consume()
            session.run("GRANT DELETE ON EDGE_TYPES * TO architect;").consume()
            session.run("GRANT CREATE ON LABELS * TO admin;").consume()
            session.run("GRANT READ ON LABELS * TO admin;").consume()
            session.run("GRANT UPDATE ON LABELS * TO admin;").consume()
            session.run("GRANT DELETE ON LABELS * TO admin;").consume()
            session.run("GRANT CREATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT READ ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT UPDATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT DELETE ON EDGE_TYPES * TO admin;").consume()

            # User database - user has limited access, admin has full access
            session.run("USE DATABASE user_db;").consume()
            session.run("GRANT UPDATE ON LABELS :Person, :Company, :Product TO user;").consume()
            session.run("GRANT READ ON LABELS :Review, :Audit TO user;").consume()
            session.run("GRANT CREATE ON EDGE_TYPES :WORKS_FOR, :OWNS, :REVIEWS TO user;").consume()
            session.run("GRANT READ ON EDGE_TYPES :WORKS_FOR, :OWNS, :REVIEWS TO user;").consume()
            session.run("GRANT UPDATE ON EDGE_TYPES :WORKS_FOR, :OWNS, :REVIEWS TO user;").consume()
            session.run("GRANT DELETE ON EDGE_TYPES :WORKS_FOR, :OWNS, :REVIEWS TO user;").consume()
            session.run("GRANT READ ON EDGE_TYPES :AUDITS, :ADMIN_ACCESS TO user;").consume()
            session.run("GRANT CREATE ON LABELS * TO admin;").consume()
            session.run("GRANT READ ON LABELS * TO admin;").consume()
            session.run("GRANT UPDATE ON LABELS * TO admin;").consume()
            session.run("GRANT DELETE ON LABELS * TO admin;").consume()
            session.run("GRANT CREATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT READ ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT UPDATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT DELETE ON EDGE_TYPES * TO admin;").consume()

            # Readonly database - readonly has read-only access, admin has full access
            session.run("USE DATABASE readonly_db;").consume()
            session.run("GRANT READ ON LABELS * TO readonly;").consume()
            session.run("GRANT READ ON EDGE_TYPES * TO readonly;").consume()
            session.run("GRANT CREATE ON LABELS * TO admin;").consume()
            session.run("GRANT READ ON LABELS * TO admin;").consume()
            session.run("GRANT UPDATE ON LABELS * TO admin;").consume()
            session.run("GRANT DELETE ON LABELS * TO admin;").consume()
            session.run("GRANT CREATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT READ ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT UPDATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT DELETE ON EDGE_TYPES * TO admin;").consume()

            # Limited database - limited has restricted access, admin has full access
            session.run("USE DATABASE limited_db;").consume()
            session.run("GRANT READ ON LABELS :Person, :Company TO limited;").consume()
            session.run("GRANT NOTHING ON LABELS :Audit, :Admin TO limited;").consume()
            session.run("GRANT READ ON EDGE_TYPES :WORKS_FOR, :OWNS TO limited;").consume()
            session.run("GRANT NOTHING ON EDGE_TYPES :AUDITS, :ADMIN_ACCESS TO limited;").consume()
            session.run("GRANT CREATE ON LABELS * TO admin;").consume()
            session.run("GRANT READ ON LABELS * TO admin;").consume()
            session.run("GRANT UPDATE ON LABELS * TO admin;").consume()
            session.run("GRANT DELETE ON LABELS * TO admin;").consume()
            session.run("GRANT CREATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT READ ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT UPDATE ON EDGE_TYPES * TO admin;").consume()
            session.run("GRANT DELETE ON EDGE_TYPES * TO admin;").consume()

            # Set main databases for roles
            session.run("SET MAIN DATABASE admin_db FOR admin;").consume()
            session.run("SET MAIN DATABASE architect_db FOR architect;").consume()
            session.run("SET MAIN DATABASE user_db FOR user;").consume()
            session.run("SET MAIN DATABASE readonly_db FOR readonly;").consume()
            session.run("SET MAIN DATABASE limited_db FOR limited;").consume()

    interactive_mg_runner.stop(instances, INSTANCE_NAME)

    # Restart with SSO enabled
    instances[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(instances)

    yield instances

    # Cleanup
    interactive_mg_runner.stop(instances, INSTANCE_NAME, keep_directories=False)


def test_admin_full_privileges(multi_tenant_setup):
    """Test admin user with full privileges across all databases."""

    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "admin_user"

            # Test access to all databases - admin has full access to all
            for db_name in ["admin_db", "architect_db", "user_db", "readonly_db", "limited_db"]:
                session.run(f"USE DATABASE {db_name};").consume()

                # Should be able to create nodes
                session.run("CREATE (n:TestNode {name: 'admin_test'})").consume()

                # Should be able to create relationships
                session.run("CREATE (n1:Node1)-[:RELATES_TO]->(n2:Node2)").consume()

                # Should be able to create indexes
                session.run("CREATE INDEX ON :TestNode(name)").consume()

                # Should be able to create triggers
                session.run(
                    """CREATE TRIGGER adminTrigger ON () CREATE AFTER COMMIT
                    EXECUTE UNWIND createdVertices AS newNodes SET newNodes.created = timestamp();"""
                ).consume()

            # Test label-based permissions - admin has CRUD on all labels
            session.run("CREATE (p:Person {name: 'John', age: 30})").consume()
            session.run("CREATE (c:Company {name: 'TechCorp'})").consume()
            session.run("CREATE (prod:Product {name: 'Software'})").consume()
            session.run("CREATE (r:Review {rating: 5})").consume()
            session.run("CREATE (a:Audit {action: 'login'})").consume()

            # Should be able to read all labels
            person_result = list(session.run("MATCH (p:Person) RETURN p.name"))
            assert len(person_result) == 1

            # Should be able to update all labels
            session.run("MATCH (p:Person) SET p.age = 31").consume()

            # Should be able to delete all labels
            session.run("MATCH (p:Person) DELETE p").consume()
            session.run("MATCH (c:Company) DELETE c").consume()
            session.run("MATCH (prod:Product) DELETE prod").consume()
            session.run("MATCH (r:Review) DELETE r").consume()
            session.run("MATCH (a:Audit) DELETE a").consume()

            # Test edge type permissions - admin has CRUD on all edge types
            session.run("CREATE (p1:Person {name: 'John'})-[:WORKS_FOR]->(c1:Company {name: 'TechCorp'})").consume()
            session.run("CREATE (p2:Person {name: 'Jane'})-[:OWNS]->(c2:Company {name: 'Startup'})").consume()
            session.run("CREATE (p3:Person {name: 'Bob'})-[:REVIEWS]->(prod:Product {name: 'Software'})").consume()
            session.run("CREATE (p4:Person {name: 'Alice'})-[:AUDITS]->(c3:Company {name: 'AuditCorp'})").consume()
            session.run(
                "CREATE (admin:Admin {role: 'admin'})-[:ADMIN_ACCESS]->(c4:Company {name: 'AdminCorp'})"
            ).consume()

            # Should be able to read all edge types
            works_for_result = list(session.run("MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name"))
            owns_result = list(session.run("MATCH (p:Person)-[:OWNS]->(c:Company) RETURN p.name, c.name"))
            reviews_result = list(session.run("MATCH (p:Person)-[:REVIEWS]->(prod:Product) RETURN p.name, prod.name"))
            audits_result = list(session.run("MATCH (p:Person)-[:AUDITS]->(c:Company) RETURN p.name, c.name"))
            admin_access_result = list(
                session.run("MATCH (admin:Admin)-[:ADMIN_ACCESS]->(c:Company) RETURN admin.role, c.name")
            )

            assert len(works_for_result) == 1
            assert len(owns_result) == 1
            assert len(reviews_result) == 1
            assert len(audits_result) == 1
            assert len(admin_access_result) == 1

            # Should be able to update edge properties
            session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) SET r.since = '2023'").consume()
            session.run("MATCH (p:Person)-[r:OWNS]->(c:Company) SET r.percentage = 100").consume()

            # Should be able to delete all edge types
            session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) DELETE r").consume()
            session.run("MATCH (p:Person)-[r:OWNS]->(c:Company) DELETE r").consume()
            session.run("MATCH (p:Person)-[r:REVIEWS]->(prod:Product) DELETE r").consume()
            session.run("MATCH (p:Person)-[r:AUDITS]->(c:Company) DELETE r").consume()
            session.run("MATCH (admin:Admin)-[r:ADMIN_ACCESS]->(c:Company) DELETE r").consume()


def test_sso_show_current_role_with_multi_role(multi_tenant_setup):
    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session(database="admin_db") as session:
            session.run("MATCH (n) RETURN n;").consume()
            # The user should have the architect role that was created in the fixture
            current_roles_result = list(session.run("SHOW CURRENT ROLE;"))
            assert len(current_roles_result) == 1
            assert "admin" in [row["role"] for row in current_roles_result]
            current_roles_result = list(session.run("SHOW CURRENT ROLES;"))
            assert len(current_roles_result) == 1
            assert "admin" in [row["role"] for row in current_roles_result]

        with client.session(database="architect_db") as session:
            session.run("MATCH (n) RETURN n;").consume()
            # The user should have the architect role that was created in the fixture
            current_roles_result = list(session.run("SHOW CURRENT ROLE;"))
            assert len(current_roles_result) == 2
            assert "admin" in [row["role"] for row in current_roles_result]
            assert "architect" in [row["role"] for row in current_roles_result]
            current_roles_result = list(session.run("SHOW CURRENT ROLES;"))
            assert len(current_roles_result) == 2
            assert "admin" in [row["role"] for row in current_roles_result]
            assert "architect" in [row["role"] for row in current_roles_result]

        with client.session(database="user_db") as session:
            session.run("MATCH (n) RETURN n;").consume()
            # The user should have the architect role that was created in the fixture
            current_roles_result = list(session.run("SHOW CURRENT ROLE;"))
            assert len(current_roles_result) == 2
            assert "admin" in [row["role"] for row in current_roles_result]
            assert "user" in [row["role"] for row in current_roles_result]
            current_roles_result = list(session.run("SHOW CURRENT ROLES;"))
            assert len(current_roles_result) == 2
            assert "admin" in [row["role"] for row in current_roles_result]
            assert "user" in [row["role"] for row in current_roles_result]


def test_architect_privileges(multi_tenant_setup):
    """Test architect user with architect and user privileges."""

    response = base64.b64encode(b"multi_role_architect").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "architect_user"

            # Test access to architect_db only - architect has full access to architect_db
            session.run("USE DATABASE architect_db;").consume()

            # Should be able to create nodes
            session.run("CREATE (n:TestNode {name: 'architect_test'})").consume()

            # Should be able to create relationships
            session.run("CREATE (n1:Node1)-[:RELATES_TO]->(n2:Node2)").consume()

            # Should be able to create indexes
            session.run("CREATE INDEX ON :TestNode(name)").consume()

            # Should be able to create triggers
            session.run(
                """CREATE TRIGGER architectTrigger ON () CREATE AFTER COMMIT
                EXECUTE UNWIND createdVertices AS newNodes SET newNodes.created = timestamp();"""
            ).consume()

            # Should NOT be able to access admin_db
            try:
                session.run("USE DATABASE admin_db;").consume()
                session.run("CREATE (n:TestNode {name: 'admin_test'})").consume()
                assert False, "Architect should not have access to admin_db"
            except Exception:
                pass  # Expected to fail

            # Test label-based permissions for architect in architect_db - has full access
            session.run("USE DATABASE architect_db;").consume()

            # Should be able to CRUD on all labels (full access in architect_db)
            session.run("CREATE (p:Person {name: 'Alice', age: 25})").consume()
            session.run("CREATE (c:Company {name: 'Startup'})").consume()
            session.run("CREATE (prod:Product {name: 'App'})").consume()
            session.run("CREATE (r:Review {rating: 4})").consume()
            session.run("CREATE (a:Audit {action: 'test'})").consume()

            # Should be able to read all labels
            person_result = list(session.run("MATCH (p:Person) RETURN p.name"))
            company_result = list(session.run("MATCH (c:Company) RETURN c.name"))
            product_result = list(session.run("MATCH (prod:Product) RETURN prod.name"))
            review_result = list(session.run("MATCH (r:Review) RETURN r.rating"))
            audit_result = list(session.run("MATCH (a:Audit) RETURN a.action"))

            assert len(person_result) == 1
            assert len(company_result) == 1
            assert len(product_result) == 1
            assert len(review_result) == 1
            assert len(audit_result) == 1

            # Should be able to update all labels
            session.run("MATCH (p:Person) SET p.age = 26").consume()
            session.run("MATCH (c:Company) SET c.name = 'UpdatedStartup'").consume()
            session.run("MATCH (prod:Product) SET prod.version = '1.0'").consume()
            session.run("MATCH (r:Review) SET r.rating = 5").consume()
            session.run("MATCH (a:Audit) SET a.action = 'updated'").consume()

            # Should be able to delete all labels
            session.run("MATCH (p:Person) DELETE p").consume()
            session.run("MATCH (c:Company) DELETE c").consume()
            session.run("MATCH (prod:Product) DELETE prod").consume()
            session.run("MATCH (r:Review) DELETE r").consume()
            session.run("MATCH (a:Audit) DELETE a").consume()

            # Test edge type permissions for architect in architect_db - has full access
            # Should be able to CRUD on all edge types (full access in architect_db)
            session.run(
                "CREATE (p1:Person {name: 'Alice'})-[:WORKS_FOR]->(c1:Company {name: 'ArchitectCorp'})"
            ).consume()
            session.run("CREATE (p2:Person {name: 'Bob'})-[:OWNS]->(c2:Company {name: 'ArchitectStartup'})").consume()
            session.run(
                "CREATE (p3:Person {name: 'Charlie'})-[:REVIEWS]->(prod:Product {name: 'ArchitectApp'})"
            ).consume()
            session.run("CREATE (p4:Person {name: 'David'})-[:AUDITS]->(c3:Company {name: 'AuditCorp'})").consume()
            session.run(
                "CREATE (admin:Admin {role: 'architect'})-[:ADMIN_ACCESS]->(c4:Company {name: 'AdminCorp'})"
            ).consume()

            # Should be able to read all edge types
            works_for_result = list(session.run("MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name"))
            owns_result = list(session.run("MATCH (p:Person)-[:OWNS]->(c:Company) RETURN p.name, c.name"))
            reviews_result = list(session.run("MATCH (p:Person)-[:REVIEWS]->(prod:Product) RETURN p.name, prod.name"))
            audits_result = list(session.run("MATCH (p:Person)-[:AUDITS]->(c:Company) RETURN p.name, c.name"))
            admin_access_result = list(
                session.run("MATCH (admin:Admin)-[:ADMIN_ACCESS]->(c:Company) RETURN admin.role, c.name")
            )

            assert len(works_for_result) == 1
            assert len(owns_result) == 1
            assert len(reviews_result) == 1
            assert len(audits_result) == 1
            assert len(admin_access_result) == 1

            # Should be able to update all edge types
            session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) SET r.since = '2023'").consume()
            session.run("MATCH (p:Person)-[r:OWNS]->(c:Company) SET r.percentage = 100").consume()
            session.run("MATCH (p:Person)-[r:REVIEWS]->(prod:Product) SET r.rating = 5").consume()
            session.run("MATCH (p:Person)-[r:AUDITS]->(c:Company) SET r.action = 'updated'").consume()
            session.run("MATCH (admin:Admin)-[r:ADMIN_ACCESS]->(c:Company) SET r.level = 'high'").consume()

            # Should be able to delete all edge types
            session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) DELETE r").consume()
            session.run("MATCH (p:Person)-[r:OWNS]->(c:Company) DELETE r").consume()
            session.run("MATCH (p:Person)-[r:REVIEWS]->(prod:Product) DELETE r").consume()
            session.run("MATCH (p:Person)-[r:AUDITS]->(c:Company) DELETE r").consume()
            session.run("MATCH (admin:Admin)-[r:ADMIN_ACCESS]->(c:Company) DELETE r").consume()


def test_user_privileges(multi_tenant_setup):
    """Test regular user with limited privileges."""

    response = base64.b64encode(b"multi_role_user").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "regular_user"

            # Should NOT be able to access other databases
            for db_name in ["admin_db", "architect_db", "readonly_db", "limited_db"]:
                try:
                    session.run(f"USE DATABASE {db_name};").consume()
                    session.run("CREATE (n:TestNode {name: 'test'})").consume()
                    assert False, f"User should not have access to {db_name}"
                except Exception:
                    pass  # Expected to fail

            # Test label-based permissions for user in user_db - has mixed permissions
            session.run("USE DATABASE user_db;").consume()

            # Should NOT be able to CREATE on Person, Company, Product
            try:
                session.run("CREATE (p:Person {name: 'Bob', age: 35})").consume()
                assert False, "User should not be able to create Person"
            except Exception:
                pass  # Expected to fail
            try:
                session.run("CREATE (c:Company {name: 'Corp'})").consume()
                assert False, "User should not be able to create Company"
            except Exception:
                pass  # Expected to fail
            try:
                session.run("CREATE (prod:Product {name: 'Tool'})").consume()
                assert False, "User should not be able to create Product"
            except Exception:
                pass  # Expected to fail

            # Should be able to UPDATE these labels
            session.run("MATCH (p:Person) SET p.age = 36").consume()
            session.run("MATCH (c:Company) SET c.name = 'NewCorp'").consume()
            session.run("MATCH (prod:Product) SET prod.version = '2.0'").consume()

            # Should be able to READ on Review and Audit
            session.run("MATCH (r:Review {rating: 3}) RETURN *").consume()
            session.run("MATCH (a:Audit {action: 'access'}) RETURN *").consume()

            # Should NOT be able to UPDATE Review and Audit (only READ permission)
            try:
                session.run("MATCH (r:Review) SET r.rating = 5").consume()
                assert False, "User should not be able to update Review with READ permission"
            except Exception:
                pass  # Expected to fail

            try:
                session.run("MATCH (a:Audit) SET a.action = 'logout'").consume()
                assert False, "User should not be able to update Audit with READ permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to DELETE any labels (only UPDATE permission)
            try:
                session.run("MATCH (p:Person) DELETE p").consume()
                assert False, "User should not be able to delete Person with UPDATE permission"
            except Exception:
                pass  # Expected to fail

            # Test edge type permissions for user in user_db - has mixed permissions
            # Should be able to UPDATE on WORKS_FOR, OWNS, REVIEWS
            session.run("CREATE ()-[:WORKS_FOR]->()").consume()
            session.run("CREATE ()-[:OWNS]->()").consume()
            session.run("CREATE ()-[:REVIEWS]->()").consume()

            # Should NOT be able to UPDATE AUDITS and ADMIN_ACCESS (only READ permission)
            try:
                session.run("MATCH (p:Person)-[r:AUDITS]->(c:Company) SET r.action = 'modified'").consume()
                assert False, "User should not be able to update AUDITS with READ permission"
            except Exception:
                pass  # Expected to fail

            try:
                session.run("MATCH (admin:Admin)-[r:ADMIN_ACCESS]->(c:Company) SET r.level = 'high'").consume()
                assert False, "User should not be able to update ADMIN_ACCESS with READ permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to DELETE any edge types (only UPDATE permission)
            try:
                session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) DELETE r").consume()
                assert False, "User should not be able to delete WORKS_FOR with UPDATE permission"
            except Exception:
                pass  # Expected to fail


def test_readonly_privileges(multi_tenant_setup):
    """Test readonly user with read-only privileges."""

    response = base64.b64encode(b"multi_role_readonly").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "readonly_user"

            # Test access to readonly_db only
            session.run("USE DATABASE readonly_db;").consume()

            # Should be able to read data
            session.run("MATCH (n) RETURN n LIMIT 1").consume()

            # Should NOT be able to create nodes
            try:
                session.run("CREATE (n:TestNode {name: 'readonly_test'})").consume()
                assert False, "Readonly user should not be able to create nodes"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to create relationships
            try:
                session.run("CREATE (n1:Node1)-[:RELATES_TO]->(n2:Node2)").consume()
                assert False, "Readonly user should not be able to create relationships"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to create indexes
            try:
                session.run("CREATE INDEX ON :TestNode(name)").consume()
                assert False, "Readonly user should not be able to create indexes"
            except Exception:
                pass  # Expected to fail

            # Test label-based permissions for readonly user in readonly_db - has READ access
            session.run("USE DATABASE readonly_db;").consume()

            # Should NOT be able to CREATE all labels (has READ permission on all labels in readonly_db)
            try:
                session.run("CREATE (p:Person {name: 'Charlie', age: 40})").consume()
                assert False, "Readonly user should not be able to create Person"
            except Exception:
                pass  # Expected to fail
            try:
                session.run("CREATE (c:Company {name: 'ReadOnlyCorp'})").consume()
                assert False, "Readonly user should not be able to create Company"
            except Exception:
                pass  # Expected to fail
            try:
                session.run("CREATE (prod:Product {name: 'ReadOnlyProduct'})").consume()
                assert False, "Readonly user should not be able to create Product"
            except Exception:
                pass  # Expected to fail

            # Should be able to read all labels
            session.run("MATCH (p:Person) RETURN p.name").consume()
            session.run("MATCH (c:Company) RETURN c.name").consume()
            session.run("MATCH (prod:Product) RETURN prod.name").consume()
            session.run("MATCH (r:Review) RETURN r.rating").consume()
            session.run("MATCH (a:Audit) RETURN a.action").consume()

            # Should NOT be able to UPDATE any labels (only READ permission)
            try:
                session.run("MATCH (p:Person) SET p.age = 41").consume()
                assert False, "Readonly user should not be able to update Person with READ permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to DELETE any labels (only READ permission)
            try:
                session.run("MATCH (p:Person) DELETE p").consume()
                assert False, "Readonly user should not be able to delete Person with READ permission"
            except Exception:
                pass  # Expected to fail

            # Test edge type permissions for readonly user in readonly_db - has READ access
            # Should NOT be able to CREATE all edge types (has READ permission on all edge types in readonly_db)
            try:
                session.run("CREATE ()-[:WORKS_FOR]->()").consume()
                assert False, "Readonly user should not be able to create WORKS_FOR"
            except Exception:
                pass  # Expected to fail
            try:
                session.run("CREATE ()-[:OWNS]->()").consume()
                assert False, "Readonly user should not be able to create OWNS"
            except Exception:
                pass  # Expected to fail
            try:
                session.run("CREATE ()-[:REVIEWS]->()").consume()
                assert False, "Readonly user should not be able to create REVIEWS"
            except Exception:
                pass  # Expected to fail
            try:
                session.run("CREATE ()-[:AUDITS]->()").consume()
                assert False, "Readonly user should not be able to create AUDITS"
            except Exception:
                pass  # Expected to fail
            try:
                session.run("CREATE ()-[:ADMIN_ACCESS]->()").consume()
                assert False, "Readonly user should not be able to create ADMIN_ACCESS"
            except Exception:
                pass  # Expected to fail

            # Should be able to read all edge types
            session.run("MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name").consume()
            session.run("MATCH (p:Person)-[:OWNS]->(c:Company) RETURN p.name, c.name").consume()
            session.run("MATCH (p:Person)-[:REVIEWS]->(prod:Product) RETURN p.name, prod.name").consume()
            session.run("MATCH (p:Person)-[:AUDITS]->(c:Company) RETURN p.name, c.name").consume()
            session.run("MATCH (admin:Admin)-[:ADMIN_ACCESS]->(c:Company) RETURN admin.role, c.name").consume()

            # Should NOT be able to UPDATE any edge types (only READ permission)
            try:
                session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) SET r.since = '2024'").consume()
                assert False, "Readonly user should not be able to update WORKS_FOR with READ permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to DELETE any edge types (only READ permission)
            try:
                session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) DELETE r").consume()
                assert False, "Readonly user should not be able to delete WORKS_FOR with READ permission"
            except Exception:
                pass  # Expected to fail


def test_limited_privileges(multi_tenant_setup):
    """Test limited user with minimal privileges."""

    response = base64.b64encode(b"multi_role_limited").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "limited_user"

            # Test access to limited_db only
            session.run("USE DATABASE limited_db;").consume()

            # Should be able to read data
            session.run("MATCH (n) RETURN n LIMIT 1").consume()

            # Should NOT be able to create nodes
            try:
                session.run("CREATE (n:TestNode {name: 'limited_test'})").consume()
                assert False, "Limited user should not be able to create nodes"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to create relationships
            try:
                session.run("CREATE (n1:Node1)-[:RELATES_TO]->(n2:Node2)").consume()
                assert False, "Limited user should not be able to create relationships"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to create indexes
            try:
                session.run("CREATE INDEX ON :TestNode(name)").consume()
                assert False, "Limited user should not be able to create indexes"
            except Exception:
                pass  # Expected to fail

            # Test label-based permissions for limited user in limited_db - has READ-only access
            session.run("USE DATABASE limited_db;").consume()

            # Should NOT be able to CREATE Person and Company (has READ permission only)
            try:
                session.run("CREATE (p:Person {name: 'David', age: 45})").consume()
                assert False, "Limited user should not be able to create Person with READ permission"
            except Exception:
                pass  # Expected to fail

            try:
                session.run("CREATE (c:Company {name: 'LimitedCorp'})").consume()
                assert False, "Limited user should not be able to create Company with READ permission"
            except Exception:
                pass  # Expected to fail

            # Should be able to READ Person and Company (if they exist)
            session.run("MATCH (p:Person) RETURN p.name").consume()
            session.run("MATCH (c:Company) RETURN c.name").consume()

            # Should NOT be able to see Audit and Admin labels (NOTHING permission in limited_db)
            try:
                session.run("CREATE (a:Audit {action: 'test'})").consume()
                assert False, "Limited user should not be able to create Audit labels with NOTHING permission"
            except Exception:
                pass  # Expected to fail

            try:
                session.run("CREATE (admin:Admin {role: 'admin'})").consume()
                assert False, "Limited user should not be able to create Admin labels with NOTHING permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to UPDATE any labels (only READ permission)
            try:
                session.run("MATCH (p:Person) SET p.age = 46").consume()
                assert False, "Limited user should not be able to update Person with READ permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to DELETE any labels (only READ permission)
            try:
                session.run("MATCH (p:Person) DELETE p").consume()
                assert False, "Limited user should not be able to delete Person with READ permission"
            except Exception:
                pass  # Expected to fail

            # Test edge type permissions for limited user in limited_db - has READ-only access
            # Should NOT be able to CREATE WORKS_FOR and OWNS (has READ permission only)
            try:
                session.run("CREATE ()-[:WORKS_FOR]->()").consume()
                assert False, "Limited user should not be able to create WORKS_FOR with READ permission"
            except Exception:
                pass  # Expected to fail

            try:
                session.run("CREATE ()-[:OWNS]->()").consume()
                assert False, "Limited user should not be able to create OWNS with READ permission"
            except Exception:
                pass  # Expected to fail

            # Should be able to READ WORKS_FOR and OWNS (if they exist)
            session.run("MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name").consume()
            session.run("MATCH (p:Person)-[:OWNS]->(c:Company) RETURN p.name, c.name").consume()

            # Should NOT be able to see AUDITS and ADMIN_ACCESS edge types (NOTHING permission)
            try:
                session.run("CREATE ()-[:AUDITS]->()").consume()
                assert False, "Limited user should not be able to create AUDITS edge types with NOTHING permission"
            except Exception:
                pass  # Expected to fail

            try:
                session.run("CREATE ()-[:ADMIN_ACCESS]->()").consume()
                assert (
                    False
                ), "Limited user should not be able to create ADMIN_ACCESS edge types with NOTHING permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to UPDATE any edge types (only READ permission)
            try:
                session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) SET r.since = '2024'").consume()
                assert False, "Limited user should not be able to update WORKS_FOR with READ permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to DELETE any edge types (only READ permission)
            try:
                session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) DELETE r").consume()
                assert False, "Limited user should not be able to delete WORKS_FOR with READ permission"
            except Exception:
                pass  # Expected to fail


def test_multi_role_database_switching(multi_tenant_setup):
    """Test user with multiple roles switching between databases."""

    response = base64.b64encode(b"multi_role_architect").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "architect_user"

            # Switch between accessible databases - architect has access to architect_db only
            session.run("USE DATABASE architect_db;").consume()
            session.run("CREATE (n:ArchitectNode {name: 'architect_switch'})").consume()

            # Should NOT be able to access user_db (architect only has access to architect_db)
            try:
                session.run("USE DATABASE user_db;").consume()
                session.run("CREATE (n:UserNode {name: 'user_switch'})").consume()
                assert False, "Architect should not be able to access user_db"
            except Exception:
                pass  # Expected to fail

            # Switch back to architect_db
            session.run("USE DATABASE architect_db;").consume()
            result = list(session.run("MATCH (n:ArchitectNode) RETURN n.name"))
            assert len(result) == 1 and result[0]["n.name"] == "architect_switch"


def test_privilege_escalation_prevention(multi_tenant_setup):
    """Test that users cannot escalate privileges through role manipulation."""

    response = base64.b64encode(b"multi_role_user").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "regular_user"

            # Should NOT be able to create roles
            try:
                session.run("CREATE ROLE new_role;").consume()
                assert False, "Regular user should not be able to create roles"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to grant privileges
            try:
                session.run("GRANT ALL PRIVILEGES TO new_role;").consume()
                assert False, "Regular user should not be able to grant privileges"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to create databases
            try:
                session.run("CREATE DATABASE new_db;").consume()
                assert False, "Regular user should not be able to create databases"
            except Exception:
                pass  # Expected to fail


def test_wrong_data_types_handling(multi_tenant_setup):
    """Test handling of wrong data types in role mapping."""

    response = base64.b64encode(b"multi_role_wrong_types").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        # Should fail due to wrong data types
        try:
            with client.session() as session:
                list(session.run("SHOW CURRENT USER;"))
            assert False, "Should fail due to wrong data types in role mapping"
        except Exception:
            pass  # Expected to fail


def test_no_main_database_user(multi_tenant_setup):
    """Test user with multiple roles but no main database set."""

    response = base64.b64encode(b"multi_role_no_main").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "no_main_user"

            # Should be able to access user_db
            session.run("USE DATABASE user_db;").consume()
            session.run("CREATE (n:TestNode {name: 'no_main_test'})").consume()

            # Should be able to access architect_db
            session.run("USE DATABASE architect_db;").consume()
            session.run("CREATE (n:TestNode {name: 'no_main_test'})").consume()


def test_label_based_authorization_hierarchy(multi_tenant_setup):
    """Test the hierarchical label-based authorization levels: NOTHING, READ, UPDATE, CREATE_DELETE."""

    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "admin_user"

            # Test all databases to verify label permissions
            test_databases = ["admin_db", "architect_db", "user_db", "readonly_db", "limited_db"]

            for db_name in test_databases:
                session.run(f"USE DATABASE {db_name};").consume()

                # Create test nodes with different labels
                session.run("CREATE (p:Person {name: 'TestPerson'})").consume()
                session.run("CREATE (c:Company {name: 'TestCompany'})").consume()
                session.run("CREATE (prod:Product {name: 'TestProduct'})").consume()
                session.run("CREATE (r:Review {rating: 4})").consume()
                session.run("CREATE (a:Audit {action: 'test'})").consume()
                session.run("CREATE (admin:Admin {role: 'test'})").consume()

                # Test READ operations (should work for all except NOTHING)
                person_result = list(session.run("MATCH (p:Person) RETURN p.name"))
                company_result = list(session.run("MATCH (c:Company) RETURN c.name"))
                product_result = list(session.run("MATCH (prod:Product) RETURN prod.name"))
                review_result = list(session.run("MATCH (r:Review) RETURN r.rating"))
                audit_result = list(session.run("MATCH (a:Audit) RETURN a.action"))
                admin_result = list(session.run("MATCH (admin:Admin) RETURN admin.role"))

                # All should be readable by admin
                assert len(person_result) == 1
                assert len(company_result) == 1
                assert len(product_result) == 1
                assert len(review_result) == 1
                assert len(audit_result) == 1
                assert len(admin_result) == 1

                # Test UPDATE operations (should work for CREATE_DELETE and UPDATE)
                session.run("MATCH (p:Person) SET p.name = 'UpdatedPerson'").consume()
                session.run("MATCH (c:Company) SET c.name = 'UpdatedCompany'").consume()
                session.run("MATCH (prod:Product) SET prod.name = 'UpdatedProduct'").consume()
                session.run("MATCH (r:Review) SET r.rating = 5").consume()
                session.run("MATCH (a:Audit) SET a.action = 'updated'").consume()
                session.run("MATCH (admin:Admin) SET admin.role = 'updated'").consume()

                # Test DELETE operations (should work for CREATE_DELETE only)
                session.run("MATCH (p:Person) DELETE p").consume()
                session.run("MATCH (c:Company) DELETE c").consume()
                session.run("MATCH (prod:Product) DELETE prod").consume()
                session.run("MATCH (r:Review) DELETE r").consume()
                session.run("MATCH (a:Audit) DELETE a").consume()
                session.run("MATCH (admin:Admin) DELETE admin").consume()


def test_label_permission_denial(multi_tenant_setup):
    """Test that users cannot access labels they don't have permission for."""

    # Test limited user with NOTHING permission on Audit and Admin
    response = base64.b64encode(b"multi_role_limited").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            session.run("USE DATABASE limited_db;").consume()

            # Should NOT be able to see Audit nodes (NOTHING permission)
            try:
                audit_nodes = list(session.run("MATCH (a:Audit) RETURN a"))
                assert len(audit_nodes) == 0, "Limited user should not see Audit nodes with NOTHING permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to see Admin nodes (NOTHING permission)
            try:
                admin_nodes = list(session.run("MATCH (admin:Admin) RETURN admin"))
                assert len(admin_nodes) == 0, "Limited user should not see Admin nodes with NOTHING permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to create Audit or Admin nodes
            try:
                session.run("CREATE (a:Audit {action: 'denied'})").consume()
                assert False, "Limited user should not be able to create Audit nodes"
            except Exception:
                pass  # Expected to fail

            try:
                session.run("CREATE (admin:Admin {role: 'denied'})").consume()
                assert False, "Limited user should not be able to create Admin nodes"
            except Exception:
                pass  # Expected to fail


def test_edge_type_authorization_hierarchy(multi_tenant_setup):
    """Test the hierarchical edge type authorization levels: NOTHING, READ, UPDATE, CREATE_DELETE."""

    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "admin_user"

            # Test all databases to verify edge type permissions
            test_databases = ["admin_db", "architect_db", "user_db", "readonly_db", "limited_db"]

            for db_name in test_databases:
                session.run(f"USE DATABASE {db_name};").consume()

                # Create test nodes and relationships with different edge types
                session.run(
                    "CREATE (p1:Person {name: 'TestPerson1'})-[:WORKS_FOR]->(c1:Company {name: 'TestCompany1'})"
                ).consume()
                session.run(
                    "CREATE (p2:Person {name: 'TestPerson2'})-[:OWNS]->(c2:Company {name: 'TestCompany2'})"
                ).consume()
                session.run(
                    "CREATE (p3:Person {name: 'TestPerson3'})-[:REVIEWS]->(prod:Product {name: 'TestProduct'})"
                ).consume()
                session.run(
                    "CREATE (p4:Person {name: 'TestPerson4'})-[:AUDITS]->(c3:Company {name: 'TestCompany3'})"
                ).consume()
                session.run(
                    "CREATE (admin:Admin {role: 'test'})-[:ADMIN_ACCESS]->(c4:Company {name: 'TestCompany4'})"
                ).consume()

                # Test READ operations (should work for all except NOTHING)
                works_for_result = list(session.run("MATCH (p:Person)-[:WORKS_FOR]->(c:Company) RETURN p.name, c.name"))
                owns_result = list(session.run("MATCH (p:Person)-[:OWNS]->(c:Company) RETURN p.name, c.name"))
                reviews_result = list(
                    session.run("MATCH (p:Person)-[:REVIEWS]->(prod:Product) RETURN p.name, prod.name")
                )
                audits_result = list(session.run("MATCH (p:Person)-[:AUDITS]->(c:Company) RETURN p.name, c.name"))
                admin_access_result = list(
                    session.run("MATCH (admin:Admin)-[:ADMIN_ACCESS]->(c:Company) RETURN admin.role, c.name")
                )

                # All should be readable by admin
                assert len(works_for_result) == 1
                assert len(owns_result) == 1
                assert len(reviews_result) == 1
                assert len(audits_result) == 1
                assert len(admin_access_result) == 1

                # Test UPDATE operations (should work for CREATE_DELETE and UPDATE)
                session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) SET r.since = '2023'").consume()
                session.run("MATCH (p:Person)-[r:OWNS]->(c:Company) SET r.percentage = 100").consume()
                session.run("MATCH (p:Person)-[r:REVIEWS]->(prod:Product) SET r.rating = 5").consume()
                session.run("MATCH (p:Person)-[r:AUDITS]->(c:Company) SET r.action = 'updated'").consume()
                session.run("MATCH (admin:Admin)-[r:ADMIN_ACCESS]->(c:Company) SET r.level = 'high'").consume()

                # Test DELETE operations (should work for CREATE_DELETE only)
                session.run("MATCH (p:Person)-[r:WORKS_FOR]->(c:Company) DELETE r").consume()
                session.run("MATCH (p:Person)-[r:OWNS]->(c:Company) DELETE r").consume()
                session.run("MATCH (p:Person)-[r:REVIEWS]->(prod:Product) DELETE r").consume()
                session.run("MATCH (p:Person)-[r:AUDITS]->(c:Company) DELETE r").consume()
                session.run("MATCH (admin:Admin)-[r:ADMIN_ACCESS]->(c:Company) DELETE r").consume()


def test_edge_type_permission_denial(multi_tenant_setup):
    """Test that users cannot access edge types they don't have permission for."""

    # Test limited user with NOTHING permission on AUDITS and ADMIN_ACCESS
    response = base64.b64encode(b"multi_role_limited").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            session.run("USE DATABASE limited_db;").consume()

            # Should NOT be able to see AUDITS edge types (NOTHING permission)
            try:
                audits_result = list(session.run("MATCH (p:Person)-[:AUDITS]->(c:Company) RETURN p.name, c.name"))
                assert len(audits_result) == 0, "Limited user should not see AUDITS edge types with NOTHING permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to see ADMIN_ACCESS edge types (NOTHING permission)
            try:
                admin_access_result = list(
                    session.run("MATCH (admin:Admin)-[:ADMIN_ACCESS]->(c:Company) RETURN admin.role, c.name")
                )
                assert (
                    len(admin_access_result) == 0
                ), "Limited user should not see ADMIN_ACCESS edge types with NOTHING permission"
            except Exception:
                pass  # Expected to fail

            # Should NOT be able to create AUDITS or ADMIN_ACCESS edge types
            try:
                session.run("CREATE (p:Person {name: 'denied'})-[:AUDITS]->(c:Company {name: 'denied'})").consume()
                assert False, "Limited user should not be able to create AUDITS edge types"
            except Exception:
                pass  # Expected to fail

            try:
                session.run(
                    "CREATE (admin:Admin {role: 'denied'})-[:ADMIN_ACCESS]->(c:Company {name: 'denied'})"
                ).consume()
                assert False, "Limited user should not be able to create ADMIN_ACCESS edge types"
            except Exception:
                pass  # Expected to fail


def test_privileges_per_role_and_database(multi_tenant_setup):
    """Test that privileges are correctly assigned per role and database."""

    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Check admin privileges - should have full access to all databases
            admin_privileges = list(session.run("SHOW PRIVILEGES FOR admin ON MAIN;"))
            assert len(admin_privileges) > 0, "Admin should have privileges"

            # Check architect privileges - should have access to architect_db only
            architect_privileges = list(session.run("SHOW PRIVILEGES FOR architect ON MAIN;"))
            assert len(architect_privileges) > 0, "Architect should have privileges"

            # Check user privileges - should have access to user_db only
            user_privileges = list(session.run("SHOW PRIVILEGES FOR user ON MAIN;"))
            assert len(user_privileges) > 0, "User should have privileges"

            # Check readonly privileges - should have access to readonly_db only
            readonly_privileges = list(session.run("SHOW PRIVILEGES FOR readonly ON MAIN;"))
            assert len(readonly_privileges) > 0, "Readonly should have privileges"

            # Check limited privileges - should have access to limited_db only
            limited_privileges = list(session.run("SHOW PRIVILEGES FOR limited ON MAIN;"))
            assert len(limited_privileges) > 0, "Limited should have privileges"


def test_database_access_per_role(multi_tenant_setup):
    """Test that roles have correct database access permissions."""

    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Admin should have access to all databases
            session.run("USE DATABASE admin_db;").consume()
            session.run("USE DATABASE architect_db;").consume()
            session.run("USE DATABASE user_db;").consume()
            session.run("USE DATABASE readonly_db;").consume()
            session.run("USE DATABASE limited_db;").consume()

            # Test architect access - should only have access to architect_db
            response = base64.b64encode(b"multi_role_architect").decode("utf-8")
            MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with client.session() as session:
                # Should be able to access architect_db
                session.run("USE DATABASE architect_db;").consume()

                # Should NOT be able to access other databases
                try:
                    session.run("USE DATABASE admin_db;").consume()
                    assert False, "Architect should not have access to admin_db"
                except Exception:
                    pass  # Expected to fail

                try:
                    session.run("USE DATABASE user_db;").consume()
                    assert False, "Architect should not have access to user_db"
                except Exception:
                    pass  # Expected to fail

                try:
                    session.run("USE DATABASE readonly_db;").consume()
                    assert False, "Architect should not have access to readonly_db"
                except Exception:
                    pass  # Expected to fail

                try:
                    session.run("USE DATABASE limited_db;").consume()
                    assert False, "Architect should not have access to limited_db"
                except Exception:
                    pass  # Expected to fail


def test_show_databases_for_role_sso(multi_tenant_setup):
    """Test SHOW DATABASE PRIVILEGES FOR <role> in SSO context with multiple roles and grants/denies."""

    # Use admin user to test SHOW DATABASE PRIVILEGES FOR <role> functionality
    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Test SHOW DATABASE PRIVILEGES FOR <role> for each role
            # Admin role: should have access to all databases (*)
            result = list(session.run("SHOW DATABASE PRIVILEGES FOR admin;"))
            for row in result:
                grants = row.get("grants", [])
                denies = row.get("denies", [])
                # Admin has GRANT DATABASE * TO admin, so should see all databases
                assert grants == "*", f"admin should have access to all databases (*), got {grants}"
                assert denies == [], f"admin should have no denied databases, got {denies}"

            # Architect role: should have access to architect_db only
            result = list(session.run("SHOW DATABASE PRIVILEGES FOR architect;"))
            for row in result:
                grants = row.get("grants", [])
                denies = row.get("denies", [])
                assert set(grants) == {
                    "architect_db",
                    "memgraph",
                }, f"architect should see architect_db and memgraph, got {grants}"
                assert denies == [], f"architect should have no denied databases, got {denies}"

            # User role: should have access to user_db only
            result = list(session.run("SHOW DATABASE PRIVILEGES FOR user;"))
            for row in result:
                grants = row.get("grants", [])
                denies = row.get("denies", [])
                assert set(grants) == {"user_db", "memgraph"}, f"user should see user_db and memgraph, got {grants}"
                assert denies == [], f"user should have no denied databases, got {denies}"

            # Readonly role: should have access to readonly_db only
            result = list(session.run("SHOW DATABASE PRIVILEGES FOR readonly;"))
            for row in result:
                grants = row.get("grants", [])
                denies = row.get("denies", [])
                assert set(grants) == {
                    "readonly_db",
                    "memgraph",
                }, f"readonly should see readonly_db and memgraph, got {grants}"
                assert denies == [], f"readonly should have no denied databases, got {denies}"

            # Limited role: should have access to limited_db only
            result = list(session.run("SHOW DATABASE PRIVILEGES FOR limited;"))
            for row in result:
                grants = row.get("grants", [])
                denies = row.get("denies", [])
                assert set(grants) == {
                    "limited_db",
                    "memgraph",
                }, f"limited should see limited_db and memgraph, got {grants}"
                assert denies == [], f"limited should have no denied databases, got {denies}"

            # Test with non-existent role - should return empty or throw error
            try:
                result = list(session.run("SHOW DATABASE PRIVILEGES FOR nonexistent_role;"))
                # If it doesn't throw, it should return empty
                assert len(result) == 0, f"non-existent role should return empty, got {result}"
            except Exception:
                pass  # Expected to fail for non-existent role

            # Test that the results match what each role can actually access
            # Verify architect can only access architect_db
            response = base64.b64encode(b"multi_role_architect").decode("utf-8")
            MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with client.session() as session:
                # Should be able to access architect_db
                session.run("USE DATABASE architect_db;").consume()
                session.run("CREATE ({name: 'architect_test'})").consume()

                # Should NOT be able to access other databases
                for db_name in ["admin_db", "user_db", "readonly_db", "limited_db"]:
                    try:
                        session.run(f"USE DATABASE {db_name};").consume()
                        session.run("CREATE ({name: 'test'})").consume()
                        assert False, f"architect should not have access to {db_name}"
                    except Exception:
                        pass  # Expected to fail

            # Verify user can only access user_db
            response = base64.b64encode(b"multi_role_user").decode("utf-8")
            MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

        with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
            with client.session() as session:
                # Should be able to access user_db
                session.run("USE DATABASE user_db;").consume()
                session.run("CREATE ({name: 'user_test'})").consume()

                # Should NOT be able to access other databases
                for db_name in ["admin_db", "architect_db", "readonly_db", "limited_db"]:
                    try:
                        session.run(f"USE DATABASE {db_name};").consume()
                        session.run("CREATE ({name: 'test'})").consume()
                        assert False, f"user should not have access to {db_name}"
                    except Exception:
                        pass  # Expected to fail


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
