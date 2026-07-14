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

import concurrent
import os
import sys
import time

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path, show_instances
from mg_utils import mg_sleep_and_assert
from neo4j import GraphDatabase

interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))
file = "auth"


@pytest.fixture
def test_name(request):
    return request.node.name


# Connect HA, MT and AUTH by giving users access only to a specific database


def get_instances_description_no_setup(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--bolt-port=7687",
                "--log-level=TRACE",
                "--management-port=10011",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--bolt-port=7688",
                "--log-level=TRACE",
                "--management-port=10012",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/instance_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/instance_2",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--bolt-port=7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port=7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port=7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
                "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
                "REGISTER INSTANCE instance_1 WITH CONFIG {'bolt_server': 'localhost:7687', 'management_server': 'localhost:10011', 'replication_server': 'localhost:10001'};",
                "REGISTER INSTANCE instance_2 WITH CONFIG {'bolt_server': 'localhost:7688', 'management_server': 'localhost:10012', 'replication_server': 'localhost:10002'};",
                "SET INSTANCE instance_1 TO MAIN",
            ],
        },
    }


@pytest.fixture(autouse=True)
def cleanup_after_test():
    # Run the test
    yield
    # Stop + delete directories after running the test
    interactive_mg_runner.kill_all(keep_directories=False)


def test_coords_env(test_name):
    # Env variable is used for all instances
    os.environ["MEMGRAPH_USER"] = "user1"
    os.environ["MEMGRAPH_PASSWORD"] = "pass1"

    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    # Test that when you set env variables, that SHOW USERS will show something on data instances
    main_cursor = connect(host="localhost", port=7687, username="user1", password="pass1").cursor()
    assert len(execute_and_fetch_all(main_cursor, "show users")) == 1

    # Test that you can connect without auth on coordinators
    leader_cursor = connect(host="localhost", port=7692).cursor()
    try:
        execute_and_fetch_all(leader_cursor, "show users")
        assert False
    except Exception as e:
        print(f"Error: {str(e)}")

    # Test that you cannot connect with auth on coordinators
    try:
        connect(host="localhost", port=7692, username="user1", password="pass1").cursor()
        assert False
    except Exception as e:
        print(f"Error: {str(e)}")

    del os.environ["MEMGRAPH_USER"]
    del os.environ["MEMGRAPH_PASSWORD"]


def test_routing_connection(test_name):
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    # Test that without any user, routing works normally
    driver = GraphDatabase.driver("neo4j://localhost:7692")

    def create_vertex(tx):
        tx.run("create (n:Greeting {id: 1})")

    def get_all_vertices(tx):
        return tx.run("match (n) return count(n) as c").single()["c"]

    with driver.session() as session:
        session.execute_write(create_vertex)
        assert session.execute_read(get_all_vertices) == 1

    # Test that the user can be created with bolt+routing
    with driver.session() as session:
        session.run("create user user1 identified by '123'")

    # Test the user was successfully created
    main_cursor = connect(host="localhost", port=7687, username="user1", password="123").cursor()
    assert len(execute_and_fetch_all(main_cursor, "show users")) == 1

    # You can send request normally with the old driver
    with driver.session() as session:
        session.run("show users")

    driver.close()

    # Create a new driver and check that you cannot send a routing request anymore
    driver = GraphDatabase.driver("neo4j://localhost:7692")
    with driver.session() as session:
        try:
            session.run("show users")
            assert False
        except Exception as e:
            print(f"Error: {str(e)}")
    driver.close()

    # Coordinators don't care about authentication details you specified
    driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("user1", "123"))
    with driver.session() as session:
        session.run("show users")
    driver.close()

    # Coordinators don't care about authentication details you specified but data instances do
    driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("not_exists", "123"))
    with driver.session() as session:
        try:
            session.run("show users")
            assert False
        except Exception as e:
            print(f"Error: {str(e)}")
    driver.close()


def test_multi_database_no_auth(test_name):
    """Test multi-database access without authentication using neo4j protocol"""
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    # Connect to coordinator without auth
    driver = GraphDatabase.driver("neo4j://localhost:7692")

    # Create databases
    with driver.session() as session:
        session.run("CREATE DATABASE products")
        session.run("CREATE DATABASE customers")
        session.run("CREATE DATABASE orders")

    driver.close()

    # Test database access using USE DATABASE command
    driver = GraphDatabase.driver("neo4j://localhost:7692")
    with driver.session() as session:
        # Start on default database (memgraph)
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "memgraph"
        session.run("CREATE (n:DefaultData {name: 'memgraph_data'})")

        # Switch to products database
        session.run("USE DATABASE products")
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "products"
        session.run("CREATE (n:Product {name: 'laptop', price: 999})")

        # Switch to customers database
        session.run("USE DATABASE customers")
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "customers"
        session.run("CREATE (n:Customer {name: 'john', age: 30})")

        # Switch to orders database
        session.run("USE DATABASE orders")
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "orders"
        session.run("CREATE (n:Order {id: 'ORD001', total: 999})")

        # Switch back to memgraph
        session.run("USE DATABASE memgraph")
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "memgraph"

        # Verify data in memgraph
        result = session.run("MATCH (n:DefaultData) RETURN n.name as name").single()
        assert result["name"] == "memgraph_data"
    driver.close()

    # Test database access using database parameter in session
    driver = GraphDatabase.driver("neo4j://localhost:7692")

    # Access memgraph database directly
    with driver.session(database="memgraph") as session:
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "memgraph"
        result = session.run("MATCH (n:DefaultData) RETURN count(n) as cnt").single()
        assert result["cnt"] == 1

    # Access products database directly
    with driver.session(database="products") as session:
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "products"
        result = session.run("MATCH (n:Product) RETURN n.name as name, n.price as price").single()
        assert result["name"] == "laptop"
        assert result["price"] == 999
        # Add more data
        session.run("CREATE (n:Product {name: 'mouse', price: 25})")

    # Access customers database directly
    with driver.session(database="customers") as session:
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "customers"
        result = session.run("MATCH (n:Customer) RETURN n.name as name, n.age as age").single()
        assert result["name"] == "john"
        assert result["age"] == 30
        # Add more data
        session.run("CREATE (n:Customer {name: 'jane', age: 25})")

    # Access orders database directly
    with driver.session(database="orders") as session:
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "orders"
        result = session.run("MATCH (n:Order) RETURN n.id as id, n.total as total").single()
        assert result["id"] == "ORD001"
        assert result["total"] == 999
        # Add more data
        session.run("CREATE (n:Order {id: 'ORD002', total: 25})")

    driver.close()

    # Test data isolation between databases
    driver = GraphDatabase.driver("neo4j://localhost:7692")
    with driver.session() as session:
        # Check memgraph has only its data
        session.run("USE DATABASE memgraph")
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 1  # Only DefaultData

        # Check products has only product data
        session.run("USE DATABASE products")
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 2  # 2 Products

        # Check customers has only customer data
        session.run("USE DATABASE customers")
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 2  # 2 Customers

        # Check orders has only order data
        session.run("USE DATABASE orders")
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 2  # 2 Orders
    driver.close()

    # Test switching databases within the same session using database parameter
    driver = GraphDatabase.driver("neo4j://localhost:7692")

    # Create cross-database relationships test data
    with driver.session(database="products") as session:
        session.run("CREATE (n:Product {name: 'tablet', price: 500})")

    with driver.session(database="customers") as session:
        session.run("CREATE (n:Customer {name: 'bob', age: 35})")

    with driver.session(database="orders") as session:
        session.run("CREATE (n:Order {id: 'ORD003', total: 500})")

    # Verify final state
    with driver.session(database="products") as session:
        result = session.run("MATCH (n:Product) RETURN count(n) as cnt").single()
        assert result["cnt"] == 3

    with driver.session(database="customers") as session:
        result = session.run("MATCH (n:Customer) RETURN count(n) as cnt").single()
        assert result["cnt"] == 3

    with driver.session(database="orders") as session:
        result = session.run("MATCH (n:Order) RETURN count(n) as cnt").single()
        assert result["cnt"] == 3

    driver.close()


def test_ha_mt_auth_scenario(test_name):
    """Test multi-database setup with different user access levels"""
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    # Connect to coordinator without auth
    driver = GraphDatabase.driver("neo4j://localhost:7692")

    # Create databases
    with driver.session() as session:
        session.run("CREATE DATABASE sales")
        session.run("CREATE DATABASE analytics")
        session.run("CREATE DATABASE reports")

    # Create users with different access levels
    with driver.session() as session:
        # Create a role before the first user so builtin role creation (admin/readwrite/readonly) is suppressed.
        session.run("CREATE ROLE _dummy_role")
        # Create admin user with access to all databases
        session.run("CREATE USER admin IDENTIFIED BY 'admin123'")
        session.run("GRANT ALL PRIVILEGES TO admin")
        session.run("GRANT READ, SET PROPERTY {*} ON NODES CONTAINING LABELS * TO admin")
        session.run("GRANT READ, SET PROPERTY {*} ON EDGES OF TYPE * TO admin")
        session.run("GRANT DATABASE * TO admin")
        session.run("GRANT MULTI_DATABASE_USE TO admin")

        # Create user for sales database
        session.run("CREATE USER sales_user IDENTIFIED BY 'sales123'")
        session.run("GRANT CREATE, MATCH, SET, DELETE TO sales_user")
        session.run("GRANT CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS * TO sales_user")
        session.run("GRANT READ, SET PROPERTY {*} ON NODES CONTAINING LABELS * TO sales_user")
        session.run("GRANT READ, SET PROPERTY {*} ON EDGES OF TYPE * TO sales_user")
        session.run("GRANT DATABASE sales TO sales_user")
        session.run("REVOKE DATABASE memgraph FROM sales_user")
        session.run("SET MAIN DATABASE sales FOR sales_user")
        session.run("GRANT MULTI_DATABASE_USE TO sales_user")

        # Create user for analytics database
        session.run("CREATE USER analytics_user IDENTIFIED BY 'analytics123'")
        session.run("GRANT CREATE, MATCH, SET, DELETE TO analytics_user")
        session.run("GRANT CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS * TO analytics_user")
        session.run("GRANT READ, SET PROPERTY {*} ON NODES CONTAINING LABELS * TO analytics_user")
        session.run("GRANT READ, SET PROPERTY {*} ON EDGES OF TYPE * TO analytics_user")
        session.run("GRANT DATABASE analytics TO analytics_user")
        session.run("REVOKE DATABASE memgraph FROM analytics_user")
        session.run("SET MAIN DATABASE analytics FOR analytics_user")
        session.run("GRANT MULTI_DATABASE_USE TO analytics_user")

        # Create user for reports database (read-only)
        session.run("CREATE USER reports_user IDENTIFIED BY 'reports123'")
        session.run("GRANT MATCH TO reports_user")
        session.run("GRANT READ {*} ON NODES CONTAINING LABELS * TO reports_user")
        session.run("GRANT READ {*} ON EDGES OF TYPE * TO reports_user")
        session.run("GRANT DATABASE reports TO reports_user")
        session.run("REVOKE DATABASE memgraph FROM reports_user")
        session.run("SET MAIN DATABASE reports FOR reports_user")
        session.run("GRANT MULTI_DATABASE_USE TO reports_user")

    driver.close()

    admin_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("admin", "admin123"))
    with admin_driver.session() as session:
        # Should be able to use all databases2
        session.run("USE DATABASE memgraph")
    admin_driver.close()

    # Test admin user can access all databases
    admin_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("admin", "admin123"))
    with admin_driver.session() as session:
        # Should be able to use all databases2
        session.run("USE DATABASE memgraph")
        session.run("CREATE (n:AdminNode {name: 'admin_memgraph'})")

        session.run("USE DATABASE sales")
        session.run("CREATE (n:AdminNode {name: 'admin_sales'})")

        session.run("USE DATABASE analytics")
        session.run("CREATE (n:AdminNode {name: 'admin_analytics'})")

        session.run("USE DATABASE reports")
        session.run("CREATE (n:AdminNode {name: 'admin_reports'})")

        # Verify admin can read from all databases
        session.run("USE DATABASE memgraph")
        result = session.run("MATCH (n:AdminNode) RETURN n.name as name").single()
        assert result["name"] == "admin_memgraph"

        session.run("USE DATABASE sales")
        result = session.run("MATCH (n:AdminNode) RETURN n.name as name").single()
        assert result["name"] == "admin_sales"
    admin_driver.close()

    # Test sales_user can only access sales database
    sales_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("sales_user", "sales123"))
    with sales_driver.session() as session:
        # Should start on sales database (main database)
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "sales"

        # Can create and modify data in sales
        session.run("CREATE (n:SalesData {amount: 1000})")
        session.run("MATCH (n:SalesData) SET n.processed = true")

        # Cannot access memgraph database
        try:
            session.run("USE DATABASE memgraph")
            assert False, "sales_user should not access memgraph database"
        except Exception:
            pass

        # Cannot access analytics database
        try:
            session.run("USE DATABASE analytics")
            assert False, "sales_user should not access analytics database"
        except Exception:
            pass
    sales_driver.close()

    # Test analytics_user can only access analytics database
    analytics_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("analytics_user", "analytics123"))
    with analytics_driver.session() as session:
        # Should start on analytics database (main database)
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "analytics"

        # Can create and modify data in analytics
        session.run("CREATE (n:Metrics {value: 42})")
        session.run("MATCH (n:Metrics) SET n.calculated = true")

        # Cannot access sales database
        try:
            session.run("USE DATABASE sales")
            assert False, "analytics_user should not access sales database"
        except Exception:
            pass

        # Cannot access reports database
        try:
            session.run("USE DATABASE reports")
            assert False, "analytics_user should not access reports database"
        except Exception:
            pass
    analytics_driver.close()

    # Test reports_user has read-only access to reports database
    reports_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("reports_user", "reports123"))
    with reports_driver.session() as session:
        # Should start on reports database (main database)
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "reports"

        # Can read data (once admin creates some)
        session.run("MATCH (n) RETURN n")

        # Cannot create data (read-only)
        try:
            session.run("CREATE (n:Report {name: 'test'})")
            assert False, "reports_user should not be able to create data"
        except Exception:
            pass

        # Cannot modify data (read-only)
        try:
            session.run("MATCH (n) SET n.updated = true")
            assert False, "reports_user should not be able to modify data"
        except Exception:
            pass

        # Cannot access other databases
        try:
            session.run("USE DATABASE memgraph")
            assert False, "reports_user should not access memgraph database"
        except Exception:
            pass
    reports_driver.close()

    # Test cross-database data isolation
    admin_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("admin", "admin123"))
    with admin_driver.session() as session:
        # Check sales database has only sales data
        session.run("USE DATABASE sales")
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 2  # AdminNode + SalesData

        # Check analytics database has only analytics data
        session.run("USE DATABASE analytics")
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 2  # AdminNode + Metrics

        # Check reports database has only admin data
        session.run("USE DATABASE reports")
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 1  # Only AdminNode
    admin_driver.close()


# ---------------------------------------------------------------------------
# Coordinator role store: 3 coordinators, no data instances.
#
# Covers the foundational roles slice: basic-auth passthrough, rejection of
# non-role auth queries, on-leader role CRUD and full-cluster-restart
# persistence. Follower forwarding and SSO are exercised by later slices.
# ---------------------------------------------------------------------------

COORD_PORTS = [7690, 7691, 7692]


def get_coords_only_description(test_name: str):
    """Three coordinators, no data instances. The role list is the only cluster state we care about here."""
    return {
        "coordinator_1": {
            "args": [
                "--bolt-port=7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--management-port=10121",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_1.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--bolt-port=7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--management-port=10122",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_2.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--bolt-port=7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--management-port=10123",
                "--coordinator-hostname",
                "localhost",
            ],
            "log_file": f"{get_logs_path(file, test_name)}/coordinator_3.log",
            "data_directory": f"{get_data_path(file, test_name)}/coordinator_3",
            "setup_queries": [
                "ADD COORDINATOR 1 WITH CONFIG {'bolt_server': 'localhost:7690', 'coordinator_server': 'localhost:10111', 'management_server': 'localhost:10121'}",
                "ADD COORDINATOR 2 WITH CONFIG {'bolt_server': 'localhost:7691', 'coordinator_server': 'localhost:10112', 'management_server': 'localhost:10122'}",
                "ADD COORDINATOR 3 WITH CONFIG {'bolt_server': 'localhost:7692', 'coordinator_server': 'localhost:10113', 'management_server': 'localhost:10123'}",
            ],
        },
    }


def try_find_leader_port():
    """Return the bolt port of the up leader coordinator, or None if none is currently reachable."""
    for port in COORD_PORTS:
        try:
            cursor = connect(host="localhost", port=port).cursor()
            for row in show_instances(cursor):
                # row = (name, bolt_server, coordinator_server, management_server, status, role)
                if row[-1] == "leader" and row[-2] == "up":
                    return int(row[1].split(":")[1])
        except Exception:
            continue
    return None


def wait_for_ready_leader_port():
    """Return the leader coordinator's bolt port once it is ready to serve role queries.

    A coordinator can report itself as leader in SHOW INSTANCES a moment before it reaches the ready state in which
    role queries are served, so we poll SHOW ROLES until it stops returning the not-leader error.
    """
    deadline = time.time() + 30
    while time.time() < deadline:
        port = try_find_leader_port()
        if port is not None:
            try:
                cursor = connect(host="localhost", port=port).cursor()
                execute_and_fetch_all(cursor, "SHOW ROLES")
                return port
            except Exception:
                pass
        time.sleep(0.5)
    assert False, "Leader coordinator not ready to serve role queries"


def get_leader_cursor():
    return connect(host="localhost", port=wait_for_ready_leader_port()).cursor()


def show_roles(cursor):
    return sorted(name for (name,) in execute_and_fetch_all(cursor, "SHOW ROLES"))


def show_privileges(cursor, role):
    return sorted(privilege for (privilege,) in execute_and_fetch_all(cursor, f"SHOW PRIVILEGES FOR ROLE {role}"))


def test_basic_auth_passthrough(test_name):
    # Coordinators with no users accept any connection; credentials are ignored (basic-auth passthrough).
    inner_instances_description = get_coords_only_description(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    leader_port = wait_for_ready_leader_port()

    # Connect without credentials and run a coordinator query.
    no_auth_cursor = connect(host="localhost", port=leader_port).cursor()
    assert show_roles(no_auth_cursor) == []

    # Connect with arbitrary username/password: the credentials are ignored and the session works the same.
    basic_auth_cursor = connect(host="localhost", port=leader_port, username="whoever", password="whatever").cursor()
    assert show_roles(basic_auth_cursor) == []
    # A basic-auth session can run role management.
    execute_and_fetch_all(basic_auth_cursor, "CREATE ROLE passthrough_role")
    assert show_roles(basic_auth_cursor) == ["passthrough_role"]


def test_disallowed_auth_queries_rejected(test_name):
    # Every auth query other than CREATE/DROP/SHOW ROLE is rejected on a coordinator.
    inner_instances_description = get_coords_only_description(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    cursor = get_leader_cursor()

    disallowed_queries = [
        "CREATE USER foo IDENTIFIED BY 'bar'",
        "DROP USER foo",
        "SHOW USERS",
        "SET PASSWORD FOR foo TO 'bar'",
        # DENY in any form is rejected on a coordinator (only GRANT/REVOKE of coordinator privileges are supported).
        "DENY COORDINATOR_READ TO foo",
        "DENY MATCH TO foo",
        "REVOKE MATCH FROM foo",
        # SHOW PRIVILEGES / GRANT / REVOKE are permitted on a coordinator only for the coordinator READ/WRITE privileges
        # on a role; a USER target or a data-instance privilege must be rejected.
        "SHOW PRIVILEGES FOR USER foo",
        "GRANT COORDINATOR_READ TO USER foo",
        "GRANT MATCH TO foo",
        "SET ROLE FOR foo TO bar",
        "CLEAR ROLE FOR foo",
        # Multi-tenancy database access grants are rejected on a coordinator (coordinators have no databases).
        "GRANT DATABASE mydb TO foo",
        "DENY DATABASE mydb FROM foo",
        "REVOKE DATABASE mydb FROM foo",
        "SET MAIN DATABASE mydb FOR foo",
        # Fine-grained access control (label/edge-type entity privileges, property permissions) is rejected on a
        # coordinator (coordinators have no graph to gate).
        "GRANT CREATE, UPDATE ON NODES CONTAINING LABELS * TO foo",
        "GRANT UPDATE ON EDGES OF TYPE * TO foo",
        "GRANT READ {*} ON NODES CONTAINING LABELS * TO foo",
    ]
    for query in disallowed_queries:
        try:
            execute_and_fetch_all(cursor, query)
            assert False, f"Query should have been rejected on a coordinator: {query}"
        except Exception as e:
            assert "Coordinator can run only coordinator queries!" in str(e), f"Unexpected error for {query}: {e}"


def test_role_crud_on_leader(test_name):
    # On the leader, CREATE/DROP/SHOW ROLE behave like on data instances.
    inner_instances_description = get_coords_only_description(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    cursor = get_leader_cursor()

    assert show_roles(cursor) == []

    # CREATE adds the role and it shows up.
    execute_and_fetch_all(cursor, "CREATE ROLE admin")
    assert show_roles(cursor) == ["admin"]

    execute_and_fetch_all(cursor, "CREATE ROLE readonly")
    assert show_roles(cursor) == ["admin", "readonly"]

    # Duplicate CREATE errors.
    try:
        execute_and_fetch_all(cursor, "CREATE ROLE admin")
        assert False, "Duplicate CREATE ROLE should error"
    except Exception as e:
        assert "already exists" in str(e)

    # IF NOT EXISTS is a no-op on an existing role.
    execute_and_fetch_all(cursor, "CREATE ROLE IF NOT EXISTS admin")
    assert show_roles(cursor) == ["admin", "readonly"]

    # DROP removes the role.
    execute_and_fetch_all(cursor, "DROP ROLE admin")
    assert show_roles(cursor) == ["readonly"]

    # DROP of a missing role errors.
    try:
        execute_and_fetch_all(cursor, "DROP ROLE admin")
        assert False, "DROP of a missing role should error"
    except Exception as e:
        assert "doesn't exist" in str(e)


def test_privilege_grant_revoke_show_on_leader(test_name):
    # On the leader, GRANT/REVOKE COORDINATOR_READ|COORDINATOR_WRITE update a role's persisted mask and
    # SHOW PRIVILEGES FOR ROLE reports it.
    inner_instances_description = get_coords_only_description(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    cursor = get_leader_cursor()

    execute_and_fetch_all(cursor, "CREATE ROLE reader")
    execute_and_fetch_all(cursor, "CREATE ROLE writer")

    # A freshly created (bare) role has no privileges.
    assert show_privileges(cursor, "reader") == []
    assert show_privileges(cursor, "writer") == []

    # GRANT reflects in SHOW PRIVILEGES FOR ROLE.
    execute_and_fetch_all(cursor, "GRANT COORDINATOR_READ TO reader")
    assert show_privileges(cursor, "reader") == ["COORDINATOR_READ"]

    # Granting COORDINATOR_WRITE too yields both (WRITE is a superset of READ, but both are reported as granted).
    execute_and_fetch_all(cursor, "GRANT COORDINATOR_WRITE TO writer")
    execute_and_fetch_all(cursor, "GRANT COORDINATOR_READ TO writer")
    assert show_privileges(cursor, "writer") == ["COORDINATOR_READ", "COORDINATOR_WRITE"]

    # REVOKE clears the specific privilege.
    execute_and_fetch_all(cursor, "REVOKE COORDINATOR_READ FROM writer")
    assert show_privileges(cursor, "writer") == ["COORDINATOR_WRITE"]

    execute_and_fetch_all(cursor, "REVOKE COORDINATOR_WRITE FROM writer")
    assert show_privileges(cursor, "writer") == []

    # The other role's grant is untouched by operations on writer.
    assert show_privileges(cursor, "reader") == ["COORDINATOR_READ"]

    # GRANT ALL PRIVILEGES grants both coordinator privileges; REVOKE ALL PRIVILEGES removes both.
    execute_and_fetch_all(cursor, "GRANT ALL PRIVILEGES TO writer")
    assert show_privileges(cursor, "writer") == ["COORDINATOR_READ", "COORDINATOR_WRITE"]
    execute_and_fetch_all(cursor, "REVOKE ALL PRIVILEGES FROM writer")
    assert show_privileges(cursor, "writer") == []

    # GRANT/REVOKE/SHOW PRIVILEGES on a non-existent role errors.
    try:
        execute_and_fetch_all(cursor, "GRANT COORDINATOR_READ TO missing_role")
        assert False, "GRANT on a missing role should error"
    except Exception as e:
        assert "doesn't exist" in str(e)


def test_privilege_queries_rejected_for_non_role_targets(test_name):
    # GRANT/REVOKE/SHOW PRIVILEGES are accepted only for coordinator privileges on a role; USER targets, data-instance
    # privileges, fine-grained access control, and database access grants are rejected with the coordinator-only error
    # -- even when the target role exists.
    inner_instances_description = get_coords_only_description(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    cursor = get_leader_cursor()
    execute_and_fetch_all(cursor, "CREATE ROLE some_role")

    rejected_queries = [
        # USER targets.
        "GRANT COORDINATOR_READ TO USER some_role",
        "REVOKE COORDINATOR_WRITE FROM USER some_role",
        "SHOW PRIVILEGES FOR USER some_role",
        # Data-instance system privileges.
        "GRANT MATCH TO some_role",
        "REVOKE AUTH FROM some_role",
        # DENY is unsupported even for a coordinator privilege.
        "DENY COORDINATOR_WRITE TO some_role",
        # Fine-grained access control (label/edge-type entity privileges, property permissions).
        "GRANT CREATE, UPDATE ON NODES CONTAINING LABELS * TO some_role",
        "GRANT UPDATE ON EDGES OF TYPE * TO some_role",
        "GRANT READ {*} ON NODES CONTAINING LABELS * TO some_role",
        # Multi-tenancy database access.
        "GRANT DATABASE mydb TO some_role",
        "REVOKE DATABASE mydb FROM some_role",
    ]
    for query in rejected_queries:
        try:
            execute_and_fetch_all(cursor, query)
            assert False, f"Query should have been rejected on a coordinator: {query}"
        except Exception as e:
            assert "Coordinator can run only coordinator queries!" in str(e), f"Unexpected error for {query}: {e}"


def test_roles_survive_full_cluster_restart(test_name):
    # The role list is Raft-persisted, so it survives a full-cluster restart.
    inner_instances_description = get_coords_only_description(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    cursor = get_leader_cursor()
    execute_and_fetch_all(cursor, "CREATE ROLE admin")
    execute_and_fetch_all(cursor, "CREATE ROLE readonly")
    execute_and_fetch_all(cursor, "CREATE ROLE readwrite")
    assert show_roles(cursor) == ["admin", "readonly", "readwrite"]

    # Restart every coordinator, preserving data directories. Clear the bootstrap setup_queries first so restart does
    # not re-issue ADD COORDINATOR against an already-formed cluster; the state comes back from the durable log.
    for name in ["coordinator_1", "coordinator_2", "coordinator_3"]:
        inner_instances_description[name]["setup_queries"] = []
        interactive_mg_runner.kill(inner_instances_description, name)

    with concurrent.futures.ThreadPoolExecutor(2) as executor:
        futures = [
            executor.submit(interactive_mg_runner.start, inner_instances_description, "coordinator_1"),
            executor.submit(interactive_mg_runner.start, inner_instances_description, "coordinator_2"),
            executor.submit(interactive_mg_runner.start, inner_instances_description, "coordinator_3"),
        ]
        # Block until both coordinators have fully started and surface any startup errors,
        # otherwise the connect below can race a coordinator whose Bolt server isn't up yet.
        for future in concurrent.futures.as_completed(futures):
            future.result()

    # After the cluster re-forms, the role list must be reconstructed from the log/snapshot.
    def get_roles_from_leader():
        port = try_find_leader_port()
        if port is None:
            return None
        try:
            return show_roles(connect(host="localhost", port=port).cursor())
        except Exception:
            return None

    mg_sleep_and_assert(["admin", "readonly", "readwrite"], get_roles_from_leader)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
