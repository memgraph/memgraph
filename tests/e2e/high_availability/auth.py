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

import os
import sys

import interactive_mg_runner
import pytest
from common import connect, execute_and_fetch_all, get_data_path, get_logs_path
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
        # Create admin user with access to all databases
        session.run("CREATE USER admin IDENTIFIED BY 'admin123'")
        session.run("GRANT ALL PRIVILEGES TO admin")
        session.run("GRANT DATABASE * TO admin")
        session.run("GRANT MULTI_DATABASE_USE TO admin")

        # Create user for sales database
        session.run("CREATE USER sales_user IDENTIFIED BY 'sales123'")
        session.run("GRANT CREATE, MATCH, SET, DELETE TO sales_user")
        session.run("GRANT DATABASE sales TO sales_user")
        session.run("REVOKE DATABASE memgraph FROM sales_user")
        session.run("SET MAIN DATABASE sales FOR sales_user")
        session.run("GRANT MULTI_DATABASE_USE TO sales_user")

        # Create user for analytics database
        session.run("CREATE USER analytics_user IDENTIFIED BY 'analytics123'")
        session.run("GRANT CREATE, MATCH, SET, DELETE TO analytics_user")
        session.run("GRANT DATABASE analytics TO analytics_user")
        session.run("REVOKE DATABASE memgraph FROM analytics_user")
        session.run("SET MAIN DATABASE analytics FOR analytics_user")
        session.run("GRANT MULTI_DATABASE_USE TO analytics_user")

        # Create user for reports database (read-only)
        session.run("CREATE USER reports_user IDENTIFIED BY 'reports123'")
        session.run("GRANT MATCH TO reports_user")
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
        assert result["database"] == "sales"

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
        assert result["database"] == "analytics"

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
        assert result["database"] == "reports"

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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
