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


@pytest.mark.skip(reason="works")
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


@pytest.mark.skip(reason="works")
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


@pytest.mark.skip(reason="works")
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


# @pytest.mark.skip(reason="works")
def test_ha_mt_auth_scenario(test_name):
    """Test multi-tenant database setup with role-based access control"""
    inner_instances_description = get_instances_description_no_setup(test_name=test_name)
    interactive_mg_runner.start_all(inner_instances_description, keep_directories=False)

    # Connect to coordinator without auth
    driver = GraphDatabase.driver("neo4j://localhost:7692")

    # Create tenant databases
    with driver.session() as session:
        session.run("CREATE DATABASE tenant1_db")
        session.run("CREATE DATABASE tenant2_db")

    # Setup role-based access control following best practices
    with driver.session() as session:
        # Create admin role with full system privileges
        session.run("CREATE ROLE system_admin")
        session.run("GRANT ALL PRIVILEGES TO system_admin")
        session.run("GRANT DATABASE * TO system_admin")
        session.run("GRANT MULTI_DATABASE_USE TO system_admin")

        # Create tenant-specific roles (no access to memgraph database)
        session.run("CREATE ROLE tenant1_admin")
        session.run("CREATE ROLE tenant1_user")
        session.run("CREATE ROLE tenant2_admin")
        session.run("CREATE ROLE tenant2_user")

        # Grant appropriate permissions to tenant roles
        session.run("GRANT MATCH, CREATE, MERGE, SET, DELETE, INDEX TO tenant1_admin")
        session.run("GRANT MATCH, CREATE, MERGE, SET, DELETE TO tenant1_user")
        session.run("GRANT MATCH, CREATE, MERGE, SET, DELETE, INDEX TO tenant2_admin")
        session.run("GRANT MATCH, CREATE, MERGE, SET, DELETE TO tenant2_user")

        # Grant multi-database use to all tenant roles
        session.run("GRANT MULTI_DATABASE_USE TO tenant1_admin")
        session.run("GRANT MULTI_DATABASE_USE TO tenant1_user")
        session.run("GRANT MULTI_DATABASE_USE TO tenant2_admin")
        session.run("GRANT MULTI_DATABASE_USE TO tenant2_user")

        # Grant access only to tenant databases
        session.run("GRANT DATABASE tenant1_db TO tenant1_admin")
        session.run("GRANT DATABASE tenant1_db TO tenant1_user")
        session.run("GRANT DATABASE tenant2_db TO tenant2_admin")
        session.run("GRANT DATABASE tenant2_db TO tenant2_user")

        # Revoke default database access from tenant roles
        session.run("REVOKE DATABASE memgraph FROM tenant1_admin")
        session.run("REVOKE DATABASE memgraph FROM tenant1_user")
        session.run("REVOKE DATABASE memgraph FROM tenant2_admin")
        session.run("REVOKE DATABASE memgraph FROM tenant2_user")

        # Create users
        session.run("CREATE USER system_admin_user IDENTIFIED BY 'admin_password'")
        session.run("CREATE USER tenant1_admin_user IDENTIFIED BY 't1_admin_pass'")
        session.run("CREATE USER tenant1_regular_user IDENTIFIED BY 't1_user_pass'")
        session.run("CREATE USER tenant2_admin_user IDENTIFIED BY 't2_admin_pass'")
        session.run("CREATE USER tenant2_regular_user IDENTIFIED BY 't2_user_pass'")

        # Assign roles
        session.run("SET ROLE FOR system_admin_user TO system_admin")
        session.run("SET ROLE FOR tenant1_admin_user TO tenant1_admin")
        session.run("SET ROLE FOR tenant1_regular_user TO tenant1_user")
        session.run("SET ROLE FOR tenant2_admin_user TO tenant2_admin")
        session.run("SET ROLE FOR tenant2_regular_user TO tenant2_user")

        # Set main databases for tenant users
        session.run("SET MAIN DATABASE tenant1_db FOR tenant1_admin_user")
        session.run("SET MAIN DATABASE tenant1_db FOR tenant1_regular_user")
        session.run("SET MAIN DATABASE tenant2_db FOR tenant2_admin_user")
        session.run("SET MAIN DATABASE tenant2_db FOR tenant2_regular_user")

    driver.close()

    # Test system admin can access all databases
    admin_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("system_admin_user", "admin_password"))

    # Admin creates initial data in each database
    with admin_driver.session(database="memgraph") as session:
        session.run("CREATE (n:SystemData {name: 'system_info'})")

    with admin_driver.session(database="tenant1_db") as session:
        session.run("CREATE (n:AdminData {name: 'tenant1_admin_data'})")

    with admin_driver.session(database="tenant2_db") as session:
        session.run("CREATE (n:AdminData {name: 'tenant2_admin_data'})")
    admin_driver.close()

    # Test tenant1_admin_user can access only tenant1_db
    t1_admin_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("tenant1_admin_user", "t1_admin_pass"))

    # Verify default database is tenant1_db
    with t1_admin_driver.session() as session:
        result = session.run("SHOW DATABASE").single()
        assert result[0] == "tenant1_db"

    # Can create and modify data in tenant1_db
    with t1_admin_driver.session(database="tenant1_db") as session:
        session.run("CREATE (n:TenantData {name: 'tenant1_specific', admin: true})")
        session.run("CREATE INDEX ON :TenantData(name)")  # Admin can create indexes
        result = session.run("MATCH (n:TenantData) RETURN n.name as name").single()
        assert result["name"] == "tenant1_specific"

    # Cannot access memgraph or tenant2_db
    try:
        with t1_admin_driver.session(database="memgraph") as session:
            session.run("MATCH (n) RETURN n")
            assert False, "tenant1_admin should not access memgraph database"
    except Exception:
        pass

    try:
        with t1_admin_driver.session(database="tenant2_db") as session:
            session.run("MATCH (n) RETURN n")
            assert False, "tenant1_admin should not access tenant2_db"
    except Exception:
        pass
    t1_admin_driver.close()

    # Test tenant1_regular_user can access only tenant1_db with limited privileges
    t1_user_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("tenant1_regular_user", "t1_user_pass"))

    with t1_user_driver.session(database="tenant1_db") as session:
        # Can create and modify data
        session.run("CREATE (n:UserData {name: 'user_created'})")
        session.run("MATCH (n:UserData) SET n.modified = true")

        # Cannot create indexes (no INDEX privilege)
        try:
            session.run("CREATE INDEX ON :UserData(name)")
            assert False, "Regular user should not be able to create indexes"
        except Exception:
            pass
    t1_user_driver.close()

    # Test tenant2_admin_user can access only tenant2_db
    t2_admin_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("tenant2_admin_user", "t2_admin_pass"))

    with t2_admin_driver.session(database="tenant2_db") as session:
        session.run("CREATE (n:TenantData {name: 'tenant2_specific', admin: true})")
        session.run("CREATE INDEX ON :TenantData(name)")  # Admin can create indexes
        result = session.run("MATCH (n:TenantData) RETURN n.name as name").single()
        assert result["name"] == "tenant2_specific"

    # Cannot access tenant1_db
    try:
        with t2_admin_driver.session(database="tenant1_db") as session:
            session.run("MATCH (n) RETURN n")
            assert False, "tenant2_admin should not access tenant1_db"
    except Exception:
        pass
    t2_admin_driver.close()

    # Test tenant2_regular_user
    t2_user_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("tenant2_regular_user", "t2_user_pass"))

    with t2_user_driver.session(database="tenant2_db") as session:
        session.run("CREATE (n:UserData {name: 'tenant2_user_data'})")
        result = session.run("MATCH (n:UserData) RETURN count(n) as cnt").single()
        assert result["cnt"] == 1
    t2_user_driver.close()

    # Verify data isolation between tenants
    admin_driver = GraphDatabase.driver("neo4j://localhost:7692", auth=("system_admin_user", "admin_password"))

    with admin_driver.session(database="tenant1_db") as session:
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 3  # AdminData + TenantData + UserData

    with admin_driver.session(database="tenant2_db") as session:
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 3  # AdminData + TenantData + UserData

    with admin_driver.session(database="memgraph") as session:
        result = session.run("MATCH (n) RETURN count(n) as cnt").single()
        assert result["cnt"] == 1  # Only SystemData
    admin_driver.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
