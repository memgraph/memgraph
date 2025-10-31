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
import pytest
from neo4j import Auth, GraphDatabase

# Setup interactive_mg_runner paths
interactive_mg_runner.SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
interactive_mg_runner.PROJECT_DIR = os.path.normpath(
    os.path.join(interactive_mg_runner.SCRIPT_DIR, "..", "..", "..", "..", "..")
)
interactive_mg_runner.BUILD_DIR = os.path.normpath(os.path.join(interactive_mg_runner.PROJECT_DIR, "memgraph", "build"))
interactive_mg_runner.MEMGRAPH_BINARY = os.path.normpath(os.path.join(interactive_mg_runner.BUILD_DIR, "memgraph"))

# SSO configuration
AUTH_MODULE_PATH = os.path.normpath(os.path.join(interactive_mg_runner.SCRIPT_DIR, "dummy_sso_module.py"))
INSTANCE_NAME = "test_instance"
MG_URI = "bolt://localhost:7687"
CLIENT_ERROR_MESSAGE = "Authentication failure"
USERNAME = "anthony"

file = "test_sso_streams"


def get_instances(test_name: str):
    return {
        INSTANCE_NAME: {
            "args": ["--bolt-port=7687", "--log-level=TRACE", "--data-recovery-on-startup=true"],
            "log_file": f"sso_streams/{file}/{test_name}/test_instance.log",
            "data_directory": f"sso_streams/{file}/{test_name}",
            "setup_queries": [],
        }
    }


@pytest.fixture(scope="function")
def sso_connection(request):
    """Fixture that sets up SSO authentication and provides a connection."""
    test_name = request.function.__name__
    instances = get_instances(test_name)

    # Start Memgraph without SSO first
    interactive_mg_runner.start_all(instances)

    # Create roles and users for SSO
    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        with client.session() as session:
            session.run("CREATE ROLE architect;").consume()
            session.run("GRANT ALL PRIVILEGES TO architect;").consume()

    interactive_mg_runner.stop(instances, INSTANCE_NAME)

    # Restart with SSO enabled
    instances[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(instances)

    # Create SSO authenticated connection
    response = base64.b64encode(b"dummy_value").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        # Verify current user
        with client.session() as session:
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == USERNAME

        yield client

    # Cleanup
    interactive_mg_runner.stop(instances, INSTANCE_NAME, keep_directories=False)


@pytest.fixture(scope="function")
def multi_role_connection(request):
    """Fixture that sets up multi-role SSO authentication with multiple databases."""
    test_name = request.function.__name__
    instances = get_instances(test_name)

    # Start Memgraph without SSO first
    interactive_mg_runner.start_all(instances)

    # Create roles, databases, and permissions
    with GraphDatabase.driver(MG_URI, auth=("", "")) as client:
        with client.session() as session:
            # Create roles
            session.run("CREATE ROLE admin;").consume()
            session.run("CREATE ROLE architect;").consume()
            session.run("CREATE ROLE user;").consume()

            # Grant privileges
            session.run("GRANT ALL PRIVILEGES TO admin;").consume()
            session.run("GRANT CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS * TO admin;").consume()
            session.run("GRANT CREATE, READ, UPDATE, DELETE ON EDGES CONTAINING TYPES * TO admin;").consume()
            session.run("GRANT ALL PRIVILEGES TO architect;").consume()
            session.run("GRANT CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS * TO architect;").consume()
            session.run("GRANT CREATE, READ, UPDATE, DELETE ON EDGES CONTAINING TYPES * TO architect;").consume()
            session.run("GRANT MATCH, CREATE, STREAM, MULTI_DATABASE_USE TO user;").consume()
            session.run("GRANT CREATE, READ, UPDATE, DELETE ON NODES CONTAINING LABELS * TO user;").consume()
            session.run("GRANT CREATE, READ, UPDATE, DELETE ON EDGES CONTAINING TYPES * TO user;").consume()

            # Create databases
            session.run("CREATE DATABASE admin_db;").consume()
            session.run("CREATE DATABASE architect_db;").consume()
            session.run("CREATE DATABASE user_db;").consume()

            # Grant database access to roles
            session.run("GRANT DATABASE admin_db TO admin;").consume()
            session.run("GRANT DATABASE architect_db TO architect;").consume()
            session.run("GRANT DATABASE user_db TO user;").consume()

            # Set main databases for roles
            session.run("SET MAIN DATABASE admin_db FOR admin;").consume()
            session.run("SET MAIN DATABASE architect_db FOR architect;").consume()
            session.run("SET MAIN DATABASE user_db FOR user;").consume()

    interactive_mg_runner.stop(instances, INSTANCE_NAME)

    # Restart with SSO enabled
    instances[INSTANCE_NAME]["args"].append(f"--auth-module-mappings=saml-entra-id:{AUTH_MODULE_PATH}")
    interactive_mg_runner.start_all(instances)

    yield instances

    # Cleanup
    interactive_mg_runner.stop(instances, INSTANCE_NAME, keep_directories=False)


def test_sso_kafka_stream_creation(kafka_topics, sso_connection):
    """Test creating a Kafka stream with SSO authentication."""
    assert len(kafka_topics) > 0
    stream_name = "sso_kafka_stream"

    with sso_connection.session() as session:
        # Create a Kafka stream
        session.run(
            f"""CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]}
            TRANSFORM kafka_transform.simple
            BOOTSTRAP_SERVERS 'localhost:29092'"""
        ).consume()

        # Verify the stream was created and check its info
        streams_result = list(session.run("SHOW STREAMS"))
        assert len(streams_result) == 1
        stream_info = streams_result[0]
        assert stream_info["name"] == stream_name
        assert stream_info["type"] == "kafka"
        assert stream_info["owner"] == USERNAME


def test_sso_pulsar_stream_creation(pulsar_topics, sso_connection):
    """Test creating a Pulsar stream with SSO authentication."""
    assert len(pulsar_topics) > 0
    stream_name = "sso_pulsar_stream"

    with sso_connection.session() as session:
        # Create a Pulsar stream
        session.run(
            f"""CREATE PULSAR STREAM {stream_name} TOPICS {pulsar_topics[0]}
            TRANSFORM pulsar_transform.simple
            SERVICE_URL 'pulsar://127.0.0.1:6650'"""
        ).consume()

        # Verify the stream was created and check its info
        streams_result = list(session.run("SHOW STREAMS"))
        assert len(streams_result) == 1
        stream_info = streams_result[0]
        assert stream_info["name"] == stream_name
        assert stream_info["type"] == "pulsar"
        assert stream_info["owner"] == USERNAME


def test_sso_multiple_streams(kafka_topics, pulsar_topics, sso_connection):
    """Test creating multiple streams (Kafka and Pulsar) with SSO authentication."""
    assert len(kafka_topics) > 0 and len(pulsar_topics) > 0

    with sso_connection.session() as session:
        # Create multiple streams
        session.run(
            f"""CREATE KAFKA STREAM sso_kafka_stream1 TOPICS {kafka_topics[0]}
            TRANSFORM kafka_transform.simple
            BOOTSTRAP_SERVERS 'localhost:29092'"""
        ).consume()

        session.run(
            f"""CREATE PULSAR STREAM sso_pulsar_stream1 TOPICS {pulsar_topics[0]}
            TRANSFORM pulsar_transform.simple
            SERVICE_URL 'pulsar://127.0.0.1:6650'"""
        ).consume()

        # Verify both streams were created
        streams_result = list(session.run("SHOW STREAMS"))
        assert len(streams_result) == 2

        stream_names = [stream["name"] for stream in streams_result]
        assert "sso_kafka_stream1" in stream_names
        assert "sso_pulsar_stream1" in stream_names

        # Verify all streams are owned by the SSO user
        for stream in streams_result:
            assert stream["owner"] == USERNAME


def test_sso_stream_ownership_verification(kafka_topics, sso_connection):
    """Test that streams created by SSO users are properly owned."""
    assert len(kafka_topics) > 0
    stream_name = "sso_ownership_test"

    with sso_connection.session() as session:
        # Create a stream
        session.run(
            f"""CREATE KAFKA STREAM {stream_name} TOPICS {kafka_topics[0]}
            TRANSFORM kafka_transform.simple
            BOOTSTRAP_SERVERS 'localhost:29092'"""
        ).consume()

        # Verify ownership
        streams_result = list(session.run("SHOW STREAMS"))
        assert len(streams_result) == 1
        assert streams_result[0]["owner"] == USERNAME

        # Also verify through SHOW CURRENT USER that we're the right user
        current_user_result = list(session.run("SHOW CURRENT USER;"))
        assert len(current_user_result) == 1 and current_user_result[0]["user"] == USERNAME


def test_multi_role_admin_stream_creation(kafka_topics, multi_role_connection):
    """Test creating streams with admin user that has multiple roles."""
    assert len(kafka_topics) > 0

    # Create SSO authenticated connection for admin user
    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "admin_user"

            # Create stream in admin database
            session.run("USE DATABASE admin_db;").consume()
            session.run(
                f"""CREATE KAFKA STREAM admin_kafka_stream TOPICS {kafka_topics[0]}
                TRANSFORM kafka_transform.simple
                BOOTSTRAP_SERVERS 'localhost:29092'"""
            ).consume()

            # Verify stream was created
            streams_result = list(session.run("SHOW STREAMS"))
            assert len(streams_result) == 1
            assert streams_result[0]["owner"] == "admin_user"


def test_multi_role_architect_stream_creation(kafka_topics, multi_role_connection):
    """Test creating streams with architect user that has multiple roles."""
    assert len(kafka_topics) > 0

    # Create SSO authenticated connection for architect user
    response = base64.b64encode(b"multi_role_architect").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "architect_user"

            # Create stream in architect database
            session.run("USE DATABASE architect_db;").consume()
            session.run(
                f"""CREATE KAFKA STREAM architect_kafka_stream TOPICS {kafka_topics[0]}
                TRANSFORM kafka_transform.simple
                BOOTSTRAP_SERVERS 'localhost:29092'"""
            ).consume()

            # Verify stream was created
            streams_result = list(session.run("SHOW STREAMS"))
            assert len(streams_result) == 1
            assert streams_result[0]["owner"] == "architect_user"


def test_multi_role_user_stream_creation(kafka_topics, multi_role_connection):
    """Test creating streams with regular user that has single role."""
    assert len(kafka_topics) > 0

    # Create SSO authenticated connection for regular user
    response = base64.b64encode(b"multi_role_user").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "regular_user"

            # Create stream in user database
            session.run("USE DATABASE user_db;").consume()
            session.run(
                f"""CREATE KAFKA STREAM user_kafka_stream TOPICS {kafka_topics[0]}
                TRANSFORM kafka_transform.simple
                BOOTSTRAP_SERVERS 'localhost:29092'"""
            ).consume()

            # Verify stream was created
            streams_result = list(session.run("SHOW STREAMS"))
            assert len(streams_result) == 1
            assert streams_result[0]["owner"] == "regular_user"


def test_multi_role_database_authorization(multi_role_connection):
    """Test that users can only access databases they have permissions for."""

    # Test admin user - should have access to all databases
    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Should be able to access admin_db
            session.run("USE DATABASE admin_db;").consume()
            session.run("CREATE (n:TestNode {name: 'admin_test'})").consume()

            # Should be able to access architect_db
            session.run("USE DATABASE architect_db;").consume()
            session.run("CREATE (n:TestNode {name: 'architect_test'})").consume()

            # Should be able to access user_db
            session.run("USE DATABASE user_db;").consume()
            session.run("CREATE (n:TestNode {name: 'user_test'})").consume()


def test_multi_role_architect_database_authorization(multi_role_connection):
    """Test architect user database access permissions."""

    # Test architect user - should have access to architect_db and user_db
    response = base64.b64encode(b"multi_role_architect").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Should NOT be able to access admin_db
            try:
                session.run("USE DATABASE admin_db;").consume()
                session.run("CREATE (n:TestNode {name: 'admin_test'})").consume()
                assert False, "Architect should not have access to admin_db"
            except Exception:
                pass  # Expected to fail

            # Should be able to access architect_db
            session.run("USE DATABASE architect_db;").consume()
            session.run("CREATE (n:TestNode {name: 'architect_test'})").consume()

            # Should be able to access user_db
            session.run("USE DATABASE user_db;").consume()
            session.run("CREATE (n:TestNode {name: 'user_test'})").consume()


def test_multi_role_user_database_authorization(multi_role_connection):
    """Test regular user database access permissions."""

    # Test regular user - should only have access to user_db
    response = base64.b64encode(b"multi_role_user").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            try:
                # Should NOT be able to access admin_db
                session.run("USE DATABASE admin_db;").consume()
                session.run("CREATE (n:TestNode {name: 'admin_test'})").consume()
                assert False, "User should not have access to admin_db"
            except Exception:
                pass  # Expected to fail

            try:
                # Should NOT be able to access architect_db
                session.run("USE DATABASE architect_db;").consume()
                session.run("CREATE (n:TestNode {name: 'architect_test'})").consume()
                assert False, "User should not have access to architect_db"
            except Exception:
                pass  # Expected to fail

            # Should be able to access user_db
            session.run("USE DATABASE user_db;").consume()
            session.run("CREATE (n:TestNode {name: 'user_test'})").consume()


def test_multi_role_no_main_database(multi_role_connection):
    """Test user with multiple roles but no main database set."""

    # Test user with multiple roles but no main database
    response = base64.b64encode(b"multi_role_no_main").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "no_main_user"

            # Should be able to access user_db
            session.run("USE DATABASE user_db;").consume()
            session.run("CREATE (n:TestNode {name: 'user_test'})").consume()

            # Should be able to access architect_db
            session.run("USE DATABASE architect_db;").consume()
            session.run("CREATE (n:TestNode {name: 'architect_test'})").consume()


def test_multi_role_invalid_database(multi_role_connection):
    """Test user with role assigned to non-existent database."""

    # Test user with invalid database
    response = base64.b64encode(b"multi_role_invalid_db").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Verify current user
            current_user_result = list(session.run("SHOW CURRENT USER;"))
            assert len(current_user_result) == 1 and current_user_result[0]["user"] == "invalid_db_user"

            # Should NOT be able to access non-existent database
            try:
                session.run("USE DATABASE nonexistent_db;").consume()
                session.run("CREATE (n:TestNode {name: 'test'})").consume()
                assert False, "User should not have access to non-existent database"
            except Exception:
                pass  # Expected to fail


def test_multi_role_wrong_types(multi_role_connection):
    """Test user with wrong data types in role mapping."""

    # Test user with wrong data types
    response = base64.b64encode(b"multi_role_wrong_types").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Should fail due to wrong data types
            try:
                list(session.run("SHOW CURRENT USER;"))
                assert False, "Should fail due to wrong data types in role mapping"
            except Exception:
                pass  # Expected to fail


def test_multi_role_stream_cross_database(kafka_topics, multi_role_connection):
    """Test creating streams across different databases with different roles."""
    assert len(kafka_topics) > 0

    # Test admin user creating streams in different databases
    response = base64.b64encode(b"multi_role_admin").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            # Create stream in admin_db
            session.run("USE DATABASE admin_db;").consume()
            session.run(
                f"""CREATE KAFKA STREAM admin_stream TOPICS {kafka_topics[0]}
                TRANSFORM kafka_transform.simple
                BOOTSTRAP_SERVERS 'localhost:29092'"""
            ).consume()

            # Create stream in architect_db
            session.run("USE DATABASE architect_db;").consume()
            session.run(
                f"""CREATE KAFKA STREAM architect_stream TOPICS {kafka_topics[0]}
                TRANSFORM kafka_transform.simple
                BOOTSTRAP_SERVERS 'localhost:29092'"""
            ).consume()

            # Create stream in user_db
            session.run("USE DATABASE user_db;").consume()
            session.run(
                f"""CREATE KAFKA STREAM user_stream TOPICS {kafka_topics[0]}
                TRANSFORM kafka_transform.simple
                BOOTSTRAP_SERVERS 'localhost:29092'"""
            ).consume()

            # Verify streams were created in each database
            session.run("USE DATABASE admin_db;").consume()
            admin_streams = list(session.run("SHOW STREAMS"))
            assert len(admin_streams) == 1
            assert admin_streams[0]["name"] == "admin_stream"

            session.run("USE DATABASE architect_db;").consume()
            architect_streams = list(session.run("SHOW STREAMS"))
            assert len(architect_streams) == 1
            assert architect_streams[0]["name"] == "architect_stream"

            session.run("USE DATABASE user_db;").consume()
            user_streams = list(session.run("SHOW STREAMS"))
            assert len(user_streams) == 1
            assert user_streams[0]["name"] == "user_stream"


def test_multi_role_architect_limited_access(kafka_topics, multi_role_connection):
    """Test architect user can only create streams in databases they have access to."""
    assert len(kafka_topics) > 0

    # Test architect user - should only be able to create streams in architect_db and user_db
    response = base64.b64encode(b"multi_role_architect").decode("utf-8")
    MG_AUTH = Auth(scheme="saml-entra-id", credentials=response, principal="")

    with GraphDatabase.driver(MG_URI, auth=MG_AUTH) as client:
        with client.session() as session:
            try:
                # Should NOT be able to create stream in admin_db
                session.run("USE DATABASE admin_db;").consume()
                session.run(
                    f"""CREATE KAFKA STREAM admin_stream TOPICS {kafka_topics[0]}
                    TRANSFORM kafka_transform.simple
                    BOOTSTRAP_SERVERS 'localhost:29092'"""
                ).consume()
                assert False, "Architect should not be able to create stream in admin_db"
            except Exception:
                pass  # Expected to fail

            # Should be able to create stream in architect_db
            session.run("USE DATABASE architect_db;").consume()
            session.run(
                f"""CREATE KAFKA STREAM architect_stream TOPICS {kafka_topics[0]}
                TRANSFORM kafka_transform.simple
                BOOTSTRAP_SERVERS 'localhost:29092'"""
            ).consume()

            # Should be able to create stream in user_db
            session.run("USE DATABASE user_db;").consume()
            session.run(
                f"""CREATE KAFKA STREAM user_stream TOPICS {kafka_topics[0]}
                TRANSFORM kafka_transform.simple
                BOOTSTRAP_SERVERS 'localhost:29092'"""
            ).consume()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
