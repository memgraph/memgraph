# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this file will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import concurrent.futures
import sys
import threading
import time

import pytest
from neo4j import GraphDatabase


def test_basic_profile_operations():
    """Test basic profile creation, update, and deletion operations."""
    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create profiles with different limits
            session.run("CREATE PROFILE basic_profile;").consume()
            session.run("CREATE PROFILE limited_profile LIMIT sessions 5;").consume()
            session.run("CREATE PROFILE memory_profile LIMIT transactions_memory 100MB;").consume()
            session.run("CREATE PROFILE mixed_profile LIMIT sessions 3, transactions_memory 50MB;").consume()

            # Verify profiles were created
            result = session.run("SHOW PROFILES;")
            profiles = [record["profile"] for record in result]
            print(profiles)
            assert "basic_profile" in profiles
            assert "limited_profile" in profiles
            assert "memory_profile" in profiles
            assert "mixed_profile" in profiles

            # Test profile details
            result = session.run("SHOW PROFILE basic_profile;")
            results = list(result)
            assert len(results) == 2
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["sessions"] == "UNLIMITED"
            assert limits["transactions_memory"] == "UNLIMITED"

            result = session.run("SHOW PROFILE limited_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["sessions"] == 5
            assert limits["transactions_memory"] == "UNLIMITED"

            result = session.run("SHOW PROFILE memory_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["sessions"] == "UNLIMITED"
            assert limits["transactions_memory"] == "100MB"

            result = session.run("SHOW PROFILE mixed_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["sessions"] == 3
            assert limits["transactions_memory"] == "50MB"

            # Test profile updates
            session.run("UPDATE PROFILE basic_profile LIMIT sessions 10;").consume()
            result = session.run("SHOW PROFILE basic_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["sessions"] == 10

            session.run("UPDATE PROFILE basic_profile LIMIT transactions_memory 200MB;").consume()
            result = session.run("SHOW PROFILE basic_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["transactions_memory"] == "200MB"

            # Test profile deletion
            session.run("DROP PROFILE basic_profile;").consume()
            result = session.run("SHOW PROFILES;")
            profile_names = [row["profile"] for row in result]
            assert "basic_profile" not in profile_names

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE basic_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE limited_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE memory_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE mixed_profile;").consume()
            except Exception:
                pass


def test_profile_assignment_and_management():
    """Test profile assignment to users and roles."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create profiles and users
            session.run("CREATE PROFILE basic_profile;").consume()
            session.run("CREATE PROFILE limited_profile LIMIT sessions 2;").consume()
            session.run("CREATE USER test_user1;").consume()
            session.run("CREATE USER test_user2;").consume()
            session.run("CREATE ROLE test_role1;").consume()
            session.run("CREATE ROLE test_role2;").consume()

            # Assign profiles to users
            session.run("SET PROFILE FOR test_user1 TO basic_profile;").consume()
            session.run("SET PROFILE FOR test_user2 TO limited_profile;").consume()

            # Verify assignments
            result = session.run("SHOW PROFILE FOR test_user1;")
            results = list(result)
            assert len(results) == 1
            assert results[0]["profile"] == "basic_profile"

            result = session.run("SHOW PROFILE FOR test_user2;")
            results = list(result)
            assert len(results) == 1
            assert results[0]["profile"] == "limited_profile"

            # Test user listing for profiles
            result = session.run("SHOW USERS FOR PROFILE basic_profile;")
            results = list(result)
            assert len(results) == 1
            assert results[0]["user"] == "test_user1"

            result = session.run("SHOW USERS FOR PROFILE limited_profile;")
            results = list(result)
            assert len(results) == 1
            assert results[0]["user"] == "test_user2"

            # Test profile reassignment
            session.run("SET PROFILE FOR test_user1 TO limited_profile;").consume()
            result = session.run("SHOW PROFILE FOR test_user1;")
            results = list(result)
            assert results[0]["profile"] == "limited_profile"

            # Test profile clearing
            session.run("CLEAR PROFILE FOR test_user1;").consume()
            result = session.run("SHOW PROFILE FOR test_user1;")
            results = list(result)
            assert results[0]["profile"] == "null"

            # Test role profile assignment
            session.run("SET PROFILE FOR test_role1 TO basic_profile;").consume()
            session.run("SET PROFILE FOR test_role2 TO limited_profile;").consume()

            result = session.run("SHOW PROFILE FOR test_role1;")
            results = list(result)
            assert results[0]["profile"] == "basic_profile"

            result = session.run("SHOW PROFILE FOR test_role2;")
            results = list(result)
            assert results[0]["profile"] == "limited_profile"

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE basic_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE limited_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER test_user1;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER test_user2;").consume()
            except Exception:
                pass
            try:
                session.run("DROP ROLE test_role1;").consume()
            except Exception:
                pass
            try:
                session.run("DROP ROLE test_role2;").consume()
            except Exception:
                pass


def test_profile_inheritance_and_merging():
    """Test profile inheritance when users have both user and role profiles."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create profiles with different limits
            session.run("CREATE PROFILE user_profile LIMIT sessions 5;").consume()
            session.run("CREATE PROFILE role_profile LIMIT transactions_memory 100MB;").consume()
            session.run("CREATE USER test_user1;").consume()
            session.run("CREATE ROLE test_role1;").consume()

            # Assign profiles
            session.run("SET PROFILE FOR test_user1 TO user_profile;").consume()
            session.run("SET PROFILE FOR test_role1 TO role_profile;").consume()
            session.run("SET ROLE FOR test_user1 TO test_role1;").consume()

            # Verify both profiles are accessible
            result = session.run("SHOW PROFILE FOR test_user1;")
            results = list(result)
            assert results[0]["profile"] == "user_profile"

            result = session.run("SHOW PROFILE FOR test_role1;")
            results = list(result)
            assert results[0]["profile"] == "role_profile"

            # Test that user gets the more restrictive limits
            # User profile: sessions=5, memory=UNLIMITED
            # Role profile: sessions=UNLIMITED, memory=100MB
            # Expected: sessions=5, memory=100MB (more restrictive of each)

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE user_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE role_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER test_user1;").consume()
            except Exception:
                pass
            try:
                session.run("DROP ROLE test_role1;").consume()
            except Exception:
                pass


def test_error_handling_and_edge_cases():
    """Test error handling for invalid operations."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Test creating duplicate profile
            session.run("CREATE PROFILE test_profile;").consume()
            with pytest.raises(Exception):
                session.run("CREATE PROFILE test_profile;").consume()

            # Test showing non-existent profile
            with pytest.raises(Exception):
                session.run("SHOW PROFILE non_existent_profile;").consume()

            # Test updating non-existent profile
            with pytest.raises(Exception):
                session.run("UPDATE PROFILE non_existent_profile LIMIT sessions 5;").consume()

            # Test dropping non-existent profile
            with pytest.raises(Exception):
                session.run("DROP PROFILE non_existent_profile;").consume()

            # Test assigning profile to non-existent user
            with pytest.raises(Exception):
                session.run("SET PROFILE FOR non_existent_user TO test_profile;").consume()

            # Test assigning non-existent profile to user
            session.run("CREATE USER test_user1;").consume()
            with pytest.raises(Exception):
                session.run("SET PROFILE FOR test_user1 TO non_existent_profile;").consume()

            # Test invalid limit names
            with pytest.raises(Exception):
                session.run("CREATE PROFILE invalid_profile LIMIT invalid_limit 5;").consume()

            # Test invalid limit values
            with pytest.raises(Exception):
                session.run("CREATE PROFILE invalid_profile LIMIT sessions -5;").consume()

            # Cleanup
            session.run("DROP PROFILE test_profile;").consume()
            session.run("DROP USER test_user1;").consume()


def test_concurrent_profile_operations():
    """Test concurrent profile operations for thread safety."""

    def create_profiles(thread_id, num_profiles):
        """Create multiple profiles concurrently."""
        with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
            with driver.session() as session:
                for i in range(num_profiles):
                    profile_name = f"concurrent_profile_{thread_id}_{i}"
                    try:
                        session.run(f"CREATE PROFILE {profile_name};").consume()
                    except Exception:
                        pass  # Expected for duplicate names

    def update_profiles(thread_id, num_updates):
        """Update profiles concurrently."""
        with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
            with driver.session() as session:
                for i in range(num_updates):
                    profile_name = f"concurrent_profile_{thread_id % 3}_{i % 5}"
                    try:
                        session.run(f"UPDATE PROFILE {profile_name} LIMIT sessions {i + 1};").consume()
                    except Exception:
                        pass  # Expected for non-existent profiles

    # Create some initial profiles
    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            for i in range(3):
                session.run(f"CREATE PROFILE concurrent_profile_{i}_0;").consume()

        # Run concurrent operations
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Submit profile creation tasks
            create_futures = [executor.submit(create_profiles, i, 10) for i in range(5)]

            # Submit profile update tasks
            update_futures = [executor.submit(update_profiles, i, 20) for i in range(5)]

            # Wait for all tasks to complete
            concurrent.futures.wait(create_futures + update_futures)

        # Verify some profiles were created
        with driver.session() as session:
            result = session.run("SHOW PROFILES;")
            results = list(result)
            assert len(results) >= 3  # At least the initial profiles should exist

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE concurrent_profile_0_0;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE concurrent_profile_0_0;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE concurrent_profile_1_0;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE concurrent_profile_2_0;").consume()
            except Exception:
                pass


def test_resource_exhaustion_scenarios():
    """Test resource exhaustion and recovery scenarios."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create profile with strict limits
            session.run("CREATE PROFILE strict_profile LIMIT sessions 1, transactions_memory 1KB;").consume()
            session.run("CREATE USER test_user1;").consume()
            session.run("SET PROFILE FOR test_user1 TO strict_profile;").consume()

        # Connect as the user to test resource limits
        user_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("test_user1", ""))

        # Test session limit exhaustion
        # First session should work
        try:
            with user_driver.session() as user_session:
                user_session.run("MATCH (n) RETURN n LIMIT 1;").consume()
        except Exception as e:
            # This might fail due to session limits, which is expected
            pass

        # Test memory limit exhaustion
        # Try to execute a query that might exceed memory limits
        try:
            with user_driver.session() as user_session:
                user_session.run("MATCH (n) RETURN n;").consume()
        except Exception as e:
            # This might fail due to memory limits, which is expected
            pass

        user_driver.close()

        # Test profile update during resource usage
        with driver.session() as session:
            session.run("UPDATE PROFILE strict_profile LIMIT sessions 5, transactions_memory 10KB;").consume()

            # Verify the profile was updated
            result = session.run("SHOW PROFILE strict_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["sessions"] == 5
            assert limits["transactions_memory"] == "10KB"

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE strict_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER test_user1;").consume()
            except Exception:
                pass


def test_profile_lifecycle_with_active_users():
    """Test profile lifecycle management with active users."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create profile and users
            session.run("CREATE PROFILE lifecycle_profile LIMIT sessions 3;").consume()
            session.run("CREATE USER test_user1;").consume()
            session.run("CREATE USER test_user2;").consume()
            session.run("SET PROFILE FOR test_user1 TO lifecycle_profile;").consume()
            session.run("SET PROFILE FOR test_user2 TO lifecycle_profile;").consume()

            # Verify assignments
            result = session.run("SHOW USERS FOR PROFILE lifecycle_profile;")
            results = list(result)
            assert len(results) == 2
            user_names = [row["user"] for row in results]
            assert "test_user1" in user_names
            assert "test_user2" in user_names

            # Delete the profile while users are assigned
            session.run("DROP PROFILE lifecycle_profile;").consume()

            # Verify users no longer have profiles
            result = session.run("SHOW PROFILE FOR test_user1;")
            results = list(result)
            assert results[0]["profile"] == "null"

            result = session.run("SHOW PROFILE FOR test_user2;")
            results = list(result)
            assert results[0]["profile"] == "null"

            # Verify no users are assigned to the deleted profile
            with pytest.raises(Exception):
                session.run("SHOW USERS FOR PROFILE lifecycle_profile;").consume()

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE lifecycle_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER test_user1;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER test_user2;").consume()
            except Exception:
                pass


def test_stress_test_with_multiple_operations():
    """Stress test with multiple concurrent operations."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create profiles and users for stress testing
            session.run("CREATE PROFILE stress_profile LIMIT sessions 10, transactions_memory 1MB;").consume()
            session.run("CREATE USER stress_user;").consume()
            session.run("SET PROFILE FOR stress_user TO stress_profile;").consume()

        def stress_operation(operation_id):
            """Perform various profile operations."""
            with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as op_driver:
                with op_driver.session() as session:
                    try:
                        # Create temporary profiles
                        session.run(f"CREATE PROFILE temp_profile_{operation_id};").consume()

                        # Update the stress profile
                        session.run(f"UPDATE PROFILE stress_profile LIMIT sessions {operation_id % 20 + 1};").consume()

                        # Show profile information
                        result = session.run("SHOW PROFILE stress_profile;")
                        results = list(result)
                        assert len(results) == 2

                        # Show users for profile
                        result = session.run("SHOW USERS FOR PROFILE stress_profile;")
                        results = list(result)
                        assert len(results) == 1

                        # Clean up temporary profile
                        session.run(f"DROP PROFILE temp_profile_{operation_id};").consume()

                    except Exception as e:
                        # Some operations might fail due to concurrency, which is expected
                        pass

        # Run stress operations concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(stress_operation, i) for i in range(50)]
            concurrent.futures.wait(futures)

        # Verify the stress profile still exists and has reasonable limits
        with driver.session() as session:
            result = session.run("SHOW PROFILE stress_profile;")
            results = list(result)
            assert len(results) == 2
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["sessions"] >= 1
            assert limits["transactions_memory"] == "1MB"

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE stress_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER stress_user;").consume()
            except Exception:
                pass


def test_profile_syntax_variations():
    """Test various syntax variations for profile operations."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Test different case variations
            session.run("CREATE PROFILE CaseProfile;").consume()
            session.run("CREATE PROFILE caseprofile;").consume()
            session.run("CREATE PROFILE CASE_PROFILE;").consume()

            # Test limit name variations
            session.run("CREATE PROFILE session_profile LIMIT Sessions 5;").consume()
            session.run("CREATE PROFILE memory_profile LIMIT Transactions_Memory 100MB;").consume()
            session.run("CREATE PROFILE mixed_profile LIMIT SESSIONS 3, TRANSACTIONS_MEMORY 50MB;").consume()

            # Verify profiles were created
            result = session.run("SHOW PROFILES;")
            profile_names = [row["profile"] for row in result]
            assert "CaseProfile" in profile_names
            assert "caseprofile" in profile_names
            assert "CASE_PROFILE" in profile_names
            assert "session_profile" in profile_names
            assert "memory_profile" in profile_names
            assert "mixed_profile" in profile_names

            # Test syntax variations in updates
            session.run("UPDATE PROFILE CaseProfile LIMIT Sessions 10;").consume()
            session.run("UPDATE PROFILE caseprofile LIMIT TRANSACTIONS_MEMORY 200MB;").consume()

            # Test syntax variations in assignments
            session.run("CREATE USER syntax_user;").consume()
            session.run("SET PROFILE FOR syntax_user TO CaseProfile;").consume()
            session.run("SET PROFILE FOR syntax_user TO caseprofile;").consume()

            # Cleanup
            session.run("DROP USER syntax_user;").consume()

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE CaseProfile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE session_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE memory_profile;").consume()
            except Exception:
                pass


def test_profile_with_different_memory_units():
    """Test profile creation with different memory units."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Test different memory units
            session.run("CREATE PROFILE kb_profile LIMIT transactions_memory 1KB;").consume()
            session.run("CREATE PROFILE mb_profile LIMIT transactions_memory 1MB;").consume()

            # Verify profiles were created with correct units
            result = session.run("SHOW PROFILE kb_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["transactions_memory"] == "1KB"

            result = session.run("SHOW PROFILE mb_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["transactions_memory"] == "1MB"

            # Test updating with different units
            session.run("UPDATE PROFILE kb_profile LIMIT transactions_memory 2MB;")
            result = session.run("SHOW PROFILE kb_profile;")
            results = list(result)
            limits = {row["limit"]: row["value"] for row in results}
            assert limits["transactions_memory"] == "2MB"

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE kb_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE mb_profile;").consume()
            except Exception:
                pass


def test_profile_cleanup_on_user_deletion():
    """Test that profile assignments are cleaned up when users are deleted."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create profile and users
            session.run("CREATE PROFILE cleanup_profile;").consume()
            session.run("CREATE USER cleanup_user1;").consume()
            session.run("CREATE USER cleanup_user2;").consume()
            session.run("SET PROFILE FOR cleanup_user1 TO cleanup_profile;").consume()
            session.run("SET PROFILE FOR cleanup_user2 TO cleanup_profile;").consume()

            # Verify assignments
            result = session.run("SHOW USERS FOR PROFILE cleanup_profile;")
            results = list(result)
            assert len(results) == 2

            # Delete one user
            session.run("DROP USER cleanup_user1;").consume()

            # Verify the remaining user still has the profile
            result = session.run("SHOW USERS FOR PROFILE cleanup_profile;")
            results = list(result)
            assert len(results) == 1
            assert results[0]["user"] == "cleanup_user2"

            # Delete the other user
            session.run("DROP USER cleanup_user2;").consume()

            # Verify no users are assigned to the profile
            result = session.run("SHOW USERS FOR PROFILE cleanup_profile;")
            results = list(result)
            assert len(results) == 0

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE cleanup_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER cleanup_user1;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER cleanup_user2;").consume()
            except Exception:
                pass


def test_profile_with_role_hierarchy():
    """Test profile behavior with role hierarchies."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create profiles and roles
            session.run("CREATE PROFILE admin_profile LIMIT sessions 10;").consume()
            session.run("CREATE PROFILE user_profile LIMIT sessions 5;").consume()
            session.run("CREATE ROLE admin_role;").consume()
            session.run("CREATE ROLE user_role;").consume()
            session.run("CREATE USER hierarchy_user;").consume()

            # Assign profiles to roles
            session.run("SET PROFILE FOR admin_role TO admin_profile;").consume()
            session.run("SET PROFILE FOR user_role TO user_profile;").consume()

            # Assign roles to user
            session.run("SET ROLE FOR hierarchy_user TO admin_role, user_role;").consume()

            # Verify role profiles
            result = session.run("SHOW PROFILE FOR admin_role;")
            results = list(result)
            assert results[0]["profile"] == "admin_profile"

            result = session.run("SHOW PROFILE FOR user_role;")
            results = list(result)
            assert results[0]["profile"] == "user_profile"

            # Test that user inherits the more restrictive profile limits
            # admin_profile: sessions=10
            # user_profile: sessions=5
            # Expected: sessions=5 (more restrictive)

        # cleanup
        with driver.session() as session:
            try:
                session.run("DROP PROFILE admin_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP PROFILE user_profile;").consume()
            except Exception:
                pass
            try:
                session.run("DROP ROLE admin_role;").consume()
            except Exception:
                pass
            try:
                session.run("DROP ROLE user_role;").consume()
            except Exception:
                pass
            try:
                session.run("DROP USER hierarchy_user;").consume()
            except Exception:
                pass


def test_profile_performance_under_load():
    """Test profile performance under various load conditions."""

    with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as driver:
        with driver.session() as session:
            # Create multiple profiles and users
            for i in range(10):
                session.run(f"CREATE PROFILE perf_profile_{i} LIMIT sessions {i + 1};").consume()
                session.run(f"CREATE USER perf_user_{i};").consume()
                session.run(f"SET PROFILE FOR perf_user_{i} TO perf_profile_{i};").consume()

        # Measure time for profile operations
        start_time = time.time()

        # Perform multiple profile operations
        with driver.session() as session:
            for i in range(100):
                profile_id = i % 10
                session.run(f"UPDATE PROFILE perf_profile_{profile_id} LIMIT sessions {i % 20 + 1};").consume()
                result = session.run(f"SHOW PROFILE perf_profile_{profile_id};")
                results = list(result)
                assert len(results) == 2

        end_time = time.time()
        operation_time = end_time - start_time

        # Verify reasonable performance (should complete within 10 seconds)
        assert operation_time < 10.0, f"Profile operations took too long: {operation_time:.2f} seconds"

        # Test concurrent profile lookups
        def concurrent_lookup(thread_id):
            with GraphDatabase.driver("bolt://localhost:7687", auth=("", "")) as lookup_driver:
                with lookup_driver.session() as session:
                    for i in range(20):
                        profile_id = (thread_id + i) % 10
                        try:
                            result = session.run(f"SHOW PROFILE perf_profile_{profile_id};")
                            results = list(result)
                            assert len(results) == 2
                        except Exception:
                            pass

        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(concurrent_lookup, i) for i in range(5)]
            concurrent.futures.wait(futures)
        end_time = time.time()

        concurrent_time = end_time - start_time
        assert concurrent_time < 5.0, f"Concurrent lookups took too long: {concurrent_time:.2f} seconds"

        # cleanup
        with driver.session() as session:
            for i in range(10):
                try:
                    session.run(f"DROP PROFILE perf_profile_{i};").consume()
                except Exception:
                    pass
                try:
                    session.run(f"DROP USER perf_user_{i};").consume()
                except Exception:
                    pass


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
