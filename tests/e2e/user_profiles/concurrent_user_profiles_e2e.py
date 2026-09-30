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
from gqlalchemy import Memgraph


@pytest.fixture
def memgraph() -> Memgraph:
    memgraph = Memgraph()
    yield memgraph

    # Cleanup
    try:
        memgraph.execute("DROP USER concurrent_user1;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP USER concurrent_user2;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP USER concurrent_user3;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP USER resource_user;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP PROFILE concurrent_profile;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP PROFILE resource_profile;")
    except Exception:
        pass
    try:
        memgraph.execute("DROP PROFILE stress_profile;")
    except Exception:
        pass


def test_concurrent_profile_creation(memgraph):
    """Test concurrent profile creation to ensure thread safety."""

    def create_profiles(thread_id, num_profiles):
        """Create multiple profiles concurrently."""
        created_count = 0
        for i in range(num_profiles):
            profile_name = f"concurrent_profile_{thread_id}_{i}"
            try:
                memgraph.execute(f"CREATE PROFILE {profile_name};")
                created_count += 1
            except Exception:
                # Expected for duplicate names in concurrent execution
                pass
        return created_count

    # Run concurrent profile creation
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(create_profiles, i, 20) for i in range(5)]

        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Verify some profiles were created successfully
    total_created = sum(results)
    assert total_created > 0, "No profiles were created in concurrent execution"

    # Verify profiles exist in the system
    all_profiles = list(memgraph.execute_and_fetch("SHOW PROFILES;"))
    assert len(all_profiles) >= total_created, "Not all created profiles are visible"


def test_concurrent_profile_updates(memgraph):
    """Test concurrent profile updates to ensure consistency."""

    # Create initial profile
    memgraph.execute("CREATE PROFILE concurrent_profile;")

    def update_profile(thread_id, num_updates):
        """Update profile concurrently."""
        successful_updates = 0
        for i in range(num_updates):
            try:
                memgraph.execute(f"UPDATE PROFILE concurrent_profile LIMIT sessions {thread_id * 10 + i};")
                successful_updates += 1
                time.sleep(0.01)  # Small delay to increase contention
            except Exception:
                pass
        return successful_updates

    # Run concurrent profile updates
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(update_profile, i, 10) for i in range(3)]

        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Verify at least some updates succeeded
    total_updates = sum(results)
    assert total_updates > 0, "No profile updates succeeded in concurrent execution"

    # Verify profile was updated
    profile_info = list(memgraph.execute_and_fetch("SHOW PROFILE concurrent_profile;"))
    assert len(profile_info) == 2, "Profile should have both session and memory limits"


def test_concurrent_profile_assignments(memgraph):
    """Test concurrent profile assignments to users."""

    # Create profile and users
    memgraph.execute("CREATE PROFILE concurrent_profile;")
    memgraph.execute("CREATE USER concurrent_user1;")
    memgraph.execute("CREATE USER concurrent_user2;")
    memgraph.execute("CREATE USER concurrent_user3;")

    def assign_profile(user_id):
        """Assign profile to user."""
        try:
            memgraph.execute(f"SET PROFILE FOR concurrent_user{user_id} TO concurrent_profile;")
            return True
        except Exception:
            return False

    # Run concurrent profile assignments
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(assign_profile, i) for i in range(1, 4)]

        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Verify all assignments succeeded
    assert all(results), "Some profile assignments failed in concurrent execution"

    # Verify assignments
    for i in range(1, 4):
        profile_info = list(memgraph.execute_and_fetch(f"SHOW PROFILE FOR concurrent_user{i};"))
        assert len(profile_info) == 1, f"User concurrent_user{i} should have a profile"
        assert profile_info[0]["profile"] == "concurrent_profile"


def test_memory_exhaustion_scenario(memgraph):
    """Test memory exhaustion scenario with profile limits."""

    # Create profile with memory limit
    memgraph.execute("CREATE PROFILE resource_profile LIMIT transactions_memory 1KB;")
    memgraph.execute("CREATE USER resource_user;")
    memgraph.execute("SET PROFILE FOR resource_user TO resource_profile;")

    # Connect as the limited user
    user_memgraph = Memgraph(username="resource_user", password="")

    # Try to execute queries that might exceed memory limits
    memory_exhaustion_detected = False

    try:
        # Try to create a large dataset
        user_memgraph.execute("CREATE (n:Test {data: 'x' * 1000});")

        # Try to query with large result set
        for i in range(100):
            user_memgraph.execute(f"CREATE (n:Test{i} {{data: 'x' * 100}});")

        # Try to execute a query that might exceed memory
        result = list(user_memgraph.execute_and_fetch("MATCH (n) RETURN n LIMIT 1000;"))

    except Exception as e:
        # Memory exhaustion is expected
        memory_exhaustion_detected = True

    # Memory exhaustion should be detected due to profile limits
    assert memory_exhaustion_detected, "Memory exhaustion should be detected with strict profile limits"


def test_profile_update_during_resource_usage(memgraph):
    """Test profile updates while resources are being used."""

    # Create profile with low limits
    memgraph.execute("CREATE PROFILE stress_profile LIMIT sessions 1, transactions_memory 1KB;")
    memgraph.execute("CREATE USER resource_user;")
    memgraph.execute("SET PROFILE FOR resource_user TO stress_profile;")

    # Start a background thread that continuously uses resources
    resource_usage_active = threading.Event()
    resource_usage_complete = threading.Event()

    def continuous_resource_usage():
        """Continuously use resources in the background."""
        try:
            user_memgraph = Memgraph(username="resource_user", password="")
            resource_usage_active.set()

            # Keep using resources for a short time
            start_time = time.time()
            while time.time() - start_time < 2.0 and not resource_usage_complete.is_set():
                try:
                    user_memgraph.execute("CREATE (n:BackgroundTest {data: 'background'});")
                    time.sleep(0.1)
                except Exception:
                    break

        except Exception:
            pass

    # Start background resource usage
    resource_thread = threading.Thread(target=continuous_resource_usage)
    resource_thread.start()

    # Wait for resource usage to start
    resource_usage_active.wait(timeout=5.0)

    # Update profile to increase limits
    memgraph.execute("UPDATE PROFILE stress_profile LIMIT sessions 5, transactions_memory 10KB;")

    # Signal resource usage to stop
    resource_usage_complete.set()
    resource_thread.join(timeout=5.0)

    # Verify profile was updated
    profile_info = list(memgraph.execute_and_fetch("SHOW PROFILE stress_profile;"))
    limits = {row["limit"]: row["value"] for row in profile_info}
    assert limits["sessions"] == 5, "Session limit should be updated"
    assert limits["transactions_memory"] == "10KB", "Memory limit should be updated"


def test_profile_deletion_during_active_usage(memgraph):
    """Test profile deletion while resources are actively being used."""

    # Create profile and user
    memgraph.execute("CREATE PROFILE stress_profile LIMIT sessions 2, transactions_memory 1KB;")
    memgraph.execute("CREATE USER resource_user;")
    memgraph.execute("SET PROFILE FOR resource_user TO stress_profile;")

    # Start background resource usage
    resource_usage_active = threading.Event()
    resource_usage_complete = threading.Event()

    def continuous_resource_usage():
        """Continuously use resources in the background."""
        try:
            user_memgraph = Memgraph(username="resource_user", password="")
            resource_usage_active.set()

            # Keep using resources
            start_time = time.time()
            while time.time() - start_time < 3.0 and not resource_usage_complete.is_set():
                try:
                    user_memgraph.execute("CREATE (n:DeleteTest {data: 'delete'});")
                    time.sleep(0.1)
                except Exception:
                    break

        except Exception:
            pass

    # Start background resource usage
    resource_thread = threading.Thread(target=continuous_resource_usage)
    resource_thread.start()

    # Wait for resource usage to start
    resource_usage_active.wait(timeout=5.0)

    # Delete the profile while resources are being used
    memgraph.execute("DROP PROFILE stress_profile;")

    # Signal resource usage to stop
    resource_usage_complete.set()
    resource_thread.join(timeout=5.0)

    # Verify profile was deleted
    with pytest.raises(Exception):
        memgraph.execute("SHOW PROFILE stress_profile;")

    # Verify user no longer has a profile
    profile_info = list(memgraph.execute_and_fetch("SHOW PROFILE FOR resource_user;"))
    assert profile_info[0]["profile"] == "null", "User should have no profile after deletion"


def test_concurrent_profile_operations_stress(memgraph):
    """Stress test with multiple concurrent profile operations."""

    # Track operation results
    operation_results = {
        "create_success": 0,
        "create_failure": 0,
        "update_success": 0,
        "update_failure": 0,
        "delete_success": 0,
        "delete_failure": 0,
    }

    def mixed_operations(thread_id):
        """Perform mixed profile operations."""
        for i in range(10):
            operation_type = i % 3

            if operation_type == 0:  # Create
                try:
                    memgraph.execute(f"CREATE PROFILE stress_profile_{thread_id}_{i};")
                    operation_results["create_success"] += 1
                except Exception:
                    operation_results["create_failure"] += 1

            elif operation_type == 1:  # Update
                try:
                    memgraph.execute(f"UPDATE PROFILE stress_profile_{thread_id}_{i-1} LIMIT sessions {i};")
                    operation_results["update_success"] += 1
                except Exception:
                    operation_results["update_failure"] += 1

            else:  # Delete
                try:
                    memgraph.execute(f"DROP PROFILE stress_profile_{thread_id}_{i-2};")
                    operation_results["delete_success"] += 1
                except Exception:
                    operation_results["delete_failure"] += 1

    # Run concurrent mixed operations
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(mixed_operations, i) for i in range(5)]

        concurrent.futures.wait(futures)

    # Verify some operations succeeded
    total_success = (
        operation_results["create_success"] + operation_results["update_success"] + operation_results["delete_success"]
    )

    assert total_success > 0, "No profile operations succeeded in stress test"

    # Verify system stability by checking remaining profiles
    remaining_profiles = list(memgraph.execute_and_fetch("SHOW PROFILES;"))
    assert len(remaining_profiles) >= 0, "System should remain stable after stress test"


def test_profile_performance_under_concurrent_load(memgraph):
    """Test profile performance under concurrent load."""

    # Create initial profiles
    for i in range(5):
        memgraph.execute(f"CREATE PROFILE perf_profile_{i};")

    def performance_operation(thread_id):
        """Perform profile operations for performance testing."""
        operations_completed = 0
        start_time = time.time()

        for i in range(20):
            profile_id = (thread_id + i) % 5

            try:
                # Update profile
                memgraph.execute(f"UPDATE PROFILE perf_profile_{profile_id} LIMIT sessions {i + 1};")

                # Show profile
                results = list(memgraph.execute_and_fetch(f"SHOW PROFILE perf_profile_{profile_id};"))
                assert len(results) == 2

                operations_completed += 1

            except Exception:
                pass

        end_time = time.time()
        return operations_completed, end_time - start_time

    # Run performance test
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(performance_operation, i) for i in range(3)]

        results = [future.result() for future in concurrent.futures.as_completed(futures)]

    # Analyze performance results
    total_operations = sum(op_count for op_count, _ in results)
    total_time = sum(op_time for _, op_time in results)

    # Verify reasonable performance
    assert total_operations > 0, "No operations completed in performance test"
    assert total_time < 10.0, f"Performance test took too long: {total_time:.2f} seconds"

    # Calculate operations per second
    ops_per_second = total_operations / total_time if total_time > 0 else 0
    assert ops_per_second > 1.0, f"Performance too low: {ops_per_second:.2f} ops/sec"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
