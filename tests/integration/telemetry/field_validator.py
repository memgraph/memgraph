#!/usr/bin/python3 -u

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

"""
Field validation logic for telemetry data.
This module contains comprehensive validation functions for all telemetry field types.
"""


def validate_telemetry(item: dict) -> bool:
    """
    Simple check to see that we have the required fields.
    These are the bare minimum.
    """
    required_fields = [
        "data",
        "event",
        "machine_id",
        "run_id",
        "timestamp",
        "type",  # used since version 2.4.2. Not strictly required by the API, but we should keep it.
    ]

    for field in required_fields:
        if field not in item:
            print(f"Missing '{field}' field in item {item}")
            return False

    return True


def verify_execution_fields(item: dict) -> bool:
    """
    A more thorough check of the execution type of telemetry data
    (i.e. "event" = "startup")
    """
    # Verify required fields
    root_fields = [
        ("data", dict),
        ("event", (int, str)),
        ("machine_id", str),
        ("run_id", str),
        ("timestamp", float),
        ("type", str),
        ("ssl", bool),
        ("version", str),
    ]

    data_fields = [
        ("microarch_level", int),
        ("version", str),
        ("architecture", str),
        ("cpu_count", int),
        ("cpu_model", str),
        ("kernel", str),
        ("memory", int),
        ("os", str),
        ("swap", int),
    ]

    for field, expected_type in root_fields:
        assert field in item, f"Missing '{field}' field in item {item}"
        assert isinstance(
            item[field], expected_type
        ), f"Invalid type for '{field}': expected {expected_type}, got {type(item[field])}"

    for field, expected_type in data_fields:
        assert field in item["data"], f"Missing '{field}' field in item {item}"
        assert isinstance(
            item["data"][field], expected_type
        ), f"Invalid type for '{field}': expected {expected_type}, got {type(item['data'][field])}"

    return True


def _verify_database_fields(database_items: list[dict]) -> bool:
    """
    Verify the database fields of the telemetry data
    """
    database_fields = [
        ("edges", int),
        ("vertices", int),
        ("memory", int),
        ("disk", int),
        ("label_indices", int),
        ("label_prop_indices", int),
        ("text_indices", int),
        ("vector_indices", int),
        ("vector_edge_indices", int),
        ("existence_constraints", int),
        ("unique_constraints", int),
        ("storage_mode", str),
        ("isolation_level", str),
        ("durability", dict),
        ("property_store_compression_enabled", bool),
        ("property_store_compression_level", str),
        ("schema_vertex_count", int),
        ("schema_edge_count", int),
    ]

    durability_fields = [
        ("snapshot_enabled", bool),
        ("WAL_enabled", bool),
    ]

    valid_storage_modes = {"IN_MEMORY_TRANSACTIONAL", "IN_MEMORY_ANALYTICAL", "ON_DISK_TRANSACTIONAL"}

    valid_isolation_levels = {"SNAPSHOT_ISOLATION", "READ_COMMITTED", "READ_UNCOMMITTED"}

    valid_compression_levels = {"low", "mid", "high"}

    for db_item in database_items:
        # Verify database fields
        for field, expected_type in database_fields:
            assert field in db_item, f"Missing '{field}' field in database item {db_item}"
            assert isinstance(
                db_item[field], expected_type
            ), f"Invalid type for '{field}': expected {expected_type}, got {type(db_item[field])}"

        # Verify durability sub-object
        for field, expected_type in durability_fields:
            assert field in db_item["durability"], f"Missing '{field}' field in durability object"
            assert isinstance(
                db_item["durability"][field], expected_type
            ), f"Invalid type for durability.{field}: expected {expected_type}, got {type(db_item['durability'][field])}"

        # Verify enum values
        assert db_item["storage_mode"] in valid_storage_modes, f"Invalid storage_mode: {db_item['storage_mode']}"
        assert (
            db_item["isolation_level"] in valid_isolation_levels
        ), f"Invalid isolation_level: {db_item['isolation_level']}"
        assert (
            db_item["property_store_compression_level"] in valid_compression_levels
        ), f"Invalid compression_level: {db_item['property_store_compression_level']}"

    return True


def _verify_client_fields(client_items: list[dict]) -> bool:
    """
    Verify the client fields of the telemetry data
    """
    client_fields = [
        ("name", str),  # Driver name
        ("supported_bolt_versions", list),  # Array of supported bolt versions
        ("bolt_version", str),  # Bolt version used
        ("connection_types", dict),  # Authentication types used
        ("sessions", int),  # Number of sessions using the same driver
        ("queries", int),  # Queries executed by the driver
    ]

    connection_type_fields = [
        ("anonymous", int),  # Anonymous connections count
        ("basic", int),  # Basic auth connections count
    ]

    for client_item in client_items:
        # Verify client fields
        for field, expected_type in client_fields:
            assert field in client_item, f"Missing '{field}' field in client item {client_item}"
            assert isinstance(
                client_item[field], expected_type
            ), f"Invalid type for '{field}': expected {expected_type}, got {type(client_item[field])}"

        # Verify supported_bolt_versions is array of strings
        for version in client_item["supported_bolt_versions"]:
            assert isinstance(
                version, str
            ), f"Invalid type in supported_bolt_versions: expected str, got {type(version)}"

        # Verify connection_types sub-object
        for field, expected_type in connection_type_fields:
            assert field in client_item["connection_types"], f"Missing '{field}' field in connection_types object"
            assert isinstance(
                client_item["connection_types"][field], expected_type
            ), f"Invalid type for connection_types.{field}: expected {expected_type}, got {type(client_item['connection_types'][field])}"

    return True


def _verify_event_counters_fields(event_counters: dict) -> bool:
    """
    Verify the event_counters fields of the telemetry data
    Only checks that all keys are strings and all values are integers, doesn't validate specific keys
    """
    for key, value in event_counters.items():
        assert isinstance(key, str), f"Event counter key must be string, got {type(key)}"
        assert isinstance(value, int), f"Event counter value must be int, got {type(value)} for key '{key}'"

    return True


def _verify_exception_fields(exception_items: list[dict]) -> bool:
    """
    Verify the exception fields of the telemetry data
    """
    exception_fields = [
        ("name", str),  # Exception name/type
        ("count", int),  # Count of occurrences
    ]

    for exception_item in exception_items:
        # Verify exception fields
        for field, expected_type in exception_fields:
            assert field in exception_item, f"Missing '{field}' field in exception item {exception_item}"
            assert isinstance(
                exception_item[field], expected_type
            ), f"Invalid type for '{field}': expected {expected_type}, got {type(exception_item[field])}"

    return True


def _verify_query_fields(query_data: dict) -> bool:
    """
    Verify the query fields of the telemetry data
    """
    query_fields = [
        ("first_successful_query", float),  # Timestamp of first successful query (with nanoseconds)
        ("first_failed_query", float),  # Timestamp of first failed query (with nanoseconds)
    ]

    for field, expected_type in query_fields:
        assert field in query_data, f"Missing '{field}' field in query data {query_data}"
        assert isinstance(
            query_data[field], expected_type
        ), f"Invalid type for '{field}': expected {expected_type}, got {type(query_data[field])}"

    return True


def _verify_query_module_counters_fields(query_module_counters: dict) -> bool:
    """
    Verify the query_module_counters fields of the telemetry data
    Only checks that all keys are strings and all values are integers, doesn't validate specific keys
    """
    for key, value in query_module_counters.items():
        assert isinstance(key, str), f"Query module counter key must be string, got {type(key)}"
        assert isinstance(value, int), f"Query module counter value must be int, got {type(value)} for key '{key}'"

    return True


def _verify_replication_fields(replication_data: dict) -> bool:
    """
    Verify the replication fields of the telemetry data
    """
    replication_fields = [
        ("async", int),  # Async replication count/status (currently -1 as placeholder)
        ("sync", int),  # Sync replication count/status (currently -1 as placeholder)
    ]

    for field, expected_type in replication_fields:
        assert field in replication_data, f"Missing '{field}' field in replication data {replication_data}"
        assert isinstance(
            replication_data[field], expected_type
        ), f"Invalid type for '{field}': expected {expected_type}, got {type(replication_data[field])}"

    return True


def _verify_resources_fields(resources_data: dict) -> bool:
    """
    Verify the resources fields of the telemetry data
    """
    resources_fields = [
        ("cpu", dict),  # CPU usage information
        ("memory", int),  # Memory usage in bytes
        ("disk", int),  # Disk usage in bytes
        ("vm_max_map_count", int),  # VM max map count
    ]

    cpu_fields = [
        ("threads", list),  # Array of thread CPU usage
        ("usage", float),  # Total CPU usage
    ]

    thread_fields = [
        ("name", str),  # Thread name
        ("usage", float),  # Thread CPU usage
    ]

    for field, expected_type in resources_fields:
        assert field in resources_data, f"Missing '{field}' field in resources data {resources_data}"
        assert isinstance(
            resources_data[field], expected_type
        ), f"Invalid type for '{field}': expected {expected_type}, got {type(resources_data[field])}"

    # Verify CPU sub-object
    if "cpu" in resources_data:
        for field, expected_type in cpu_fields:
            assert field in resources_data["cpu"], f"Missing '{field}' field in cpu object"
            assert isinstance(
                resources_data["cpu"][field], expected_type
            ), f"Invalid type for cpu.{field}: expected {expected_type}, got {type(resources_data['cpu'][field])}"

        # Verify threads array
        for thread in resources_data["cpu"]["threads"]:
            for field, expected_type in thread_fields:
                assert field in thread, f"Missing '{field}' field in thread object"
                assert isinstance(
                    thread[field], expected_type
                ), f"Invalid type for thread.{field}: expected {expected_type}, got {type(thread[field])}"

    return True


def _verify_storage_fields(storage_data: dict) -> bool:
    """
    Verify the storage fields of the telemetry data
    """
    storage_fields = [
        ("edges", int),  # Sum of edges in every database
        ("vertices", int),  # Sum of vertices in every database
        ("triggers", int),  # Sum of triggers in every database
        ("streams", int),  # Sum of streams in every database
        ("users", int),  # Number of defined users
        ("roles", int),  # Number of defined roles
        ("labels", int),  # Number of distinct labels
        ("edge_types", int),  # Number of distinct edge types
        ("databases", int),  # Number of isolated databases
        ("indices", int),  # Sum of indices in every database
        ("constraints", int),  # Sum of constraints in every database
        ("storage_modes", dict),  # Number of databases in each storage mode
        ("isolation_levels", dict),  # Number of databases in each isolation level
        ("durability", dict),  # Durability settings
        ("property_store_compression_enabled", int),  # Number of databases with compression enabled
        ("property_store_compression_level", dict),  # Number of databases with each compression level
    ]

    durability_fields = [
        ("snapshot_enabled", int),  # Number of databases with snapshots enabled
        ("WAL_enabled", int),  # Number of databases with WAL enabled
    ]

    valid_storage_modes = {"IN_MEMORY_TRANSACTIONAL", "IN_MEMORY_ANALYTICAL", "ON_DISK_TRANSACTIONAL"}

    valid_isolation_levels = {"SNAPSHOT_ISOLATION", "READ_COMMITTED", "READ_UNCOMMITTED"}

    valid_compression_levels = {"low", "mid", "high"}

    for field, expected_type in storage_fields:
        assert field in storage_data, f"Missing '{field}' field in storage data {storage_data}"
        assert isinstance(
            storage_data[field], expected_type
        ), f"Invalid type for '{field}': expected {expected_type}, got {type(storage_data[field])}"

    # Verify durability sub-object
    for field, expected_type in durability_fields:
        assert field in storage_data["durability"], f"Missing '{field}' field in durability object"
        assert isinstance(
            storage_data["durability"][field], expected_type
        ), f"Invalid type for durability.{field}: expected {expected_type}, got {type(storage_data['durability'][field])}"

    # Verify storage_modes enum values
    for mode, count in storage_data["storage_modes"].items():
        assert mode in valid_storage_modes, f"Invalid storage_mode: {mode}"
        assert isinstance(count, int), f"Invalid type for storage_modes[{mode}]: expected int, got {type(count)}"

    # Verify isolation_levels enum values
    for level, count in storage_data["isolation_levels"].items():
        assert level in valid_isolation_levels, f"Invalid isolation_level: {level}"
        assert isinstance(count, int), f"Invalid type for isolation_levels[{level}]: expected int, got {type(count)}"

    # Verify property_store_compression_level enum values
    for level, count in storage_data["property_store_compression_level"].items():
        assert level in valid_compression_levels, f"Invalid compression_level: {level}"
        assert isinstance(
            count, int
        ), f"Invalid type for property_store_compression_level[{level}]: expected int, got {type(count)}"

    return True


def verify_measurement_fields(item: dict) -> bool:
    """
    A more thorough check of the measurement type of telemetry data
    (i.e. "event" != "startup")
    """
    root_fields = [
        ("data", dict),
        ("event", (int, str)),
        ("machine_id", str),
        ("run_id", str),
        ("timestamp", float),
        ("type", str),
        ("ssl", bool),
        ("version", str),
    ]

    data_fields = [
        ("database", list),  # Changed from dict to list - database is an array of database objects
        ("client", list),
        ("exception", list),
        ("query", dict),
        ("query_module_counters", dict),
        ("replication", dict),
        ("resources", dict),
        ("storage", dict),
        ("uptime", float),
    ]

    for field, expected_type in root_fields:
        assert field in item, f"Missing '{field}' field in item {item}"
        assert isinstance(
            item[field], expected_type
        ), f"Invalid type for '{field}': expected {expected_type}, got {type(item[field])}"

    for field, expected_type in data_fields:
        assert field in item["data"], f"Missing '{field}' field in item {item}"
        assert isinstance(
            item["data"][field], expected_type
        ), f"Invalid type for '{field}': expected {expected_type}, got {type(item['data'][field])}"

    # Verify database fields specifically
    if "database" in item["data"] and item["data"]["database"]:
        _verify_database_fields(item["data"]["database"])

    # Verify client fields specifically
    if "client" in item["data"] and item["data"]["client"]:
        _verify_client_fields(item["data"]["client"])

    # Verify event_counters fields specifically
    if "event_counters" in item["data"] and item["data"]["event_counters"]:
        _verify_event_counters_fields(item["data"]["event_counters"])

    # Verify exception fields specifically
    if "exception" in item["data"] and item["data"]["exception"]:
        _verify_exception_fields(item["data"]["exception"])

    # Verify query fields specifically
    if "query" in item["data"] and item["data"]["query"]:
        _verify_query_fields(item["data"]["query"])

    # Verify query_module_counters fields specifically
    if "query_module_counters" in item["data"] and item["data"]["query_module_counters"]:
        _verify_query_module_counters_fields(item["data"]["query_module_counters"])

    # Verify replication fields specifically
    if "replication" in item["data"] and item["data"]["replication"]:
        _verify_replication_fields(item["data"]["replication"])

    # Verify resources fields specifically
    if "resources" in item["data"] and item["data"]["resources"]:
        _verify_resources_fields(item["data"]["resources"])

    # Verify storage fields specifically
    if "storage" in item["data"] and item["data"]["storage"]:
        _verify_storage_fields(item["data"]["storage"])

    return True


def verify_telemetry_item(item: dict) -> bool:
    """
    Verify the telemetry item
    """
    if item["event"] == "startup":
        return verify_execution_fields(item)
    else:
        return verify_measurement_fields(item)
