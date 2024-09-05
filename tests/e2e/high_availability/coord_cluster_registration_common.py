# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import tempfile

TEMP_DIR = tempfile.TemporaryDirectory().name


def get_instances_description(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/instance_1.log",
            "data_directory": f"{TEMP_DIR}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/instance_2.log",
            "data_directory": f"{TEMP_DIR}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/instance_3.log",
            "data_directory": f"{TEMP_DIR}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/coordinator1.log",
            "data_directory": f"{TEMP_DIR}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--coordinator-hostname=localhost",
                "--management-port=10122",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/coordinator2.log",
            "data_directory": f"{TEMP_DIR}/coordinator_2",
            "setup_queries": [],
        },
        "coordinator_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7692",
                "--log-level=TRACE",
                "--coordinator-id=3",
                "--coordinator-port=10113",
                "--coordinator-hostname=localhost",
                "--management-port=10123",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/coordinator3.log",
            "data_directory": f"{TEMP_DIR}/coordinator_3",
            "setup_queries": [],
        },
    }


def get_instances_description_no_coord(test_name: str):
    return {
        "instance_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7687",
                "--log-level",
                "TRACE",
                "--management-port",
                "10011",
                "--data-recovery-on-startup=false",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/instance_1.log",
            "data_directory": f"{TEMP_DIR}/instance_1",
            "setup_queries": [],
        },
        "instance_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7688",
                "--log-level",
                "TRACE",
                "--management-port",
                "10012",
                "--data-recovery-on-startup=false",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/instance_2.log",
            "data_directory": f"{TEMP_DIR}/instance_2",
            "setup_queries": [],
        },
        "instance_3": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7689",
                "--log-level",
                "TRACE",
                "--management-port",
                "10013",
                "--data-recovery-on-startup=false",
                "--replication-restore-state-on-startup=true",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/instance_3.log",
            "data_directory": f"{TEMP_DIR}/instance_3",
            "setup_queries": [],
        },
        "coordinator_1": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7690",
                "--log-level=TRACE",
                "--coordinator-id=1",
                "--coordinator-port=10111",
                "--coordinator-hostname=localhost",
                "--management-port=10121",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/coordinator1.log",
            "data_directory": f"{TEMP_DIR}/coordinator_1",
            "setup_queries": [],
        },
        "coordinator_2": {
            "args": [
                "--experimental-enabled=high-availability",
                "--bolt-port",
                "7691",
                "--log-level=TRACE",
                "--coordinator-id=2",
                "--coordinator-port=10112",
                "--coordinator-hostname=localhost",
                "--management-port=10122",
            ],
            "log_file": f"high_availability/coord_cluster_registration/{test_name}/coordinator2.log",
            "data_directory": f"{TEMP_DIR}/coordinator_2",
            "setup_queries": [],
        },
    }
