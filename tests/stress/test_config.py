import itertools
import os
from dataclasses import dataclass
from typing import List

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
STATS_FILE = os.path.join(SCRIPT_DIR, ".long_running_stats")


class DatasetConstants:
    TEST = "test"
    OPTIONS = "options"
    TIMEOUT = "timeout"
    MODE = "mode"
    MEMGRAPH_OPTIONS = "memgraph_options"


@dataclass
class DatabaseMode:
    storage_mode: str
    isolation_level: str


class StorageModeConstants:
    IN_MEMORY_TRANSACTIONAL = "IN_MEMORY_TRANSACTIONAL"
    IN_MEMORY_ANALYTICAL = "IN_MEMORY_ANALYTICAL"

    @classmethod
    def to_list(cls) -> List[str]:
        return [cls.IN_MEMORY_TRANSACTIONAL, cls.IN_MEMORY_ANALYTICAL]


class IsolationLevelConstants:
    SNAPSHOT_ISOLATION = "SNAPSHOT ISOLATION"
    READ_COMMITED = "READ COMMITED"
    READ_UNCOMMITED = "READ UNCOMMITED"

    @classmethod
    def to_list(cls) -> List[str]:
        return [cls.SNAPSHOT_SERIALIZATION, cls.READ_COMMITED, cls.READ_UNCOMMITED]


def get_default_database_mode() -> DatabaseMode:
    return DatabaseMode(StorageModeConstants.IN_MEMORY_TRANSACTIONAL, IsolationLevelConstants.SNAPSHOT_ISOLATION)


def get_all_database_modes() -> List[DatabaseMode]:
    return [
        DatabaseMode(x[0], x[1])
        for x in itertools.product(StorageModeConstants.to_list(), IsolationLevelConstants.to_list())
    ]


# dataset calibrated for running on Apollo (total 4min)
# bipartite.py runs for approx. 30s
# create_match.py runs for approx. 30s
# long_running runs for 1min
# long_running runs for 2min
SMALL_DATASET = [
    {
        DatasetConstants.TEST: "bipartite.py",
        DatasetConstants.OPTIONS: ["--u-count", "100", "--v-count", "100"],
        DatasetConstants.TIMEOUT: 5,
        DatasetConstants.MODE: [get_default_database_mode()],
    },
    {
        DatasetConstants.TEST: "detach_delete.py",
        DatasetConstants.OPTIONS: ["--worker-count", "4", "--repetition-count", "100"],
        DatasetConstants.TIMEOUT: 5,
        DatasetConstants.MODE: [get_default_database_mode()],
    },
    {
        DatasetConstants.TEST: "memory_tracker.py",
        DatasetConstants.OPTIONS: ["--worker-count", "5", "--repetition-count", "100"],
        DatasetConstants.TIMEOUT: 5,
        DatasetConstants.MODE: [get_default_database_mode()],
        DatasetConstants.MEMGRAPH_OPTIONS: ["--memory-limit=2048"],
    },
    {
        DatasetConstants.TEST: "memory_limit.py",
        DatasetConstants.OPTIONS: ["--worker-count", "5", "--repetition-count", "100"],
        DatasetConstants.TIMEOUT: 5,
        DatasetConstants.MODE: [get_default_database_mode()],
        DatasetConstants.MEMGRAPH_OPTIONS: ["--memory-limit=2048"],
    },
    {
        DatasetConstants.TEST: "create_match.py",
        DatasetConstants.OPTIONS: ["--vertex-count", "40000", "--create-pack-size", "100"],
        DatasetConstants.TIMEOUT: 5,
        DatasetConstants.MODE: [get_default_database_mode()],
    },
    {
        DatasetConstants.TEST: "parser.cpp",
        DatasetConstants.OPTIONS: ["--per-worker-query-count", "1000"],
        DatasetConstants.TIMEOUT: 5,
        DatasetConstants.MODE: [get_default_database_mode()],
    },
    {
        DatasetConstants.TEST: "long_running.cpp",
        DatasetConstants.OPTIONS: [
            "--vertex-count",
            "1000",
            "--edge-count",
            "5000",
            "--max-time",
            "1",
            "--verify",
            "20",
        ],
        DatasetConstants.TIMEOUT: 5,
        DatasetConstants.MODE: [get_default_database_mode()],
    },
    {
        DatasetConstants.TEST: "long_running.cpp",
        DatasetConstants.OPTIONS: [
            "--vertex-count",
            "10000",
            "--edge-count",
            "50000",
            "--max-time",
            "2",
            "--verify",
            "30",
            "--stats-file",
            STATS_FILE,
        ],
        DatasetConstants.TIMEOUT: 5,
        DatasetConstants.MODE: [get_default_database_mode()],
    },
]

# dataset calibrated for running on daily stress instance (total 9h)
# bipartite.py and create_match.py run for approx. 15min
# long_running runs for 5min x 6 times = 30min
# long_running runs for 8h
LARGE_DATASET = (
    [
        {
            DatasetConstants.TEST: "long_running.cpp",
            DatasetConstants.OPTIONS: [
                "--vertex-count",
                "200000",
                "--edge-count",
                "1000000",
                "--max-time",
                "480",
                "--verify",
                "300",
                "--stats-file",
                STATS_FILE,
            ],
            DatasetConstants.TIMEOUT: 500,
            DatasetConstants.MODE: [get_default_database_mode()],
        },
    ]
    + [
        {
            DatasetConstants.TEST: "bipartite.py",
            DatasetConstants.OPTIONS: ["--u-count", "300", "--v-count", "300"],
            DatasetConstants.TIMEOUT: 30,
            DatasetConstants.MODE: [get_default_database_mode()],
        },
        {
            DatasetConstants.TEST: "detach_delete.py",
            DatasetConstants.OPTIONS: ["--worker-count", "4", "--repetition-count", "300"],
            DatasetConstants.TIMEOUT: 5,
            DatasetConstants.MODE: [get_default_database_mode()],
        },
        {
            DatasetConstants.TEST: "create_match.py",
            DatasetConstants.OPTIONS: ["--vertex-count", "500000", "--create-pack-size", "500"],
            DatasetConstants.TIMEOUT: 30,
            DatasetConstants.MODE: [get_default_database_mode()],
        },
    ]
    + [
        {
            DatasetConstants.TEST: "long_running.cpp",
            DatasetConstants.OPTIONS: [
                "--vertex-count",
                "10000",
                "--edge-count",
                "40000",
                "--max-time",
                "5",
                "--verify",
                "60",
            ],
            DatasetConstants.TIMEOUT: 16,
            DatasetConstants.MODE: [get_default_database_mode()],
        },
    ]
    * 6
)
