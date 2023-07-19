import itertools
from typing import List, Tuple


class StorageModeConstants:
    IN_MEMORY_TRANSACTIONAL = "IN_MEMORY_TRANSACTIONAL"
    IN_MEMORY_ANALYTICAL = "IN_MEMORY_ANALYTICAL"
    ON_DISK_TRANSACTIONAL = "ON_DISK_TRANSACTIONAL"

    @classmethod
    def to_list(cls) -> List[str]:
        return [cls.IN_MEMORY_TRANSACTIONAL, cls.IN_MEMORY_ANALYTICAL, cls.ON_DISK_TRANSACTIONAL]


class IsolationModeConstants:
    READ_UNCOMMITED = "READ_UNCOMMITED"
    READ_COMMITED = "READ_COMMITED"
    SNAPSHOT_SERIALIZATION = "SNAPSHOT_SERIALIZATION"

    @classmethod
    def to_list(cls) -> List[str]:
        return [cls.SNAPSHOT_SERIALIZATION, cls.READ_COMMITED, cls.READ_UNCOMMITED]


def get_default_database_mode() -> Tuple[str, str]:
    return (StorageModeConstants.IN_MEMORY_TRANSACTIONAL, IsolationModeConstants.SNAPSHOT_SERIALIZATION)


def get_all_database_modes() -> List[Tuple[str, str]]:
    return list(itertools.product(StorageModeConstants.to_list(), IsolationModeConstants.to_list()))


# dataset calibrated for running on Apollo (total 4min)
# bipartite.py runs for approx. 30s
# create_match.py runs for approx. 30s
# long_running runs for 1min
# long_running runs for 2min
SMALL_DATASET = [
    {
        "test": "bipartite.py",
        "options": ["--u-count", "100", "--v-count", "100"],
        "timeout": 5,
        "mode": [get_default_database_mode()],
    },
    {
        "test": "create_match.py",
        "options": ["--vertex-count", "40000", "--create-pack-size", "100"],
        "timeout": 5,
        "mode": [get_default_database_mode()],
    },
    {
        "test": "parser.cpp",
        "options": ["--per-worker-query-count", "1000"],
        "timeout": 5,
        "mode": [get_default_database_mode()],
    },
    {
        "test": "long_running.cpp",
        "options": ["--vertex-count", "1000", "--edge-count", "5000", "--max-time", "1", "--verify", "20"],
        "timeout": 5,
        "mode": [get_default_database_mode()],
    },
    {
        "test": "long_running.cpp",
        "options": ["--vertex-count", "10000", "--edge-count", "50000", "--max-time", "2", "--verify", "30"],
        "timeout": 5,
        "mode": [get_default_database_mode()],
    },
]

# dataset calibrated for running on daily stress instance (total 9h)
# bipartite.py and create_match.py run for approx. 15min
# long_running runs for 5min x 6 times = 30min
# long_running runs for 8h
LARGE_DATASET = (
    [
        {
            "test": "bipartite.py",
            "options": ["--u-count", "300", "--v-count", "300"],
            "timeout": 30,
            "mode": [get_default_database_mode()],
        },
        {
            "test": "create_match.py",
            "options": ["--vertex-count", "500000", "--create-pack-size", "500"],
            "timeout": 30,
            "mode": [get_default_database_mode()],
        },
    ]
    + [
        {
            "test": "long_running.cpp",
            "options": ["--vertex-count", "10000", "--edge-count", "40000", "--max-time", "5", "--verify", "60"],
            "timeout": 16,
            "mode": [get_default_database_mode()],
        },
    ]
    * 6
    + [
        {
            "test": "long_running.cpp",
            "options": ["--vertex-count", "200000", "--edge-count", "1000000", "--max-time", "480", "--verify", "300"],
            "timeout": 500,
            "mode": [get_default_database_mode()],
        },
    ]
)
