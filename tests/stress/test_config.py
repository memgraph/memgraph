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
    },
    {
        "test": "create_match.py",
        "options": ["--vertex-count", "40000", "--create-pack-size", "100"],
        "timeout": 5,
    },
    {
        "test": "parser.cpp",
        "options": ["--per-worker-query-count", "1000"],
        "timeout": 5,
    },
    {
        "test": "long_running.cpp",
        "options": ["--vertex-count", "1000", "--edge-count", "5000", "--max-time", "1", "--verify", "20"],
        "timeout": 5,
    },
    {
        "test": "long_running.cpp",
        "options": ["--vertex-count", "10000", "--edge-count", "50000", "--max-time", "2", "--verify", "30"],
        "timeout": 5,
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
        },
        {
            "test": "create_match.py",
            "options": ["--vertex-count", "500000", "--create-pack-size", "500"],
            "timeout": 30,
        },
    ]
    + [
        {
            "test": "long_running.cpp",
            "options": ["--vertex-count", "10000", "--edge-count", "40000", "--max-time", "5", "--verify", "60"],
            "timeout": 16,
        },
    ]
    * 6
    + [
        {
            "test": "long_running.cpp",
            "options": ["--vertex-count", "200000", "--edge-count", "1000000", "--max-time", "480", "--verify", "300"],
            "timeout": 500,
        },
    ]
)
