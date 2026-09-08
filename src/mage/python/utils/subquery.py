QUERY_PATTERNS = [
    "CREATE INDEX ON",
    "DROP INDEX ON",
    "CREATE CONSTRAINT ON",
    "DROP CONSTRAINT ON",
    "SET GLOBAL TRANSACTION ISOLATION LEVEL",
    "STORAGE MODE IN_MEMORY_TRANSACTIONAL",
    "STORAGE MODE IN_MEMORY_ANALYTICAL",
]


def is_global_operation(subquery: str) -> bool:
    return any(subquery.upper().startswith(pattern) for pattern in QUERY_PATTERNS)
