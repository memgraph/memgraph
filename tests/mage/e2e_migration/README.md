# E2E Migration Testing Framework

End-to-end tests for migrating data from MySQL and PostgreSQL to Memgraph using the `migrate` module.

## What it does

- Tests data migration from MySQL and PostgreSQL to Memgraph
- Validates data type conversions and error handling
- Uses per-data-type test approach for granular coverage
- Automatically sets up database containers and test data

## Running Tests

### Prerequisites
- Docker and Docker Compose
- Python with PyTest, PyYAML

### Run Options

**All tests (recommended):**
```bash
./test_e2e_migration.sh
```

**Run specific tests, i.e. MySQL tests only:**
```bash
./test_e2e_migration.sh -k mysql
```

**Using pytest directly:**
```bash
python3 -m pytest e2e_migration/ -v
```

**Show help:**
```bash
./test_e2e_migration.sh --help
```
