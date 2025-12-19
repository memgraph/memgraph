# Stage 4 Summary: RNG Investigation

## getrandom Sources (17 events)

### Event Timing Distribution
```
time=7,754      - Run ID generation (startup)
time=3,786,693  - Python hash initialization
time=7,634,695  - Main function UUID
time=8,321,981  - Auth User UUID
time=8,944,018  - Replication State UUID
time=9,722,444+ - Additional UUIDs during init
```

### Source Categories

#### 1. UUID Generation (via libuuid)
Most getrandom calls come from `uuid_generate()`:

| Location | Purpose |
|----------|---------|
| `src/glue/run_id.cpp:15` | Global run ID (startup) |
| `src/memgraph.cpp:394` | Main function UUID |
| `src/auth/models.hpp:869` | Auth User creation |
| `src/replication/state.cpp:36` | Replication state UUID |
| `src/utils/uuid.hpp:44` | UUID constructor |

**Call Flow:**
```
memgraph::utils::UUID::UUID() (uuid.hpp:44)
└── uuid_generate() [libuuid]
    └── __uuid_generate_random()
        └── getrandom (syscall)
```

#### 2. Python Hash Randomization
**File**: Python interpreter initialization

**Purpose**: `_Py_HashRandomization_Init` - Security feature for Python's hash function

**Call Flow:**
```
main() (memgraph.cpp:202)
└── Py_InitializeEx()
    └── Py_InitializeFromConfig()
        └── _Py_HashRandomization_Init()
            └── getrandom (syscall)
```

---

## Key Insight: RNG is NOT a Runtime Issue

Unlike what might be expected, **random number generation is not a significant source of non-determinism at runtime**:

| Finding | Details |
|---------|---------|
| Total getrandom calls | 17 (0.01% of all events) |
| When they occur | Startup and initialization only |
| Skip list PRNG | Seeded once, uses mt19937 thereafter |
| Query rand() | Uses thread-local seeded PRNG |

The skip list and other data structures use `std::mt19937` which is seeded once with `std::random_device` but then generates pseudorandom numbers without syscalls.

---

## Remediation Opportunities

### Low Priority - UUIDs
UUIDs are necessary for:
- Run identification
- User tracking
- Replication coordination

**Possible approaches:**
1. For testing/debugging: Allow fixed/deterministic UUIDs via flag
2. Use deterministic UUID generation (e.g., UUID v5 based on stable input)

### Very Low Priority - Python
Python hash randomization is a security feature and should not be disabled.

---

## Files with UUID Generation

| File | Line | Purpose |
|------|------|---------|
| `src/utils/uuid.hpp` | 44 | UUID constructor |
| `src/utils/uuid.cpp` | 34 | GenerateUUID function |
| `src/glue/run_id.cpp` | 15 | Global run ID |
| `src/auth/models.hpp` | 869 | User model |
| `src/replication/state.hpp` | 37 | Replication state |

---

## Conclusion

**RNG is not a priority for reducing non-determinism.**

The 17 getrandom calls represent only 0.01% of all events and occur during initialization. The main sources of non-determinism are:
1. Time operations (clock_gettime, gettimeofday) - ~38%
2. Thread switching (THREADSWITCH, futex) - ~38%

Focus efforts on reducing scheduler frequency and thread operations.
