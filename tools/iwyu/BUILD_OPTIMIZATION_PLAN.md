# Build Time Optimization Plan

Source: ClangBuildAnalyzer on 315 TUs — **2657s total frontend (parsing) time**.

---

## ClangBuildAnalyzer Summary (Expensive Headers)

| Header | Total ms | Includes | Avg ms/TU |
|--------|----------|----------|-----------|
| `dbms/database.hpp` | 167,472 | 19 | 8,814 |
| `storage/v2/transaction.hpp` | 161,434 | 71 | 2,273 |
| `storage/v2/storage.hpp` | 152,750 | 58 | 2,633 |
| `storage/v2/edge.hpp` | 151,484 | 110 | 1,377 |
| `storage/v2/delta.hpp` | 148,935 | 120 | 1,241 |
| `storage/v2/edge_accessor.hpp` | 148,150 | 93 | 1,593 |
| `query/typed_value.hpp` | 136,587 | 64 | 2,134 |
| `storage/v2/indices/indices.hpp` | 122,266 | 62 | 1,972 |
| `storage/v2/durability/durability.hpp` | 112,389 | 15 | 7,492 |

Top template instantiation costs:
- `PropertyValueImpl` — 117 times × 1027ms avg = **120,169ms**
- `nlohmann::json::parse` — 116 times × 456ms avg = **52,960ms**
- `boost::container::flat_map` — 117 times × 797ms avg = **93,317ms**

## `-ftime-trace` Analysis (Top 25 Project Headers, Post-Phase-1)

Aggregated from 487 TUs using `tools/iwyu/analyze_ftime_trace.py`.

| Rank | Total (ms) | Avg (ms) | TUs | Header |
|------|-----------|---------|-----|--------|
| 1 | 237,648 | 3,008 | 79 | `storage/v2/inmemory/storage.hpp` |
| 2 | 205,251 | 2,534 | 81 | `storage/v2/durability/durability.hpp` |
| 3 | 198,236 | 2,509 | 79 | `storage/v2/inmemory/replication/recovery.hpp` |
| 4 | 191,062 | 2,302 | 83 | `storage/v2/durability/wal.hpp` |
| 5 | 159,391 | 590 | 270 | `storage/v2/property_value.hpp` |
| 6 | 156,468 | 1,118 | 140 | `storage/v2/storage.hpp` |
| 7 | 149,422 | 970 | 154 | `storage/v2/transaction.hpp` |
| 8 | 145,532 | 3,465 | 42 | `tests/unit/disk_test_utils.hpp` |
| 9 | 137,272 | 3,813 | 36 | `dbms/database.hpp` |
| 10 | 131,459 | 907 | 145 | `storage/v2/indices/indices.hpp` |
| 11 | 129,596 | 2,700 | 48 | `storage/v2/disk/storage.hpp` |
| 12 | 122,425 | 1,345 | 91 | `query/plan/operator.hpp` |
| 13 | 108,708 | 863 | 126 | `query/typed_value.hpp` |
| 14 | 107,693 | 1,455 | 74 | `query/trigger.hpp` |
| 15 | 106,184 | 727 | 146 | `storage/v2/indices/text_edge_index.hpp` |
| 16 | 100,761 | 800 | 126 | `query/path.hpp` |
| 17 | 100,448 | 510 | 197 | `storage/v2/edge.hpp` |
| 18 | 99,324 | 1,399 | 71 | `query/db_accessor.hpp` |
| 19 | 98,965 | 553 | 179 | `storage/v2/edge_accessor.hpp` |
| 20 | 96,478 | 1,304 | 74 | `query/cypher_query_interpreter.hpp` |
| 21 | 94,940 | 913 | 104 | `query/frontend/ast/ast.hpp` |
| 22 | 94,861 | 637 | 149 | `storage/v2/inmemory/label_index.hpp` |
| 23 | 92,661 | 718 | 129 | `query/edge_accessor.hpp` |
| 24 | 89,239 | 599 | 149 | `storage/v2/indices/indices_utils.hpp` |
| 25 | 84,534 | 494 | 171 | `replication/replication_client.hpp` |

Notable 3rd-party headers:
- `range/v3/all.hpp` — 43,581ms across 122 TUs (rank 60, down from ~270 TUs pre-narrowing)
- `nlohmann/json.hpp` — 47,674ms across 216 TUs (rank 56)
- `gtest/gtest.h` — 61,284ms across 177 TUs (rank 40, test-only)
- `<chrono>` — 85,114ms across 428 TUs (rank 25, STL)

---

## Phase 1: Include Narrowing (DONE)

### Applied Changes
- **1B** Forward-declare `rocksdb::Transaction` in `transaction.hpp` (71 TUs shed heavy header)
- **1C** Forward-declare `Vertex` in `schema_info_types.hpp` (71 TUs shed vertex→delta→property_value chain)
- **1F** Remove redundant `json_fwd.hpp` from `query/stream/common.hpp`

- **1A** Narrow `range/v3/all.hpp` in `property_value.hpp` and `enum_store.hpp` (manually fixed ~22 .cpp transitive consumers)

---

## Phase 2: Header Splitting & Forward Declarations (IN PROGRESS)

### 2A. Create `property_value_fwd.hpp` (DONE)

Created `src/storage/v2/property_value_fwd.hpp` — lightweight forward-declaration
header with enum, tag types, aliases. See PHASE2_CHANGES.md for details.

### 2B. Break `delta.hpp` → `property_value.hpp` chain (DONE)

Replaced include in `delta.hpp` with `property_value_fwd.hpp`. Created `delta.cpp`
with out-of-lined `SetPropertyTag` constructors. Limited impact due to
`property_store.hpp` still including full `property_value.hpp` in the same chain.

### 2B'. Break `type_constraints_kind.hpp` → `property_value.hpp` (DONE)

Moved two inline PropertyValue-dependent functions to `type_constraints.cpp`.

### 2B''. Break `property_store.hpp` → `property_value.hpp` (ATTEMPTED, REVERTED)

Not feasible: `vertex_accessor.hpp` uses `Result<PropertyValue>` (`std::expected`)
which requires complete type. Removing `property_value.hpp` from vertex chain
doesn't help when the accessor headers bring it back for ~179 TUs.

### 2D'. Remove `trigger.hpp` from `context.hpp` (DONE — HIGH IMPACT)

`context.hpp` (55 TUs) was including `trigger.hpp` (1455ms/TU) unnecessarily.
Replaced with forward declarations of `TriggerContextCollector` and
`FineGrainedAuthChecker`. Fixed 4 transitive consumers. Expected savings: **~55s**.

### 2C. Split `storage.hpp` → extract `storage_fwd.hpp` and `storage_info.hpp`

**File:** `src/storage/v2/storage.hpp` (918 lines, 58 TUs, avg 2633ms)

**Split:**
```
NEW: src/storage/v2/storage_fwd.hpp (~50 lines)
  - Forward declare: Storage, Storage::Accessor
  - Exception classes: SharedAccessTimeout, UniqueAccessTimeout, ReadOnlyAccessTimeout
  - No heavy includes

NEW: src/storage/v2/storage_info.hpp (~60 lines)
  - IndicesInfo, ConstraintsInfo, StorageInfo, EventInfo structs
  - Only includes: id_types.hpp, <vector>, <optional>, <string>

KEEP: src/storage/v2/storage.hpp
  - #include "storage_fwd.hpp", "storage_info.hpp"
  - Full Storage class + Accessor
```

**Impact:** 30+ files that only need forward declarations or info types avoid the
full 918-line file and its transitive includes.

### 2D. PIMPL `database.hpp` heavy members

**File:** `src/dbms/database.hpp` (195 lines, 19 TUs but **8,814ms each** — heaviest per-TU)

**Problem:** Embedded members require full includes:
```cpp
query::TriggerStore trigger_store_;
query::stream::Streams streams_;
query::PlanCacheLRU plan_cache_;
```

**Change:** Replace with `std::unique_ptr<>` members, move full includes to `.cpp`:
```cpp
std::unique_ptr<query::TriggerStore> trigger_store_;
std::unique_ptr<query::stream::Streams> streams_;
std::unique_ptr<query::PlanCacheLRU> plan_cache_;
```

**Impact:** 19 TUs × ~4000ms saved from avoiding streams.hpp, trigger.hpp chains.

---

## Phase 3: Break Include Chains

### 3A. Break `delta.hpp` → `property_value.hpp` chain

Move Delta's `SetPropertyTag` constructors (which take PropertyValue by value)
to `delta.cpp`. Forward-declare PropertyValue in header.

**Impact:** 120 TUs lose ~1500ms each from property_value.hpp chain.

### 3B. PIMPL schema/text-index members in `transaction.hpp`

`transaction.hpp` includes `schema_info.hpp`, `text_index_utils.hpp`,
`active_indices.hpp`, `active_constraints.hpp` etc. Many can be hidden behind
opaque pointers.

**Impact:** 71 TUs × ~500ms saved from index/constraint includes.

---

## Phase 4: AST Split (MEDIUM-HIGH IMPACT, HIGH EFFORT)

### 4A. Create `ast_fwd.hpp`

**File:** `src/query/frontend/ast/ast.hpp` (4326 lines, heavily included)

**Split:**
```
NEW: src/query/frontend/ast/ast_fwd.hpp
  - Forward declarations of all 60+ AST node classes
  - Base class signatures (Expression, Clause)
  - Purpose: Break circular deps, allow consumers to use pointers only

EXISTING: ast.hpp includes ast_fwd.hpp
```

### 4B. Move heavy includes out of `ast.hpp`

- Replace `#include "query/typed_value.hpp"` with forward declaration
- Replace `#include "query/interpret/awesome_memgraph_functions.hpp"` with extern declarations
- Move `storage/v2/constraints/type_constraints.hpp` to `.cpp` files

---

## Phase 5: Template Instantiation Reduction

### 5A. Explicit template instantiation for `PropertyValueImpl`

**Problem:** `PropertyValueImpl` instantiated 117 times × 1027ms = 120s total.

**Change:** Add explicit instantiation in `property_value.cpp` for the two
commonly-used allocator types, use `extern template` in the header.

### 5B. Explicit template instantiation for `nlohmann::json::parse`

**Problem:** `json::parse` instantiated 116 times × 456ms = 53s total.

**Change:** Provide explicit instantiation for `parse<const char*>` and
`parse<std::string>` in a central `.cpp`, `extern template` elsewhere.

---

## range-v3 Include Narrowing

### Problem

`<range/v3/all.hpp>` is included in two widely-used headers:
1. `src/storage/v2/property_value.hpp` — included by ~120 TUs
2. `src/storage/v2/enum_store.hpp` — included via `schema_info_types.hpp` → `transaction.hpp` → ~71 TUs

### Why We Can't Just Replace Yet

~25 `.cpp`/`.hpp` files rely on `range/v3/all.hpp` transitively. IWYU didn't flag
these because the old mapping (`range/v3/detail/*` → `range/v3/all.hpp`) made
IWYU treat `all.hpp` as the canonical public header.

### Solution: IWYU Mapping + Re-run

**Step 1** (DONE): Created `tools/iwyu/range-v3.imp` marking umbrella headers as
private. Updated `iwyu.imp` to reference it.

**Step 2**: Run IWYU with updated mappings to add direct includes to all consumers.

**Step 3**: Apply IWYU fixes (add specific `<range/v3/view/*.hpp>` includes).

**Step 4**: Narrow `property_value.hpp` and `enum_store.hpp` to specific sub-headers.

---

## Verification

After each phase:
```bash
source /opt/toolchain-v7/activate && cmake --preset conan-debug
cmake --build --preset conan-debug -j6 --target memgraph
cmake --build --preset conan-debug -j6 --target memgraph__unit
/home/ivan.linux/work/ClangBuildAnalyzer/build/ClangBuildAnalyzer --analyze build/build_timings.bin
```
