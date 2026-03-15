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

---

## Phase 1: Include Narrowing (DONE)

### Applied Changes
- **1B** Forward-declare `rocksdb::Transaction` in `transaction.hpp` (71 TUs shed heavy header)
- **1C** Forward-declare `Vertex` in `schema_info_types.hpp` (71 TUs shed vertex→delta→property_value chain)
- **1F** Remove redundant `json_fwd.hpp` from `query/stream/common.hpp`

### Deferred: Narrow `range/v3/all.hpp`
Requires IWYU re-run with updated mappings first. See [range-v3 section](#range-v3-include-narrowing) below.

---

## Phase 2: Header Splitting (HIGH IMPACT)

### 2A. Split `property_value.hpp` → extract `property_value_types.hpp`

**File:** `src/storage/v2/property_value.hpp` (1825 lines, 120+ TUs)

**Problem:** The file contains both lightweight type definitions and the massive
`PropertyValueImpl` template class (800+ lines). Most includers only need the
enum and types, not the full template.

**Split:**
```
NEW: src/storage/v2/property_value_types.hpp (~100 lines)
  - PropertyValueType enum (lines 59-76)
  - Tag types: IntListTag, DoubleListTag, NumericListTag (lines 78-83)
  - PropertyValueException class
  - Utility functions: AreComparableTypes, CompareNumericValues
  - ExtendedPropertyType struct
  - Minimal includes: <cstdint>, <string>, storage/v2/enum.hpp, utils/exceptions.hpp
  - NO range/v3, NO boost

KEEP: src/storage/v2/property_value.hpp
  - #include "property_value_types.hpp"
  - PropertyValueImpl template class
  - Heavy includes (range/v3, boost/container/flat_map)
```

**Impact:** 80+ TUs that only need type info avoid 1825-line file + heavy deps.

### 2B. Update `delta.hpp` to use `property_value_types.hpp`

**File:** `src/storage/v2/delta.hpp` (420 lines, 120 TUs)

**Problem:** Includes full `property_value.hpp` (1825 lines) but likely only needs
`PropertyValueType` enum and forward-declared `PropertyValue`.

**Change:** Replace `#include "storage/v2/property_value.hpp"` with
`#include "storage/v2/property_value_types.hpp"` + forward declaration of
`PropertyValueImpl`. Move constructors that take `PropertyValue` to `delta.cpp`.

**Impact:** 120 TUs × ~1000ms saved per TU from avoiding full property_value.hpp.

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
