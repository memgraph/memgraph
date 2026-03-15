# Phase 1 Build Optimization — Changes Applied

## 1B. Forward-declare RocksDB Transaction in `transaction.hpp`

**File:** `src/storage/v2/transaction.hpp`
**Change:** Replaced `#include <rocksdb/utilities/transaction.h>` with:
```cpp
namespace rocksdb {
class Transaction;
}  // namespace rocksdb
```
**Rationale:** The only usage in the header is a raw pointer member
`rocksdb::Transaction *disk_transaction_{}` (line 216), which doesn't need the
complete type. The only `.cpp` file that dereferences it
(`src/storage/v2/disk/storage.cpp`) already has its own direct include.
**Impact:** 71 TUs no longer parse the heavy RocksDB transaction header.

## 1C. Forward-declare `Vertex` in `schema_info_types.hpp`

**File:** `src/storage/v2/schema_info_types.hpp`
**Change:** Replaced `#include "storage/v2/vertex.hpp"` with:
```cpp
#include "storage/v2/edge_ref.hpp"  // needed for EdgeRef in SchemaInfoEdge

struct Vertex;  // forward declaration (only used as pointer)
```
**Rationale:** `Vertex` is only used as `Vertex*` in `SchemaInfoEdge` (lines 207-208)
and as `const Vertex*` map key in `SchemaInfoPostProcess` (line 293). The hash/equal_to
specializations for `SchemaInfoEdge` use `edge_ref.gid`, not the Vertex pointer.
Files that dereference Vertex (`schema_info.hpp`, `schema_info.cpp`) already have
their own `#include "storage/v2/vertex.hpp"`.
**Impact:** `schema_info_types.hpp` no longer pulls in `vertex.hpp` → `delta.hpp` →
`property_store.hpp` chain for its 71+ includers via `transaction.hpp`.

## 1F. Remove redundant `json_fwd.hpp` from `common.hpp`

**File:** `src/query/stream/common.hpp`
**Change:** Removed `#include <nlohmann/json_fwd.hpp>` (line 18).
**Rationale:** The full `#include "nlohmann/json.hpp"` is already present on line 25,
making the forward-declaration header redundant.
**Impact:** Negligible build time savings but cleaner include list.

## 1A. Narrow `<range/v3/all.hpp>` in `property_value.hpp` — DEFERRED

**Status:** Requires IWYU re-run first.
**Reason:** Replacing `range/v3/all.hpp` with specific sub-headers breaks ~25 files
that relied on it transitively. IWYU didn't catch these because the old mapping
(`range/v3/detail/*` → `range/v3/all.hpp`) made IWYU treat `all.hpp` as canonical.
**Plan:** Updated IWYU mappings in `tools/iwyu/range-v3.imp` to mark umbrella headers
as private. After re-running IWYU to add direct includes to all consumers, the
narrowing can be applied safely. See `tools/iwyu/BUILD_OPTIMIZATION_PLAN.md`.

## 1D. Remove `property_value.hpp` from `schema_info_types.hpp` — NOT POSSIBLE

**Reason:** `ExtendedPropertyType` (struct) and `PropertyValueType` (enum) are both
defined in `property_value.hpp` and used by value in `schema_info_types.hpp`'s
`PropertyInfo` template (line 101 and lines 113-161). Cannot forward-declare.

## 1E. Remove `nlohmann/json.hpp` from `schema_info_types.hpp` — DEFERRED

**Reason:** `ToJson()` methods are templates (parameterized over `TContainer`) that
return `nlohmann::json` and use `json::object_t`/`json::array_t`. Moving to `.cpp`
requires explicit template instantiation for each `TContainer` type. Feasible but
needs more investigation into instantiation sites.

## 1G. Remove `nlohmann/json.hpp` from `coordinator_state_manager.hpp` — NOT POSSIBLE

**Reason:** `GetCoordinatorInstancesAux()` is an inline template method that calls
`nlohmann::json::parse()` and `.template get<>()`. Cannot use `json_fwd.hpp`.
