# Phase 2 Build Optimization — Changes Applied

## 2A. Create `property_value_fwd.hpp` forward-declaration header

**File:** `src/storage/v2/property_value_fwd.hpp` (NEW)
**Content:** Lightweight header (~85 lines) containing:
- `PropertyValueType` enum
- Tag types (`IntListTag`, `DoubleListTag`, `NumericListTag`)
- `PropertyValueException` class
- `AreComparableTypes()` and `CompareNumericValues()` helper functions
- Forward declaration of `PropertyValueImpl` template
- `PropertyValue`, `ExternalPropertyValue`, and `pmr::PropertyValue` type aliases

**Dependencies:** `<cstdint>`, `<compare>`, `<memory>`, `<memory_resource>`, `<variant>`,
`storage/v2/id_types.hpp`, `utils/exceptions.hpp` — all lightweight.

**Impact:** Enables headers to use `PropertyValue` in pointer/reference contexts
without pulling in the full 1825-line `property_value.hpp` and its heavy deps
(range-v3, boost::flat_map, etc.).

## 2B. Break `delta.hpp` → `property_value.hpp` chain

**File:** `src/storage/v2/delta.hpp`
**Change:** Replaced `#include "storage/v2/property_value.hpp"` with
`#include "storage/v2/property_value_fwd.hpp"`. Moved two `SetPropertyTag`
constructor definitions to new `src/storage/v2/delta.cpp`.

**Rationale:** `delta.hpp` only needs the full `PropertyValue` type in two
constructor bodies (for `new_object<pmr::PropertyValue>()`). The header itself
only uses `pmr::PropertyValue*` (pointer member on line 391) and `PropertyValue`
in function parameter declarations — both work with forward declarations.

**New file:** `src/storage/v2/delta.cpp` — contains the two out-of-lined
`SetPropertyTag` constructors.

**Impact:** 11 direct includers of `delta.hpp` no longer transitively parse
`property_value.hpp`. The performance impact is limited because `vertex.hpp`
(main delta.hpp consumer) still gets `property_value.hpp` via `property_store.hpp`.

## 2C. Remove `property_value.hpp` from `type_constraints_kind.hpp`

**File:** `src/storage/v2/constraints/type_constraints_kind.hpp`
**Change:** Replaced `#include "storage/v2/property_value.hpp"` with
`#include "storage/v2/property_value_fwd.hpp"`. Moved two inline functions
(`PropertyValueToTypeConstraintKind`, `PropertyValueMatchesTypeConstraint`)
to `src/storage/v2/constraints/type_constraints.cpp`.

**Rationale:** The header defines a `TypeConstraintKind` enum and conversion
functions. Most of these only use `PropertyStoreType` and don't need
`PropertyValue`. The two functions that do use the full type are not
performance-critical and can be non-inline.

**Impact:** `type_constraints_kind.hpp` (rank 34, 208 TUs) no longer pulls in
`property_value.hpp`. Since it's included via `type_constraints_validator.hpp` →
`property_store.hpp`, the savings are absorbed by `property_store.hpp` still
including `property_value.hpp` directly. Net effect: cleaner dependency graph.

## 2D. Remove `trigger.hpp` from `context.hpp`

**File:** `src/query/context.hpp`
**Change:** Removed `#include "query/trigger.hpp"`. Added forward declarations:
```cpp
class FineGrainedAuthChecker;
class TriggerContextCollector;
```

**Rationale:** `context.hpp` only uses `TriggerContextCollector*` (pointer, line 115)
and `FineGrainedAuthChecker` as `shared_ptr<FineGrainedAuthChecker>` (line 124).
Neither requires the full type. The `trigger.hpp` include was pulling in
`cypher_query_interpreter.hpp` (15+ includes), `kvstore.hpp`, `auth_checker.hpp`,
and the entire query parsing infrastructure.

**Transitive breakage fixed:**
- `query/plan/operator.cpp` — added `#include "query/auth_checker.hpp"`
- `query/procedure/mg_procedure_impl.hpp` — added `#include "query/frontend/ast/query/auth_query.hpp"`
- `query/frontend/semantic/required_privileges.cpp` — added `#include "query/frontend/ast/query/auth_query.hpp"`
- `query/interpret/awesome_memgraph_functions.cpp` — added `#include "query/query_user.hpp"`

**Impact:** `context.hpp` (rank 31, 77,627ms, 55 TUs) sheds `trigger.hpp`
(1,455ms/TU, rank 14). Expected savings: ~55 TUs × ~1000ms = **~55s of frontend
parsing time**. This is the highest-impact change in Phase 2.

## 2E. Attempted but reverted: Break `property_store.hpp` → `property_value.hpp`

**Status:** REVERTED

**What was attempted:** Replace `#include "property_value.hpp"` with
`property_value_fwd.hpp` in `property_store.hpp`, breaking the `vertex.hpp` →
`property_store.hpp` → `property_value.hpp` chain.

**Why reverted:** `vertex_accessor.hpp` uses `Result<PropertyValue>` (which is
`std::expected<PropertyValue, Error>`) in method declarations. `std::expected`
requires its value type to be complete. Since `vertex_accessor.hpp` is included
by ~179 TUs, adding `property_value.hpp` back to it would negate almost all
savings from removing it from `vertex.hpp`.

**Lesson:** Splitting `property_value.hpp` out of the vertex chain requires
also fixing the accessor headers, which is not feasible without changing their
API (e.g., opaque return types or PIMPL). This is a Phase 3+ task if pursued.

## 2F. Narrow `range/v3/all.hpp` in `query/common.hpp`

**File:** `src/query/common.hpp`
**Change:** Replaced `#include "range/v3/all.hpp"` with `#include <range/v3/view/zip.hpp>`.

**Rationale:** `query/common.hpp` only uses `ranges::views::zip` (line 149).
The umbrella header pulls in the entire range-v3 library unnecessarily for 92 TUs.

**Transitive breakage fixed:**
- `dbms/inmemory/replication_handlers.cpp` — added `#include <range/v3/algorithm/find_if.hpp>`
  (was getting `ranges::find_if` transitively via `type_constraints_kind.hpp` →
  `property_value.hpp` → `range/v3/all.hpp`, broken by 2C above)

**Impact:** 92 TUs that include `query/common.hpp` no longer parse the full
range-v3 umbrella header (~360ms/TU).
