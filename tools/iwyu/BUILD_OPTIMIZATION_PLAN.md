# Build Time Optimization — range-v3 Include Narrowing

## Problem

`<range/v3/all.hpp>` is one of the heaviest single headers in the codebase.
It's included in two widely-used headers:

1. `src/storage/v2/property_value.hpp` (line 18) — included by ~120 TUs
2. `src/storage/v2/enum_store.hpp` (line 18) — included via `schema_info_types.hpp` → `transaction.hpp` → ~71 TUs

Narrowing these to specific range-v3 sub-headers (e.g., `<range/v3/view/zip.hpp>`)
would eliminate the heavy parse cost for all TUs that don't use range-v3 directly.

## Why We Can't Just Replace the Includes

Many `.cpp` and `.hpp` files rely on `range/v3/all.hpp` transitively through
`property_value.hpp` or `enum_store.hpp`. IWYU didn't flag these as needing
direct includes because:

- The `iwyu.imp` mapping had `range/v3/detail/*` → `range/v3/all.hpp`, which
  made IWYU treat `all.hpp` as THE canonical public header for range-v3
- IWYU considers transitive includes as valid providers — if `all.hpp` is
  already available through any include chain, IWYU is satisfied

## Solution: IWYU Mapping + Re-run

### Step 1: IWYU Mapping (DONE)

Created `tools/iwyu/range-v3.imp` which:
- Marks `<range/v3/all.hpp>` as **private** so IWYU never considers it a valid provider
- Marks umbrella headers (`view.hpp`, `algorithm.hpp`, `action.hpp`) as private too
- Leaves all specific sub-headers (`view/transform.hpp`, `algorithm/sort.hpp`, etc.) as public
- Updated main `iwyu.imp` to reference this new mapping file

### Step 2: Run IWYU

Run IWYU with the updated mappings. It will now recommend specific range-v3
headers for every file that uses range-v3 symbols:

```bash
# Run IWYU on the full build (generates recommendations)
source /opt/toolchain-v7/activate
cmake --preset conan-debug
# Use iwyu_tool.py or the wrapper script with updated mappings
python3 /path/to/iwyu_tool.py -p build -- -Xiwyu --mapping_file=iwyu.imp 2> iwyu-range-output.log
```

### Step 3: Apply IWYU Fixes

Review the IWYU output and apply the recommended changes. For each file:
- Add the specific `<range/v3/view/*.hpp>` or `<range/v3/algorithm/*.hpp>` includes
- Remove any `<range/v3/all.hpp>` includes that IWYU flags

### Step 4: Narrow the Headers

Once all files have their direct includes:
- Replace `<range/v3/all.hpp>` in `property_value.hpp` with:
  ```cpp
  #include <range/v3/view/zip.hpp>
  #include <range/v3/algorithm/transform.hpp>
  ```
- Replace `<range/v3/all.hpp>` in `enum_store.hpp` with:
  ```cpp
  #include <range/v3/view/enumerate.hpp>
  #include <range/v3/view/zip.hpp>
  ```

### Step 5: Build & Verify

```bash
source /opt/toolchain-v7/activate && cmake --preset conan-debug
cmake --build --preset conan-debug -j6 --target memgraph
cmake --build --preset conan-debug -j6 --target memgraph__test
```

## Expected Savings

- ~120 TUs will no longer parse `range/v3/all.hpp` through `property_value.hpp`
- ~71 TUs will no longer parse `range/v3/all.hpp` through `enum_store.hpp`
- Only files that actually use range-v3 (~25 files) will pay the parse cost,
  and only for the specific sub-headers they need

## Files Affected

Key headers to narrow:
- `src/storage/v2/property_value.hpp` — uses zip, transform
- `src/storage/v2/enum_store.hpp` — uses enumerate, zip

Files that will need direct range-v3 includes added (from IWYU):
- ~20-25 `.cpp` files across `src/storage/v2/`, `src/query/`, `src/glue/`
- ~2-3 `.hpp` files (`label_property_index.hpp`, `index_lookup.hpp`)
