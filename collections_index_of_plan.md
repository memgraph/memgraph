# collections.index_of — measured ground truth, decisions, plan

Worktree `~/work/memgraph-rev-indices`, branch `feat/collections-index-of` off master `be32bab6be`.
Reference: docker `neo4j-apoc-indexof` (neo4j:2026.02.2 + APOC), bolt 7690.
Spec: `openCypher9.pdf` pp. 28-34 (repo root).

## Two reference implementations, and they disagree

| # | name | scope | args |
|---|------|-------|------|
| 1 | `apoc.coll.indexOf(coll, value)` | Cypher 5 only (`@QueryLanguageScope(CYPHER_5)`), **deprecated in Cypher 25** | `coll`, `value` |
| 2 | `coll.indexOf(list, value)` | **built-in**, Cypher 25 only; errors under `CYPHER 5` | `list`, `value` |

The request's query calls form 2. openCypher 9 mentions `indexOf` **nowhere** — it is Neo4j-proprietary,
so the standard constrains the *relation* used, not the function.

## What openCypher 9 settles

Two relations, differing **only** on nulls (direct or nested) — pp. 29, 33, 34:

| relation | used by | `{a:null}` vs `{a:null}` |
|---|---|---|
| Equality | `=`, `<>`, **`IN`** | NULL. *"plain equality is not reflexive for all values (consider: `{a: null} = {a: null}`, `[null] = [null]`)"* |
| Equivalence | `DISTINCT`, grouping | equal. *"Any two null values are equivalent (both directly or inside nested structures)"* |

Map equality (p.33, "New map equality"): equal iff **same keys, including keys mapping to null**, and for
each such key `m1.k = m2.k` is true. Neo4j matches: `{a:null} = {a:null}` -> NULL,
`{a:null} IN [{a:null}]` -> NULL, `count(DISTINCT [{a:null},{a:null}])` -> 1, `{a:null} = {b:2}` -> FALSE.

## Measured behaviour

| case | `apoc.coll.indexOf` | `coll.indexOf` |
|---|---|---|
| `([1,3,5,7,9], 3)` | 1 | 1 |
| `([5,7,7,5], 7)` / `(...,5)` | 1 / 0 (first occurrence) | same |
| `([1,2,3], 9)` | -1 | -1 |
| `([1,2], '1')` | -1 | -1 |
| `([], 1)` | -1 | -1 |
| `(null, 1)` | -1 | **NULL** |
| `([1,2,3], null)` | -1 | **NULL** |
| `([], null)` | -1 | **NULL** (null checked before empty) |
| `([1,null,3], 3)` / `([null,1], 1)` | 2 / 1 | 2 / 1 |
| `([1,2,3], 2.0)` / `([1.0,2.0], 2)` | 1 / 1 | 1 / 1 |
| `([[1,2],[3]], [3])` | 1 | 1 |
| `([{a:1}], {a:1})` | 0 | 0 |
| `([{a:1.0}], {a:1})` | 0 | 0 |
| `([true,false], false)` | 1 | 1 |
| `([node], node)` | 0 | - |
| `('abc','b')` | type error | type error |
| `([{a:null}], {a:null})` | **0** (equivalence) | **-1** (equality) |
| `([[null]], [null])` / `([[1,null]],[1,null])` | 0 / 0 | -1 / -1 |

### `apoc.coll.indexOf` map matching is broken — do not mirror it

`Util.valueEquals` is `ValueUtils.of(a).equals(ValueUtils.of(b))`, and the kernel's `MapValue.equals`
reads a **missing** key back as `NO_VALUE` — the same thing a **null-valued** key reads back as. It
conflates them, and the relation is not symmetric:

```
apoc.coll.indexOf([{a:null}], {b:2})          ->  0    matched
apoc.coll.indexOf([{b:2}], {a:null})          -> -1    same pair reversed, not matched
apoc.coll.indexOf([{a:1,b:null}], {a:1,c:9})  ->  0
apoc.coll.indexOf([{a:null,c:3}], {b:2})      -> -1    (size differs)
apoc.coll.contains([{a:null}], {b:2})         -> FALSE (contains is correct)
```

### The two APOC algorithms, exactly

```java
// Coll.java
if (coll == null || coll.isEmpty()) return -1;
return Util.indexOf(coll, value);
// Util.indexOf
IntStream.range(0, list.size()).filter(i -> valueEquals(list.get(i), value)).findFirst().orElse(-1);
// Util.valueEquals
if (one == null || other == null) return false;
return ValueUtils.of(one).equals(ValueUtils.of(other));
```

Cypher 25 built-in: `if (list == null || value == null) return null;` then first `i` where
`list[i] = value` is **true**, else -1.

## Pre-existing defects found in `include/mgp.hpp`

Both measured against master's built `collections.so` on a local server (bolt 7692).

1. **`MapsEqual` never matches non-empty maps.** Lines 2125/2128 compare each item's key and value to
   **itself**, so the first iteration returns false:
   ```cpp
   if (mgp::map_item_key(item) == mgp::map_item_key(item)) return false;              // always true
   if (!util::ValuesEqual(mgp::map_item_value(item), mgp::map_item_value(item))) ...   // self-compare
   ```
   Measured: `collections.contains([{a:1}],{a:1})` -> **false** (reference TRUE);
   `collections.to_set([{a:1},{a:1}])` -> **`[{a:1},{a:1}]`** (reference `[{a:1}]`).
   Also leaks `items_it` on every early return.
   Blast radius: every function comparing elements — contains, contains_all, to_set, intersection,
   subtract, disjunction, duplicates, union, remove_all, frequencies_as_map — plus other modules.

2. **`hash<mgp::Map>` is only accidentally sound, and collides catastrophically.**
   `FnvCollection` combines items with `hash *= prime; hash ^= item_hash` — **order-dependent** — and
   `hash<mgp::MapItem>` hashes the **key only**. It works today solely because `mgp_map::items` is a
   `std::variant` whose ordered `pmr::map` alternative is the default and the only one any in-tree path
   builds (the unordered alternative is reachable only via `mgp_unordered_map_make_empty`, which no
   module calls), so equal maps iterate key-sorted and hash equally.
   Key-only hashing was harmless while `MapsEqual` always returned false — collisions were rejected in
   O(1). Once it actually compares, `[{id:1},{id:2},...]` lands every element in one bucket and dedup
   goes quadratic. **Neo4j hashes values**: 400,000 distinct `{id:i}` maps through `count(DISTINCT m)`
   take the same ~1.2s (round-trip-dominated) as 400,000 identical ones; key-only hashing would be
   8e10 comparisons.

## Decisions

| # | decision | rationale |
|---|---|---|
| 1 | **Cypher 25 null semantics**: null list or null value -> `NULL`; not found -> `-1` | spec-aligned (*"comparability and equality produce unknown null values"*); APOC's -1 is not, and APOC's own function is deprecated in favour of the built-in |
| 2 | Name **`collections.index_of`** | all 22 existing collections callables are snake_case; `text.indexOf` sits in a module that is itself inconsistent |
| 3 | Args **`coll`, `value`** | what `apoc.coll.indexOf` uses (the alias users reach it by) and the module's plurality (`contains`, `contains_sorted`, `contains_all`, `duplicates`, `sort`); standard is silent |
| 4 | Mapping: **`"apoc.coll.indexOf"` only**, no bare `"coll.indexOf"` | the alias mapper is a generic string map consulted only when no module of that name exists, so a bare key *would* work — but built-ins resolve before modules with no warning, so it would answer -1 today and flip to NULL when a real Cypher 25 `coll.indexOf` lands |
| 5 | **Equality relation** for element matching, via a new `ValuesDefinitelyEqual` | openCypher assigns list membership (`IN`) to equality, and `index_of` is `IN` with a position; the built-in agrees |
| 6 | Fix `MapsEqual` **in this PR** (equivalence relation + iterator leak) | `index_of` would otherwise ship a known-wrong map answer; equivalence is the right header contract since `ValuesEqual` feeds `unordered_set` dedup |
| 7 | Harden `hash<mgp::Map>` **in this PR**: order-independent, keys **and** values | removes the hidden dependency on a container choice made elsewhere, and avoids the quadratic cliff the `MapsEqual` fix would otherwise introduce |
| 8 | `distinct` **out of scope** | no customer demand in the row (a triage note, not the request), and no `apoc.coll.distinct` exists to alias |
| 9 | Tests: unit in `tests/unit/cpp_api.cpp` **and** mage e2e | `cpp_api.cpp` has 46 tests but none touch `ValuesEqual`/`MapsEqual`; no existing e2e pins map behaviour |
| 10 | Validate: `collections_test` + `map_test` e2e, plus grep other modules for value equality | exposure is enumerable; no collections e2e test uses map literals today |
| 11 | Deliverable: **dev PR + checklist comment**; no docs PR, no backlog entries | user's call |

### Deliberate divergences to state in the PR

- `collections.index_of` returns `NULL` for a null argument where `apoc.coll.indexOf` returned `-1`.
- It returns `-1` for `([{a:null}], {a:null})` where `apoc.coll.indexOf` returned `0`.
- It does **not** reproduce APOC's asymmetric null-key/missing-key conflation.
- Map equality now works in every `collections.*` function — `contains([{a:1}],{a:1})` flips false -> true,
  `to_set` now dedups equal maps.

## Two relations in the header after this PR

```
ValuesEqual(a, b)             equivalence: any two nulls equal, directly or nested
                              -> to_set, duplicates, intersection, subtract, disjunction,
                                 union, remove_all, contains, contains_all, frequencies_as_map
ValuesDefinitelyEqual(a, b)   equality: a null anywhere makes it undecided -> no match
                              -> index_of
```

NaN needs no special handling: `value_get_numeric` comparison already makes NaN != NaN, which is the
correct no-match under equality. Under equivalence the spec wants two NaNs equivalent and `ValuesEqual`
says false — a pre-existing header divergence, untouched here.

## Work items

1. `include/mgp.hpp` — fix `MapsEqual` (same key set + per-key `ValuesEqual`, via `map_at`, which returns
   nullptr for a missing key; destroy the iterator once). Harden `hash<mgp::MapItem>` to combine key and
   value, and `hash<mgp::Map>` to combine items commutatively. Add `ValuesDefinitelyEqual`.
2. `tests/unit/cpp_api.cpp` — equality/equivalence/hash cases.
3. `src/mage/cpp/collections_module/algorithm/collections.{hpp,cpp}` — `kProcedureIndexOf`,
   `kIndexOfArg1`/`kIndexOfArg2`, `Collections::IndexOf`.
4. `src/mage/cpp/collections_module/collections_module.cpp` — register with
   `ListOfNullable()` + `NullableAny()` (the module is already on the low-level path).
5. `config/mappings.json` — `"apoc.coll.indexOf": "collections.index_of"`, alphabetical.
6. `tests/mage/e2e/collections_test/test_index_of_*/` — one dir per case, values from the reference.
