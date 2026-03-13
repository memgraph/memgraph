# Planner TODOs

## Join Order Optimization

**Current behavior**: `compute_join_order` selects the anchor pattern by "most variables" heuristic.

**Problem**: This doesn't account for selectivity. Example:

```
Bind(_, ?sym, ?expr)    -- anchor (2 vars)
?ident = Ident(?sym)
?sym = Symbol()
```

`Bind` might match thousands of nodes, while `Symbol()` matches few.

**Better approach**: Start from the most selective pattern:

1. Anchor = `Symbol()` (leaf pattern, likely few matches)
2. Find `Ident(?sym)` via parent traversal
3. Find `Bind(_, ?sym, _)` via parent traversal

**Potential heuristics**:
- Leaf patterns (`Symbol()`, `IntLiteral()`) are often more selective
- Lower arity = fewer matches
- Deeper/more constrained patterns are more specific
- Could use e-graph statistics for cost-based optimization

**Location**: `compiler.hpp` in `compute_join_order()`

## Multi-Child Enode Iteration Can Produce Duplicate Matches

**Current behavior**: The VM's MarkSeen dedup mechanism works per-slot, not per-tuple.
MarkSeen is only emitted for iteration levels that bind exactly one slot per step:
- `IterSymbolEClasses`/`IterAllEClasses` — one root binding per eclass
- `IterParents` — one path binding per parent enode

MarkSeen is deliberately NOT emitted for `IterENodes` loops.

**Problem**: When a symbol has multiple children, different enodes in the same eclass
can share a child eclass (congruence only guarantees the *tuple* of children is unique).
Iterating the shared child eclass multiple times can produce duplicate output tuples.

Example: `Mul(Neg(?y), Neg(?z))` where the Mul eclass has `Mul(E1, E2)` and `Mul(E1, E3)`.
Both share child[0]=E1, so ?y values from E1 are produced twice. If E2 and E3 also share
Neg enodes (e.g., both contain `Neg(F1)`), the tuple `(?y=A, ?z=F1)` is yielded twice.

**Why MarkSeen can't fix this**: Per-slot MarkSeen for any child slot causes false positives.
Marking `?y=A` as seen would reject ALL future tuples with `?y=A`, including valid ones like
`(?y=A, ?z=F3)` where `F3` only exists in E3.

**Current mitigation**: The rewrite engine tolerates duplicate matches. Applying the same
rewrite rule to the same substitution is idempotent.

**Potential solutions** (if duplicates become a performance problem):
1. **Yield-time tuple dedup**: Maintain `HashSet<tuple>` in executor, check before each Yield.
   Correct but O(results) memory.
2. **Consumer dedup**: Collect all results, deduplicate after.
3. **Generic join / worst-case optimal join**: Restructure iteration to visit each unique
   child eclass exactly once per position, verify enode existence. Eliminates duplicates by
   construction but requires significant VM redesign.

**Location**: `compiler.hpp` in `emit_symbol_structure()`, `test_vm_bytecode.hpp` `ExpectMarkSeenNotOnENodeLoop`

## Rewrite Rules

- [ ] Constant folding (e.g., 1+1 -> 2)
- [ ] Dead store elimination with cost model + extraction

## Query Integration

- [ ] Full plan_v2 pipeline integration
- [ ] Bridge to existing v1 planner
- [ ] Migration strategy

## Query Plan Optimization Rules

- [ ] Match ordering (Match -> Scan transformation)
- [ ] Expansion ordering optimization
- [ ] Symbol usage analysis (used/needed symbols per operator)

## Future Optimizations

- [ ] Analysis-guided rewrites (e-class analysis)
- [ ] Parallel rule application

## Known Performance Characteristics

### E-Matching: O(n²) Chain Pattern

For patterns like `Neg(Neg(Neg(...?x...)))` with depth n against a similarly-
structured e-graph, matching takes O(n²) time.

**Root cause:** Matcher iterates ALL e-classes containing root symbol as candidates.
Each candidate k does O(k) work before failing. Total: 1+2+...+n = O(n²).

**Potential mitigations (if needed):**

1. **Grounded subpattern lookup**
   Direct hash lookup for subpatterns where all variables are already bound.
   Helps: `Add(?x, Const(0))` where `Const(0)` is ground.
   Does NOT help chains with variables at leaves.

2. **Inverted parent index** (not implemented)
   Maintain: `child_eclass -> Set<(parent_eclass, symbol)>`
   Enables bottom-up matching: O(n) for depth-n chains.
   Current state: only parent traversal exists (`state.hpp` ParentsIter),
   which iterates all parents linearly. An indexed structure would enable
   O(1) lookup by symbol vs O(n) iterate+filter.
   Profile first to confirm parent traversal is actually a bottleneck.

3. **Generic relational e-matching**
   Treat matching as database join with selectivity-based ordering.
   Most general but significant implementation effort.
