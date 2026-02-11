# Planner Module

Core e-graph infrastructure for the V2 query planner based on equality saturation.

> **Status:** Core infrastructure complete. See `src/query/plan_v2/README.md` for integration status.

## Directory Structure

```
src/planner/
├── include/planner/
│   ├── egraph/       # E-graph data structure
│   ├── pattern/      # Pattern matching (including VM executor)
│   ├── rewrite/      # Rewrite rules and saturation
│   └── extract/      # Cost-based extraction
├── src/              # Implementation files
├── test/             # Unit tests and fuzzers
└── bench/            # Performance benchmarks
```

## Components

### E-Graph (`egraph/`)
- `egraph.hpp` - E-graph with union-find and congruence closure
- `eclass.hpp` - E-class (equivalence class of e-nodes)
- `enode.hpp` - E-node (expression node)
- `processing_context.hpp` - Reusable buffers for rebuild operations

### Pattern Matching (`pattern/`)
- `pattern.hpp` - Pattern DSL for e-matching
  - `PatternVar` - Pattern variables (?x, ?y)
  - `Var`, `Wildcard` - Pattern elements
  - `Pattern::build()` - Fluent pattern construction
- `matcher.hpp` - E-matching engine
  - `MatcherIndex` - Finds all pattern matches in an e-graph
  - `EMatchContext` - Reusable matching buffers
- `match.hpp` - Match results
  - `Match` - Variable bindings from successful matches
  - `MatchArena` - Pool for match storage

#### VM Executor (`pattern/vm/`)
Bytecode-based pattern matching for performance:
- `compiler.hpp` - Compiles patterns to bytecode
- `executor.hpp` - Executes bytecode against e-graph
- `instruction.hpp` - VM instruction set
- `state.hpp` - VM execution state with deduplication
- `parent_index.hpp` - Parent chain traversal optimization

### Rewrite System (`rewrite/`)
- `rule.hpp` - Rewrite rules
  - `RewriteRule` - Pattern(s) + apply function
  - `RuleSet` - Immutable collection of rules
  - `RuleContext` - Safe e-graph modifications in apply functions
  - `RewriteContext` - Combined reusable buffers
- `rewriter.hpp` - Equality saturation engine
  - `Rewriter` - Orchestrates rule application until saturation
  - `RewriteConfig` - Limits (iterations, e-nodes, timeout)
  - `RewriteResult` - Statistics and stop reason

### Extraction (`extract/`)
- `extractor.hpp` - Cost-based expression extraction

## Testing

```bash
# Build planner library and tests
cmake --build build --preset conan-relwithdebinfo --target planner -j 15

# Run all unit tests
./build/src/planner/test/planner

# Run specific test suites
./build/src/planner/test/planner --gtest_filter="EGraph*"
./build/src/planner/test/planner --gtest_filter="MatcherIndex*"
./build/src/planner/test/planner --gtest_filter="Rewrite*"

# Run fuzzers
./build/src/planner/test/fuzz_egraph      # E-graph correctness
./build/src/planner/test/fuzz_ematch      # Pattern matching correctness
```

## Benchmarking

```bash
# Build benchmarks
cmake --build build --preset conan-relwithdebinfo --target memgraph__benchmark__new_planner -j 15

# Run all benchmarks
./build/src/planner/bench/memgraph__benchmark__new_planner

# Run specific benchmarks
./build/src/planner/bench/memgraph__benchmark__new_planner --benchmark_filter="SimplePattern"
./build/src/planner/bench/memgraph__benchmark__new_planner --benchmark_filter="VMSimplePattern"
./build/src/planner/bench/memgraph__benchmark__new_planner --benchmark_filter="Rewrite"
./build/src/planner/bench/memgraph__benchmark__new_planner --benchmark_filter="Join"
```

## Usage Example

```cpp
#include "planner/egraph/egraph.hpp"
#include "planner/pattern/pattern.hpp"
#include "planner/rewrite/rewriter.hpp"

using namespace memgraph::planner::core;

// Define symbol type (must satisfy ENodeSymbol concept) and analysis
enum class Op { Add, Neg, Var, Const };
struct NoAnalysis {};

// Create e-graph and add expressions
EGraph<Op, NoAnalysis> egraph;
auto x = egraph.emplace(Op::Var, 1);
auto neg_x = egraph.emplace(Op::Neg, {x.eclass_id});
auto neg_neg_x = egraph.emplace(Op::Neg, {neg_x.eclass_id});

// Create double-negation elimination rule: Neg(Neg(?x)) -> ?x
constexpr PatternVar kRoot{10};
auto pattern = Pattern<Op>::build(Op::Neg, {
    Pattern<Op>::build(Op::Neg, {Var{0}})
}, kRoot);

auto rule = RewriteRule<Op, NoAnalysis>::Builder{"double_negation"}
    .pattern(std::move(pattern))
    .apply([](RuleContext<Op, NoAnalysis> &ctx, Match const &match) {
        ctx.merge(match[kRoot], match[PatternVar{0}]);
    });

// Run equality saturation
auto rules = RuleSet<Op, NoAnalysis>::Build(std::move(rule));
Rewriter<Op, NoAnalysis> rewriter(egraph, rules);
auto result = rewriter.saturate(RewriteConfig::Default());
// result.saturated() == true when fixed point reached
```

## Related Documentation

- `TODO.txt` - Task tracking and known issues
- `src/query/plan_v2/README.md` - Query integration status
- `IMPLEMENTATION_PLAN.md` (repo root) - Overall implementation plan
