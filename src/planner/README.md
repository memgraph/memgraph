# Planner Module

New query planner based on equality saturation. Under development.

## Directory Structure

```
src/planner/
├── include/planner/
│   ├── egraph/       # E-graph data structure
│   ├── pattern/      # Pattern matching
│   ├── rewrite/      # Rewrite rules and saturation
│   └── extract/      # Cost-based extraction
├── src/              # Implementation files
├── test/             # Unit tests
└── bench/            # Benchmarks
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
  - `Pattern::build()` - Fluent pattern construction
- `matcher.hpp` - E-matching engine
  - `EMatcher` - Finds all pattern matches in an e-graph
  - `EMatchContext` - Reusable matching buffers
- `match.hpp` - Match results
  - `Match` - Variable bindings from successful matches
  - `MatchArena` - Pool for match storage

### Rewrite System (`rewrite/`)
- `rule.hpp` - Rewrite rules
  - `RewriteRule` - Pattern(s) + apply function
  - `RuleSet` - Immutable collection of rules
  - `RuleContext` - Safe e-graph modifications in apply functions
  - `RewriteContext` - Combined reusable buffers
- `join.hpp` - Multi-pattern join operations
  - `JoinStep` - Hash join with greedy ordering
  - `JoinContext` - Reusable join buffers
- `rewriter.hpp` - Equality saturation engine
  - `Rewriter` - Orchestrates rule application until saturation
  - `RewriteConfig` - Limits (iterations, e-nodes, timeout)
  - `RewriteResult` - Statistics and stop reason

### Extraction (`extract/`)
- `extractor.hpp` - Cost-based expression extraction

## Testing

```bash
# Build and run unit tests
cmake --build build --preset conan-relwithdebinfo --target planner -j 15
./build/src/planner/test/planner

# Run e-graph fuzzer
./build/src/planner/test/fuzz_egraph
```

## Benchmarking

```bash
# Build benchmarks
cmake --build build --preset conan-relwithdebinfo --target memgraph__benchmark__new_planner -j 15

# Run all benchmarks
./build/src/planner/bench/memgraph__benchmark__new_planner

# Run specific benchmark
./build/src/planner/bench/memgraph__benchmark__new_planner --benchmark_filter="BM_EMatch"
./build/src/planner/bench/memgraph__benchmark__new_planner --benchmark_filter="BM_Rewrite"
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
Rewriter<Op, NoAnalysis> rewriter(egraph, std::move(rules));
auto result = rewriter.saturate(RewriteConfig::Default());
// result.saturated() == true when fixed point reached
```
