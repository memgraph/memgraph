# Planner Module

New (2025) query planner based on equality saturation. Under development.

## Components

### Core
- `union_find.hpp` - Disjoint-set datastructure
- `enode.hpp` - E-node representation (expression nodes in the e-graph)
- `eclass.hpp` - E-class (equivalence classes of expressions)
- `egraph.hpp` - E-graph data structure (core equality saturation engine)
- `eids.hpp` - E-graph identifiers
- `fwd.hpp` - Forward declarations
- `concepts.hpp` - C++ concepts for type constraints
- `constants.hpp` - Core constants
- `processing_context.hpp` - Context for e-graph operations

## Testing
- `memgraph__unit__planner` - Unit tests
- `fuzz_egraph` - E-graph fuzzer

## Benchmarking
- `memgraph__benchmark__new_planner`
