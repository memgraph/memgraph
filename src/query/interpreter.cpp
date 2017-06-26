#include "query/interpreter.hpp"

// TODO: Remove this flag. Ast caching can be disabled by setting this flag to
// false, this is useful for recerating antlr crashes in highly concurrent test.
// Once antlr bugs are fixed, or real test is written this flag can be removed.
DEFINE_bool(ast_cache, true, "Use ast caching.");

DEFINE_bool(query_cost_planner, true,
            "Use the cost estimator to generate plans for queries.");
