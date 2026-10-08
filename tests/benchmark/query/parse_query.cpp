// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// What a query costs the frontend through the entry the interpreter uses, which is the whole of
// parsing, caching and handing the caller an AST of its own. Measured on both sides of the cache,
// because a query the cache holds pays only the last of those and pays it on every execution.
//
// The `nodes` counter is what the caller is handed, which is what it goes on to plan against, and
// what a cache entry of the same query costs to keep.

#include <benchmark/benchmark.h>

#include <string>

#include "query/config.hpp"
#include "query/cypher_query_interpreter.hpp"
#include "query/frontend/ast/ast.hpp"

namespace {

struct Shape {
  const char *name;
  const char *query;
};

// clang-format off
const Shape kShapes[] = {
    {"small",      "MATCH (n:Person) WHERE n.age > 30 RETURN n.name"},
    {"subquery",   "MATCH (n) CALL { WITH n MATCH (n)-[:R]->(m) RETURN m } RETURN n, m"},
    {"shared_bound", "MATCH ()-[r*42]->() RETURN r"},
    {"long_body",
     "MATCH (n:A)-[e]->(m) WHERE n.a > n.b AND m.c IN n.list "
     "WITH n, m, n.a + n.b AS s, count(*) AS c ORDER BY n.a + n.b, m.c "
     "RETURN n.a, n.b, m.c, s, c ORDER BY s"},
};
// clang-format on

/// A projection of `width` items, each naming its own two properties, which is the shape a
/// generated query takes and the one that grows the storage's name table.
std::string WideDistinct(int width) {
  std::string query = "MATCH (n) RETURN ";
  for (int item = 0; item < width; ++item) {
    if (item != 0) query += ", ";
    auto const i = std::to_string(item);
    query += "n.a" + i + " + n.b" + i + " AS c" + i;
  }
  return query;
}

memgraph::query::InterpreterConfig::Query QueryConfig() { return {}; }

/// A query the cache has never seen: the parse, and whatever the entry costs to make.
void ParseUncached(benchmark::State &state, std::string query) {
  for (auto _ : state) {
    memgraph::query::AstCache cache{16};
    auto parsed = memgraph::query::ParseQuery(query, {}, &cache, QueryConfig(), "uuid", nullptr);
    benchmark::DoNotOptimize(parsed.query);
  }
  memgraph::query::AstCache cache{16};
  auto parsed = memgraph::query::ParseQuery(query, {}, &cache, QueryConfig(), "uuid", nullptr);
  state.counters["nodes"] = static_cast<double>(parsed.ast_storage.storage_.size());
  state.SetItemsProcessed(state.iterations());
}

/// A query the cache holds, which is what nearly every execution meets.
void ParseCached(benchmark::State &state, std::string query) {
  memgraph::query::AstCache cache{16};
  auto warm = memgraph::query::ParseQuery(query, {}, &cache, QueryConfig(), "uuid", nullptr);
  state.counters["nodes"] = static_cast<double>(warm.ast_storage.storage_.size());
  for (auto _ : state) {
    auto parsed = memgraph::query::ParseQuery(query, {}, &cache, QueryConfig(), "uuid", nullptr);
    benchmark::DoNotOptimize(parsed.query);
  }
  state.SetItemsProcessed(state.iterations());
}

}  // namespace

int main(int argc, char **argv) {
  for (const auto &shape : kShapes) {
    benchmark::RegisterBenchmark((std::string{"Uncached/"} + shape.name).c_str(), ParseUncached, shape.query)
        ->Unit(benchmark::kMicrosecond);
    benchmark::RegisterBenchmark((std::string{"Cached/"} + shape.name).c_str(), ParseCached, shape.query)
        ->Unit(benchmark::kMicrosecond);
  }
  for (int width : {8, 64, 256}) {
    auto const name = "wide_" + std::to_string(width);
    benchmark::RegisterBenchmark(("Uncached/" + name).c_str(), ParseUncached, WideDistinct(width))
        ->Unit(benchmark::kMicrosecond);
    benchmark::RegisterBenchmark(("Cached/" + name).c_str(), ParseCached, WideDistinct(width))
        ->Unit(benchmark::kMicrosecond);
  }
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  return 0;
}
