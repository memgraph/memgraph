// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#define LOG_NO_INFO 1

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "query/frontend/stripped.hpp"

// clang-format off
const char *kQueries[] = {
"MATCH (a) RETURN size(collect(a))",
"CREATE (a:L), (b1), (b2) CREATE (a)-[:A]->(b1), (a)-[:A]->(b2)",
"MATCH (a:L)-[rel]->(b) RETURN a, count(*)",
"CREATE ({division: 'Sweden'})",
"MATCH (n) RETURN n.division, count(*) ORDER BY count(*) DESC, n.division ASC",
"UNWIND ['a', 'b', 'B', null, 'abc', 'abc1'] AS i RETURN max(i)",
"CREATE ({created: true})",
"MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*) AS c",
"MATCH (u:User) WITH {key: u} AS nodes DELETE nodes.key",
"CREATE ()-[:T {id: 42, alive: true, name: kifla, height: 4.2}]->()",
"MATCH p = ()-[r:T]-() WHERE r.id = 42 DELETE r",
"UNWIND range(0, 1000) AS i CREATE (:A {id: i}) MERGE (:B {id: i % 10})",
"MATCH (n) WHERE NOT(n.name = 'apa' AND false) RETURN n",
"CREATE ()-[:REL {property1: 12, property2: 24}]->()",
"MATCH (n:A) WHERE n.name = 'Andres' SET n.name = 'Michael' RETURN n",
"MATCH (n:A) SET (n).name = 'memgraph' RETURN n",
"CREATE (a {foo: [1, 2, 3]}) SET a.foo = a.foo + [4, 5] RETURN a.foo",
"MATCH (n:X {foo: 'A'}) SET n = {foo: 'B', baz: 'C'} RETURN n",
"MATCH (n:X {foo: 'A'}) SET n += {foo: null} RETURN n",
"MATCH (n) WITH n LIMIT toInteger(ceil(1.7)) RETURN count(*) AS count",
"MATCH (a:A), (b:B) MERGE (a)-[r:TYPE]->(b) ON CREATE SET r.name = 'Lola' RETURN count(r)",
"CREATE (:L1:L2:L3:L4:L5:L6:L7 {p1: true, p2: 42, p3: \"Here is some text that is not extremely short\", p4:\"Short text\", p5: 234.434, p6: 11.11, p7: false})",
};
// clang-format on

auto BM_Strip = [](benchmark::State &state, auto &function, std::string query) {
  while (state.KeepRunning()) {
    for (int start = 0; start < state.range(0); start++) {
      function(query);
    }
  }
  state.SetComplexityN(state.range(0));
};

int main(int argc, char *argv[]) {
  auto preprocess = [](const std::string &query) { return memgraph::query::frontend::StrippedQuery(query); };

  for (auto test : kQueries) {
    benchmark::RegisterBenchmark(test, BM_Strip, preprocess, test)->Range(1, 1)->Complexity(benchmark::oN);
  }

  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();

  return 0;
}
