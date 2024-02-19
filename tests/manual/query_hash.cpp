// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <iostream>
#include <vector>

#include <gflags/gflags.h>

#include "query/frontend/stripped.hpp"
#include "storage/v2/fmt.hpp"

DEFINE_string(q, "CREATE (n) RETURN n", "Query");

/**
 * Useful when somebody wants to get a hash for some query.
 *
 * Usage:
 *     ./query_hash -q "CREATE (n {name: \"test\n"}) RETURN n"
 */
int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // take query from input args
  auto query = FLAGS_q;

  // run preprocessing
  memgraph::query::frontend::StrippedQuery preprocessed(query);

  // print query, stripped query, hash and variable values (propertie values)
  std::cout << fmt::format("Query: {}\n", query);
  std::cout << fmt::format("Stripped query: {}\n", preprocessed.query());
  std::cout << fmt::format("Query hash: {}\n", preprocessed.hash());
  std::cout << fmt::format("Property values:\n");
  for (int i = 0; i < preprocessed.literals().size(); ++i) {
    fmt::format("    {}", preprocessed.literals().At(i).second);
  }
  std::cout << std::endl;

  return 0;
}
