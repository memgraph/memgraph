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

#include <cstddef>
#include <cstdint>

#include "cypher_query_generator.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/cypher_main_visitor.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/parameters.hpp"
#include "utils/exceptions.hpp"

extern "C" int LLVMFuzzerTestOneInput(uint8_t const *data, size_t size) {
  if (size == 0) return 0;

  auto gen = memgraph::query::fuzz::QueryGenerator(data, size);
  auto const query = gen.Generate();

  try {
    memgraph::query::frontend::opencypher::Parser parser(query);
    memgraph::query::AstStorage storage;
    memgraph::query::Parameters parameters;
    memgraph::query::frontend::ParsingContext ctx{};
    memgraph::query::frontend::CypherMainVisitor visitor(ctx, &storage, &parameters);
    visitor.visit(parser.tree());
  } catch (memgraph::utils::BasicException const &) {
    // Expected: syntax errors, semantic errors, etc.
  }

  return 0;
}
