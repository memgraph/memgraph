// Copyright 2025 Memgraph Ltd.
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

#include "antlr4-runtime/antlr4-runtime.h"
#include "query/frontend/opencypher/generated/MemgraphCypher.h"
#include "query/frontend/opencypher/generated/MemgraphCypherLexer.h"

using namespace antlropencypher;
using namespace antlr4;

int main(int, const char **a) {
  std::string_view query{a[1]};

  ANTLRInputStream input(query);
  MemgraphCypherLexer lexer(&input);
  CommonTokenStream tokens(&lexer);

  const auto &vocabulary = lexer.getVocabulary();
  tokens.fill();
  for (auto token : tokens.getTokens()) {
    std::cout << "TYPE: " << vocabulary.getDisplayName(token->getType()) << "; TEXT: " << token->getText()
              << "; STRING: " << token->toString() << std::endl;
  }

  MemgraphCypher parser(&tokens);
  tree::ParseTree *tree = parser.cypher();

  std::cout << tree->toStringTree(&parser) << std::endl << std::endl;

  return 0;
}
