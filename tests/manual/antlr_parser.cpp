#include <iostream>

#include "antlr4-runtime.h"
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
