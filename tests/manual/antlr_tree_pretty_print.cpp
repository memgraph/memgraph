#include <iostream>
#include <istream>
#include <iterator>
#include <ostream>
#include <string>

#include "antlr4-runtime.h"
#include "query/frontend/opencypher/generated/MemgraphCypher.h"
#include "query/frontend/opencypher/generated/MemgraphCypherLexer.h"

using namespace antlropencypher;
using namespace antlr4;

std::string ReadAllInput() {
  // don't skip the whitespace while reading
  std::cin >> std::noskipws;

  // use stream iterators to copy the stream to a string
  std::istream_iterator<char> it(std::cin);
  std::istream_iterator<char> end;
  std::string results(it, end);
  return results;
}

int main(int, const char **) {
  std::string input_string = ReadAllInput();
  ANTLRInputStream input(input_string);
  MemgraphCypherLexer lexer(&input);
  CommonTokenStream tokens(&lexer);

  tokens.fill();
  for (auto token : tokens.getTokens()) {
    std::cout << token->toString() << std::endl;
  }

  MemgraphCypher parser(&tokens);
  tree::ParseTree *tree = parser.cypher();

  // Print tree indented. This is a hacky implementation and not very correct.
  std::string indent;
  std::string string_tree = tree->toStringTree(&parser);
  for (int i = 0; i < (int)string_tree.size(); ++i) {
    char c = string_tree[i];
    char next_c = i + 1 != (int)string_tree.size() ? string_tree[i + 1] : '\0';
    char prev_c = i - 1 != (int)string_tree.size() ? string_tree[i - 1] : '\0';
    if (c == '(' && next_c != ' ') {
      indent.push_back(' ');
      std::cout << "(";
    } else if (c == ')' && prev_c != ' ') {
      indent.pop_back();
      std::cout << ")";
    } else {
      if (c == ' ' && prev_c != ' ') {
        std::cout << "\n" << indent;
      } else if (c != ' ') {
        std::cout << c;
      }
    }
  }
  std::cout << std::endl;
  return 0;
}
