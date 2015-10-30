#ifndef MEMGRAPH_CYPHER_COMPILER_HPP
#define MEMGRAPH_CYPHER_COMPILER_HPP

#include "cypher_lexer.hpp"
#include "parser.hpp"

namespace cypher
{

class Compiler
{
public:
    Compiler() = default;

    ast::Ast syntax_tree(const std::string& input)
    {
        auto parser = cypher::Parser();
        auto tokenizer = lexer.tokenize(input);
        auto tree = parser.parse(tokenizer);
        return std::move(tree);
    }

private:
    CypherLexer lexer;
};

}

#endif
