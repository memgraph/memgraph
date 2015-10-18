#ifndef MEMGRAPH_CYPHER_COMPILER_HPP
#define MEMGRAPH_CYPHER_COMPILER_HPP

#include "cypher_lexer.hpp"
#include "parser.hpp"

#include "debug/tree_print.hpp"

namespace cypher
{

class Compiler
{
public:
    Compiler() = default;

    void compile(const std::string& input)
    {
        auto parser = cypher::Parser();
        auto tokenizer = lexer.tokenize(input);
        auto tree = parser.parse(tokenizer);

        PrintVisitor printer(std::cout);
        tree.root->accept(printer);
    }

private:
    CypherLexer lexer;
};

}

#endif
