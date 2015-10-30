#ifndef MEMGRAPH_CYPHER_PARSER_HPP
#define MEMGRAPH_CYPHER_PARSER_HPP

#include "cypher.h"
#include "token.hpp"

#include "cypher_lexer.hpp"
#include "ast/tree.hpp"

void* cypher_parserAlloc(void* (*allocProc)(size_t));
void  cypher_parser(void*, int, Token*, ast::Ast* ast);
void  cypher_parserFree(void*, void(*freeProc)(void*));

namespace cypher
{

class Parser
{
public:
    Parser()
    {
        parser = cypher_parserAlloc(malloc);
    }

    ~Parser()
    {
        cypher_parserFree(parser, free);
    }

    ast::Ast parse(Lexer::Tokenizer tokenizer)
    {
        auto tree = ast::Ast();
        std::list<Token> tokens;

        do
        {
            tokens.emplace_back(tokenizer.lookup());
            auto& token = tokens.back();
            // std::cout << token << std::endl;
            cypher_parser(parser, token.id, &token, &tree);

        } while(tokens.back().id != 0);

        return std::move(tree);
    }

private:
    void* parser;
};

}

#endif
