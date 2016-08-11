#pragma once

#include "cypher/cypher.h"
#include "token.hpp"
#include "ast/tree.hpp"
#include "tokenizer/cypher_lexer.hpp"
#include "logging/default.hpp"

void* cypher_parserAlloc(void* (*allocProc)(size_t));
void  cypher_parser(void*, int, Token*, ast::Ast* ast);
void  cypher_parserFree(void*, void(*freeProc)(void*));

namespace cypher
{

class Parser
{
public:
    Parser() : logger(logging::log->logger("LexicalParser"))
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
            // TODO: resolve fmt error with {
            // logger.debug(token.repr());
            cypher_parser(parser, token.id, &token, &tree);

        } while(tokens.back().id != 0);

        return std::move(tree);
    }

protected:
    Logger logger;

private:
    void* parser;
};

}
