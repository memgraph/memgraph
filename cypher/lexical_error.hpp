#ifndef MEMGRAPH_CYPHER_LEXER_LEXICAL_ERROR_HPP
#define MEMGRAPH_CYPHER_LEXER_LEXICAL_ERROR_HPP

#include <stdexcept>

#include "token.hpp"

class LexicalError : public std::runtime_error
{
public:
    LexicalError(const Token& token)
        : std::runtime_error("Unrecognized token '" + token.value + "'.") {}
};

#endif
