#ifndef MEMGRAPH_CYPHER_ERRORS_HPP
#define MEMGRAPH_CYPHER_ERRORS_HPP

#include <stdexcept>
#include "token.hpp"

class SyntaxError : public std::runtime_error
{
public:
    SyntaxError(const std::string& near)
        : std::runtime_error("Syntax error near '" + near + "'.") {}
};

class LexicalError : public std::runtime_error
{
public:
    LexicalError(const Token& token)
        : std::runtime_error("Unrecognized token '" + token.value + "'.") {}
};

class ParserError : public std::runtime_error
{
    using runtime_error::runtime_error;
};

#endif
