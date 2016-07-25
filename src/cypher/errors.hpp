#pragma once

#include "token.hpp"
#include <stdexcept>

// TODO: optimaze exceptions in respect to
//       query_engine/exceptions/error.hpp

class SyntaxError : public std::runtime_error
{
public:
    SyntaxError(const std::string &near)
        : std::runtime_error("Syntax error: near '" + near + "'.")
    {
    }
};

class LexicalError : public std::runtime_error
{
public:
    LexicalError(const Token &token)
        : std::runtime_error("Lexical error: unrecognized token '" +
                             token.value + "'.")
    {
    }
};

class ParserError : public std::runtime_error
{
    using runtime_error::runtime_error;
};
