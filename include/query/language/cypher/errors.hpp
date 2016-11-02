#pragma once

#include <stdexcept>

#include "token.hpp"
#include "utils/exceptions/basic_exception.hpp"

class CypherParsingError : public BasicException
{
public:
    CypherParsingError(const std::string &what)
        : BasicException("Cypher Parsing Error: " + what)
    {
    }
};

class CypherLexicalError : public BasicException
{
public:
    CypherLexicalError(const Token &token)
        : BasicException("Cypher Lexical Error: unrecognized token '" +
                         token.value + "'.")
    {
    }
};

class CypherSyntaxError : public BasicException
{
public:
    CypherSyntaxError(const std::string &near)
        : BasicException("Cypher Syntax Error: near '" + near + "'.")
    {
    }
};

class CypherSemanticError : public BasicException
{
public:
    CypherSemanticError(const std::string &what)
        : BasicException("Cypher Semanic Error: " + what)
    {
    }
};
