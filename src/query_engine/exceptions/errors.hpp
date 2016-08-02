#pragma once

#include "utils/exceptions/basic_exception.hpp"

// TODO: optimaze exceptions in respect to cypher/errors.hpp

class SemanticError : public BasicException
{
public:
    SemanticError(const std::string& what) :
        BasicException("Semantic error: " + what) {}
};

class CodeGenerationError : public BasicException
{
public:
    CodeGenerationError(const std::string& what) :
        BasicException("Code Generation error: " + what) {}
};
