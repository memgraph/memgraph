#pragma once

#include "query/backend/code_generator.hpp"

/*
 * Traverses the intermediate representation tree and generates
 * C++ code.
 */
class CppCodeGenerator : public CodeGenerator
{
public:
    CppCodeGenerator()
    {
        throw std::runtime_error("TODO: implementation");
    }

    void process(ir::Node *) override
    {
        throw std::runtime_error("TODO: implementation");

    }
};
