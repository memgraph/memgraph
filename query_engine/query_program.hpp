#pragma once

#include "i_code_cpu.hpp"
#include "query_stripped.hpp"

struct QueryProgram
{
    QueryProgram(ICodeCPU* code, QueryStripped&& stripped) :
        code(code),
        stripped(std::forward<QueryStripped>(stripped)) {}

    QueryProgram(QueryProgram& other) = delete;
    QueryProgram(QueryProgram&& other) = default;

    ICodeCPU *code;
    QueryStripped stripped;
};
