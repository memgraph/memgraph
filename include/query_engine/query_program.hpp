#pragma once

#include "query_engine/i_code_cpu.hpp"
#include "query_engine/query_stripped.hpp"

template <typename Stream>
struct QueryProgram
{
    using code_t = ICodeCPU<Stream>;

    QueryProgram(code_t *code, QueryStripped &&stripped)
        : code(code), stripped(std::forward<QueryStripped>(stripped))
    {
    }

    QueryProgram(QueryProgram &other) = delete;
    QueryProgram(QueryProgram &&other) = default;

    code_t *code;
    QueryStripped stripped;
};
