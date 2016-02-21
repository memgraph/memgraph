#pragma once

#include "program_loader.hpp"
#include "program_executor.hpp"
#include "query_result.hpp"

//
// Current arhitecture:
// query -> code_loader -> query_stripper -> [code_generator]
// -> [code_compiler] -> code_executor

class QueryEngine
{
public:

    auto execute(const std::string& query)
    {
        auto program = program_loader.load(query);
        auto result = program_executor.execute(program);
        return result;
    }

private:
    ProgramExecutor program_executor;
    ProgramLoader program_loader;
};
