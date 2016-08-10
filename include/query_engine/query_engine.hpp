#pragma once

#include "program_executor.hpp"
#include "program_loader.hpp"
#include "query_result.hpp"
#include "database/db.hpp"

//
// Current arhitecture:
// query -> code_loader -> query_stripper -> [code_generator]
// -> [code_compiler] -> code_executor

// TODO
//     * query engine will get a pointer to currently active database
//     * TCP server session will have pointer to dbms and currently active
//     * database
        
class QueryEngine
{
public:
    auto execute(const std::string &query, Db& db)
    {
        // TODO: error handling
        auto program = program_loader.load(query);
        auto result = program_executor.execute(program, db);
        return result;
    }

private:
    ProgramExecutor program_executor;
    ProgramLoader program_loader;
};
