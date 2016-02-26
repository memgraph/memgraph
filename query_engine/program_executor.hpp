#pragma once

#include <string>

#include "query_program.hpp"
#include "database/db.hpp"
#include "utils/log/logger.hpp"
#include "query_engine/debug.hpp"

//  preparations before execution
//  execution
//  postprocess the results

class ProgramExecutor
{
public:

    auto execute(QueryProgram& program)
    {
        auto result = program.code->run(db, program.stripped.arguments);
        PRINT_PROPS(*result->data["n"]->data[0]);
        return result;
    }

public:

    Db db;
};
