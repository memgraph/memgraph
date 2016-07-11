#pragma once

#include <string>

#include "database/db.hpp"
#include "query_engine/debug.hpp"
#include "query_program.hpp"
#include "utils/log/logger.hpp"

//  preparations before execution
//  execution
//  postprocess the results

class ProgramExecutor
{
public:
    auto execute(QueryProgram &program)
    {
        return program.code->run(db, program.stripped.arguments);
    }

public:
    Db db;
};
