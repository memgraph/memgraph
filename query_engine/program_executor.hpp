#pragma once

#include <string>

#include "query_program.hpp"
#include "database/db.hpp"
#include "utils/log/logger.hpp"

//  preparations before execution
//  execution
//  postprocess the results

#define DEBUG 1
#include "query_engine/debug.hpp"

using std::cout;
using std::endl;

class ProgramExecutor
{
public:

    auto execute(QueryProgram& program)
    {
        auto result = program.code->run(db, program.stripped.arguments);
        print_props(*result->data["n"]->data[0]);
        return result;
    }

public:

    Db db;
};
