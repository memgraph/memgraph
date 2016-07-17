#pragma once

#include <string>

#include "database/db.hpp"
#include "query_engine/util.hpp"
#include "query_program.hpp"
#include "utils/log/logger.hpp"
#include "query_engine/exceptions/query_engine_exception.hpp"

//  preparations before execution
//  execution
//  postprocess the results

class ProgramExecutor
{
public:
    auto execute(QueryProgram &program)
    {
        try {
            // TODO: return result of query/code exection
            return program.code->run(db, program.stripped.arguments);
        } catch (...) {
            // TODO: return more information about the error
            throw QueryEngineException("code execution error");
        }
    }

public:
    // TODO: here shoud be one database from DBMS, not local instance of
    // DB
    Db db;
};
