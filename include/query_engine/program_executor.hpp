#pragma once

#include <string>

#include "database/db.hpp"
#include "query_engine/exceptions/exceptions.hpp"
#include "query_engine/util.hpp"
#include "query_program.hpp"

//  preparations before execution
//  execution
//  postprocess the results

class ProgramExecutor
{
public:
    auto execute(QueryProgram &program, Db &db,
                 communication::OutputStream &stream)
    {
        try {
            // TODO: return result of query/code exection
            return program.code->run(db, program.stripped.arguments, stream);
        } catch (...) {
            // TODO: return more information about the error
            throw QueryEngineException("code execution error");
        }
    }
};
