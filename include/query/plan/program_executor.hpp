#pragma once

#include <string>

#include "database/db.hpp"
#include "query/exception/query_engine.hpp"
#include "query/exception/plan_execution.hpp"
#include "query/plan/program.hpp"
#include "query/util.hpp"

//  preparations before execution
//  execution
//  postprocess the results

template <typename Stream>
class ProgramExecutor
{
public:
    // QueryProgram has to know about the Stream
    // Stream has to be passed in this function for every execution
    auto execute(QueryProgram<Stream> &program, Db &db, Stream &stream)
    {
        try {
            return program.plan->run(db, program.stripped.arguments, stream);
        // TODO: catch more exceptions
        } catch (...) {
            throw PlanExecutionException("");
        }
    }
};
