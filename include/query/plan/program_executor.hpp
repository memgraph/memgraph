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

// BARRIER!
#ifdef BARRIER
namespace barrier
{
Db &trans(::Db &ref);
}
#endif

template <typename Stream>
class ProgramExecutor
{
public:
    // QueryProgram has to know about the Stream
    // Stream has to be passed in this function for every execution
    auto execute(QueryProgram<Stream> &program, Db &db, Stream &stream)
    {
        try {
// TODO: return result of query/code exection
#ifdef BARRIER
            return program.plan->run(barrier::trans(db),
                                     program.stripped.arguments, stream);
#else
            return program.plan->run(db, program.stripped.arguments, stream);
#endif
        // TODO: catch more exceptions
        } catch (...) {
            throw PlanExecutionException("");
        }
    }
};
