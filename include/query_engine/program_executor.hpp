#pragma once

#include <string>

#include "database/db.hpp"
#include "query_engine/exceptions/exceptions.hpp"
#include "query_engine/util.hpp"
#include "query_program.hpp"

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
            return program.code->run(barrier::trans(db),
                                     program.stripped.arguments, stream);
#else
            return program.code->run(db, program.stripped.arguments, stream);
#endif
        } catch (...) {
            // TODO: return more information about the error
            throw QueryEngineException("code execution error");
        }
    }
};
