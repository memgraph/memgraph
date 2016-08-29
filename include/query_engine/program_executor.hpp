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
namespace barrier
{
    Db& trans(::Db& ref);
}

template <typename Stream>
class ProgramExecutor
{
public:
    // QueryProgram has to know about the Stream
    // Stream has to be passed in this function for every execution
    auto execute(QueryProgram<Stream> &program, Db &db,
                 Stream &stream)
    {
        try {
            // TODO: return result of query/code exection
            return program.code->run(barrier::trans(db), program.stripped.arguments, stream);
        } catch (...) {
            // TODO: return more information about the error
            throw QueryEngineException("code execution error");
        }
    }
};
