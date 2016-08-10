#pragma once

#include "database/db.hpp"
#include "query_engine/query_result.hpp"
#include "query_engine/query_stripped.hpp"

class ICodeCPU
{
public:
    virtual QueryResult::sptr run(Db &db, code_args_t &args) = 0;
    virtual ~ICodeCPU() {}
};

using produce_t = ICodeCPU *(*)();
using destruct_t = void (*)(ICodeCPU *);
