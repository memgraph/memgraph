#pragma once

#include "query_engine/query_result.hpp"
#include "database/db.hpp"

class ICodeCPU
{
public:
    virtual QueryResult::sptr run(Db& db) = 0;
    virtual ~ICodeCPU() {}
};

using produce_t = ICodeCPU*(*)();
using destruct_t = void (*)(ICodeCPU*);
