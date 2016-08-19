#pragma once

#include "communication/communication.hpp"
#include "database/db.hpp"
#include "database/db_accessor.hpp"
#include "query_engine/query_stripped.hpp"

class ICodeCPU
{
public:
    virtual bool run(Db &db, code_args_t &args,
                     communication::OutputStream &stream) = 0;
    virtual ~ICodeCPU() {}
};

using produce_t = ICodeCPU *(*)();
using destruct_t = void (*)(ICodeCPU *);
