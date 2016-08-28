#pragma once

#include "communication/communication.hpp"
#include "database/db.hpp"
#include "database/db_accessor.hpp"
#include "query_engine/query_stripped.hpp"

template <typename Stream>
class ICodeCPU
{
public:
    virtual bool run(Db &db, code_args_t &args,
                     Stream &stream) = 0;
    virtual ~ICodeCPU() {}
};

template <typename Stream>
using produce_t = ICodeCPU<Stream> *(*)();

template <typename Stream>
using destruct_t = void (*)(ICodeCPU<Stream> *);
