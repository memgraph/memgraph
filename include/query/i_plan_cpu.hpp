#pragma once

#include "communication/communication.hpp"
#include "database/graph_db.hpp"
#include "database/db_accessor.hpp"
#include "query/strip/stripped.hpp"

template <typename Stream>
class IPlanCPU
{
public:
    virtual bool run(Db &db, plan_args_t &args, Stream &stream) = 0;
    virtual ~IPlanCPU() {}
};

template <typename Stream>
using produce_t = IPlanCPU<Stream> *(*)();

template <typename Stream>
using destruct_t = void (*)(IPlanCPU<Stream> *);
