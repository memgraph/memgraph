#pragma once

#include "query/strip/stripped.hpp"

#ifdef BARRIER
#include "barrier/barrier.hpp"
#include "io/network/socket.hpp"
#else
#include "communication/communication.hpp"
#include "database/db.hpp"
#include "database/db_accessor.hpp"
#endif

template <typename Stream>
class IPlanCPU
{
public:
#ifdef BARRIER
    virtual bool run(barrier::Db &db, plan_args_t &args, Stream &stream) = 0;
#else
    virtual bool run(Db &db, plan_args_t &args, Stream &stream) = 0;
#endif
    virtual ~IPlanCPU() {}
};

template <typename Stream>
using produce_t = IPlanCPU<Stream> *(*)();

template <typename Stream>
using destruct_t = void (*)(IPlanCPU<Stream> *);
