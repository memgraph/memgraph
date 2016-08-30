#pragma once

#include "query_engine/query_stripped.hpp"

#ifdef BARRIER
#include "barrier/barrier.hpp"
#include "io/network/socket.hpp"
#else
#include "communication/communication.hpp"
#include "database/db.hpp"
#include "database/db_accessor.hpp"
#endif

template <typename Stream>
class ICodeCPU
{
public:
#ifdef BARRIER
    virtual bool run(barrier::Db &db, code_args_t &args, Stream &stream) = 0;
#else
    virtual bool run(Db &db, code_args_t &args, Stream &stream) = 0;
#endif
    virtual ~ICodeCPU() {}
};

template <typename Stream>
using produce_t = ICodeCPU<Stream> *(*)();

template <typename Stream>
using destruct_t = void (*)(ICodeCPU<Stream> *);
