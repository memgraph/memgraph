#ifndef MEMGRAPH_DATABASE_DB_HPP
#define MEMGRAPH_DATABASE_DB_HPP

#include "transactions/transaction_engine.hpp"
#include "transactions/commit_log.hpp"

class Db
{
public:
    static Db& get()
    {
        static Db db;
        return db;
    }

    tx::CommitLog commit_log;
    tx::TransactionEngine transaction_engine;
};

#endif
