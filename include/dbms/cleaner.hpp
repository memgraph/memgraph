#pragma once

#include "database/db.hpp"
#include "threading/thread.hpp"

class Thread;

class Cleaning
{

public:
    // How much sec is a cleaning_cycle in which cleaner will clean at most
    // once. Starts cleaner thread.
    Cleaning(ConcurrentMap<std::string, Db> &dbs, size_t cleaning_cycle);

    // Destroys this object after this thread joins cleaning thread.
    ~Cleaning();

private:
    ConcurrentMap<std::string, Db> &dbms;

    const size_t cleaning_cycle;

    std::vector<std::unique_ptr<Thread>> cleaners;

    // Should i continue cleaning.
    std::atomic<bool> cleaning = {true};
};
