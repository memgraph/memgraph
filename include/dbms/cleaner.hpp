#pragma once

#include "database/db.hpp"
#include "threading/thread.hpp"

class Thread;

class Cleaning
{

public:
    // How much sec is a cleaning_cycle in which cleaner will clean at most
    // once.
    Cleaning(ConcurrentMap<std::string, Db> &dbs, size_t cleaning_cycle);

    ~Cleaning();

private:
    ConcurrentMap<std::string, Db> &dbms;

    const size_t cleaning_cycle;

    std::vector<std::unique_ptr<Thread>> cleaners;

    std::atomic<bool> cleaning = {true};
};
