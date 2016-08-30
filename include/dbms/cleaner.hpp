#pragma once

#include "database/db.hpp"

class Thread;

// How much sec is a cleaning_cycle in which cleaner will clean at most
// once.
constexpr size_t cleaning_cycle = 60;

class Cleaning
{

public:
    Cleaning(ConcurrentMap<std::string, Db> &dbs);

    ~Cleaning();

private:
    ConcurrentMap<std::string, Db> &dbms;

    std::vector<std::unique_ptr<Thread>> cleaners;

    std::atomic<bool> cleaning = {true};
};
