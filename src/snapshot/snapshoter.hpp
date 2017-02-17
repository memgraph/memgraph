#pragma once

#include <unordered_map>

#include "database/graph_db.hpp"
#include "logging/default.hpp"
#include "threading/thread.hpp"

class SnapshotEncoder;
class SnapshotDecoder;

// Captures snapshots.
class Snapshoter
{

public:
    // How much sec is between snapshots
    // snapshot_folder is path to common folder for all snapshots.
    Snapshoter(ConcurrentMap<std::string, Db> &dbs, size_t snapshot_cycle);

    ~Snapshoter();

private:
    void run();

    // Makes snapshot of given type
    void make_snapshots();

    Logger logger;

    const size_t snapshot_cycle;
    std::unique_ptr<Thread> thread = {nullptr};
    ConcurrentMap<std::string, Db> &dbms;
    std::atomic<bool> snapshoting = {true};
};
