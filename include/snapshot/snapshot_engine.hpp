#pragma once

#include <mutex>
#include <unordered_map>

#include "logging/default.hpp"
#include "transactions/transaction.hpp"

class SnapshotEncoder;
class SnapshotDecoder;
class Db;
class DbTransaction;
class DbAccessor;

// Captures snapshots. Only one per database should exist.
class SnapshotEngine
{

public:
    SnapshotEngine(Db &db, std::string const &name);

    ~SnapshotEngine() = default;

    // Returns number of succesffuly created snapshots.
    size_t snapshoted_no() { return snapshoted_no_v.load(); }

    // Imports latest snapshot into the databse. Blocks until other calls don't
    // end.
    bool import();

    // Makes snapshot of given type. Blocks until other calls don't end.
    bool make_snapshot();

private:
    // Removes excess of snapshots starting with oldest one.
    void clean_snapshots();

    // Makes snapshot of given type
    bool make_snapshot(std::time_t now, const char *type);

    // Makes snapshot. It only saves records which have changed since old_trans.
    void snapshot(DbTransaction const &dt, SnapshotEncoder &snap,
                  tx::TransactionRead const &old_trans);

    // Loads snapshot. True if success
    bool snapshot_load(DbAccessor &t, SnapshotDecoder &snap);

    std::string snapshot_file(std::time_t const &now, const char *type);

    std::string snapshot_commit_file();

    // Path to directory of database. Ensures that all necessary directorys
    // exist.
    std::string snapshot_db_dir();

    Logger logger;

    Db &db;
    std::mutex guard;
    const std::string snapshot_folder;

    // Determines how many newest snapshot will be preserved, while the other
    // ones will be deleted.
    const size_t max_retained_snapshots;

    std::atomic<size_t> snapshoted_no_v = {0};
};
