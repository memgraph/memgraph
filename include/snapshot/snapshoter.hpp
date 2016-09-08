#pragma once

#include <unordered_map>

#include "database/db.hpp"
#include "logging/default.hpp"

class Thread;
class SnapshotEncoder;
class SnapshotDecoder;

// Captures snapshots.
class Snapshoter
{

public:
    // How much sec is between snapshots
    // snapshot_folder is path to common folder for all snapshots.
    Snapshoter(ConcurrentMap<std::string, Db> &dbs, size_t snapshot_cycle,
               std::string &&snapshot_folder);

    ~Snapshoter();

    // Imports latest snapshot into the databse
    void import(Db &db);

private:
    void run(Logger &logger);

    // Makes snapshot of given type
    void make_snapshot(std::time_t now, const char *type);

    // Makes snapshot. It only saves records which have changed since old_trans.
    void snapshot(DbTransaction const &dt, SnapshotEncoder &snap,
                  tx::TransactionId const &old_trans);

    // Loads snapshot. True if success
    bool snapshot_load(DbTransaction const &dt, SnapshotDecoder &snap);

    std::string snapshot_file(Db &db, std::time_t const &now, const char *type)
    {
        return snapshot_db_dir(db) + "/" + std::to_string(now) + "_" + type;
    }

    std::string snapshot_commit_file(Db &db)
    {
        return snapshot_db_dir(db) + "/snapshot_commit.txt";
    }

    // Path to directory of database. Ensures that all necessary directorys
    // exist.
    std::string snapshot_db_dir(Db &db)
    {
        if (!sys::ensure_directory_exists(snapshot_folder)) {
            logger.error("Error while creating directory \"{}\"",
                         snapshot_folder);
        }
        auto db_path = snapshot_folder + "/" + db.name();
        if (!sys::ensure_directory_exists(db_path)) {
            logger.error("Error while creating directory \"{}\"", db_path);
        }
        return db_path;
    }

    Logger logger;

    const size_t snapshot_cycle;
    const std::string snapshot_folder;

    std::unique_ptr<Thread> thread = {nullptr};

    ConcurrentMap<std::string, Db> &dbms;

    std::atomic<bool> snapshoting = {true};
};
