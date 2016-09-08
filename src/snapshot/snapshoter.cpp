#include "snapshot/snapshoter.hpp"

#include "database/db_accessor.hpp"
#include "logging/default.hpp"
#include "snapshot/snapshot_decoder.hpp"
#include "snapshot/snapshot_encoder.hpp"
#include "storage/indexes/indexes.hpp"
#include "threading/thread.hpp"
#include "utils/sys.hpp"

Snapshoter::Snapshoter(ConcurrentMap<std::string, Db> &dbs,
                       size_t snapshot_cycle, std::string &&snapshot_folder)
    : snapshot_cycle(snapshot_cycle), snapshot_folder(snapshot_folder),
      dbms(dbs)
{
    thread = std::make_unique<Thread>([&]() {
        logger = logging::log->logger("Snapshoter");

        try {
            run(logger);
        } catch (const std::exception &e) {
            logger.error("Irreversible error occured in snapshoter");
            logger.error("{}", e.what());
        }

        logger.info("Shutting down snapshoter");
    });
}

Snapshoter::~Snapshoter()
{
    snapshoting.store(false, std::memory_order_release);
    thread.get()->join();
}

void Snapshoter::run(Logger &logger)
{
    std::time_t last_snapshot = std::time(nullptr);

    while (snapshoting.load(std::memory_order_acquire)) {
        std::time_t now = std::time(nullptr);

        if (now >= last_snapshot + snapshot_cycle) {
            // It's time for snapshot
            make_snapshot(now, "full");

            last_snapshot = now;
        } else {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }
}

void Snapshoter::make_snapshot(std::time_t now, const char *type)
{
    logger.info(std::string("Started ") + type + " snapshot cycle");

    for (auto &db : dbms.access()) {
        auto snapshot_file_name = snapshot_file(db.second, now, type);

        logger.info(std::string("Writing ") + type + " snapshot of database "
                                                     "\"{}\" to file \"{}\"",
                    db.first, snapshot_file_name);

        DbTransaction t(db.second);

        bool success = false;
        try {
            std::ofstream snapshot_file(
                snapshot_file_name, std::fstream::binary | std::fstream::trunc);

            SnapshotEncoder snap(snapshot_file);

            auto old_trans = tx::TransactionId(db.second.tx_engine);
            snapshot(t, snap, old_trans);

            auto res = sys::flush_file_to_disk(snapshot_file);
            if (res == 0) {
                t.trans.commit();
                success = true;
            } else {
                logger.error("Error {} occured while flushing snapshot file",
                             res);
                t.trans.abort();
            }

        } catch (const std::exception &e) {
            logger.error(std::string("Error occured while creating ") + type +
                             " "
                             "snapshot of database \"{}\"",
                         db.first);
            logger.error("{}", e.what());

            t.trans.abort();
        }

        if (success) {
            std::ofstream commit_file(snapshot_commit_file(db.second),
                                      std::fstream::app);

            commit_file << snapshot_file_name << std::endl;

            auto res = sys::flush_file_to_disk(commit_file);
            if (res == 0) {
                commit_file.close();
            } else {
                logger.error("Error {} occured while flushing commit file",
                             res);
            }
        }
    }

    logger.info(std::string("Finished ") + type + " snapshot cycle");
}

void Snapshoter::snapshot(DbTransaction const &dt, SnapshotEncoder &snap,
                          tx::TransactionId const &old_trans)
{
    Db &db = dt.db;
    DbAccessor t(db, dt.trans);

    // Anounce property names
    for (auto &family : db.graph.vertices.property_family_access()) {
        snap.property_name_init(family.first);
    }
    for (auto &family : db.graph.edges.property_family_access()) {
        snap.property_name_init(family.first);
    }

    // Anounce label names
    for (auto &labels : db.graph.label_store.access()) {
        snap.label_name_init(labels.first.to_string());
    }

    // Store vertices
    snap.start_vertices();
    t.vertex_access()
        .fill()
        .filter([&](auto va) { return !va.is_visble_to(old_trans); })
        .for_all([&](auto va) { serialization::serialize_vertex(va, snap); });

    // Store edges
    snap.start_edges();
    t.edge_access()
        .fill()
        .filter([&](auto va) { return !va.is_visble_to(old_trans); })
        .for_all([&](auto ea) { serialization::serialize_edge(ea, snap); });

    // Store info on existing indexes.
    snap.start_indexes();
    db.indexes().vertex_indexes([&](auto &i) { snap.index(i.definition()); });
    db.indexes().edge_indexes([&](auto &i) { snap.index(i.definition()); });

    snap.end();
}

void Snapshoter::import(Db &db)
{
    logger.info("Started import for database \"{}\"", db.name());

    try {

        std::ifstream commit_file(snapshot_commit_file(db));

        std::vector<std::string> snapshots;
        std::string line;
        while (std::getline(commit_file, line)) {
            snapshots.push_back(line);
        }

        while (snapshots.size() > 0) {
            logger.info("Importing data from snapshot \"{}\" into "
                        "database \"{}\"",
                        snapshots.back(), db.name());

            DbTransaction t(db);

            try {
                std::ifstream snapshot_file(snapshots.back(),
                                            std::fstream::binary);
                SnapshotDecoder decoder(snapshot_file);

                if (snapshot_load(t, decoder)) {
                    t.trans.commit();
                    logger.info("Succesfully imported snapshot \"{}\" into "
                                "database \"{}\"",
                                snapshots.back(), db.name());
                    break;
                } else {
                    t.trans.abort();
                    logger.info(
                        "Unuccesfully tryed to import snapshot \"{}\" into "
                        "database \"{}\"",
                        snapshots.back(), db.name());
                }

            } catch (const std::exception &e) {
                logger.error(
                    "Error occured while importing snapshot \"{}\" into "
                    "database \"{}\"",
                    snapshots.back(), db.name());
                logger.error("{}", e.what());
                t.trans.abort();
            }

            snapshots.pop_back();
        }

    } catch (const std::exception &e) {
        logger.error(
            "Error occured while importing snapshot for database \"{}\"",
            db.name());
        logger.error("{}", e.what());
    }

    logger.info("Finished import for database \"{}\"", db.name());
}

bool Snapshoter::snapshot_load(DbTransaction const &dt, SnapshotDecoder &snap)
{
    // TODO
}
