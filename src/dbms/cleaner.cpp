#include "dbms/cleaner.hpp"

#include <chrono>
#include <ctime>
#include <thread>

#include "database/db_transaction.hpp"
#include "threading/thread.hpp"

#include "logging/default.hpp"

Cleaning::Cleaning(ConcurrentMap<std::string, Db> &dbs) : dbms(dbs)
{
    cleaners.push_back(std::make_unique<Thread>([&]() {
        Logger logger = logging::log->logger("Cleaner");
        std::time_t last_clean = std::time(nullptr);
        while (cleaning.load(std::memory_order_acquire)) {
            std::time_t now = std::time(nullptr);

            if (now >= last_clean + cleaning_cycle) {
                logger.info("Started cleaning cyle");
                for (auto &db : dbs.access()) {
                    logger.info("Cleaning database \"{}\"", db.first);
                    DbTransaction t(db.second);
                    try {
                        logger.info("Cleaning edges");
                        t.clean_edge_section();
                        logger.info("Cleaning vertices");
                        t.clean_vertex_section();
                    } catch (const std::exception &e) {
                        logger.error(
                            "Error occured while cleaning database \"{}\"",
                            db.first);
                        logger.error("{}", e.what());
                    }
                    t.trans.commit();
                }
                last_clean = now;
                logger.info("Finished cleaning cyle");
            } else {
                std::this_thread::sleep_for(std::chrono::seconds(1));
            }
        }
    }));
}

Cleaning::~Cleaning()
{
    cleaning.store(false, std::memory_order_release);
    for (auto &t : cleaners) {
        t.get()->join();
    }
}
