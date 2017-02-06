#include "dbms/cleaner.hpp"

#include <chrono>
#include <ctime>
#include <thread>

#include "database/db_transaction.hpp"
#include "threading/thread.hpp"

#include "logging/default.hpp"

Cleaning::Cleaning(ConcurrentMap<std::string, GraphDb> &dbs, size_t cleaning_cycle)
    : dbms(dbs), cleaning_cycle(cleaning_cycle)
{
    // Start the cleaning thread
    cleaners.push_back(
        std::make_unique<Thread>([&, cleaning_cycle = cleaning_cycle ]() {
            Logger logger = logging::log->logger("Cleaner");
            logger.info("Started with cleaning cycle of {} sec",
                        cleaning_cycle);

            std::time_t last_clean = std::time(nullptr);
            while (cleaning.load(std::memory_order_acquire)) {
                std::time_t now = std::time(nullptr);

                // Maybe it's cleaning time.
                if (now >= last_clean + cleaning_cycle) {
                    logger.info("Started cleaning cyle");

                    // Clean all databases
                    for (auto &db : dbs.access()) {
                        logger.info("Cleaning database \"{}\"", db.first);
                        DbTransaction t(db.second);
                        try {
                            logger.info("Cleaning edges");
                            t.clean_edge_section();

                            logger.info("Cleaning vertices");
                            t.clean_vertex_section();

                            logger.info("Cleaning garbage");
                            db.second.garbage.clean();

                        } catch (const std::exception &e) {
                            logger.error(
                                "Error occured while cleaning database \"{}\"",
                                db.first);
                            logger.error("{}", e.what());
                        }
                        // NOTE: Whe should commit even if error occured.
                        t.trans.commit();
                    }
                    last_clean = now;
                    logger.info("Finished cleaning cyle");

                } else {

                    // Cleaning isn't scheduled for now so i should sleep.
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
            }
        }));
}

Cleaning::~Cleaning()
{
    // Stop cleaning
    cleaning.store(false, std::memory_order_release);
    for (auto &t : cleaners) {
        // Join with cleaners
        t.get()->join();
    }
}
