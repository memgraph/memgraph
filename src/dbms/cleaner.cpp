#include "dbms/cleaner.hpp"

#include <chrono>
#include <ctime>
#include <thread>

#include "database/db_transaction.hpp"
#include "threading/thread.hpp"

Cleaning::Cleaning(ConcurrentMap<std::string, Db> &dbs) : dbms(dbs)
{
    cleaners.push_back(std::make_unique<Thread>([&]() {
        std::time_t last_clean = std::time(nullptr);
        while (cleaning.load(std::memory_order_acquire)) {
            std::time_t now = std::time(nullptr);

            if (now >= last_clean + cleaning_cycle) {
                for (auto &db : dbs.access()) {
                    DbTransaction t(db.second);
                    t.clean_edge_section();
                    t.clean_vertex_section();
                }
                last_clean = now;
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
