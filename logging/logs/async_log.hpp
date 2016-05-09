#pragma once

#include <thread>

#include "logging/log.hpp"
#include "data_structures/queue/mpsc_queue.hpp"

class AsyncLog : public Log
{
public:
    ~AsyncLog();

protected:
    void emit(Record::uptr) override;

private:
    lockfree::MpscQueue<Record> records;
    std::atomic<bool> alive {true};
    std::thread worker {[this]() { work(); }};

    void work();
};
