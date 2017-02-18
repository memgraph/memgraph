#pragma once

#include <thread>

#include "data_structures/queue/mpsc_queue.hpp"
#include "logging/log.hpp"

class AsyncLog : public Log {
 public:
  ~AsyncLog();

 protected:
  void emit(Record::uptr) override;
  std::string type() override;

 private:
  lockfree::MpscQueue<Record> records;
  std::atomic<bool> alive{true};
  std::thread worker{[this]() { work(); }};

  void work();
};
