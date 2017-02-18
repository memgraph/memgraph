#include "logging/logs/async_log.hpp"

AsyncLog::~AsyncLog() {
  alive.store(false);
  worker.join();
}

void AsyncLog::emit(Record::uptr record) { records.push(std::move(record)); }

std::string AsyncLog::type() { return "AsyncLog"; }

void AsyncLog::work() {
  using namespace std::chrono_literals;

  while (true) {
    auto record = records.pop();

    if (record != nullptr) {
      dispatch(*record);
      continue;
    }

    if (!alive) return;

    std::this_thread::sleep_for(10ms);
  }
}
