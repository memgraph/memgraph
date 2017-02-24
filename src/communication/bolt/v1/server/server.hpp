#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "communication/bolt/v1/bolt.hpp"
#include "io/network/server.hpp"
#include "logging/default.hpp"
#include "utils/assert.hpp"

namespace bolt {

template <class Worker>
class Server : public io::Server<Server<Worker>> {
 public:
  Server(io::Socket&& socket)
      : io::Server<Server<Worker>>(std::forward<io::Socket>(socket)),
        logger(logging::log->logger("bolt::Server")) {}

  void start(size_t n) {
    workers.reserve(n);

    for (size_t i = 0; i < n; ++i) {
      workers.push_back(std::make_shared<Worker>(bolt));
      workers.back()->start(alive);
    }

    while (alive) {
      this->wait_and_process_events();
    }
  }

  void shutdown() {
    alive.store(false);

    for (auto& worker : workers) worker->thread.join();
  }

  void on_connect() {
    debug_assert(idx < workers.size(), "Invalid worker id.");

    logger.trace("on connect");

    if (UNLIKELY(!workers[idx]->accept(this->socket))) return;

    idx = idx == workers.size() - 1 ? 0 : idx + 1;
  }

  void on_wait_timeout() {}

 private:
  Bolt bolt;

  std::vector<typename Worker::sptr> workers;
  std::atomic<bool> alive{true};

  int idx{0};
  Logger logger;
};
}
