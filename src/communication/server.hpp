#pragma once

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "dbms/dbms.hpp"
#include "query/engine.hpp"

#include "communication/worker.hpp"
#include "io/network/event_listener.hpp"
#include "logging/default.hpp"
#include "utils/assert.hpp"

namespace communication {

/**
 * TODO (mferencevic): document methods
 */

/**
 * Communication server.
 * Listens for incomming connections on the server port and assings them in a
 * round-robin manner to it's workers.
 *
 * Current Server achitecture:
 * incomming connection -> server -> worker -> session
 *
 * @tparam Session the server can handle different Sessions, each session
 *         represents a different protocol so the same network infrastructure
 *         can be used for handling different protocols
 * @tparam OutputStream the server has to get the output stream as a template
           parameter because the output stream is templated
 * @tparam Socket the input/output socket that should be used
 */
template <typename Session, typename OutputStream, typename Socket>
class Server
    : public io::network::EventListener<Server<Session, OutputStream, Socket>> {
  using Event = io::network::Epoll::Event;

 public:
  Server(Socket &&socket, Dbms &dbms, QueryEngine<OutputStream> &query_engine)
      : socket_(std::forward<Socket>(socket)),
        dbms_(dbms),
        query_engine_(query_engine),
        logger_(logging::log->logger("communication::Server")) {
    event_.data.fd = socket_;

    // TODO: EPOLLET is hard to use -> figure out how should EPOLLET be used
    // event.events = EPOLLIN | EPOLLET;
    event_.events = EPOLLIN;

    this->listener_.Add(socket_, &event_);
  }

  void Start(size_t n) {
    logger_.info("Starting {} workers", n);
    workers_.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      workers_.push_back(
          std::make_shared<Worker<Session, OutputStream, Socket>>(
              dbms_, query_engine_));
      workers_.back()->Start(alive_);
    }
    logger_.info("Server is fully armed and operational");
    logger_.info("Listening on {} at {}", socket_.endpoint().address(),
                 socket_.endpoint().port());
    while (alive_) {
      this->WaitAndProcessEvents();
    }

    logger_.info("Shutting down...");
    for (auto &worker : workers_) worker->thread_.join();
  }

  void Shutdown() {
    // This should be as simple as possible, so that it can be called inside a
    // signal handler.
    alive_.store(false);
  }

  void OnConnect() {
    debug_assert(idx_ < workers_.size(), "Invalid worker id.");

    logger_.trace("on connect");

    if (UNLIKELY(!workers_[idx_]->Accept(socket_))) return;

    idx_ = idx_ == (int)workers_.size() - 1 ? 0 : idx_ + 1;
  }

  void OnWaitTimeout() {}

  void OnDataEvent(Event &event) {
    if (UNLIKELY(socket_ != event.data.fd)) return;

    this->derived().OnConnect();
  }

  template <class... Args>
  void OnExceptionEvent(Event &event, Args &&... args) {
    // TODO: Do something about it
    logger_.warn("epoll exception");
  }

  void OnCloseEvent(Event &event) { close(event.data.fd); }

  void OnErrorEvent(Event &event) { close(event.data.fd); }

 private:
  std::vector<typename Worker<Session, OutputStream, Socket>::sptr> workers_;
  std::atomic<bool> alive_{true};
  int idx_{0};

  Socket socket_;
  Dbms &dbms_;
  QueryEngine<OutputStream> &query_engine_;
  Event event_;
  Logger logger_;
};

}  // namespace communication
