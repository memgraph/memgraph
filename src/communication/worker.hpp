#pragma once

#include <atomic>
#include <cstdio>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>

#include "dbms/dbms.hpp"
#include "query/engine.hpp"

#include "communication/bolt/v1/session.hpp"
#include "io/network/network_error.hpp"
#include "io/network/stream_reader.hpp"
#include "logging/default.hpp"

namespace communication {

/**
 * TODO (mferencevic): document methods
 */

/**
 * Communication worker.
 * Listens for incomming data on connections and accepts new connections.
 * Also, executes sessions on incomming data.
 *
 * @tparam Session the worker can handle different Sessions, each session
 *         represents a different protocol so the same network infrastructure
 *         can be used for handling different protocols
 * @tparam OutputStream the worker has to get the output stream as a template
           parameter because the output stream is templated
 * @tparam Socket the input/output socket that should be used
 */
template <typename Session, typename OutputStream, typename Socket>
class Worker

    : public io::network::StreamReader<Worker<Session, OutputStream, Socket>,
                                       Session> {
  using StreamBuffer = io::network::StreamBuffer;

 public:
  using sptr = std::shared_ptr<Worker<Session, OutputStream, Socket>>;

  Worker(Dbms &dbms, QueryEngine<OutputStream> &query_engine)
      : dbms_(dbms),
        query_engine_(query_engine),
        logger_(logging::log->logger("communication::Worker")) {}

  Session &OnConnect(Socket &&socket) {
    logger_.trace("Accepting connection on socket {}", socket.id());

    // TODO fix session lifecycle handling
    // dangling pointers are not cool :)
    // TODO attach currently active Db
    return *(new Session(std::forward<Socket>(socket), dbms_, query_engine_));
  }

  void OnError(Session &session) {
    logger_.error("Error occured in this session");
    OnClose(session);
  }

  void OnWaitTimeout() {}

  void OnRead(Session &session) {
    logger_.trace("OnRead");

    try {
      session.Execute();
    } catch (const std::exception &e) {
      logger_.error("Error occured while executing statement.");
      logger_.error("{}", e.what());
      // TODO: report to client
    }
  }

  void OnClose(Session &session) {
    logger_.trace("Client closed the connection");
    // TODO: remove socket from epoll object
    session.Close();
    delete &session;
  }

  template <class... Args>
  void OnException(Session &session, Args &&... args) {
    logger_.error("Error occured in this session");
    logger_.error(args...);

    // TODO: Do something about it
  }

  std::thread thread_;

  void Start(std::atomic<bool> &alive) {
    thread_ = std::thread([&, this]() {
      while (alive) this->WaitAndProcessEvents();
    });
  }

 private:
  Dbms &dbms_;
  QueryEngine<OutputStream> &query_engine_;
  Logger logger_;
};
}
