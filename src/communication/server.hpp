#pragma once

#include <atomic>
#include <experimental/optional>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <glog/logging.h>

#include "communication/worker.hpp"
#include "io/network/socket_event_dispatcher.hpp"
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
 * @tparam Socket the input/output socket that should be used
 * @tparam SessionData the class with objects that will be forwarded to the
 *         session
 */
// TODO: Remove Socket templatisation. Socket requirements are very specific.
// It needs to be in non blocking mode, etc.
template <typename Session, typename Socket, typename SessionData>
class Server {
 public:
  using worker_t = Worker<Session, Socket, SessionData>;

  Server(Socket &&socket, SessionData &session_data)
      : socket_(std::move(socket)), session_data_(session_data) {}

  void Start(size_t n) {
    std::cout << fmt::format("Starting {} workers", n) << std::endl;
    workers_.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      workers_.push_back(std::make_unique<worker_t>(session_data_));
      worker_threads_.emplace_back(
          [this](worker_t &worker) -> void { worker.Start(alive_); },
          std::ref(*workers_.back()));
    }
    std::cout << "Server is fully armed and operational" << std::endl;
    std::cout << fmt::format("Listening on {} at {}",
                             socket_.endpoint().address(),
                             socket_.endpoint().port())
              << std::endl;

    io::network::SocketEventDispatcher<ConnectionAcceptor> dispatcher;
    ConnectionAcceptor acceptor(socket_, *this);
    dispatcher.AddListener(socket_.fd(), acceptor, EPOLLIN);
    while (alive_) {
      dispatcher.WaitAndProcessEvents();
    }

    std::cout << "Shutting down..." << std::endl;
    for (auto &worker_thread : worker_threads_) {
      worker_thread.join();
    }
  }

  void Shutdown() {
    // This should be as simple as possible, so that it can be called inside a
    // signal handler.
    alive_.store(false);
  }

 private:
  class ConnectionAcceptor : public io::network::BaseListener {
   public:
    ConnectionAcceptor(Socket &socket,
                       Server<Session, Socket, SessionData> &server)
        : io::network::BaseListener(socket), server_(server) {}

    void OnData() {
      DCHECK(server_.idx_ < server_.workers_.size()) << "Invalid worker id.";
      DLOG(INFO) << "On connect";
      auto connection = AcceptConnection();
      if (UNLIKELY(!connection)) {
        // Connection is not available anymore or configuration failed.
        return;
      }
      server_.workers_[server_.idx_]->AddConnection(std::move(*connection));
      server_.idx_ = (server_.idx_ + 1) % server_.workers_.size();
    }

   private:
    // Accepts connection on socket_ and configures new connections. If done
    // successfuly new socket (connection) is returner, nullopt otherwise.
    std::experimental::optional<Socket> AcceptConnection() {
      DLOG(INFO) << "Accept new connection on socket: " << socket_.fd();

      // Accept a connection from a socket.
      auto s = socket_.Accept();
      if (!s) return std::experimental::nullopt;

      DLOG(INFO) << fmt::format(
          "Accepted a connection: socket {}, address '{}', family {}, port {}",
          s->fd(), s->endpoint().address(), s->endpoint().family(),
          s->endpoint().port());

      if (!s->SetKeepAlive()) return std::experimental::nullopt;
      if (!s->SetNoDelay()) return std::experimental::nullopt;
      return s;
    }

    Server<Session, Socket, SessionData> &server_;
  };

  std::vector<std::unique_ptr<worker_t>> workers_;
  std::vector<std::thread> worker_threads_;
  std::atomic<bool> alive_{true};
  int idx_{0};

  Socket socket_;
  SessionData &session_data_;
};

}  // namespace communication
