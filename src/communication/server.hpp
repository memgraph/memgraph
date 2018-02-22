#pragma once

#include <atomic>
#include <experimental/optional>
#include <iostream>
#include <memory>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <glog/logging.h>

#include "communication/listener.hpp"
#include "io/network/socket.hpp"

namespace communication {

/**
 * Communication server.
 *
 * Listens for incoming connections on the server port and assigns them to the
 * connection listener. The listener processes the events with a thread pool
 * that has `num_workers` threads. It is started automatically on constructor,
 * and stopped at destructor.
 *
 * Current Server achitecture:
 * incoming connection -> server -> listener -> session
 *
 * @tparam TSession the server can handle different Sessions, each session
 *         represents a different protocol so the same network infrastructure
 *         can be used for handling different protocols
 * @tparam TSessionData the class with objects that will be forwarded to the
 *         session
 */
template <typename TSession, typename TSessionData>
class Server {
 public:
  using Socket = io::network::Socket;

  /**
   * Constructs and binds server to endpoint, operates on session data and
   * invokes workers_count workers
   */
  Server(const io::network::Endpoint &endpoint, TSessionData &session_data,
         bool check_for_timeouts,
         size_t workers_count = std::thread::hardware_concurrency())
      : listener_(session_data, check_for_timeouts) {
    // Without server we can't continue with application so we can just
    // terminate here.
    if (!socket_.Bind(endpoint)) {
      LOG(FATAL) << "Cannot bind to socket on " << endpoint;
    }
    socket_.SetTimeout(1, 0);
    if (!socket_.Listen(1024)) {
      LOG(FATAL) << "Cannot listen on socket!";
    }

    thread_ = std::thread([this, workers_count]() {
      std::cout << fmt::format("Starting {} workers", workers_count)
                << std::endl;
      for (size_t i = 0; i < workers_count; ++i) {
        worker_threads_.emplace_back([this]() {
          while (alive_) {
            listener_.WaitAndProcessEvents();
          }
        });
      }

      std::cout << "Server is fully armed and operational" << std::endl;
      std::cout << "Listening on " << socket_.endpoint() << std::endl;

      while (alive_) {
        AcceptConnection();
      }

      std::cout << "Shutting down..." << std::endl;
      for (auto &worker_thread : worker_threads_) {
        worker_thread.join();
      }
    });
  }

  ~Server() {
    Shutdown();
    AwaitShutdown();
  }

  Server(const Server &) = delete;
  Server(Server &&) = delete;
  Server &operator=(const Server &) = delete;
  Server &operator=(Server &&) = delete;

  const auto &endpoint() const { return socket_.endpoint(); }

  /// Stops server manually
  void Shutdown() {
    // This should be as simple as possible, so that it can be called inside a
    // signal handler.
    alive_.store(false);
    // Shutdown the socket to return from any waiting `Accept` calls.
    socket_.Shutdown();
  }

  /// Waits for the server to be signaled to shutdown
  void AwaitShutdown() {
    if (thread_.joinable()) thread_.join();
  }

 private:
  void AcceptConnection() {
    // Accept a connection from a socket.
    auto s = socket_.Accept();
    if (!s) {
      // Connection is not available anymore or configuration failed.
      return;
    }
    LOG(INFO) << "Accepted a connection from " << s->endpoint();
    listener_.AddConnection(std::move(*s));
  }

  std::atomic<bool> alive_{true};
  std::thread thread_;
  std::vector<std::thread> worker_threads_;

  Socket socket_;
  Listener<TSession, TSessionData> listener_;
};

}  // namespace communication
