// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <utility>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "requests/requests.hpp"
#include "utils/logging.hpp"

using memgraph::requests::DownloadFailure;
using memgraph::requests::DownloadToSink;

namespace {

constexpr std::size_t kBodyBytes = 64U * 1024U;
constexpr uint64_t kConnectionTimeoutSec = 5;

// Serves one request on a loopback port the kernel picks, so tests can run alongside each other.
// Whether the body follows the headers is what each test varies.
class OneShotServer {
 public:
  enum class Behaviour : std::uint8_t {
    /// Sends the whole body and closes, as a healthy server does.
    SendWholeBody,
    /// Announces a body and then sends none of it, as a server that has died mid-response does.
    AnnounceBodyThenGoSilent,
  };

  explicit OneShotServer(Behaviour behaviour) : behaviour_{behaviour} {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    MG_ASSERT(listen_fd_ != -1, "could not open a listening socket");
    int const reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    MG_ASSERT(::bind(listen_fd_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) == 0, "could not bind");
    MG_ASSERT(::listen(listen_fd_, 1) == 0, "could not listen");

    socklen_t len = sizeof(addr);
    MG_ASSERT(::getsockname(listen_fd_, reinterpret_cast<sockaddr *>(&addr), &len) == 0, "could not read the port");
    port_ = ::ntohs(addr.sin_port);

    thread_ = std::thread{[this] { Serve(); }};
  }

  ~OneShotServer() {
    stop_ = true;
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    thread_.join();
  }

  OneShotServer(OneShotServer const &) = delete;
  OneShotServer &operator=(OneShotServer const &) = delete;
  OneShotServer(OneShotServer &&) = delete;
  OneShotServer &operator=(OneShotServer &&) = delete;

  [[nodiscard]] auto Url() const -> std::string { return fmt::format("http://127.0.0.1:{}/body", port_); }

 private:
  void Serve() {
    auto const conn = ::accept(listen_fd_, nullptr, nullptr);
    if (conn == -1) return;

    // Enough of the request to know it has all arrived; the tests only ever send a plain GET.
    std::string request;
    std::array<char, 1024> buf{};
    while (request.find("\r\n\r\n") == std::string::npos) {
      auto const got = ::recv(conn, buf.data(), buf.size(), 0);
      if (got <= 0) {
        ::close(conn);
        return;
      }
      request.append(buf.data(), static_cast<std::size_t>(got));
    }

    auto const headers = fmt::format("HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n", kBodyBytes);
    ::send(conn, headers.data(), headers.size(), MSG_NOSIGNAL);

    if (behaviour_ == Behaviour::SendWholeBody) {
      std::string const body(kBodyBytes, 'x');
      ::send(conn, body.data(), body.size(), MSG_NOSIGNAL);
    } else {
      while (!stop_) {
        std::this_thread::sleep_for(std::chrono::milliseconds{50});
      }
    }
    ::close(conn);
  }

  Behaviour behaviour_;
  int listen_fd_{-1};
  uint16_t port_{0};
  std::atomic<bool> stop_{false};
  std::thread thread_;
};

}  // namespace

// Whether a transfer is making progress is a question about the server. A consumer holding the sink
// is not a stalled transfer, and ending the download because of one turns a load that would have
// finished into a failure that reports itself as worth retrying.
TEST(TransferStall, AConsumerThatPausesForLongerThanTheStallWindowDoesNotEndTheTransfer) {
  OneShotServer server{OneShotServer::Behaviour::SendWholeBody};

  auto paused = false;
  std::size_t received = 0;
  auto const sink = [&paused, &received](char const * /*data*/, std::size_t const size) -> std::size_t {
    if (!std::exchange(paused, true)) {
      std::this_thread::sleep_for(std::chrono::seconds{11});
    }
    received += size;
    return size;
  };

  auto const result = DownloadToSink(server.Url(), sink, kConnectionTimeoutSec);

  ASSERT_TRUE(result.has_value()) << "a paused consumer ended the transfer: " << result.error().message;
  EXPECT_EQ(received, kBodyBytes) << "the whole body should still have been delivered";
}

// The case the stall window exists for: a server that accepted the request and then stopped sending
// leaves nothing to wait for, and the caller has to be told rather than left blocked.
TEST(TransferStall, AServerThatStopsSendingEndsTheTransfer) {
  OneShotServer server{OneShotServer::Behaviour::AnnounceBodyThenGoSilent};

  auto const sink = [](char const * /*data*/, std::size_t const size) -> std::size_t { return size; };

  constexpr uint64_t kStallWindowSec = 2;
  auto const started = std::chrono::steady_clock::now();
  auto const result = DownloadToSink(server.Url(), sink, kConnectionTimeoutSec, nullptr, kStallWindowSec);
  auto const elapsed = std::chrono::steady_clock::now() - started;

  ASSERT_FALSE(result.has_value()) << "a body that never arrives is not a completed download";
  EXPECT_EQ(result.error().kind, DownloadFailure::Stalled);
  EXPECT_TRUE(result.error().Retryable()) << "the same request may well be served next time";
  EXPECT_LT(elapsed, std::chrono::seconds{30}) << "the transfer should end on the stall window it was given";
}
