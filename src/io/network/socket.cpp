// Copyright 2025 Memgraph Ltd.
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
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>

#include "io/network/addrinfo.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/network_error.hpp"
#include "io/network/socket.hpp"
#include "utils/event_histogram.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"
#include "utils/metrics_timer.hpp"

namespace {
constexpr int timeout_ms = 5000;
}  // namespace

namespace memgraph::metrics {
extern const Event SocketConnect_us;
}  // namespace memgraph::metrics

namespace memgraph::io::network {

Socket::Socket(Socket &&other) noexcept : socket_(other.socket_), endpoint_(std::move(other.endpoint_)) {
  other.socket_ = -1;
}

Socket &Socket::operator=(Socket &&other) noexcept {
  if (this != &other) {
    if (socket_ != -1) close(socket_);
    socket_ = other.socket_;
    endpoint_ = std::move(other.endpoint_);
    other.socket_ = -1;
  }
  return *this;
}

Socket::~Socket() noexcept { Close(); }

void Socket::Close() noexcept {
  if (socket_ == -1) return;
  Close(socket_, endpoint_.SocketAddress());
  socket_ = -1;
}

// Not const because of C-API
// NOLINTNEXTLINE
void Socket::Shutdown() {
  if (socket_ == -1) return;
  shutdown(socket_, SHUT_RDWR);
}

bool Socket::IsOpen() const { return socket_ != -1; }

void Socket::Close(int const sfd, std::string_view socket_addr) {
  if (close(sfd) != 0) {
    int err_sc = errno;
    spdlog::error("Failed to close fd for {}. Errno: {}", socket_addr, err_sc);
  }
}

bool Socket::Connect(const Endpoint &endpoint) {
  if (socket_ != -1) {
    spdlog::trace("Socket::Connect failed, socket_ not ready!");
    return false;
  }

  auto const socket_addr = endpoint.SocketAddress();

  try {
    for (const auto &it : AddrInfo{endpoint}) {
      utils::MetricsTimer const timer{metrics::SocketConnect_us};

      int const sfd = socket(it.ai_family, it.ai_socktype, it.ai_protocol);
      if (sfd == -1) {
        spdlog::trace("Socket creation failed while connecting to {}. File descriptor is -1", socket_addr);
        continue;
      }

      const int sockfd_flags_orig = fcntl(sfd, F_GETFL, 0);
      if (sockfd_flags_orig < 0) {
        spdlog::error("Failed to read file status and file access mode during connect");
        Close(sfd, socket_addr);
        continue;
      }

      if (fcntl(sfd, F_SETFL, sockfd_flags_orig | O_NONBLOCK) < 0) {
        spdlog::error("Failed to set socket to non-blocking mode during connect.");
        Close(sfd, socket_addr);
        continue;
      }

      auto async_connect = [&](int const fd, const sockaddr *addr, socklen_t addrlen) -> bool {
        // if connect succeeded
        if (connect(fd, addr, addrlen) == 0) {
          return true;
        }

        // Failure which we don't handle
        if (errno != EINPROGRESS && errno != EWOULDBLOCK) {
          spdlog::error("Failed to connect to socket with err code {}", errno);
          return false;
        }

        timespec now{};
        if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) {
          spdlog::error("Failed to get monotonic time from clock_gettime");
          return false;
        }

        int64_t const deadline_ns = static_cast<int64_t>(now.tv_sec) * 1'000'000'000 + now.tv_nsec +
                                    static_cast<int64_t>(timeout_ms) * 1'000'000;

        while (true) {
          if (clock_gettime(CLOCK_MONOTONIC, &now) < 0) {
            spdlog::error("Failed to get monotonic time from clock_gettime");
            return false;
          }

          int64_t const now_ns = static_cast<int64_t>(now.tv_sec) * 1'000'000'000 + now.tv_nsec;
          int const ms_remaining = static_cast<int>((deadline_ns - now_ns) / 1'000'000);
          if (ms_remaining <= 0) {
            errno = ETIMEDOUT;
            return false;
          }

          pollfd pfds[] = {{.fd = fd, .events = POLLOUT}};
          int const poll_status = poll(pfds, 1, ms_remaining);

          // Socket is ready, likely writeable
          if (poll_status > 0) {
            int error = 0;
            socklen_t len = sizeof(error);

            // Check if the connection was successful
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) == 0 && error == 0) {
              return true;
            }

            // Update errno if the connection wasn't successful
            errno = error;
            return false;
          }

          // Poll status of 0 indicated timeout
          if (poll_status == 0) {
            errno = ETIMEDOUT;
            return false;
          }

          // A signal occurred before any requested event
          if (errno != EINTR) {
            return false;
          }
        }
      };

      if (!async_connect(sfd, it.ai_addr, it.ai_addrlen)) {
        Close(sfd, socket_addr);
        continue;
      }

      if (fcntl(sfd, F_SETFL, sockfd_flags_orig) < 0) {
        spdlog::error("Failed to set socket to blocking mode during connect");
        Close(sfd, socket_addr);
        continue;
      }

      // Success
      socket_ = sfd;
      endpoint_ = endpoint;
      break;
    }
  } catch (const NetworkError &e) {
    spdlog::trace("Error occurred while connecting to {}. Error: {}", endpoint.SocketAddress(), e.what());
    return false;
  }

  return socket_ != -1;
}

bool Socket::Bind(const Endpoint &endpoint) {
  if (socket_ != -1) {
    spdlog::trace("Socket::Bind failed, socket_ not ready!");
    return false;
  }

  auto const socket_addr = endpoint.SocketAddress();

  try {
    for (const auto &it : AddrInfo{endpoint}) {
      int sfd = socket(it.ai_family, it.ai_socktype, it.ai_protocol);
      if (sfd == -1) {
        spdlog::trace("Socket creation failed in Socket::Bind for socket address {}. File descriptor is -1",
                      endpoint.SocketAddress());
        continue;
      }

      int on = 1;
      if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
        spdlog::trace("setsockopt in Socket::Bind failed for socket address {}", socket_addr);
        // If the setsockopt failed close the file descriptor to prevent file
        // descriptors being leaked
        if (close(sfd) != 0) {
          int err_sc = errno;
          spdlog::error(
              "Failed to close fd for {}. Closing started because 'setsockopt' failed while binding. Errno: {}",
              socket_addr, err_sc);
        }
        continue;
      }

      if (bind(sfd, it.ai_addr, it.ai_addrlen) == 0) {
        socket_ = sfd;
        break;
      }
      // If the bind failed close the file descriptor to prevent file
      // descriptors being leaked
      spdlog::trace("Socket::Bind failed. Closing file descriptor for socket address {}", endpoint.SocketAddress());
      if (close(sfd) != 0) {
        int err_sc = errno;
        spdlog::error("Failed to close fd for {} while trying to bind. Errno: {}", socket_addr, err_sc);
      }
    }
  } catch (NetworkError const &e) {
    spdlog::trace("Error happened while trying to bind to {}. Error: {}", endpoint.SocketAddress(), e.what());
    return false;
  }

  if (socket_ == -1) {
    spdlog::trace("Socket::Bind failed. socket_ is -1 for socket address {}", endpoint.SocketAddress());
    return false;
  }

  // detect bound port, used when the server binds to a random port
  struct sockaddr_in6 portdata;
  socklen_t portdatalen = sizeof(portdata);
  if (getsockname(socket_, reinterpret_cast<sockaddr *>(&portdata), &portdatalen) < 0) {
    // If the getsockname failed close the file descriptor to prevent file
    // descriptors being leaked
    if (close(socket_) != 0) {
      int err_sc = errno;
      spdlog::error("Failed to close fd for {}. Closing started because 'getsockname' failed. Errno: {}", socket_addr,
                    err_sc);
    }
    socket_ = -1;
    spdlog::trace("Socket::Bind failed. getsockname failed, closing file descriptor for socket address {}",
                  endpoint.SocketAddress());
    return false;
  }

  endpoint_ = Endpoint(endpoint.GetAddress(), ntohs(portdata.sin6_port));

  return true;
}

// Not const because of C-API
// NOLINTNEXTLINE
void Socket::SetNonBlocking() {
  const unsigned flags = fcntl(socket_, F_GETFL);
  constexpr unsigned o_nonblock = O_NONBLOCK;
  MG_ASSERT(flags != -1, "Can't get socket mode");
  MG_ASSERT(fcntl(socket_, F_SETFL, flags | o_nonblock) != -1, "Can't set socket nonblocking");
}

// Not const because of C-API
// NOLINTNEXTLINE
void Socket::SetKeepAlive() {
  int optval = 1;
  MG_ASSERT(!setsockopt(socket_, SOL_SOCKET, SO_KEEPALIVE, &optval, sizeof(optval)), "Can't set socket keep alive");

  optval = 20;  // wait 20s before sending keep-alive packets
  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_KEEPIDLE, (void *)&optval, sizeof(optval)),
            "Can't set socket keep alive");

  optval = 4;  // 4 keep-alive packets must fail to close
  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_KEEPCNT, (void *)&optval, sizeof(optval)), "Can't set socket keep alive");

  optval = 15;  // send keep-alive packets every 15s
  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_KEEPINTVL, (void *)&optval, sizeof(optval)),
            "Can't set socket keep alive");
}

// Not const because of C-API
// NOLINTNEXTLINE
void Socket::SetNoDelay() {
  int optval = 1;
  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_NODELAY, (void *)&optval, sizeof(optval)), "Can't set socket no delay");
}

// NOLINTNEXTLINE(readability-make-member-function-const)
void Socket::SetTimeout(int64_t sec, int64_t usec) {
  struct timeval tv;
  tv.tv_sec = sec;
  tv.tv_usec = usec;

  MG_ASSERT(!setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)), "Can't set socket timeout");

  MG_ASSERT(!setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)), "Can't set socket timeout");
}

int Socket::ErrorStatus() const {
  int optval = 0;
  socklen_t optlen = sizeof(optval);
  auto status = getsockopt(socket_, SOL_SOCKET, SO_ERROR, &optval, &optlen);
  MG_ASSERT(!status, "getsockopt failed");
  return optval;
}

// Not const because of C-API
// NOLINTNEXTLINE
bool Socket::Listen(int backlog) { return listen(socket_, backlog) == 0; }

// Not const because of C-API
// NOLINTNEXTLINE
std::optional<Socket> Socket::Accept() {
  sockaddr_storage addr;
  socklen_t addr_size = sizeof addr;
  char addr_decoded[INET6_ADDRSTRLEN];

  int sfd = accept(socket_, reinterpret_cast<sockaddr *>(&addr), &addr_size);
  if (UNLIKELY(sfd == -1)) return std::nullopt;

  void *addr_src = nullptr;
  uint16_t port = 0;

  if (addr.ss_family == AF_INET) {
    addr_src = &reinterpret_cast<sockaddr_in &>(addr).sin_addr;
    port = ntohs(reinterpret_cast<sockaddr_in &>(addr).sin_port);
  } else {
    addr_src = &reinterpret_cast<sockaddr_in6 &>(addr).sin6_addr;
    port = ntohs(reinterpret_cast<sockaddr_in6 &>(addr).sin6_port);
  }

  inet_ntop(addr.ss_family, addr_src, addr_decoded, sizeof(addr_decoded));

  Endpoint const endpoint(addr_decoded, port);

  return Socket(sfd, endpoint);
}

// Not const because of C-API
// NOLINTNEXTLINE
bool Socket::Write(const uint8_t *data, size_t len, bool have_more, std::optional<int> timeout_ms) {
  // MSG_NOSIGNAL is here to disable raising a SIGPIPE signal when a
  // connection dies mid-write, the socket will only return an EPIPE error.
  constexpr unsigned msg_nosignal = MSG_NOSIGNAL;
  constexpr unsigned msg_more = MSG_MORE;
  const unsigned flags = msg_nosignal | (have_more ? msg_more : 0);
  while (len > 0) {
    auto written = send(socket_, data, len, static_cast<int>(flags));
    if (written == -1) {
      if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        // Terminal error, return failure.
        return false;
      }
      // Non-fatal error, retry after the socket is ready. This is here to
      // implement a non-busy wait. If we just continue with the loop we have a
      // busy wait.
      if (!WaitForReadyWrite(timeout_ms)) return false;
    } else if (written == 0) {
      // The client closed the connection.
      return false;
    } else {
      len -= written;
      data += written;
    }
  }
  return true;
}

bool Socket::Write(std::string_view s, bool have_more, std::optional<int> timeout_ms) {
  return Write(reinterpret_cast<const uint8_t *>(s.data()), s.size(), have_more, timeout_ms);
}

// Not const because of C-API
// NOLINTNEXTLINE
ssize_t Socket::Read(void *buffer, size_t len, bool nonblock) {
  return recv(socket_, buffer, len, nonblock ? MSG_DONTWAIT : 0);
}

bool Socket::WaitForReadyRead(std::optional<int> timeout_ms) const {
  struct pollfd p;
  p.fd = socket_;
  p.events = POLLIN;
  // We call poll with one element in the poll fds array (first and second
  // arguments), also we set the timeout to -1 to block indefinitely until an
  // event occurs.

  // -1 for blocking indefinitely, otherwise wait for timeout_ms.
  int const timeout = timeout_ms ? *timeout_ms : -1;
  int const ret = poll(&p, 1, timeout);
  if (ret == -1) {
    spdlog::error("Error occurred while polling for file descriptors.");
    return false;
  }
  if (ret == 0) {
    spdlog::error("Waiting too long to get in ready state for reading. Timeout occurred.");
    return false;
  }

  constexpr unsigned pollin = POLLIN;
  return static_cast<unsigned>(p.revents) & pollin;
}

bool Socket::WaitForReadyWrite(std::optional<int> timeout_ms) const {
  struct pollfd p;
  p.fd = socket_;
  p.events = POLLOUT;
  // We call poll with one element in the poll fds array (first and second
  // arguments), also we set the timeout to -1 to block indefinitely until an
  // event occurs.

  // -1 for blocking indefinitely, otherwise wait for timeout_ms.
  int const timeout = timeout_ms ? *timeout_ms : -1;
  int const ret = poll(&p, 1, timeout);
  if (ret == -1) {
    spdlog::error("Error occurred while polling for file descriptors.");
    return false;
  }
  if (ret == 0) {
    spdlog::error("Waiting too long to get in ready state for writing. Timeout occurred.");
    return false;
  }
  constexpr unsigned pollout = POLLOUT;
  return static_cast<unsigned>(p.revents) & pollout;
}

}  // namespace memgraph::io::network
