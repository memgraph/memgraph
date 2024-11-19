// Copyright 2024 Memgraph Ltd.
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
#include "utils/likely.hpp"
#include "utils/logging.hpp"

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

Socket::~Socket() noexcept {
  if (socket_ != -1) close(socket_);
}

void Socket::Close() {
  if (socket_ == -1) return;
  close(socket_);
  socket_ = -1;
}

void Socket::Shutdown() const {
  if (socket_ == -1) return;
  shutdown(socket_, SHUT_RDWR);
}

bool Socket::IsOpen() const { return socket_ != -1; }

bool Socket::Connect(const Endpoint &endpoint) {
  if (socket_ != -1) {
    spdlog::trace("Socket::Connect failed, socket_ not ready!");
    return false;
  }

  try {
    for (const auto &it : AddrInfo{endpoint}) {
      int sfd = socket(it.ai_family, it.ai_socktype, it.ai_protocol);
      if (sfd == -1) {
        spdlog::trace("Socket creation failed in Socket::Connect for socket address {}. File descriptor is -1",
                      endpoint.SocketAddress());
        continue;
      }
      if (connect(sfd, it.ai_addr, it.ai_addrlen) == 0) {
        socket_ = sfd;
        endpoint_ = endpoint;
        spdlog::trace("Successfully connected to socket in Socket::Connect for socket address {}",
                      endpoint.SocketAddress());
        break;
      }
      spdlog::trace("Connect failed, closing file descriptor in Socket::Connect for socket address {}",
                    endpoint.SocketAddress());
      // If the connect failed close the file descriptor to prevent file
      // descriptors being leaked
      close(sfd);
    }
  } catch (const NetworkError &e) {
    spdlog::trace("Error in Socket::Connect {}", e.trace());
    return false;
  }

  return !(socket_ == -1);
}

bool Socket::Bind(const Endpoint &endpoint) {
  if (socket_ != -1) {
    spdlog::trace("Socket::Bind failed, socket_ not ready!");
    return false;
  }

  for (const auto &it : AddrInfo{endpoint}) {
    int sfd = socket(it.ai_family, it.ai_socktype, it.ai_protocol);
    if (sfd == -1) {
      spdlog::trace("Socket creation failed in Socket::Bind for socket address {}. File descriptor is -1",
                    endpoint.SocketAddress());
      continue;
    }

    int on = 1;
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
      spdlog::trace("setsockopt in Socket::Bind failed for socket address {}", endpoint.SocketAddress());
      // If the setsockopt failed close the file descriptor to prevent file
      // descriptors being leaked
      close(sfd);
      continue;
    }

    if (bind(sfd, it.ai_addr, it.ai_addrlen) == 0) {
      socket_ = sfd;
      break;
    }
    // If the bind failed close the file descriptor to prevent file
    // descriptors being leaked
    spdlog::trace("Socket::Bind failed. Closing file descriptor for socket address {}", endpoint.SocketAddress());
    close(sfd);
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
    close(socket_);
    socket_ = -1;
    spdlog::trace("Socket::Bind failed. getsockname failed, closing file descriptor for socket address {}",
                  endpoint.SocketAddress());
    return false;
  }

  // TODO: (andi) This runs again resolving, shouldn't be needed.
  endpoint_ = Endpoint(endpoint.GetResolvedIPAddress(), ntohs(portdata.sin6_port));

  return true;
}

void Socket::SetNonBlocking() const {
  const unsigned flags = fcntl(socket_, F_GETFL);
  constexpr unsigned o_nonblock = O_NONBLOCK;
  MG_ASSERT(flags != -1, "Can't get socket mode");
  MG_ASSERT(fcntl(socket_, F_SETFL, flags | o_nonblock) != -1, "Can't set socket nonblocking");
}

void Socket::SetKeepAlive() const {
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

void Socket::SetNoDelay() const {
  int optval = 1;
  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_NODELAY, (void *)&optval, sizeof(optval)), "Can't set socket no delay");
}

void Socket::HandleErrno(int err_code) {
  switch (err_code) {
    case EBADF:
      spdlog::error("The socket argument is not a valid file descriptor.");
      break;
    case EDOM:
      spdlog::error(
          "The send and receive timeout values are too big to fit into the timeout fields in the socket structure.");
      break;
    case EINVAL:
      spdlog::error("The specified option is invalid at the specified socket level or the socket has been shut down.");
      break;
    case EISCONN:
      spdlog::error(
          "The socket is already connected, and a specified option cannot be set while the socket is connected.");
      break;
    case ENOPROTOOPT:
      spdlog::error("The option is not supported by the protocol.");
      break;
    case ENOTSOCK:
      spdlog::error("The socket argument does not refer to a socket.");
      break;
    case ENOMEM:
      spdlog::error("There was insufficient memory available for the operation to complete.");
      break;
    case ENOBUFS:
      spdlog::error("Insufficient resources are available in the system to complete the call.");
      break;
    default:
      spdlog::error("Unknown error code.");
      break;
  }
  MG_ASSERT(false);
}

// NOLINTNEXTLINE(readability-make-member-function-const)
void Socket::SetTimeout(int64_t sec, int64_t usec) {
  struct timeval tv;
  tv.tv_sec = sec;
  tv.tv_usec = usec;

  spdlog::trace("Trying to set timeouts.");

  if (setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == -1) {
    int const err_code = errno;
    HandleErrno(err_code);
  }
  spdlog::trace("Set rcv timeout.");

  if (setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) == -1) {
    int const err_code = errno;
    HandleErrno(err_code);
  }
  spdlog::trace("Set snd timeout.");
}

int Socket::ErrorStatus() const {
  int optval = 0;
  socklen_t optlen = sizeof(optval);
  auto status = getsockopt(socket_, SOL_SOCKET, SO_ERROR, &optval, &optlen);
  MG_ASSERT(!status, "getsockopt failed");
  return optval;
}

bool Socket::Listen(int backlog) const { return listen(socket_, backlog) == 0; }

std::optional<Socket> Socket::Accept() const {
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

  Endpoint endpoint(addr_decoded, port);

  return Socket(sfd, endpoint);
}

bool Socket::Write(const uint8_t *data, size_t len, bool have_more) const {
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
      if (!WaitForReadyWrite()) return false;
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

bool Socket::Write(std::string_view s, bool have_more) const {
  return Write(reinterpret_cast<const uint8_t *>(s.data()), s.size(), have_more);
}

ssize_t Socket::Read(void *buffer, size_t len, bool nonblock) const {
  return recv(socket_, buffer, len, nonblock ? MSG_DONTWAIT : 0);
}

bool Socket::WaitForReadyRead() const {
  struct pollfd p;
  p.fd = socket_;
  p.events = POLLIN;
  // We call poll with one element in the poll fds array (first and second
  // arguments), also we set the timeout to -1 to block indefinitely until an
  // event occurs.
  int ret = poll(&p, 1, -1);
  if (ret < 1) return false;
  constexpr unsigned pollin = POLLIN;
  return static_cast<unsigned>(p.revents) & pollin;
}

bool Socket::WaitForReadyWrite() const {
  struct pollfd p;
  p.fd = socket_;
  p.events = POLLOUT;
  // We call poll with one element in the poll fds array (first and second
  // arguments), also we set the timeout to -1 to block indefinitely until an
  // event occurs.
  int ret = poll(&p, 1, -1);
  if (ret < 1) return false;
  constexpr unsigned pollout = POLLOUT;
  return static_cast<unsigned>(p.revents) & pollout;
}

}  // namespace memgraph::io::network
