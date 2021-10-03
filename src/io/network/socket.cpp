// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "io/network/socket.hpp"

#include <cstdio>
#include <cstring>
#include <iostream>
#include <stdexcept>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "io/network/addrinfo.hpp"
#include "utils/likely.hpp"
#include "utils/logging.hpp"

namespace io::network {

Socket::Socket(Socket &&other) {
  socket_ = other.socket_;
  endpoint_ = std::move(other.endpoint_);
  other.socket_ = -1;
}

Socket &Socket::operator=(Socket &&other) {
  if (this != &other) {
    socket_ = other.socket_;
    endpoint_ = std::move(other.endpoint_);
    other.socket_ = -1;
  }
  return *this;
}

Socket::~Socket() {
  if (socket_ == -1) return;
  close(socket_);
}

void Socket::Close() {
  if (socket_ == -1) return;
  close(socket_);
  socket_ = -1;
}

void Socket::Shutdown() {
  if (socket_ == -1) return;
  shutdown(socket_, SHUT_RDWR);
}

bool Socket::IsOpen() const { return socket_ != -1; }

bool Socket::Connect(const Endpoint &endpoint) {
  if (socket_ != -1) return false;

  auto info = AddrInfo::Get(endpoint.address.c_str(), std::to_string(endpoint.port).c_str());

  for (struct addrinfo *it = info; it != nullptr; it = it->ai_next) {
    int sfd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (sfd == -1) continue;
    if (connect(sfd, it->ai_addr, it->ai_addrlen) == 0) {
      socket_ = sfd;
      endpoint_ = endpoint;
      break;
    } else {
      // If the connect failed close the file descriptor to prevent file
      // descriptors being leaked
      close(sfd);
    }
  }

  if (socket_ == -1) return false;
  return true;
}

bool Socket::Bind(const Endpoint &endpoint) {
  if (socket_ != -1) return false;

  auto info = AddrInfo::Get(endpoint.address.c_str(), std::to_string(endpoint.port).c_str());

  for (struct addrinfo *it = info; it != nullptr; it = it->ai_next) {
    int sfd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (sfd == -1) continue;

    int on = 1;
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0) {
      // If the setsockopt failed close the file descriptor to prevent file
      // descriptors being leaked
      close(sfd);
      continue;
    }

    if (bind(sfd, it->ai_addr, it->ai_addrlen) == 0) {
      socket_ = sfd;
      break;
    } else {
      // If the bind failed close the file descriptor to prevent file
      // descriptors being leaked
      close(sfd);
    }
  }

  if (socket_ == -1) return false;

  // detect bound port, used when the server binds to a random port
  struct sockaddr_in6 portdata;
  socklen_t portdatalen = sizeof(portdata);
  if (getsockname(socket_, (struct sockaddr *)&portdata, &portdatalen) < 0) {
    // If the getsockname failed close the file descriptor to prevent file
    // descriptors being leaked
    close(socket_);
    socket_ = -1;
    return false;
  }

  endpoint_ = Endpoint(endpoint.address, ntohs(portdata.sin6_port));

  return true;
}

void Socket::SetNonBlocking() {
  int flags = fcntl(socket_, F_GETFL, 0);
  MG_ASSERT(flags != -1, "Can't get socket mode");
  flags |= O_NONBLOCK;
  MG_ASSERT(fcntl(socket_, F_SETFL, flags) != -1, "Can't set socket nonblocking");
}

void Socket::SetKeepAlive() {
  int optval = 1;
  socklen_t optlen = sizeof(optval);

  MG_ASSERT(!setsockopt(socket_, SOL_SOCKET, SO_KEEPALIVE, &optval, optlen), "Can't set socket keep alive");

  optval = 20;  // wait 20s before sending keep-alive packets
  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_KEEPIDLE, (void *)&optval, optlen), "Can't set socket keep alive");

  optval = 4;  // 4 keep-alive packets must fail to close
  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_KEEPCNT, (void *)&optval, optlen), "Can't set socket keep alive");

  optval = 15;  // send keep-alive packets every 15s
  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_KEEPINTVL, (void *)&optval, optlen), "Can't set socket keep alive");
}

void Socket::SetNoDelay() {
  int optval = 1;
  socklen_t optlen = sizeof(optval);

  MG_ASSERT(!setsockopt(socket_, SOL_TCP, TCP_NODELAY, (void *)&optval, optlen), "Can't set socket no delay");
}

void Socket::SetTimeout(long sec, long usec) {
  struct timeval tv;
  tv.tv_sec = sec;
  tv.tv_usec = usec;

  MG_ASSERT(!setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)), "Can't set socket timeout");

  MG_ASSERT(!setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)), "Can't set socket timeout");
}

int Socket::ErrorStatus() const {
  int optval;
  socklen_t optlen = sizeof(optval);
  auto status = getsockopt(socket_, SOL_SOCKET, SO_ERROR, &optval, &optlen);
  MG_ASSERT(!status, "getsockopt failed");
  return optval;
}

bool Socket::Listen(int backlog) { return listen(socket_, backlog) == 0; }

std::optional<Socket> Socket::Accept() {
  sockaddr_storage addr;
  socklen_t addr_size = sizeof addr;
  char addr_decoded[INET6_ADDRSTRLEN];
  void *addr_src;
  unsigned short port;

  int sfd = accept(socket_, (struct sockaddr *)&addr, &addr_size);
  if (UNLIKELY(sfd == -1)) return std::nullopt;

  if (addr.ss_family == AF_INET) {
    addr_src = (void *)&(((sockaddr_in *)&addr)->sin_addr);
    port = ntohs(((sockaddr_in *)&addr)->sin_port);
  } else {
    addr_src = (void *)&(((sockaddr_in6 *)&addr)->sin6_addr);
    port = ntohs(((sockaddr_in6 *)&addr)->sin6_port);
  }

  inet_ntop(addr.ss_family, addr_src, addr_decoded, INET6_ADDRSTRLEN);

  Endpoint endpoint(addr_decoded, port);

  return Socket(sfd, endpoint);
}

bool Socket::Write(const uint8_t *data, size_t len, bool have_more) {
  // MSG_NOSIGNAL is here to disable raising a SIGPIPE signal when a
  // connection dies mid-write, the socket will only return an EPIPE error.
  int flags = MSG_NOSIGNAL | (have_more ? MSG_MORE : 0);
  while (len > 0) {
    auto written = send(socket_, data, len, flags);
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

bool Socket::Write(const std::string &s, bool have_more) {
  return Write(reinterpret_cast<const uint8_t *>(s.data()), s.size(), have_more);
}

ssize_t Socket::Read(void *buffer, size_t len, bool nonblock) {
  return recv(socket_, buffer, len, nonblock ? MSG_DONTWAIT : 0);
}

bool Socket::WaitForReadyRead() {
  struct pollfd p;
  p.fd = socket_;
  p.events = POLLIN;
  // We call poll with one element in the poll fds array (first and second
  // arguments), also we set the timeout to -1 to block indefinitely until an
  // event occurs.
  int ret = poll(&p, 1, -1);
  if (ret < 1) return false;
  return p.revents & POLLIN;
}

bool Socket::WaitForReadyWrite() {
  struct pollfd p;
  p.fd = socket_;
  p.events = POLLOUT;
  // We call poll with one element in the poll fds array (first and second
  // arguments), also we set the timeout to -1 to block indefinitely until an
  // event occurs.
  int ret = poll(&p, 1, -1);
  if (ret < 1) return false;
  return p.revents & POLLOUT;
}

}  // namespace io::network
