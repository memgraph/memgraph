#include "io/network/socket.hpp"

#include <cassert>
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
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "io/network/addrinfo.hpp"
#include "utils/likely.hpp"

namespace io::network {

Socket::Socket() : socket_(-1) {}

Socket::Socket(int sock, const NetworkEndpoint &endpoint)
    : socket_(sock), endpoint_(endpoint) {}

Socket::Socket(const Socket &s) : socket_(s.id()) {}

Socket::Socket(Socket &&other) { *this = std::forward<Socket>(other); }

Socket &Socket::operator=(Socket &&other) {
  socket_ = other.socket_;
  endpoint_ = other.endpoint_;
  other.socket_ = -1;
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

bool Socket::IsOpen() { return socket_ != -1; }

bool Socket::Connect(const NetworkEndpoint &endpoint) {
  if (UNLIKELY(socket_ != -1)) return false;

  auto info = AddrInfo::Get(endpoint.address(), endpoint.port_str());

  for (struct addrinfo *it = info; it != nullptr; it = it->ai_next) {
    int sfd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (sfd == -1) continue;
    if (connect(sfd, it->ai_addr, it->ai_addrlen) == 0) {
      socket_ = sfd;
      endpoint_ = endpoint;
      break;
    }
  }

  if (socket_ == -1) return false;
  return true;
}

bool Socket::Bind(const NetworkEndpoint &endpoint) {
  if (UNLIKELY(socket_ != -1)) return false;

  auto info = AddrInfo::Get(endpoint.address(), endpoint.port_str());

  for (struct addrinfo *it = info; it != nullptr; it = it->ai_next) {
    int sfd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (sfd == -1) continue;

    int on = 1;
    if (setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) != 0)
      continue;

    if (bind(sfd, it->ai_addr, it->ai_addrlen) == 0) {
      socket_ = sfd;
      break;
    }
  }

  if (socket_ == -1) return false;

  // detect bound port, used when the server binds to a random port
  struct sockaddr_in6 portdata;
  socklen_t portdatalen = sizeof(portdata);
  if (getsockname(socket_, (struct sockaddr *)&portdata, &portdatalen) < 0) {
    return false;
  }

  endpoint_ = NetworkEndpoint(endpoint.address(), ntohs(portdata.sin6_port));

  return true;
}

bool Socket::SetNonBlocking() {
  int flags = fcntl(socket_, F_GETFL, 0);

  if (UNLIKELY(flags == -1)) return false;

  flags |= O_NONBLOCK;
  int ret = fcntl(socket_, F_SETFL, flags);

  if (UNLIKELY(ret == -1)) return false;
  return true;
}

bool Socket::SetKeepAlive() {
  int optval = 1;
  socklen_t optlen = sizeof(optval);

  if (setsockopt(socket_, SOL_SOCKET, SO_KEEPALIVE, &optval, optlen) < 0)
    return false;

  optval = 20;  // wait 120s before seding keep-alive packets
  if (setsockopt(socket_, SOL_TCP, TCP_KEEPIDLE, (void *)&optval, optlen) < 0)
    return false;

  optval = 4;  // 4 keep-alive packets must fail to close
  if (setsockopt(socket_, SOL_TCP, TCP_KEEPCNT, (void *)&optval, optlen) < 0)
    return false;

  optval = 15;  // send keep-alive packets every 15s
  if (setsockopt(socket_, SOL_TCP, TCP_KEEPINTVL, (void *)&optval, optlen) < 0)
    return false;

  return true;
}

bool Socket::SetNoDelay() {
  int optval = 1;
  socklen_t optlen = sizeof(optval);

  if (setsockopt(socket_, SOL_TCP, TCP_NODELAY, (void *)&optval, optlen) < 0)
    return false;

  return true;
}

bool Socket::SetTimeout(long sec, long usec) {
  struct timeval tv;
  tv.tv_sec = sec;
  tv.tv_usec = usec;

  if (setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0)
    return false;

  if (setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) < 0)
    return false;

  return true;
}

bool Socket::Listen(int backlog) { return listen(socket_, backlog) == 0; }

bool Socket::Accept(Socket *s) {
  sockaddr_storage addr;
  socklen_t addr_size = sizeof addr;
  char addr_decoded[INET6_ADDRSTRLEN];
  void *addr_src;
  unsigned short port;
  unsigned char family;

  int sfd = accept(socket_, (struct sockaddr *)&addr, &addr_size);
  if (UNLIKELY(sfd == -1)) return false;

  if (addr.ss_family == AF_INET) {
    addr_src = (void *)&(((sockaddr_in *)&addr)->sin_addr);
    port = ntohs(((sockaddr_in *)&addr)->sin_port);
    family = 4;
  } else {
    addr_src = (void *)&(((sockaddr_in6 *)&addr)->sin6_addr);
    port = ntohs(((sockaddr_in6 *)&addr)->sin6_port);
    family = 6;
  }

  inet_ntop(addr.ss_family, addr_src, addr_decoded, INET6_ADDRSTRLEN);

  NetworkEndpoint endpoint;
  try {
    endpoint = NetworkEndpoint(addr_decoded, port);
  } catch (NetworkEndpointException &e) {
    return false;
  }

  *s = Socket(sfd, endpoint);

  return true;
}

Socket::operator int() { return socket_; }

int Socket::id() const { return socket_; }
const NetworkEndpoint &Socket::endpoint() const { return endpoint_; }

bool Socket::Write(const std::string &str) {
  return Write(str.c_str(), str.size());
}

bool Socket::Write(const char *data, size_t len) {
  return Write(reinterpret_cast<const uint8_t *>(data), len);
}

bool Socket::Write(const uint8_t *data, size_t len) {
  while (len > 0) {
    // MSG_NOSIGNAL is here to disable raising a SIGPIPE
    // signal when a connection dies mid-write, the socket
    // will only return an EPIPE error
    auto written = send(socket_, data, len, MSG_NOSIGNAL);
    if (UNLIKELY(written == -1)) return false;
    len -= written;
    data += written;
  }
  return true;
}

int Socket::Read(void *buffer, size_t len) {
  return read(socket_, buffer, len);
}
}
