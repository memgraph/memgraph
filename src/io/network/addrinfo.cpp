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

#include <netdb.h>
#include <cstring>

#include "io/network/addrinfo.hpp"

#include "io/network/network_error.hpp"

namespace io::network {

AddrInfo::AddrInfo(struct addrinfo *info) : info(info) {}

AddrInfo::~AddrInfo() { freeaddrinfo(info); }

AddrInfo AddrInfo::Get(const char *addr, const char *port) {
  struct addrinfo hints;
  memset(&hints, 0, sizeof(struct addrinfo));

  hints.ai_family = AF_UNSPEC;      // IPv4 and IPv6
  hints.ai_socktype = SOCK_STREAM;  // TCP socket
  hints.ai_flags = AI_PASSIVE;

  struct addrinfo *result;
  auto status = getaddrinfo(addr, port, &hints, &result);

  if (status != 0) throw NetworkError(gai_strerror(status));

  return AddrInfo(result);
}

AddrInfo::operator struct addrinfo *() { return info; }
}  // namespace io::network
