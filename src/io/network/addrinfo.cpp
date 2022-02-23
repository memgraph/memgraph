// Copyright 2022 Memgraph Ltd.
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

#include "io/network/addrinfo.hpp"
#include "io/network/endpoint.hpp"
#include "io/network/network_error.hpp"

namespace memgraph::io::network {

AddrInfo::~AddrInfo() { freeaddrinfo(info); }

AddrInfo::AddrInfo(const Endpoint &endpoint) : AddrInfo(endpoint.address, endpoint.port) {}

AddrInfo::AddrInfo(const std::string &addr, uint16_t port) {
  addrinfo hints{
      .ai_flags = AI_PASSIVE,
      .ai_family = AF_UNSPEC,     // IPv4 and IPv6
      .ai_socktype = SOCK_STREAM  // TCP socket
  };
  auto status = getaddrinfo(addr.c_str(), std::to_string(port).c_str(), &hints, &info);
  if (status != 0) throw NetworkError(gai_strerror(status));
}

}  // namespace memgraph::io::network
