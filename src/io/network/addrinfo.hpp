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

#pragma once

#include <netdb.h>
#include <string>

namespace memgraph::io::network {

struct Endpoint;

/**
 * Wrapper class for getaddrinfo.
 * see: man 3 getaddrinfo
 */
class AddrInfo {
 public:
  struct Iterator {
    explicit Iterator(addrinfo *p) : ptr(p) {}
    addrinfo &operator*() const { return *ptr; }
    Iterator &operator++() {
      ptr = ptr->ai_next;
      return *this;
    }
    bool operator!=(const Iterator &rhs) { return rhs.ptr != ptr; };

   private:
    addrinfo *ptr;
  };

  ~AddrInfo();

  AddrInfo(const std::string &addr, uint16_t port);
  explicit AddrInfo(const Endpoint &endpoint);

  auto begin() const { return Iterator(info); }
  auto end() const { return Iterator(nullptr); }

 private:
  struct addrinfo *info;
};
}  // namespace memgraph::io::network
