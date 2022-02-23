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

#include <memory>
#include <string>

#include "io/network/endpoint.hpp"

namespace memgraph::io::network {

/**
 * Wrapper class for getaddrinfo.
 * see: man 3 getaddrinfo
 */
class AddrInfo {
 public:
  struct Iterator {
    explicit Iterator(addrinfo *p) noexcept : ptr(p) {}
    addrinfo &operator*() const noexcept { return *ptr; }
    Iterator &operator++() noexcept {
      ptr = ptr->ai_next;
      return *this;
    }
    friend bool operator!=(const Iterator &lhs, const Iterator &rhs) noexcept { return lhs.ptr != rhs.ptr; };

   private:
    addrinfo *ptr;
  };

  AddrInfo(const std::string &addr, uint16_t port);
  explicit AddrInfo(const Endpoint &endpoint);

  auto begin() const noexcept { return Iterator(info_.get()); }
  auto end() const noexcept { return Iterator(nullptr); }

 private:
  std::unique_ptr<addrinfo, void (*)(addrinfo *)> info_;
};
}  // namespace memgraph::io::network
