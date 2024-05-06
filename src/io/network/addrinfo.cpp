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

#include "io/network/addrinfo.hpp"

#include <concepts>
#include <iterator>

#include "io/network/network_error.hpp"

namespace memgraph::io::network {

static_assert(std::forward_iterator<AddrInfo::Iterator> && std::equality_comparable<AddrInfo::Iterator>);

AddrInfo::AddrInfo(const Endpoint &endpoint) : AddrInfo(endpoint.GetAddress(), endpoint.GetPort()) {}

AddrInfo::AddrInfo(const std::string &addr, uint16_t port) : info_{nullptr, nullptr} {
  addrinfo hints{
      .ai_flags = AI_PASSIVE,
      .ai_family = AF_UNSPEC,     // IPv4 and IPv6
      .ai_socktype = SOCK_STREAM  // TCP socket
  };
  addrinfo *info = nullptr;
  auto status = getaddrinfo(addr.c_str(), std::to_string(port).c_str(), &hints, &info);
  if (status != 0) throw NetworkError(gai_strerror(status));
  info_ = std::unique_ptr<addrinfo, decltype(&freeaddrinfo)>(info, &freeaddrinfo);
}

AddrInfo::Iterator::Iterator(addrinfo *p) noexcept : ptr_(p) {}

AddrInfo::Iterator::reference AddrInfo::Iterator::operator*() const noexcept { return *ptr_; }

AddrInfo::Iterator::pointer AddrInfo::Iterator::operator->() const noexcept { return ptr_; }

// NOLINTNEXTLINE(cert-dcl21-cpp)
AddrInfo::Iterator AddrInfo::Iterator::operator++(int) noexcept {
  auto it = *this;
  ++(*this);
  return it;
}
AddrInfo::Iterator &AddrInfo::Iterator::operator++() noexcept {
  ptr_ = ptr_->ai_next;
  return *this;
}

bool operator==(const AddrInfo::Iterator &lhs, const AddrInfo::Iterator &rhs) noexcept { return lhs.ptr_ == rhs.ptr_; };

bool operator!=(const AddrInfo::Iterator &lhs, const AddrInfo::Iterator &rhs) noexcept { return !(lhs == rhs); };

void swap(AddrInfo::Iterator &lhs, AddrInfo::Iterator &rhs) noexcept { std::swap(lhs.ptr_, rhs.ptr_); };

}  // namespace memgraph::io::network
