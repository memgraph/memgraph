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

#include <iterator>
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
    using iterator_category = std::forward_iterator_tag;
    using value_type = addrinfo;
    using difference_type = std::ptrdiff_t;
    using pointer = addrinfo *;
    using reference = addrinfo &;

    Iterator() = default;
    Iterator(const Iterator &) = default;
    explicit Iterator(addrinfo *p) noexcept;
    Iterator &operator=(const Iterator &) = default;
    reference operator*() const noexcept;
    pointer operator->() const noexcept;
    const Iterator operator++(int) noexcept;
    Iterator &operator++() noexcept;

    friend bool operator==(const Iterator &lhs, const Iterator &rhs) noexcept;
    friend bool operator!=(const Iterator &lhs, const Iterator &rhs) noexcept;
    friend void swap(Iterator &lhs, Iterator &rhs) noexcept;

   private:
    addrinfo *ptr_{nullptr};
  };

  AddrInfo(const std::string &addr, uint16_t port);
  explicit AddrInfo(const Endpoint &endpoint);

  auto begin() const noexcept { return Iterator(info_.get()); }
  auto end() const noexcept { return Iterator{nullptr}; }

 private:
  std::unique_ptr<addrinfo, void (*)(addrinfo *)> info_;
};
}  // namespace memgraph::io::network
