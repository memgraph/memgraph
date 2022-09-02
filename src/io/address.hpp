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

#include <compare>

#include <fmt/format.h>
#include <boost/asio/ip/tcp.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace memgraph::io {
struct Address {
  // It's important for all participants to have a
  // unique identifier - IP and port alone are not
  // enough, and may change over the lifecycle of
  // the nodes. Particularly storage nodes may change
  // their IP addresses over time, and the system
  // should gracefully update its information
  // about them.
  boost::uuids::uuid unique_id;
  boost::asio::ip::address last_known_ip;
  uint16_t last_known_port;

  static Address TestAddress(uint16_t port) {
    return Address{
        .unique_id = boost::uuids::uuid{boost::uuids::random_generator()()},
        .last_known_port = port,
    };
  }

  static Address UniqueLocalAddress() {
    return Address{
        .unique_id = boost::uuids::uuid{boost::uuids::random_generator()()},
    };
  }

  friend bool operator==(const Address &lhs, const Address &rhs) = default;

  /// unique_id is most dominant for ordering, then last_known_ip, then last_known_port
  friend bool operator<(const Address &lhs, const Address &rhs) {
    if (lhs.unique_id != rhs.unique_id) {
      return lhs.unique_id < rhs.unique_id;
    }

    if (lhs.last_known_ip != rhs.last_known_ip) {
      return lhs.last_known_ip < rhs.last_known_ip;
    }

    return lhs.last_known_port < rhs.last_known_port;
  }

  std::string ToString() const {
    return fmt::format("Address {{ unique_id: {}, last_known_ip: {}, last_known_port: {} }}",
                       boost::uuids::to_string(unique_id), last_known_ip.to_string(), last_known_port);
  }
};
};  // namespace memgraph::io
