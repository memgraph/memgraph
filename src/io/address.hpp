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
    Address ret;
    ret.last_known_port = port;
    return ret;
  }

  bool operator==(const Address &other) const = default;

  /// unique_id is most dominant for ordering, then last_known_ip, then last_known_port
  bool operator<(const Address &other) const {
    if (unique_id != other.unique_id) {
      return unique_id < other.unique_id;
    }

    if (last_known_ip != other.last_known_ip) {
      return last_known_ip < other.last_known_ip;
    }

    return last_known_port < other.last_known_port;
  }

  std::string ToString() const {
    return fmt::format("Address {{ unique_id: {}, last_known_ip: {}, last_known_port: {} }}",
                       boost::uuids::to_string(unique_id), last_known_ip.to_string(), last_known_port);
  }
};
};  // namespace memgraph::io
