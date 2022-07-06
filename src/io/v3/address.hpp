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

#include <boost/asio/ip/tcp.hpp>
#include <boost/uuid/uuid.hpp>

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

  bool operator==(const Address &other) const {
    return (last_known_ip == other.last_known_ip) && (last_known_port == other.last_known_port);
  }

  bool operator<(const Address &other) const {
    if (last_known_ip == other.last_known_ip) {
      return last_known_port < other.last_known_port;
    } else {
      return last_known_ip < other.last_known_ip;
    }
  }
};
