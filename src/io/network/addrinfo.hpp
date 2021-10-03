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

#pragma once

namespace io::network {

/**
 * Wrapper class for getaddrinfo.
 * see: man 3 getaddrinfo
 */
class AddrInfo {
  explicit AddrInfo(struct addrinfo *info);

 public:
  ~AddrInfo();

  static AddrInfo Get(const char *addr, const char *port);

  operator struct addrinfo *();

 private:
  struct addrinfo *info;
};
}  // namespace io::network
