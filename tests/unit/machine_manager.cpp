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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "io/local_transport/local_system.hpp"
#include "io/local_transport/local_transport.hpp"
#include "io/transport.hpp"

namespace memgraph::io::tests {

using memgraph::io::local_transport::LocalSystem;
using memgraph::io::local_transport::LocalTransport;

TEST(LocalTransport, BasicRequest) {
  LocalSystem local_system;

  // rely on uuid to be unique on default Address
  auto cli_addr = Address::UniqueLocalAddress();
  auto srv_addr = Address::UniqueLocalAddress();

  Io<LocalTransport> io = local_system.Register(cli_addr);
}
