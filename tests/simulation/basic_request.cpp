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

//#include <gtest/gtest.h>

#include <string>

#include "io/v3/simulator.hpp"
#include "io/v3/transport.hpp"
#include "utils/logging.hpp"

int main() {
  auto simulator = Simulator();
  auto addr_1 = Address();
  auto addr_2 = Address();
  auto addr_3 = Address();

  auto sim_transport_1 = simulator.Register(addr_1, true);
  auto sim_transport_2 = simulator.Register(addr_2, true);
  auto sim_transport_3 = simulator.Register(addr_3, true);

  return 0;
}
