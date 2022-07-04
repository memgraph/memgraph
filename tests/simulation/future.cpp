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

#include <string>
#include <thread>

#include "io/v3/future.hpp"
#include "utils/logging.hpp"

void Fill(MgPromise<std::string> promise) { promise.Fill("success"); }

int main() {
  auto [future, promise] = FuturePromisePair<std::string>();

  std::jthread t1(Fill, std::move(promise));

  std::string result = future.Wait();
  t1.join();

  MG_ASSERT(result == "success");

  return 0;
}
