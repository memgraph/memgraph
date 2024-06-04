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

#include <unistd.h>

#include <storage/v2/inmemory/storage.hpp>
#include <storage/v2/storage.hpp>
#include <utils/logging.hpp>

namespace {

void CheckPeriodic() {
  using namespace std::chrono_literals;
  int called_cnt = 0;
  auto gc_call_counter = [&called_cnt](auto...) { called_cnt++; };

  auto periodic_config = memgraph::storage::Config{};
  periodic_config.gc.type = memgraph::storage::Config::Gc::Type::PERIODIC;
  periodic_config.gc.interval = 1s;

  auto storage = memgraph::storage::InMemoryStorage(periodic_config, gc_call_counter);
  sleep(3);
  MG_ASSERT(called_cnt > 0, "Periodic gc never got called");
}

}  // namespace

int main() { CheckPeriodic(); }
