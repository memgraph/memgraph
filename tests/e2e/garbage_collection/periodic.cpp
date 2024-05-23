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
  auto storage = memgraph::storage::InMemoryStorage();
  int called_cnt = 0;
  storage.SetFreeMemoryFuncPtr([&called_cnt](std::unique_lock<memgraph::utils::ResourceLock>, bool) { called_cnt++; });
  sleep(5);
  MG_ASSERT(called_cnt > 0, "Periodic gc never got called");
}

}  // namespace

int main() { CheckPeriodic(); }
