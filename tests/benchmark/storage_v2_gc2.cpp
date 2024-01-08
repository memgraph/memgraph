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

#include <iostream>

#include <gflags/gflags.h>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "utils/timer.hpp"

using memgraph::replication::ReplicationRole;

// This benchmark should be run for a fixed amount of time that is
// large compared to GC interval to make the output relevant.

DEFINE_int32(num_poperties, 10'000, "number of set property per transaction");
DEFINE_int32(num_iterations, 5'000, "number of iterations");

std::pair<std::string, memgraph::storage::Config> TestConfigurations[] = {
    //{"NoGc", memgraph::storage::Config{.gc = {.type = memgraph::storage::Config::Gc::Type::NONE}}},
    {"100msPeriodicGc", memgraph::storage::Config{.gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC,
                                                         .interval = std::chrono::milliseconds(100)}}},
    {"1000msPeriodicGc", memgraph::storage::Config{.gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC,
                                                          .interval = std::chrono::milliseconds(1000)}}}};

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  for (const auto &config : TestConfigurations) {
    memgraph::utils::Timer timer;
    std::chrono::duration<double> end_bench;
    {
      std::unique_ptr<memgraph::storage::Storage> storage(new memgraph::storage::InMemoryStorage(config.second));
      std::array<memgraph::storage::Gid, 1> vertices;
      memgraph::storage::PropertyId pid;
      {
        auto acc = storage->Access(ReplicationRole::MAIN);
        vertices[0] = acc->CreateVertex().Gid();
        pid = acc->NameToProperty("NEW_PROP");
        MG_ASSERT(!acc->Commit().HasError());
      }

      for (int iter = 0; iter != FLAGS_num_iterations; ++iter) {
        auto acc = storage->Access(ReplicationRole::MAIN);
        auto vertex1 = acc->FindVertex(vertices[0], memgraph::storage::View::OLD);
        for (auto i = 0; i != FLAGS_num_poperties; ++i) {
          MG_ASSERT(!vertex1.value().SetProperty(pid, memgraph::storage::PropertyValue{i}).HasError());
        }
        MG_ASSERT(!acc->Commit().HasError());
      }

      end_bench = timer.Elapsed();
      std::cout << "Config: " << config.first << ", Time: " << end_bench.count() << std::endl;
    }
    auto end_shutdown = timer.Elapsed();
    std::cout << "Shutdown: " << (end_shutdown - end_bench).count() << std::endl;
  }
}
