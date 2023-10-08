// Copyright 2023 Memgraph Ltd.
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

// This benchmark should be run for a fixed amount of time that is
// large compared to GC interval to make the output relevant.

const int kNumIterations = 5000000;
const int kNumVertices = 1000000;

DEFINE_int32(num_threads, 4, "number of threads");
DEFINE_int32(num_vertices, kNumVertices, "number of vertices");
DEFINE_int32(num_iterations, kNumIterations, "number of iterations");

std::pair<std::string, memgraph::storage::Config> TestConfigurations[] = {
    {"NoGc", memgraph::storage::Config{.gc = {.type = memgraph::storage::Config::Gc::Type::NONE}}},
    {"100msPeriodicGc", memgraph::storage::Config{.gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC,
                                                         .interval = std::chrono::milliseconds(100)}}},
    {"1000msPeriodicGc", memgraph::storage::Config{.gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC,
                                                          .interval = std::chrono::milliseconds(1000)}}}};

void UpdateLabelFunc(int thread_id, memgraph::storage::Storage *storage,
                     const std::vector<memgraph::storage::Gid> &vertices, int num_iterations) {
  std::mt19937 gen(thread_id);
  std::uniform_int_distribution<uint64_t> vertex_dist(0, vertices.size() - 1);
  std::uniform_int_distribution<uint64_t> label_dist(0, 100);

  memgraph::utils::Timer timer;
  for (int iter = 0; iter < num_iterations; ++iter) {
    auto acc = storage->Access();
    memgraph::storage::Gid gid = vertices.at(vertex_dist(gen));
    auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
    MG_ASSERT(vertex.has_value(), "Vertex with GID {} doesn't exist", gid.AsUint());
    if (vertex->AddLabel(memgraph::storage::LabelId::FromUint(label_dist(gen))).HasValue()) {
      MG_ASSERT(!acc->Commit().HasError());
    } else {
      acc->Abort();
    }
  }
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  for (const auto &config : TestConfigurations) {
    std::unique_ptr<memgraph::storage::Storage> storage(new memgraph::storage::InMemoryStorage(config.second));
    std::vector<memgraph::storage::Gid> vertices;
    {
      auto acc = storage->Access();
      for (int i = 0; i < FLAGS_num_vertices; ++i) {
        vertices.push_back(acc->CreateVertex().Gid());
      }
      MG_ASSERT(!acc->Commit().HasError());
    }

    memgraph::utils::Timer timer;
    std::vector<std::thread> threads;
    threads.reserve(FLAGS_num_threads);
    for (int i = 0; i < FLAGS_num_threads; ++i) {
      threads.emplace_back(UpdateLabelFunc, i, storage.get(), vertices, FLAGS_num_iterations);
    }

    for (int i = 0; i < FLAGS_num_threads; ++i) {
      threads[i].join();
    }

    std::cout << "Config: " << config.first << ", Time: " << timer.Elapsed().count() << std::endl;
  }

  return 0;
}
