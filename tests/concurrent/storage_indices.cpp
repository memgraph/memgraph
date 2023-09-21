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

#include <thread>
#include <unordered_map>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage_error.hpp"
#include "utils/thread.hpp"

const uint64_t kNumVerifiers = 5;
const uint64_t kNumMutators = 1;

const uint64_t kNumIterations = 2000;
const uint64_t kVerifierBatchSize = 10;
const uint64_t kMutatorBatchSize = 1000;

TEST(Storage, LabelIndex) {
  std::unique_ptr<memgraph::storage::Storage> store{new memgraph::storage::InMemoryStorage()};

  auto label = store->NameToLabel("label");
  {
    auto unique_acc = store->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateIndex(label).HasError());
  }

  std::vector<std::thread> verifiers;
  verifiers.reserve(kNumVerifiers);
  for (uint64_t i = 0; i < kNumVerifiers; ++i) {
    verifiers.emplace_back([&store, label, num = i] {
      memgraph::utils::ThreadSetName(fmt::format("verifier{}", num));
      std::unordered_map<memgraph::storage::Gid, bool> gids;
      gids.reserve(kNumIterations * kVerifierBatchSize);
      for (uint64_t i = 0; i < kNumIterations; ++i) {
        for (uint64_t j = 0; j < kVerifierBatchSize; ++j) {
          auto acc = store->Access();
          auto vertex = acc->CreateVertex();
          gids.emplace(vertex.Gid(), false);
          auto ret = vertex.AddLabel(label);
          ASSERT_TRUE(ret.HasValue());
          ASSERT_TRUE(*ret);
          ASSERT_FALSE(acc->Commit().HasError());
        }
        {
          auto acc = store->Access();
          auto vertices = acc->Vertices(label, memgraph::storage::View::OLD);
          for (auto vertex : vertices) {
            auto it = gids.find(vertex.Gid());
            if (it != gids.end()) {
              ASSERT_FALSE(it->second);
              it->second = true;
            }
          }
          for (auto &item : gids) {
            ASSERT_TRUE(item.second);
            item.second = false;
          }
        }
      }
    });
  }

  std::vector<std::thread> mutators;
  std::atomic<bool> mutators_run = true;
  mutators.reserve(kNumMutators);
  for (uint64_t i = 0; i < kNumMutators; ++i) {
    mutators.emplace_back([&store, &mutators_run, label, num = i] {
      memgraph::utils::ThreadSetName(fmt::format("mutator{}", num));
      std::vector<memgraph::storage::Gid> gids;
      gids.resize(kMutatorBatchSize);
      while (mutators_run.load(std::memory_order_acquire)) {
        for (uint64_t i = 0; i < kMutatorBatchSize; ++i) {
          auto acc = store->Access();
          auto vertex = acc->CreateVertex();
          gids[i] = vertex.Gid();
          auto ret = vertex.AddLabel(label);
          ASSERT_TRUE(ret.HasValue());
          ASSERT_TRUE(*ret);
          ASSERT_FALSE(acc->Commit().HasError());
        }
        for (uint64_t i = 0; i < kMutatorBatchSize; ++i) {
          auto acc = store->Access();
          auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
          ASSERT_TRUE(vertex);
          ASSERT_TRUE(acc->DeleteVertex(&*vertex).HasValue());
          ASSERT_FALSE(acc->Commit().HasError());
        }
      }
    });
  }

  for (uint64_t i = 0; i < kNumVerifiers; ++i) {
    verifiers[i].join();
  }

  mutators_run.store(false, std::memory_order_release);
  for (uint64_t i = 0; i < kNumMutators; ++i) {
    mutators[i].join();
  }
}

TEST(Storage, LabelPropertyIndex) {
  std::unique_ptr<memgraph::storage::Storage> store{new memgraph::storage::InMemoryStorage()};

  auto label = store->NameToLabel("label");
  auto prop = store->NameToProperty("prop");
  {
    auto unique_acc = store->UniqueAccess();
    ASSERT_FALSE(unique_acc->CreateIndex(label, prop).HasError());
  }

  std::vector<std::thread> verifiers;
  verifiers.reserve(kNumVerifiers);
  for (uint64_t i = 0; i < kNumVerifiers; ++i) {
    verifiers.emplace_back([&store, label, prop, num = i] {
      memgraph::utils::ThreadSetName(fmt::format("verifier{}", num));
      std::unordered_map<memgraph::storage::Gid, bool> gids;
      gids.reserve(kNumIterations * kVerifierBatchSize);
      for (uint64_t i = 0; i < kNumIterations; ++i) {
        for (uint64_t j = 0; j < kVerifierBatchSize; ++j) {
          auto acc = store->Access();
          auto vertex = acc->CreateVertex();
          gids.emplace(vertex.Gid(), false);
          {
            auto ret = vertex.AddLabel(label);
            ASSERT_TRUE(ret.HasValue());
            ASSERT_TRUE(*ret);
          }
          {
            auto old_value = vertex.SetProperty(prop, memgraph::storage::PropertyValue(vertex.Gid().AsInt()));
            ASSERT_TRUE(old_value.HasValue());
            ASSERT_TRUE(old_value->IsNull());
          }
          ASSERT_FALSE(acc->Commit().HasError());
        }
        {
          auto acc = store->Access();
          auto vertices = acc->Vertices(label, prop, memgraph::storage::View::OLD);
          for (auto vertex : vertices) {
            auto it = gids.find(vertex.Gid());
            if (it != gids.end()) {
              ASSERT_FALSE(it->second);
              it->second = true;
            }
          }
          for (auto &item : gids) {
            ASSERT_TRUE(item.second);
            item.second = false;
          }
        }
      }
    });
  }

  std::vector<std::thread> mutators;
  std::atomic<bool> mutators_run = true;
  mutators.reserve(kNumMutators);
  for (uint64_t i = 0; i < kNumMutators; ++i) {
    mutators.emplace_back([&store, &mutators_run, label, prop, num = i] {
      memgraph::utils::ThreadSetName(fmt::format("mutator{}", num));
      std::vector<memgraph::storage::Gid> gids;
      gids.resize(kMutatorBatchSize);
      while (mutators_run.load(std::memory_order_acquire)) {
        for (uint64_t i = 0; i < kMutatorBatchSize; ++i) {
          auto acc = store->Access();
          auto vertex = acc->CreateVertex();
          gids[i] = vertex.Gid();
          {
            auto ret = vertex.AddLabel(label);
            ASSERT_TRUE(ret.HasValue());
            ASSERT_TRUE(*ret);
          }
          {
            auto old_value = vertex.SetProperty(prop, memgraph::storage::PropertyValue(vertex.Gid().AsInt()));
            ASSERT_TRUE(old_value.HasValue());
            ASSERT_TRUE(old_value->IsNull());
          }
          ASSERT_FALSE(acc->Commit().HasError());
        }
        for (uint64_t i = 0; i < kMutatorBatchSize; ++i) {
          auto acc = store->Access();
          auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
          ASSERT_TRUE(vertex);
          ASSERT_TRUE(acc->DeleteVertex(&*vertex).HasValue());
          ASSERT_FALSE(acc->Commit().HasError());
        }
      }
    });
  }

  for (uint64_t i = 0; i < kNumVerifiers; ++i) {
    verifiers[i].join();
  }

  mutators_run.store(false, std::memory_order_release);
  for (uint64_t i = 0; i < kNumMutators; ++i) {
    mutators[i].join();
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
