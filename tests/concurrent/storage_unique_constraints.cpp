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

#include <thread>

#include <gtest/gtest.h>

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/inmemory/storage.hpp"

using memgraph::replication::ReplicationRole;

const int kNumThreads = 8;

#define ASSERT_OK(x) ASSERT_FALSE((x).HasError())

using memgraph::storage::LabelId;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyValue;

class StorageUniqueConstraints : public ::testing::Test {
 protected:
  StorageUniqueConstraints()
      : label(storage->NameToLabel("label")),
        prop1(storage->NameToProperty("prop1")),
        prop2(storage->NameToProperty("prop2")),
        prop3(storage->NameToProperty("prop3")) {}

  void SetUp() override {
    // Create initial vertices.
    auto acc = storage->Access(ReplicationRole::MAIN);
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (int i = 0; i < kNumThreads; ++i) {
      auto vertex = acc->CreateVertex();
      gids[i] = vertex.Gid();
    }
    ASSERT_OK(acc->Commit());
  }

  std::unique_ptr<memgraph::storage::Storage> storage{new memgraph::storage::InMemoryStorage()};
  LabelId label;
  PropertyId prop1;
  PropertyId prop2;
  PropertyId prop3;
  memgraph::storage::Gid gids[kNumThreads];
};

void SetProperties(memgraph::storage::Storage *storage, memgraph::storage::Gid gid,
                   const std::vector<PropertyId> &properties, const std::vector<PropertyValue> &values,
                   bool *commit_status) {
  ASSERT_EQ(properties.size(), values.size());
  auto acc = storage->Access(ReplicationRole::MAIN);
  auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(vertex);
  int value = 0;
  for (int iter = 0; iter < 40000; ++iter) {
    for (const auto &property : properties) {
      ASSERT_OK(vertex->SetProperty(property, PropertyValue(value++)));
    }
  }
  for (size_t i = 0; i < properties.size(); ++i) {
    ASSERT_OK(vertex->SetProperty(properties[i], values[i]));
  }
  *commit_status = !acc->Commit().HasError();
}

void AddLabel(memgraph::storage::Storage *storage, memgraph::storage::Gid gid, LabelId label, bool *commit_status) {
  auto acc = storage->Access(ReplicationRole::MAIN);
  auto vertex = acc->FindVertex(gid, memgraph::storage::View::OLD);
  ASSERT_TRUE(vertex);
  for (int iter = 0; iter < 40000; ++iter) {
    ASSERT_OK(vertex->AddLabel(label));
    ASSERT_OK(vertex->RemoveLabel(label));
  }
  ASSERT_OK(vertex->AddLabel(label));
  *commit_status = !acc->Commit().HasError();
}

TEST_F(StorageUniqueConstraints, ChangeProperties) {
  {
    auto unique_acc = storage->UniqueAccess(ReplicationRole::MAIN);
    auto res = unique_acc->CreateUniqueConstraint(label, {prop1, prop2, prop3});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  {
    auto acc = storage->Access(ReplicationRole::MAIN);
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (int i = 0; i < kNumThreads; ++i) {
      auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_OK(vertex->AddLabel(label));
    }
    ASSERT_OK(acc->Commit());
  }

  std::vector<PropertyId> properties{prop1, prop2, prop3};

  // There is fixed set of property values that is tried to be set to all
  // vertices in all iterations.
  {
    std::vector<PropertyValue> values{PropertyValue(1), PropertyValue(2), PropertyValue(3)};
    for (int iter = 0; iter < 20; ++iter) {
      bool status[kNumThreads];
      std::vector<std::thread> threads;
      threads.reserve(kNumThreads);
      for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back(SetProperties, storage.get(), gids[i], properties, values, &status[i]);
      }
      int count_ok = 0;
      for (int i = 0; i < kNumThreads; ++i) {
        threads[i].join();
        count_ok += status[i];
      }
      ASSERT_EQ(count_ok, 1);
    }
  }

  // The same set of property values is tried to be set to all vertices in
  // every iteration, but each iteration uses a different set of values.
  {
    for (int iter = 0; iter < 20; ++iter) {
      bool status[kNumThreads];
      std::vector<PropertyValue> values{PropertyValue(iter), PropertyValue(iter + 1), PropertyValue(iter + 2)};
      std::vector<std::thread> threads;
      threads.reserve(kNumThreads);
      for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back(SetProperties, storage.get(), gids[i], properties, values, &status[i]);
      }
      int count_ok = 0;
      for (int i = 0; i < kNumThreads; ++i) {
        threads[i].join();
        count_ok += status[i];
      }
      ASSERT_EQ(count_ok, 1);
    }
  }

  // Different property values are being set to vertices. In that case, all
  // transactions should succeed.
  {
    int value = 1000000;
    for (int iter = 0; iter < 20; ++iter) {
      bool status[kNumThreads];
      std::vector<std::thread> threads;
      threads.reserve(kNumThreads);
      for (int i = 0; i < kNumThreads; ++i) {
        std::vector<PropertyValue> values{PropertyValue(value++), PropertyValue(value++), PropertyValue(value++)};
        threads.emplace_back(SetProperties, storage.get(), gids[i], properties, values, &status[i]);
      }
      int count_ok = 0;
      for (int i = 0; i < kNumThreads; ++i) {
        threads[i].join();
        count_ok += status[i];
      }
      ASSERT_EQ(count_ok, kNumThreads);
    }
  }
}

TEST_F(StorageUniqueConstraints, ChangeLabels) {
  {
    auto unique_acc = storage->UniqueAccess(ReplicationRole::MAIN);
    auto res = unique_acc->CreateUniqueConstraint(label, {prop1, prop2, prop3});
    ASSERT_TRUE(res.HasValue());
    ASSERT_EQ(res.GetValue(), memgraph::storage::UniqueConstraints::CreationStatus::SUCCESS);
    ASSERT_FALSE(unique_acc->Commit().HasError());
  }

  // In the first part of the test, each transaction tries to add the same label
  // to different vertices, assuming that each vertex had the same set of
  // property values initially. In that case, exactly one transaction should
  // succeed, as the others should result with constraint violation.

  {
    auto acc = storage->Access(ReplicationRole::MAIN);
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (int i = 0; i < kNumThreads; ++i) {
      auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_OK(vertex->SetProperty(prop1, PropertyValue(1)));
      ASSERT_OK(vertex->SetProperty(prop2, PropertyValue(2)));
      ASSERT_OK(vertex->SetProperty(prop3, PropertyValue(3)));
    }
    ASSERT_OK(acc->Commit());
  }

  for (int iter = 0; iter < 20; ++iter) {
    // Clear labels.
    {
      auto acc = storage->Access(ReplicationRole::MAIN);
      // NOLINTNEXTLINE(modernize-loop-convert)
      for (int i = 0; i < kNumThreads; ++i) {
        auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_OK(vertex->RemoveLabel(label));
      }
      ASSERT_OK(acc->Commit());
    }

    bool status[kNumThreads];
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (int i = 0; i < kNumThreads; ++i) {
      threads.emplace_back(AddLabel, storage.get(), gids[i], label, &status[i]);
    }
    int count_ok = 0;
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (int i = 0; i < kNumThreads; ++i) {
      threads[i].join();
      count_ok += status[i];
    }
    ASSERT_EQ(count_ok, 1);
  }

  // In the second part of the test, it's assumed that all vertices has
  // different set of property values initially. In that case, all transactions
  // should succeed.

  {
    auto acc = storage->Access(ReplicationRole::MAIN);
    // NOLINTNEXTLINE(modernize-loop-convert)
    for (int i = 0; i < kNumThreads; ++i) {
      auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
      ASSERT_TRUE(vertex);
      ASSERT_OK(vertex->SetProperty(prop1, PropertyValue(3 * i)));
      ASSERT_OK(vertex->SetProperty(prop2, PropertyValue(3 * i + 1)));
      ASSERT_OK(vertex->SetProperty(prop3, PropertyValue(3 * i + 2)));
    }
    ASSERT_OK(acc->Commit());
  }

  for (int iter = 0; iter < 20; ++iter) {
    // Clear labels.
    {
      auto acc = storage->Access(ReplicationRole::MAIN);
      // NOLINTNEXTLINE(modernize-loop-convert)
      for (int i = 0; i < kNumThreads; ++i) {
        auto vertex = acc->FindVertex(gids[i], memgraph::storage::View::OLD);
        ASSERT_TRUE(vertex);
        ASSERT_OK(vertex->RemoveLabel(label));
      }
      ASSERT_OK(acc->Commit());
    }

    bool status[kNumThreads];
    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int i = 0; i < kNumThreads; ++i) {
      threads.emplace_back(AddLabel, storage.get(), gids[i], label, &status[i]);
    }
    int count_ok = 0;
    for (int i = 0; i < kNumThreads; ++i) {
      threads[i].join();
      count_ok += status[i];
    }
    ASSERT_EQ(count_ok, kNumThreads);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
