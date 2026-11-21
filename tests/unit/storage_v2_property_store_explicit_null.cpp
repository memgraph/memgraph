// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <gtest/gtest.h>

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

using namespace memgraph::storage;

namespace {
const PropertyId kP = PropertyId::FromUint(1);
const PropertyId kQ = PropertyId::FromUint(2);
}  // namespace

TEST(PropertyStoreExplicitNull, TriStateLookup) {
  PropertyStore store;
  EXPECT_FALSE(store.FindProperty(kP).has_value());

  EXPECT_TRUE(store.SetExplicitNull(kP));
  auto found = store.FindProperty(kP);
  ASSERT_TRUE(found.has_value());
  EXPECT_TRUE(found->IsNull());
  EXPECT_TRUE(store.GetProperty(kP).IsNull());
  EXPECT_FALSE(store.FindProperty(kQ).has_value());
}

TEST(PropertyStoreExplicitNull, PropertiesReportsExplicitNullAsNull) {
  PropertyStore store;
  store.SetProperty(kQ, PropertyValue{7});
  store.SetExplicitNull(kP);
  auto props = store.Properties();
  ASSERT_EQ(props.size(), 2);
  EXPECT_TRUE(props.at(kP).IsNull());
  EXPECT_EQ(props.at(kQ).ValueInt(), 7);
}

TEST(PropertyStoreExplicitNull, OverwriteNullValueNull) {
  PropertyStore store;
  store.SetExplicitNull(kP);
  EXPECT_FALSE(store.SetProperty(kP, PropertyValue{"v"}));  // replaced an existing entry
  EXPECT_EQ(store.FindProperty(kP)->ValueString(), "v");
  EXPECT_FALSE(store.SetExplicitNull(kP));
  ASSERT_TRUE(store.FindProperty(kP).has_value());
  EXPECT_TRUE(store.FindProperty(kP)->IsNull());
}

TEST(PropertyStoreExplicitNull, PlainSetNullStillErases) {
  PropertyStore store;
  store.SetExplicitNull(kP);
  store.SetProperty(kQ, PropertyValue{7});
  EXPECT_FALSE(store.SetProperty(kP, PropertyValue{}));
  EXPECT_FALSE(store.FindProperty(kP).has_value());
  EXPECT_TRUE(store.FindProperty(kQ).has_value());
}

#ifdef MG_ENTERPRISE
TEST(PropertyStoreExplicitNull, AbortRestoresExplicitNull) {
  Config config{};
  config.gc.type = Config::Gc::Type::NONE;
  InMemoryStorage storage(config);

  auto gid = Gid::FromUint(0);
  {
    auto acc = storage.Access(WRITE);
    auto v = acc->CreateVertex();
    gid = v.Gid();
    v.vertex_->properties.SetExplicitNull(kP);
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  {
    auto acc = storage.Access(WRITE);
    auto v = acc->FindVertex(gid, View::NEW);
    ASSERT_TRUE(v.has_value());
    auto *vertex = v->vertex_;
    auto *tx = v->transaction_;
    {
      std::lock_guard guard{vertex->lock};
      ASSERT_TRUE(PrepareForWrite(tx, vertex));
      CreateAndLinkDelta(tx, vertex, Delta::SetPropertyTag(), kP, PropertyValue{});
      vertex->delta()->property.value = &kExplicitNullBeforeImage;
      vertex->properties.SetProperty(kP, PropertyValue{5});
    }
    ASSERT_EQ(vertex->properties.FindProperty(kP)->ValueInt(), 5);
    acc->Abort();
  }

  auto acc = storage.Access(WRITE);
  auto v = acc->FindVertex(gid, View::NEW);
  ASSERT_TRUE(v.has_value());
  auto restored = v->vertex_->properties.FindProperty(kP);
  ASSERT_TRUE(restored.has_value()) << "abort erased the explicit null";
  EXPECT_TRUE(restored->IsNull());
}
#endif
