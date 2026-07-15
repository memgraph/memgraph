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

#include <optional>

#include "helpers/stub_property_fga_checker.hpp"
#include "query/db_accessor.hpp"
#include "query/overlay_authorization.hpp"
#include "query/synthetic_gid.hpp"
#include "query/virtual_node.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory.hpp"

namespace memgraph::query::test {

using storage::Gid;
using storage::View;

// ---- ExternalId: the single synthetic-axis id fallback ----

TEST(SyntheticExternalId, UsesMapperWhenPresent) {
  SyntheticIdMapper mapper;
  const auto a = Gid::FromUint(1000);
  const auto b = Gid::FromUint(2000);
  // The mapper hands out dense negative ids and repeats them for the same Gid.
  EXPECT_EQ(ExternalId(a, &mapper), -1);
  EXPECT_EQ(ExternalId(b, &mapper), -2);
  EXPECT_EQ(ExternalId(a, &mapper), -1);
}

TEST(SyntheticExternalId, FallsBackToSignedRawGidWhenNull) {
  // With no mapper the raw Gid is read as a signed int - the same axis id() and the wire agree on.
  const auto g = Gid::FromUint(42);
  EXPECT_EQ(ExternalId(g, nullptr), g.AsInt());
}

// ---- OverlayReadAuthorization: one per-property READ decision for overlay/synthetic nodes ----

class OverlayReadAuthorizationTest : public ::testing::Test {
 protected:
  std::unique_ptr<storage::Storage> storage_ = std::make_unique<storage::InMemoryStorage>();

  // Inserts a vertex with a single label and returns its accessor (kept valid by the passed dba).
  static VertexAccessor InsertLabelled(DbAccessor &dba, std::string_view label) {
    auto v = dba.InsertVertex();
    EXPECT_TRUE(v.AddLabel(dba.NameToLabel(label)).has_value());
    return v;
  }
};

TEST_F(OverlayReadAuthorizationTest, SyntheticNodeAlwaysReadable) {
  auto acc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{acc.get()};
  const auto ssn = dba.NameToProperty("ssn");
  const tests::StubPropertyFGAChecker<DbAccessor> checker{&dba, {{"Secret", "ssn"}}};

  // A synthetic node has no origin, so it mints no real-graph data and is fully readable.
  VirtualNode synthetic{{"Secret"}, {}};
  auto auth = OverlayReadAuthorization::For(synthetic, View::NEW, &checker);
  ASSERT_TRUE(auth.has_value());
  EXPECT_TRUE(auth->IsReadable(synthetic, ssn));
}

TEST_F(OverlayReadAuthorizationTest, NullCheckerAllReadable) {
  auto acc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{acc.get()};
  const auto ssn = dba.NameToProperty("ssn");
  auto origin = InsertLabelled(dba, "Secret");

  VirtualNode overlay{{}, {}, VirtualNode::allocator_type{}, origin};
  auto auth = OverlayReadAuthorization::For(overlay, View::NEW, nullptr);
  ASSERT_TRUE(auth.has_value());
  EXPECT_TRUE(auth->IsReadable(overlay, ssn));
}

TEST_F(OverlayReadAuthorizationTest, OriginReadThroughGatedByPermission) {
  auto acc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{acc.get()};
  const auto ssn = dba.NameToProperty("ssn");
  const auto name = dba.NameToProperty("name");
  auto origin = InsertLabelled(dba, "Secret");
  const tests::StubPropertyFGAChecker<DbAccessor> checker{&dba, {{"Secret", "ssn"}}};

  // An overlay over the origin reads through: the denied (Secret, ssn) property is hidden, an
  // un-denied property stays visible.
  VirtualNode overlay{{}, {}, VirtualNode::allocator_type{}, origin};
  auto auth = OverlayReadAuthorization::For(overlay, View::NEW, &checker);
  ASSERT_TRUE(auth.has_value());
  EXPECT_FALSE(auth->IsReadable(overlay, ssn));
  EXPECT_TRUE(auth->IsReadable(overlay, name));
}

TEST_F(OverlayReadAuthorizationTest, OverlayBoundOverrideExempt) {
  auto acc = storage_->Access(storage::StorageAccessType::WRITE);
  DbAccessor dba{acc.get()};
  const auto ssn = dba.NameToProperty("ssn");
  auto origin = InsertLabelled(dba, "Secret");
  const tests::StubPropertyFGAChecker<DbAccessor> checker{&dba, {{"Secret", "ssn"}}};

  // The overlay carries its own ssn value: an overlay-bound override is the author's own computed
  // value and is exempt from the origin's per-property permission, even though (Secret, ssn) is denied.
  VirtualNode overlay{{}, {{ssn, storage::PropertyValue(1)}}, VirtualNode::allocator_type{}, origin};
  ASSERT_TRUE(overlay.IsOverlayBound(ssn));
  auto auth = OverlayReadAuthorization::For(overlay, View::NEW, &checker);
  ASSERT_TRUE(auth.has_value());
  EXPECT_TRUE(auth->IsReadable(overlay, ssn));
}

}  // namespace memgraph::query::test
