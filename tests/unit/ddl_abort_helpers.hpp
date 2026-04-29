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

#pragma once

// Parameterized helpers for the DDL abort/ghost/restore test matrix shared
// across the storage unit-test files (storage_v2_indices, storage_v2_constraints,
// text_index, vector_index, vector_edge_index). Each helper captures the common
// shape: open accessor → DDL call → abort/commit → verify. Per-DDL-kind
// variation lives in the lambdas the call site supplies.
//
// SKIP_IF_NOT_IN_MEMORY must stay at the call site — `if constexpr` on TypeParam
// needs to be lexically inside the TYPED_TEST body. Plain TEST_F bodies with a
// non-templated fixture should just inline the abort/retry sequence (no skip
// needed).

#include <gtest/gtest.h>

#include <memory>

// Helpers are templates; the concrete storage types they touch (InMemoryStorage,
// Accessor::Abort/PrepareForCommitPhase, ...) are required only at instantiation
// time, where every caller already includes "storage/v2/inmemory/storage.hpp".
// Keep this header lean — pulling storage.hpp here adds ~1200 lines to every
// test TU that depends on the helpers.
#include "storage/v2/access_type.hpp"  // memgraph::storage::WRITE
#include "tests/test_commit_args_helper.hpp"

namespace memgraph::storage {
class InMemoryStorage;  // forward decl for the SKIP_IF_NOT_IN_MEMORY type-check
}  // namespace memgraph::storage

#define SKIP_IF_NOT_IN_MEMORY()                                                   \
  if constexpr (!std::is_same_v<TypeParam, memgraph::storage::InMemoryStorage>) { \
    GTEST_SKIP() << "Disk storage has different DDL semantics";                   \
  }

#ifndef ASSERT_NO_ERROR
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define ASSERT_NO_ERROR(result) ASSERT_TRUE((result).has_value())
#endif

namespace memgraph::tests {

// Common shape: aborted CREATE must not leave a ghost; a retry CREATE must
// succeed. `make_acc(self)` opens an accessor of the right type for the DDL.
// `create(acc.get())` runs the CREATE call; its return is expected to expose
// `.has_value()`.
template <typename Self, typename AccFn, typename CreateFn>
void ExpectCreateAbortLeavesNoGhostEntry(Self *self, AccFn make_acc, CreateFn create) {
  {
    auto a = make_acc(self);
    ASSERT_TRUE(create(a.get()).has_value());
    a->Abort();
  }
  {
    auto a = make_acc(self);
    ASSERT_TRUE(create(a.get()).has_value()) << "retry after aborted CREATE must succeed; ghost left behind.";
    ASSERT_NO_ERROR(a->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
}

// Common shape: aborted DROP must restore the entry, observable via the
// `verify_ready(acc.get())` predicate on a fresh WRITE accessor.
template <typename Self, typename CreateAccFn, typename DropAccFn, typename CreateFn, typename DropFn,
          typename VerifyReadyFn>
void ExpectDropAbortRestoresIndex(Self *self, CreateAccFn make_create_acc, DropAccFn make_drop_acc, CreateFn create,
                                  DropFn drop, VerifyReadyFn verify_ready) {
  {
    auto a = make_create_acc(self);
    ASSERT_TRUE(create(a.get()).has_value());
    ASSERT_NO_ERROR(a->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
  }
  {
    auto a = make_drop_acc(self);
    ASSERT_TRUE(drop(a.get()).has_value());
    a->Abort();
  }
  {
    auto a = self->storage->Access(memgraph::storage::WRITE);
    EXPECT_TRUE(verify_ready(a.get())) << "after aborted DROP, the index must remain visible.";
  }
}

// Common accessor lambdas: instantiate per-fixture by calling the fixture's
// public CreateXxxAccessor / DropXxxAccessor methods. Fixtures must expose
// these as public members.
inline auto IndexAcc = [](auto *self) { return self->CreateIndexAccessor(); };
inline auto DropAcc = [](auto *self) { return self->DropIndexAccessor(); };
inline auto ConstraintAcc = [](auto *self) { return self->CreateConstraintAccessor(); };
inline auto UniqueAcc = [](auto *self) { return self->storage->UniqueAccess(); };

}  // namespace memgraph::tests
