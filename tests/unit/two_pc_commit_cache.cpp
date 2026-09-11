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

// Group A below locks in TwoPCCommitCache's slot-management contract (Store/TakeForTenant/
// TakeMatching/TakeAny) in isolation. Those tests keep their backing storage alive for the whole
// test, so on their own they do NOT reproduce the use-after-free this cache exists to fix -- see
// Group B for the test that fails without the ~Database fix (AbortTwoPCForTenant call).

#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string_view>

#include "dbms/database.hpp"
#include "dbms/inmemory/replication_handlers.hpp"
#include "dbms/inmemory/two_pc_commit_cache.hpp"
#include "memory/db_arena.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"

namespace {

using memgraph::dbms::InMemoryReplicationHandlers;
using memgraph::dbms::TwoPCCommitCache;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::ReplicationAccessor;
using memgraph::storage::StorageAccessType;
using memgraph::utils::UUID;

// Two fixed, distinct, valid UUID strings -- explicit rather than relying on Config{}'s
// default-constructed (randomly generated) uuid, so uuid-mismatch coverage cannot flake on a
// collision.
constexpr std::string_view kUuidA = "11111111-1111-1111-1111-111111111111";
constexpr std::string_view kUuidB = "22222222-2222-2222-2222-222222222222";

// A durability_commit_timestamp low enough that CommitLog::MarkFinished (invoked transitively by
// AbortAndResetCommitTs, on the path exercised in Group B) accepts it unconditionally --
// CommitLog::FindOrCreateBlock handles any id, so any small constant works; verified empirically
// by running this suite.
constexpr uint64_t kCommitTs = 1;

auto MakeStorage(std::string_view uuid_str) -> std::unique_ptr<InMemoryStorage> {
  memgraph::storage::Config config{};
  config.salient.uuid.set(uuid_str);
  return std::make_unique<InMemoryStorage>(config);
}

// Mirrors production's downcast at InMemoryReplicationHandlers::ReadAndApplyDeltasSingleTxn
// (commit_accessor.reset(static_cast<storage::ReplicationAccessor *>(acc.release()))):
// ReplicationAccessor adds no data members over InMemoryStorage::InMemoryAccessor, so a WRITE
// accessor from Access() may be released and re-owned as a ReplicationAccessor.
auto TakeReplicationAccessor(InMemoryStorage *storage) -> std::unique_ptr<ReplicationAccessor> {
  auto acc = storage->Access(StorageAccessType::WRITE);
  return std::unique_ptr<ReplicationAccessor>(static_cast<ReplicationAccessor *>(acc.release()));
}

// The cache is a process-wide singleton (its state lives in TwoPCCommitCache's static slot), so tests share it.
// TearDown drains any leftover slot with TakeAny() so one test's leftovers cannot make a later
// test pass (or fail) for the wrong reason. Storage members are declared in the fixture (not as
// TEST_F-local variables) so they outlive TearDown()'s drain -- letting an accessor destruct
// against a storage that's still alive, exactly like Group A's file-level comment promises.
class TwoPCCommitCacheTest : public ::testing::Test {
 protected:
  void TearDown() override {
    // See the class comment above: drain the slot while storage_a_/storage_b_ are still alive.
    auto leftover = TwoPCCommitCache::TakeAny();
  }

  std::unique_ptr<InMemoryStorage> storage_a_ = MakeStorage(kUuidA);
  std::unique_ptr<InMemoryStorage> storage_b_ = MakeStorage(kUuidB);
};

}  // namespace

// TakeForTenant with a non-matching uuid must not consume the slot: the pending 2PC belongs to a
// different tenant and must survive for that tenant's own FinalizeCommitRpc/TakeForTenant to find.
TEST_F(TwoPCCommitCacheTest, TakeForTenantNonMatchingUuidLeavesSlotPopulated) {
  UUID uuid_a;
  uuid_a.set(kUuidA);
  UUID uuid_b;
  uuid_b.set(kUuidB);

  TwoPCCommitCache::Store(TakeReplicationAccessor(storage_a_.get()), kCommitTs, uuid_a);

  // Wrong tenant: must return nullptr and must NOT empty the slot.
  auto wrong_take = TwoPCCommitCache::TakeForTenant(uuid_b);
  ASSERT_EQ(wrong_take, nullptr);

  // Prove "still populated" by having the real owner successfully take it afterwards.
  auto right_take = TwoPCCommitCache::TakeForTenant(uuid_a);
  ASSERT_NE(right_take, nullptr);

  // Slot is empty now; a second take (by anyone) must fail.
  ASSERT_EQ(TwoPCCommitCache::TakeForTenant(uuid_a), nullptr);
}

// Mirrors FinalizeCommitHandler's "reply true, keep the slot" path: a durability_commit_timestamp
// mismatch is not terminal, so the accessor must stay cached for a later, matching call.
TEST_F(TwoPCCommitCacheTest, TakeMatchingNonMatchingTimestampLeavesSlotPopulated) {
  UUID uuid_a;
  uuid_a.set(kUuidA);
  constexpr uint64_t kCachedTs = 42;
  constexpr uint64_t kWrongTs = 43;

  TwoPCCommitCache::Store(TakeReplicationAccessor(storage_a_.get()), kCachedTs, uuid_a);

  auto mismatch = TwoPCCommitCache::TakeMatching(kWrongTs);
  ASSERT_EQ(mismatch.accessor, nullptr);
  ASSERT_TRUE(mismatch.mismatched_durability_commit_timestamp.has_value());
  EXPECT_EQ(*mismatch.mismatched_durability_commit_timestamp, kCachedTs);

  // Slot must still be populated -- a matching call afterwards succeeds.
  auto match = TwoPCCommitCache::TakeMatching(kCachedTs);
  ASSERT_NE(match.accessor, nullptr);
  ASSERT_FALSE(match.mismatched_durability_commit_timestamp.has_value());
}

TEST_F(TwoPCCommitCacheTest, TakeMatchingMatchingTimestampEmptiesSlot) {
  UUID uuid_a;
  uuid_a.set(kUuidA);
  constexpr uint64_t kCachedTs = 7;

  TwoPCCommitCache::Store(TakeReplicationAccessor(storage_a_.get()), kCachedTs, uuid_a);

  auto match = TwoPCCommitCache::TakeMatching(kCachedTs);
  ASSERT_NE(match.accessor, nullptr);

  // Slot is empty now; even the same timestamp must miss.
  auto second = TwoPCCommitCache::TakeMatching(kCachedTs);
  ASSERT_EQ(second.accessor, nullptr);
  ASSERT_FALSE(second.mismatched_durability_commit_timestamp.has_value());
}

TEST_F(TwoPCCommitCacheTest, TakeAnyIgnoresUuidAndEmptiesSlot) {
  UUID uuid_b;
  uuid_b.set(kUuidB);

  // Cached for tenant B; TakeAny must return it regardless of tenant.
  TwoPCCommitCache::Store(TakeReplicationAccessor(storage_b_.get()), kCommitTs, uuid_b);

  auto taken = TwoPCCommitCache::TakeAny();
  ASSERT_NE(taken, nullptr);

  // Slot is empty now.
  ASSERT_EQ(TwoPCCommitCache::TakeAny(), nullptr);
}

// Regression coverage for InMemoryReplicationHandlers::AbortPrevTxnIfNeeded's tenant scoping: it
// used to call the tenant-oblivious DestroyReplAccessor(), so a recovery/prepare RPC for tenant B
// would steal and abort tenant A's cached 2PC accessor -- a cross-tenant use-after-free race
// against A's own concurrent teardown, and (if A survived the race) silent divergence where
// FinalizeCommitHandler later finds an empty slot and replies "committed" to MAIN for a txn the
// replica had actually aborted. Drives the exact fixed call (AbortPrevTxnIfNeeded is public static)
// with two real storages, no RPC handshake required. This pair (...LeavesOtherTenantsEntryIntact /
// ...ClearsOwnTenantsEntry) pins the tenant-scoped behaviour on both sides: must not touch a
// different tenant's slot, must still clear its own.
TEST_F(TwoPCCommitCacheTest, AbortPrevTxnIfNeededLeavesOtherTenantsEntryIntact) {
  UUID uuid_a;
  uuid_a.set(kUuidA);

  auto accessor_a = TakeReplicationAccessor(storage_a_.get());
  // See DestroyingDatabaseDiscardsItsCachedAccessor's comment above: AbortAndResetCommitTs
  // unconditionally dereferences commit_timestamp_, so seed it even though this build's DMG_ASSERT
  // would not catch an unseeded one.
  accessor_a->GetCommitTimestamp().emplace(kCommitTs);
  TwoPCCommitCache::Store(std::move(accessor_a), kCommitTs, uuid_a);

  // Fixed behavior: AbortPrevTxnIfNeeded is scoped to storage_b_'s own uuid, so it must be a no-op
  // against A's populated slot. Before the fix, this called the tenant-oblivious
  // DestroyReplAccessor() and would have stolen and aborted A's accessor here.
  InMemoryReplicationHandlers::AbortPrevTxnIfNeeded(storage_b_.get());

  // A's entry must have survived. Take it back out and let it destruct while storage_a_ is still a
  // live fixture member (drained here rather than deferred to TearDown, matching Group A's pattern).
  auto still_a = TwoPCCommitCache::TakeForTenant(uuid_a);
  ASSERT_NE(still_a, nullptr);
}

TEST_F(TwoPCCommitCacheTest, AbortPrevTxnIfNeededClearsOwnTenantsEntry) {
  UUID uuid_a;
  uuid_a.set(kUuidA);

  auto accessor_a = TakeReplicationAccessor(storage_a_.get());
  accessor_a->GetCommitTimestamp().emplace(kCommitTs);
  TwoPCCommitCache::Store(std::move(accessor_a), kCommitTs, uuid_a);

  // AbortPrevTxnIfNeeded(storage_a_) is scoped to storage_a_'s own uuid, so the cached accessor
  // (which belongs to that same uuid) must be taken, aborted, and reset by this call.
  InMemoryReplicationHandlers::AbortPrevTxnIfNeeded(storage_a_.get());

  // Slot is empty now -- AbortPrevTxnIfNeeded already consumed it via AbortTwoPCForTenant.
  ASSERT_EQ(TwoPCCommitCache::TakeAny(), nullptr);
}

namespace {

// RAII temp directory for the Group B Database fixtures below -- mirrors the TmpDirManager
// pattern in tests/unit/storage_v2_recover_snapshot.cpp.
class TmpDirManager final {
 public:
  explicit TmpDirManager(std::string_view directory) : path_{std::filesystem::temp_directory_path() / directory} {
    std::filesystem::remove_all(path_);
    std::filesystem::create_directories(path_);
  }

  ~TmpDirManager() { std::filesystem::remove_all(path_); }

  TmpDirManager(const TmpDirManager &) = delete;
  TmpDirManager &operator=(const TmpDirManager &) = delete;
  TmpDirManager(TmpDirManager &&) = delete;
  TmpDirManager &operator=(TmpDirManager &&) = delete;

  const std::filesystem::path &Path() const { return path_; }

 private:
  std::filesystem::path path_;
};

auto MakeDatabaseConfig(std::filesystem::path const &storage_directory) -> memgraph::storage::Config {
  memgraph::storage::Config config{};
  config.durability.storage_directory = storage_directory;
  return config;
}

}  // namespace

// Group B: the load-bearing coverage. Without ~Database's AbortTwoPCForTenant call, a cached
// accessor keeps pointing into a Database's storage after that Database (and its per-DB arena)
// has been freed -- this must fail on a revert of that call.
class TwoPCCommitCacheDatabaseTest : public ::testing::Test {
 protected:
  void TearDown() override {
    // Both TEST_Fs below already drain the slot themselves before their Database(s) go out of
    // scope, so this always finds it empty; drain anyway so a future test added to this fixture
    // can't leak an entry into the next one.
    auto leftover = TwoPCCommitCache::TakeAny();
  }
};

TEST_F(TwoPCCommitCacheDatabaseTest, DestroyingDatabaseDiscardsItsCachedAccessor) {
  TmpDirManager tmp_dir{"MG_test_unit_two_pc_commit_cache_owning"};
  UUID db_uuid;

  {
    memgraph::dbms::Database db{MakeDatabaseConfig(tmp_dir.Path())};
    const memgraph::memory::DbArenaScope arena_scope{&db.Arena()};
    db_uuid = db.uuid();

    auto *inmemory_storage = static_cast<InMemoryStorage *>(db.storage());
    auto accessor = TakeReplicationAccessor(inmemory_storage);
    // AbortAndResetCommitTs (reached via ~Database -> AbortTwoPCForTenant -> TakeForTenant) does
    // Abort() then DMG_ASSERT(commit_timestamp_) and dereferences it unconditionally. This build
    // is RelWithDebInfo/NDEBUG, so DMG_ASSERT compiles out -- an unseeded commit_timestamp_ would
    // be a real UB dereference of an empty optional, not a caught assertion. Seed it so the
    // aborted-transaction path this test exercises is well-defined regardless of build type.
    accessor->GetCommitTimestamp().emplace(kCommitTs);
    TwoPCCommitCache::Store(std::move(accessor), kCommitTs, db_uuid);

    // db (and its storage/arena) is destroyed at the end of this scope.
  }

  // ~Database must have discarded the cached accessor via AbortTwoPCForTenant(uuid()). Without
  // that call, this cache entry is untouched and TakeAny() below returns a ReplicationAccessor
  // whose storage_ points at the just-freed Database's InMemoryStorage -- a dangling pointer, not
  // merely a leaked slot. Capture it and release() before asserting: on the FAILING path `taken`
  // owns that dangling accessor, and letting it destruct here (~InMemoryAccessor -> Abort(),
  // ~ResourceLockGuard -> unlock a freed main_lock_) would crash the test binary against freed
  // memory instead of reporting this ASSERT_EQ's clean gtest failure. TakeAny() has already
  // emptied the slot by this point regardless of what we do with the returned pointer.
  auto taken = TwoPCCommitCache::TakeAny();
  auto *const raw = taken.get();
  if (taken != nullptr) {
    taken.release();
  }
  ASSERT_EQ(raw, nullptr);
}

TEST_F(TwoPCCommitCacheDatabaseTest, DestroyingUnrelatedDatabaseLeavesOtherTenantsEntryInPlace) {
  TmpDirManager tmp_dir_a{"MG_test_unit_two_pc_commit_cache_kept_a"};
  TmpDirManager tmp_dir_b{"MG_test_unit_two_pc_commit_cache_kept_b"};

  auto db_a = std::make_unique<memgraph::dbms::Database>(MakeDatabaseConfig(tmp_dir_a.Path()));
  const memgraph::memory::DbArenaScope arena_scope_a{&db_a->Arena()};
  auto const uuid_a = db_a->uuid();

  {
    auto *inmemory_storage_a = static_cast<InMemoryStorage *>(db_a->storage());
    auto accessor_a = TakeReplicationAccessor(inmemory_storage_a);
    accessor_a->GetCommitTimestamp().emplace(kCommitTs);
    TwoPCCommitCache::Store(std::move(accessor_a), kCommitTs, uuid_a);
  }

  {
    memgraph::dbms::Database db_b{MakeDatabaseConfig(tmp_dir_b.Path())};
    const memgraph::memory::DbArenaScope arena_scope_b{&db_b.Arena()};
    // db_b is destroyed at the end of this scope. It never cached anything, so its
    // AbortTwoPCForTenant(uuid_b) call must be a no-op against A's still-populated slot.
  }

  // A's entry must have survived db_b's destruction.
  auto still_a = TwoPCCommitCache::TakeForTenant(uuid_a);
  ASSERT_NE(still_a, nullptr);
}
