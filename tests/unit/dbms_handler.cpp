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

#include "query/auth_query_handler.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#ifdef MG_ENTERPRISE
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <system_error>
#include <thread>

#include <nlohmann/json.hpp>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/global.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_handler.hpp"
#include "kvstore/kvstore.hpp"
#include "memory/db_arena.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/uuid.hpp"

namespace {
std::set<std::string> GetDirs(auto path) {
  std::set<std::string> dirs;
  // Clean the unused directories
  for (const auto &entry : std::filesystem::directory_iterator(path)) {
    const auto &name = entry.path().filename().string();
    if (entry.is_directory() && !name.empty() && name.front() != '.') {
      dirs.emplace(name);
    }
  }
  return dirs;
}

int64_t AbsDiff(int64_t lhs, int64_t rhs) { return lhs > rhs ? lhs - rhs : rhs - lhs; }

// Bounded poll: a fixed sleep would flake (too short) or waste time (too long) waiting on a
// deferred-destruction background thread; an unbounded wait would hang forever on a real regression.
template <typename Pred>
bool WaitUntil(std::chrono::milliseconds timeout, Pred &&pred) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  do {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  } while (std::chrono::steady_clock::now() < deadline);
  return pred();
}
}  // namespace

// Global
std::filesystem::path storage_directory{std::filesystem::temp_directory_path() / "MG_test_unit_dbms_handler"};
std::filesystem::path db_dir{storage_directory / "databases"};
static memgraph::storage::Config storage_conf;
std::unique_ptr<memgraph::auth::SynchedAuth> auth;
std::unique_ptr<memgraph::system::System> system_state;

// Let this be global so we can test it different states throughout

class TestEnvironment : public ::testing::Environment {
 public:
  static memgraph::dbms::DbmsHandler *get() { return ptr_.get(); }

  void SetUp() override {
    // Setup config
    memgraph::storage::UpdatePaths(storage_conf, storage_directory);
    storage_conf.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    // Clean storage directory (running multiple parallel test, run only if the first process)
    if (std::filesystem::exists(storage_directory)) {
      memgraph::utils::OutputFile lock_file_handle_;
      lock_file_handle_.Open(storage_directory / ".lock", memgraph::utils::OutputFile::Mode::OVERWRITE_EXISTING);
      if (lock_file_handle_.AcquireLock()) {
        std::filesystem::remove_all(storage_directory);
      }
    }
    auth = std::make_unique<memgraph::auth::SynchedAuth>(storage_directory / "auth",
                                                         memgraph::auth::Auth::Config{/* default */});
    system_state = std::make_unique<memgraph::system::System>();
    ptr_ = std::make_unique<memgraph::dbms::DbmsHandler>(storage_conf);
  }

  void TearDown() override {
    ptr_.reset();
    system_state.reset();
    auth.reset();
    std::filesystem::remove_all(storage_directory);
  }

  static std::unique_ptr<memgraph::dbms::DbmsHandler> ptr_;
};

std::unique_ptr<memgraph::dbms::DbmsHandler> TestEnvironment::ptr_ = nullptr;

class DBMS_Handler : public testing::Test {};

using DBMS_HandlerDeath = DBMS_Handler;

TEST(DBMS_Handler, Init) {
  // Check that the default db has been created successfully
  std::vector<std::string> dirs = {"snapshots", "streams", "triggers", "wal"};
  for (const auto &dir : dirs)
    ASSERT_TRUE(std::filesystem::exists(storage_directory / dir)) << (storage_directory / dir);
  const auto db_path = db_dir / memgraph::dbms::kDefaultDB;
  ASSERT_TRUE(std::filesystem::exists(db_path));
  for (const auto &dir : dirs) {
    std::error_code ec;
    const auto test_link = std::filesystem::read_symlink(db_path / dir, ec);
    ASSERT_TRUE(!ec) << ec.message();
    ASSERT_EQ(test_link, "../../" + dir);
  }
}

TEST(DBMS_Handler, New) {
  auto &dbms = *TestEnvironment::get();
  {
    const auto all = dbms.All();
    ASSERT_EQ(all.size(), 1);
    ASSERT_EQ(all[0], memgraph::dbms::kDefaultDB);
  }
  {
    const auto dirs = GetDirs(db_dir);
    auto db1 = dbms.New("db1");
    ASSERT_TRUE(db1.has_value());
    ASSERT_TRUE(db1.value());
    // New flow doesn't make db named directories
    ASSERT_FALSE(std::filesystem::exists(db_dir / "db1"));
    const auto dirs_w_db1 = GetDirs(db_dir);
    ASSERT_EQ(dirs_w_db1.size(), dirs.size() + 1);
    ASSERT_TRUE(db1.value()->storage() != nullptr);
    ASSERT_TRUE(db1.value()->streams() != nullptr);
    ASSERT_TRUE(db1.value()->trigger_store() != nullptr);
    ASSERT_TRUE(db1.value()->thread_pool() != nullptr);
    const auto all = dbms.All();
    ASSERT_EQ(all.size(), 2);
    ASSERT_TRUE(std::find(all.begin(), all.end(), memgraph::dbms::kDefaultDB) != all.end());
    ASSERT_TRUE(std::find(all.begin(), all.end(), "db1") != all.end());
  }
  {
    // Fail if name exists
    auto db2 = dbms.New("db1");
    ASSERT_EQ(db2, std::unexpected{memgraph::dbms::NewError::EXISTS});
  }
  {
    const auto dirs = GetDirs(db_dir);
    auto db3 = dbms.New("db3");
    ASSERT_TRUE(db3.has_value());
    // New flow doesn't make db named directories
    ASSERT_FALSE(std::filesystem::exists(db_dir / "db3"));
    const auto dirs_w_db3 = GetDirs(db_dir);
    ASSERT_EQ(dirs_w_db3.size(), dirs.size() + 1);
    ASSERT_TRUE(db3.value()->storage() != nullptr);
    ASSERT_TRUE(db3.value()->streams() != nullptr);
    ASSERT_TRUE(db3.value()->trigger_store() != nullptr);
    ASSERT_TRUE(db3.value()->thread_pool() != nullptr);
    const auto all = dbms.All();
    ASSERT_EQ(all.size(), 3);
    ASSERT_TRUE(std::find(all.begin(), all.end(), "db3") != all.end());
  }
}

TEST(DBMS_Handler, Get) {
  auto &dbms = *TestEnvironment::get();
  auto default_db = dbms.Get(memgraph::dbms::kDefaultDB);
  ASSERT_TRUE(default_db);
  ASSERT_TRUE(default_db->storage() != nullptr);
  ASSERT_TRUE(default_db->streams() != nullptr);
  ASSERT_TRUE(default_db->trigger_store() != nullptr);
  ASSERT_TRUE(default_db->thread_pool() != nullptr);

  ASSERT_ANY_THROW(dbms.Get("non-existent"));

  auto db1 = dbms.Get("db1");
  ASSERT_TRUE(db1);
  ASSERT_TRUE(db1->storage() != nullptr);
  ASSERT_TRUE(db1->streams() != nullptr);
  ASSERT_TRUE(db1->trigger_store() != nullptr);
  ASSERT_TRUE(db1->thread_pool() != nullptr);

  auto db3 = dbms.Get("db3");
  ASSERT_TRUE(db3);
  ASSERT_TRUE(db3->storage() != nullptr);
  ASSERT_TRUE(db3->streams() != nullptr);
  ASSERT_TRUE(db3->trigger_store() != nullptr);
  ASSERT_TRUE(db3->thread_pool() != nullptr);
}

TEST(DBMS_Handler, Delete) {
  auto &dbms = *TestEnvironment::get();

  auto db1_acc = dbms.Get("db1");  // Holds access to database

  {
    auto del = dbms.TryDelete(memgraph::dbms::kDefaultDB);
    ASSERT_EQ(del, std::unexpected{memgraph::dbms::DeleteError::DEFAULT_DB});
  }
  {
    auto del = dbms.TryDelete("non-existent");
    ASSERT_EQ(del, std::unexpected{memgraph::dbms::DeleteError::NON_EXISTENT});
  }
  {
    // db1_acc is using db1
    auto del = dbms.TryDelete("db1");
    ASSERT_EQ(del, std::unexpected{memgraph::dbms::DeleteError::USING});
  }
  {
    // Reset db1_acc (releases access) so delete will succeed
    db1_acc.reset();
    ASSERT_FALSE(db1_acc);
    auto del = dbms.TryDelete("db1");
    ASSERT_TRUE(del.has_value()) << (int)del.error();
    auto del2 = dbms.TryDelete("db1");
    ASSERT_EQ(del2, std::unexpected{memgraph::dbms::DeleteError::NON_EXISTENT});
  }
  {
    const auto dirs = GetDirs(db_dir);
    auto del = dbms.TryDelete("db3");
    ASSERT_TRUE(del.has_value());
    const auto dirs_wo_db3 = GetDirs(db_dir);
    ASSERT_EQ(dirs_wo_db3.size(), dirs.size() - 1);
  }
}

// Coverage gap: the durability V1 -> V2 migration path (DbmsHandler.cpp's file-local `Durability::Migrate`,
// run unconditionally at the top of the DbmsHandler ctor) had zero unit coverage. `Durability` is a struct
// defined entirely inside dbms_handler.cpp (not declared in the header), so it cannot be driven directly
// from a test -- the only way to exercise Migrate's V1 branch is to hand-seed a durability kvstore with a
// V1-shaped entry on disk and then observe DbmsHandler's ctor behavior (restore loop) from the outside.
//
// This test uses its OWN isolated DbmsHandler instance (own temp dir), NOT the shared TestEnvironment
// above: TestEnvironment's DbmsHandler is a fresh (V0-then-migrated-empty) instance created once for the
// whole binary, so there is no seam to pre-seed a V1 entry into its durability kvstore before construction.
//
// Entry shape chosen: a plain V1 HOT entry (`{"uuid":.., "rel_dir":..}`, no `cold` marker) -- V1 durability
// predates hot/cold entirely, so every V1 entry is implicitly HOT (see the "V1 -> V2 is purely additive"
// comment in Migrate, dbms_handler.cpp). No pre-existing snapshot/WAL data is required: InMemoryStorage's
// constructor creates the tenant's `snapshots/`/`wal/` subdirectories itself (EnsureDirOrDie) and recovers
// cleanly against an empty pair of directories, exactly as it does for a brand-new tenant created via
// DbmsHandler::New() -- so a bare `{uuid, rel_dir}` durability entry with no on-disk data is a faithful,
// minimal V1 fixture.
TEST(DBMS_Handler, MigratesV1DurabilityAndRestoresTenant) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;

  const fs::path root = fs::temp_directory_path() / "MG_test_unit_dbms_handler_v1_migration";
  fs::remove_all(root);
  fs::create_directories(root);

  // Mirrors the DbmsHandler ctor's own layout (dbms_handler.cpp): <root>/databases/.durability
  const fs::path db_dir_local = root / std::string(memgraph::dbms::kMultiTenantDir);
  const fs::path durability_dir = db_dir_local / ".durability";
  fs::create_directories(durability_dir);

  const memgraph::utils::UUID tenant_uuid;
  const std::string tenant_uuid_str{tenant_uuid};
  // Same convention Migrate's V0->V1 upgrade uses for a non-default DB: a path relative to `root`,
  // rooted at <kMultiTenantDir>/<uuid>. The directory itself need not pre-exist (see comment above);
  // storage construction creates it.
  const fs::path rel_dir = fs::path(std::string(memgraph::dbms::kMultiTenantDir)) / tenant_uuid_str;

  {
    // Seed the durability kvstore BEFORE constructing DbmsHandler, then let this handle go out of
    // scope so its RocksDB LOCK is released -- KVStore's own contract (kvstore.hpp) forbids two live
    // instances open on the same directory at once.
    memgraph::kvstore::KVStore seed_kv{durability_dir};
    ASSERT_TRUE(seed_kv.Put("version", "V1"));

    // Exact shape of Durability::GenVal(uuid, rel_dir) (dbms_handler.cpp): {"uuid": <uuid>, "rel_dir": <path>}.
    // UUID serializes via its ADL to_json as the raw 16-byte array (utils/uuid.cpp); std::filesystem::path
    // has native nlohmann support in this vendored version, matching GenVal's `json[kRelDirKey] = rel_dir`.
    nlohmann::json v1_entry;
    v1_entry["uuid"] = tenant_uuid;
    v1_entry["rel_dir"] = rel_dir;
    ASSERT_TRUE(seed_kv.Put("database:db1", v1_entry.dump()));
  }

  // Construct a fresh DbmsHandler over the pre-seeded durability dir. Migrate() runs first in the ctor
  // (unconditionally) and must upgrade V1 -> V2 and leave the "database:db1" entry intact (V1 -> V2 is
  // purely additive); the restore loop must then bring db1 up HOT, with no throw/abort.
  memgraph::storage::Config conf;
  memgraph::storage::UpdatePaths(conf, root);
  conf.durability.snapshot_wal_mode =
      memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;

  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf))
      << "A well-formed V1 entry must migrate and restore cleanly, not be treated as corrupt";
  ASSERT_TRUE(handler);

  // The tenant must be restored HOT: present in All(), not suspended, and Get() must yield a live
  // accessor (a COLD/suspended restore, or a failed-and-skipped corrupt entry, would fail one of these).
  const auto all = handler->All();
  EXPECT_TRUE(std::find(all.begin(), all.end(), "db1") != all.end()) << "db1 must be in the HOT set after restore";
  EXPECT_FALSE(handler->IsSuspended("db1")) << "a V1 entry has no cold marker and must restore HOT, not COLD";

  auto db1_acc = handler->Get("db1");
  ASSERT_TRUE(db1_acc) << "Get() on the restored tenant must succeed";
  EXPECT_EQ(std::string(db1_acc->storage()->uuid()), tenant_uuid_str)
      << "the restored tenant must keep the UUID from the migrated V1 entry";
  db1_acc.reset();

  // The durability kvstore must now read back "V2": Migrate() bumps the version unconditionally as part
  // of the V1 upgrade. Re-open only after releasing the handler (same one-writer-at-a-time KVStore
  // contract as above).
  handler.reset();
  {
    memgraph::kvstore::KVStore verify_kv{durability_dir};
    auto version = verify_kv.Get("version");
    ASSERT_TRUE(version.has_value());
    EXPECT_EQ(*version, "V2") << "Migrate() must bump a V1 durability store to V2";

    // The database:db1 entry itself must have survived the migration untouched (V1 -> V2 is additive,
    // no data movement for an existing HOT entry).
    auto entry = verify_kv.Get("database:db1");
    ASSERT_TRUE(entry.has_value());
    const auto entry_json = nlohmann::json::parse(*entry);
    EXPECT_EQ(entry_json.at("uuid").get<memgraph::utils::UUID>(), tenant_uuid);
    EXPECT_FALSE(entry_json.value("cold", false)) << "a migrated V1 entry must not gain a cold marker";
  }

  fs::remove_all(root);
}

TEST(DBMS_Handler, MigratesV0DefaultDbDurabilityAndRestoresTenant) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;
  using memgraph::dbms::kDefaultDB;

  const fs::path root = fs::temp_directory_path() / "MG_test_unit_dbms_handler_v0_migration";
  fs::remove_all(root);
  fs::create_directories(root);

  // Mirrors the DbmsHandler ctor's own layout (dbms_handler.cpp): <root>/databases/.durability
  const fs::path db_dir_local = root / std::string(memgraph::dbms::kMultiTenantDir);
  const fs::path durability_dir = db_dir_local / ".durability";
  fs::create_directories(durability_dir);

  {
    // Seed a V0 durability kvstore BEFORE constructing DbmsHandler: no "version" key at all (so
    // VersionCheck reads V0), and a single BARE (un-prefixed) entry under the default DB's name.
    // Migrate's V0 loop only reads the key to decide whether/how to rewrite it -- the value itself
    // is discarded for every V0 entry (see `for (const auto &[key, _] : *durability)` in
    // dbms_handler.cpp, which binds the value to `_` and never reads it) -- so any placeholder
    // string is a faithful stand-in for whatever pre-V1 format actually lived there.
    //
    // The default DB is the special case in that same loop: `if (key != kDefaultDB)` skips the
    // directory-rename branch entirely for it, so its storage stays directly under `root` (no
    // kMultiTenantDir/<uuid> subdirectory, no pre-existing on-disk layout required here) --
    // identical to how a fresh single-tenant V0 instance is laid out.
    memgraph::kvstore::KVStore seed_kv{durability_dir};
    ASSERT_TRUE(seed_kv.Put(std::string{kDefaultDB}, "pre-v1-placeholder-value"));
  }

  // Construct a fresh DbmsHandler over the pre-seeded durability dir. Migrate() runs first in the
  // ctor (unconditionally) and must upgrade V0 -> V1 -> V2 in the SAME atomic batch (the fix under
  // test: version must not advance to V2 while the V0->V1 key rewrite is still pending), rewriting
  // the bare "memgraph" key into "database:memgraph"; the restore loop must then bring the default
  // DB up HOT, with no throw/abort.
  memgraph::storage::Config conf;
  memgraph::storage::UpdatePaths(conf, root);
  conf.durability.snapshot_wal_mode =
      memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;

  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf))
      << "A well-formed V0 default-DB entry must migrate and restore cleanly, not be treated as corrupt";
  ASSERT_TRUE(handler);

  // The default DB must be restored HOT: present in All(), not suspended, and Get() must yield a
  // live accessor (a COLD/suspended restore, or a failed-and-skipped corrupt entry, would fail one
  // of these).
  const auto all = handler->All();
  EXPECT_TRUE(std::find(all.begin(), all.end(), std::string{kDefaultDB}) != all.end())
      << "the default DB must be in the HOT set after restore";
  EXPECT_FALSE(handler->IsSuspended(kDefaultDB)) << "a V0 entry has no cold marker and must restore HOT, not COLD";

  auto default_acc = handler->Get(kDefaultDB);
  ASSERT_TRUE(default_acc) << "Get() on the restored default DB must succeed";
  default_acc.reset();

  // The durability kvstore must now read back "V2", and the bare "memgraph" key must have been
  // rewritten to the "database:"-prefixed key with a generated uuid + rel_dir -- both landing in the
  // SAME atomic batch that bumped the version (the fix under test). Re-open only after releasing the
  // handler (KVStore's one-writer-at-a-time contract).
  handler.reset();
  {
    memgraph::kvstore::KVStore verify_kv{durability_dir};
    auto version = verify_kv.Get("version");
    ASSERT_TRUE(version.has_value());
    EXPECT_EQ(*version, "V2") << "Migrate() must bump a V0 durability store to V2";

    // The bare, un-prefixed key must no longer exist: Migrate's V0 loop unconditionally rewrites it.
    EXPECT_FALSE(verify_kv.Get(std::string{kDefaultDB}).has_value()) << "the bare V0 key must not survive migration";

    // "database:" is Durability::kDBPrefix (dbms_handler.cpp, file-local) -- mirrored here as a
    // literal exactly like the V1 test above does for "database:db1", since that prefix isn't
    // exposed via any header this test can include.
    const std::string key = std::string{"database:"} + std::string{kDefaultDB};
    auto entry = verify_kv.Get(key);
    ASSERT_TRUE(entry.has_value()) << "the migrated default-DB entry must live under the database:-prefixed key";
    const auto entry_json = nlohmann::json::parse(*entry);
    EXPECT_TRUE(entry_json.contains("uuid")) << "Migrate's V0->V1 rewrite generates a fresh uuid";
    EXPECT_TRUE(entry_json.contains("rel_dir")) << "Migrate's V0->V1 rewrite records the tenant's rel_dir";
    EXPECT_FALSE(entry_json.value("cold", false)) << "a migrated V0 entry must not gain a cold marker";
  }

  fs::remove_all(root);
}

// A force-deleted Database vanishes from Handler::items_ immediately, but stays alive -- and its
// db_memory_tracker_ keeps parenting into the global graph_memory_tracker -- until every accessor is
// released and DeferDelete's deferred destructor runs. This test guards the fix giving each deferred
// destruction its own thread: a tenant nobody can drain must not block another tenant's release.
TEST(DBMS_Handler, StuckOrphanDoesNotStarveAnotherTenantsDeferredDelete) {
  auto &dbms = *TestEnvironment::get();

#if USE_JEMALLOC
  const int64_t global_baseline = memgraph::utils::graph_memory_tracker.Amount();
#endif

  auto new_t1 = dbms.New("starve_orphan_t1");
  ASSERT_TRUE(new_t1.has_value()) << (int)new_t1.error();
  memgraph::dbms::DatabaseAccess t1_acc = std::move(new_t1.value());

  auto new_t2 = dbms.New("starve_orphan_t2");
  ASSERT_TRUE(new_t2.has_value()) << (int)new_t2.error();
  memgraph::dbms::DatabaseAccess t2_acc = std::move(new_t2.value());

  // post_delete_func (dbms_handler.cpp:836) deletes these directories, so their disappearance is a
  // direct signal a deferred destruction ran -- unlike the memory tracker, independent of purge timing.
  const auto t1_dir = t1_acc->config().durability.storage_directory;
  const auto t2_dir = t2_acc->config().durability.storage_directory;

  constexpr size_t kNumVertices = 2000;
  constexpr size_t kPropertyBytes = 1024;
  const std::string blob(kPropertyBytes, 'y');
  auto write_payload = [&](memgraph::dbms::DatabaseAccess &acc) {
    // DbArenaScope required -- without it, writes land in an unattributed arena and db_memory_tracker_ never sees them.
    memgraph::memory::DbArenaScope db_arena_scope{acc.get()};
    auto storage_acc = acc->Access();
    ASSERT_TRUE(storage_acc);
    const auto property = storage_acc->NameToProperty("payload");
    for (size_t i = 0; i < kNumVertices; ++i) {
      auto vertex = storage_acc->CreateVertex();
      ASSERT_TRUE(vertex.SetProperty(property, memgraph::storage::PropertyValue(blob)).has_value());
    }
    ASSERT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  };
  write_payload(t1_acc);
  write_payload(t2_acc);

#if USE_JEMALLOC
  constexpr int64_t kTightToleranceBytes = 64 * 1024;
  const int64_t global_with_both = memgraph::utils::graph_memory_tracker.Amount();
  ASSERT_GT(global_with_both - global_baseline, static_cast<int64_t>(2 * kNumVertices * kPropertyBytes))
      << "both t1 and t2 must have an unambiguous, measurable footprint before either is deleted";
#endif

  // Both accessors are still held, so both deletes take DeferDelete's deferred path: try_delete()'s
  // count_==1 check fails while t1_acc / t2_acc are outstanding.
  auto del1 = dbms.Delete("starve_orphan_t1");
  ASSERT_TRUE(del1.has_value()) << (int)del1.error();
  auto del2 = dbms.Delete("starve_orphan_t2");
  ASSERT_TRUE(del2.has_value()) << (int)del2.error();

  // t2 has nothing else holding it now; t1 stays pinned by t1_acc and can never drain.
  t2_acc.reset();

  const bool t2_destroyed = WaitUntil(std::chrono::seconds(10), [&] { return !std::filesystem::exists(t2_dir); });
  EXPECT_TRUE(t2_destroyed) << "t2's deferred destruction must complete even though t1 is still pinned; its storage "
                               "directory is still present: "
                            << t2_dir;

  // t1 must NOT have been dragged along -- without this, the test would also pass for the wrong
  // reason if something else had released t1, without the two tenants actually being decoupled.
  EXPECT_TRUE(std::filesystem::exists(t1_dir))
      << "t1 is still held by t1_acc, so its destruction must NOT have completed";

#if USE_JEMALLOC
  // The customer-visible symptom: roughly one tenant's worth of memory (t2's) comes back while
  // roughly one tenant's worth (t1's) is still held.
  const int64_t after_t2 = memgraph::utils::graph_memory_tracker.Amount();
  EXPECT_GT(global_with_both - after_t2, static_cast<int64_t>(kNumVertices * kPropertyBytes))
      << "releasing t2 must return t2's memory even while t1 is stuck; with_both=" << global_with_both
      << " now=" << after_t2;
  EXPECT_GT(after_t2 - global_baseline, static_cast<int64_t>(kNumVertices * kPropertyBytes))
      << "t1's memory must still be accounted for while t1_acc is alive; now=" << after_t2
      << " baseline=" << global_baseline;
#endif

  t1_acc.reset();

#if USE_JEMALLOC
  const bool both_recovered = WaitUntil(std::chrono::seconds(10), [&] {
    return AbsDiff(memgraph::utils::graph_memory_tracker.Amount(), global_baseline) <= kTightToleranceBytes;
  });
  EXPECT_TRUE(both_recovered) << "both t1 and t2 must eventually be reclaimed once t1_acc is released; "
                                 "current amount: "
                              << memgraph::utils::graph_memory_tracker.Amount() << ", baseline: " << global_baseline;
#else
  // Without jemalloc the memory tracker reads 0, so t1_dir's disappearance is the completion signal
  // that both deferred destructions ran once t1_acc was released.
  EXPECT_TRUE(WaitUntil(std::chrono::seconds(10), [&] { return !std::filesystem::exists(t1_dir); }))
      << "t1's deferred destruction must complete once its accessor is released; " << t1_dir << " is still present";
#endif
  EXPECT_FALSE(std::filesystem::exists(t1_dir))
      << "t1's deferred destruction must complete once its accessor is released; " << t1_dir << " is still present";
}

// Pins the deferred-drop invariant: a force-dropped tenant with a live accessor is unaddressable by
// name immediately, but stays attributable (TenantMemorySum/AllDetached) until the drain retires it.
TEST(DBMS_Handler, DetachedTenantMemoryStaysAttributableWhileUnaddressable) {
  auto &dbms = *TestEnvironment::get();

  auto new_t1 = dbms.New("detached_mem_t1");
  ASSERT_TRUE(new_t1.has_value()) << (int)new_t1.error();
  memgraph::dbms::DatabaseAccess acc = std::move(new_t1.value());
  const auto tenant_uuid = acc->uuid();

  constexpr size_t kNumVertices = 4000;
  constexpr size_t kPropertyBytes = 1024;
  const std::string blob(kPropertyBytes, 'z');
  {
    // DbArenaScope required -- see StuckOrphanDoesNotStarveAnotherTenantsDeferredDelete above.
    memgraph::memory::DbArenaScope db_arena_scope{acc.get()};
    auto storage_acc = acc->Access();
    ASSERT_TRUE(storage_acc);
    const auto property = storage_acc->NameToProperty("payload");
    for (size_t i = 0; i < kNumVertices; ++i) {
      auto vertex = storage_acc->CreateVertex();
      ASSERT_TRUE(vertex.SetProperty(property, memgraph::storage::PropertyValue(blob)).has_value());
    }
    ASSERT_TRUE(storage_acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

#if USE_JEMALLOC
  const int64_t footprint = acc->DbMemoryUsage();
  ASSERT_GT(footprint, static_cast<int64_t>(kNumVertices * kPropertyBytes))
      << "the footprint must be unambiguous before it is used as a tolerance baseline below";

  const auto before = dbms.TenantMemorySum();
  ASSERT_GE(before.hot, footprint);
#endif

  // Force-drop while acc is still held: try_delete() times out and the destruction is deferred onto
  // its own drain thread (see DbmsHandler::Delete's single-arg, no-transaction overload).
  auto del = dbms.Delete("detached_mem_t1");
  ASSERT_TRUE(del.has_value()) << (int)del.error();

  ASSERT_ANY_THROW(dbms.Get("detached_mem_t1"));
  bool seen_by_foreach = false;
  dbms.ForEach([&](memgraph::dbms::DatabaseAccess db_acc) {
    if (db_acc->name() == "detached_mem_t1") seen_by_foreach = true;
  });
  EXPECT_FALSE(seen_by_foreach) << "a detached tenant must not be walkable via ForEach";
  {
    const auto statuses = dbms.AllWithHotColdStatus();
    EXPECT_TRUE(std::ranges::none_of(statuses, [](auto const &kv) {
      return kv.first == "detached_mem_t1" && kv.second == "HOT";
    })) << "a detached tenant must not be reported HOT";
  }

  {
    const auto all_detached = dbms.AllDetached();
    const auto it = std::ranges::find_if(
        all_detached, [&](memgraph::dbms::DbmsHandler::DetachedTenant const &d) { return d.uuid == tenant_uuid; });
    ASSERT_NE(it, all_detached.end()) << "the force-dropped, still-held tenant must have a detached row";
    EXPECT_EQ(it->name, "detached_mem_t1");
    EXPECT_EQ(it->reason, memgraph::dbms::DbmsHandler::DetachReason::DROP);
    EXPECT_GE(it->holders_at_detach, 1u);
#if USE_JEMALLOC
    EXPECT_LE(AbsDiff(it->memory_at_detach, footprint), footprint / 10)
        << "memory_at_detach=" << it->memory_at_detach << " footprint=" << footprint;
#endif
  }
  {
    const auto statuses = dbms.AllWithHotColdStatus();
    EXPECT_TRUE(std::ranges::any_of(
        statuses, [](auto const &kv) { return kv.first == "detached_mem_t1" && kv.second == "DETACHED"; }));
  }
#if USE_JEMALLOC
  {
    // The two halves are asserted separately on purpose: a regression that simply stopped counting the
    // tenant anywhere would still pass a test that only checked the (hot + detached) total.
    const auto after = dbms.TenantMemorySum();
    const int64_t tolerance = footprint / 10;
    EXPECT_GE(after.detached, footprint - tolerance) << "the bytes must have moved into the detached half";
    EXPECT_LE(after.hot, before.hot - (footprint - tolerance)) << "and must have left the hot half";
  }
#endif

  acc.reset();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  bool retired = false;
  do {
    const auto all_detached = dbms.AllDetached();
    retired = std::ranges::none_of(
        all_detached, [&](memgraph::dbms::DbmsHandler::DetachedTenant const &d) { return d.uuid == tenant_uuid; });
    if (retired) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ASSERT_LT(std::chrono::steady_clock::now(), deadline)
        << "detached_mem_t1's row must be retired once its drain completes";
  } while (true);
  EXPECT_TRUE(retired);

  const auto statuses_after_drain = dbms.AllWithHotColdStatus();
  EXPECT_TRUE(std::ranges::none_of(statuses_after_drain, [](auto const &kv) {
    return kv.first == "detached_mem_t1" && kv.second == "DETACHED";
  })) << "the DETACHED row must disappear from AllWithHotColdStatus once the row is retired";
}

// Negative control: with no accessor held, try_delete() succeeds inline, so the row must be retired
// synchronously too (see the detached_lock_ lock-order note, dbms_handler.hpp) or it leaks forever.
TEST(DBMS_Handler, DroppedTenantWithNoHoldersLeavesNoDetachedRow) {
  auto &dbms = *TestEnvironment::get();

  auto new_t2 = dbms.New("detached_mem_t2");
  ASSERT_TRUE(new_t2.has_value()) << (int)new_t2.error();
  memgraph::dbms::DatabaseAccess acc = std::move(new_t2.value());
  const auto tenant_uuid = acc->uuid();

  // Release before dropping so the destruction happens inline, not deferred.
  acc.reset();

  auto del = dbms.Delete("detached_mem_t2");
  ASSERT_TRUE(del.has_value()) << (int)del.error();

  const auto all_detached = dbms.AllDetached();
  EXPECT_TRUE(std::ranges::none_of(all_detached, [&](memgraph::dbms::DbmsHandler::DetachedTenant const &d) {
    return d.uuid == tenant_uuid;
  })) << "the inline fast path must never leave a detached row behind";

  const auto statuses = dbms.AllWithHotColdStatus();
  EXPECT_TRUE(std::ranges::none_of(statuses, [](auto const &kv) { return kv.first == "detached_mem_t2"; }))
      << "a fast-path-dropped tenant must not appear under any status";
}

// Pins the uuid-keyed registry against name reuse: DROP x (held) -> CREATE x -> DROP x (held) again
// must leave TWO rows in AllDetached() (one per uuid), while AllWithHotColdStatus() -- a name-keyed
// listing -- still reports the name exactly once.
TEST(DBMS_Handler, TwoDetachedTenantsCanShareANameAndAreCountedByUuid) {
  auto &dbms = *TestEnvironment::get();

  auto new_t1 = dbms.New("detached_reuse");
  ASSERT_TRUE(new_t1.has_value()) << (int)new_t1.error();
  memgraph::dbms::DatabaseAccess acc1 = std::move(new_t1.value());
  const auto uuid1 = acc1->uuid();

  auto del1 = dbms.Delete("detached_reuse");
  ASSERT_TRUE(del1.has_value()) << (int)del1.error();
  {
    const auto all_detached = dbms.AllDetached();
    EXPECT_TRUE(std::ranges::any_of(
        all_detached, [&](memgraph::dbms::DbmsHandler::DetachedTenant const &d) { return d.uuid == uuid1; }));
  }

  // The name is free again -- DeferDelete erased it from items_ unconditionally -- so re-creating it
  // must succeed; that is itself load-bearing, since it's what forces two rows to share a name below.
  auto new_t2 = dbms.New("detached_reuse");
  ASSERT_TRUE(new_t2.has_value()) << (int)new_t2.error();
  memgraph::dbms::DatabaseAccess acc2 = std::move(new_t2.value());
  const auto uuid2 = acc2->uuid();
  ASSERT_NE(uuid2, uuid1);

  auto del2 = dbms.Delete("detached_reuse");
  ASSERT_TRUE(del2.has_value()) << (int)del2.error();

  {
    const auto all_detached = dbms.AllDetached();
    EXPECT_EQ(std::ranges::count_if(
                  all_detached, [&](memgraph::dbms::DbmsHandler::DetachedTenant const &d) { return d.uuid == uuid1; }),
              1)
        << "a name-keyed registry would have clobbered uuid1's row when uuid2 was recorded";
    EXPECT_EQ(std::ranges::count_if(
                  all_detached, [&](memgraph::dbms::DbmsHandler::DetachedTenant const &d) { return d.uuid == uuid2; }),
              1);
  }
  {
    // AllWithHotColdStatus's own de-dup is load-bearing here: the interpreter push_backs one row per
    // returned pair with no de-dup of its own, so an un-collapsed duplicate would render as two lines.
    const auto statuses = dbms.AllWithHotColdStatus();
    EXPECT_EQ(std::ranges::count_if(
                  statuses, [](auto const &kv) { return kv.first == "detached_reuse" && kv.second == "DETACHED"; }),
              1);
  }

  acc1.reset();
  acc2.reset();
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  bool retired = false;
  do {
    const auto all_detached = dbms.AllDetached();
    retired = std::ranges::none_of(all_detached, [&](memgraph::dbms::DbmsHandler::DetachedTenant const &d) {
      return d.uuid == uuid1 || d.uuid == uuid2;
    });
    if (retired) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    ASSERT_LT(std::chrono::steady_clock::now(), deadline)
        << "both detached_reuse rows must be retired once their drains complete";
  } while (true);
  EXPECT_TRUE(retired);

  const auto statuses_after_drain = dbms.AllWithHotColdStatus();
  EXPECT_TRUE(std::ranges::none_of(statuses_after_drain, [](auto const &kv) { return kv.first == "detached_reuse"; }));
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  // gtest takes ownership of the TestEnvironment ptr - we don't delete it.
  ::testing::AddGlobalTestEnvironment(new TestEnvironment);
  return RUN_ALL_TESTS();
}

#endif
