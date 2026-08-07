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
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <latch>
#include <mutex>
#include <optional>
#include <system_error>
#include <thread>
#include <utility>

#include <nlohmann/json.hpp>

#include "dbms/constants.hpp"
#include "dbms/dbms_handler.hpp"
#include "dbms/global.hpp"
#include "dbms/tenant_profiles.hpp"
#include "glue/auth_checker.hpp"
#include "glue/auth_handler.hpp"
#include "kvstore/kvstore.hpp"
#include "memory/db_arena.hpp"
#include "query/config.hpp"
#include "query/interpreter.hpp"
#include "system/system.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/on_scope_exit.hpp"
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

// Seeding helpers for the startup-reconciliation tests. Each test seeds its kvstore inside a scope and
// releases it before constructing the handler: a second live KVStore on one dir throws (kvstore.cpp:38).

// Mirrors kDBPrefix (dbms_handler.cpp:46), which is in an anonymous namespace and cannot be named here.
constexpr std::string_view kDBPrefixLiteral = "database:";

struct SeededRoot {
  std::filesystem::path root;
  std::filesystem::path db_dir;          // <root>/databases
  std::filesystem::path durability_dir;  // <root>/databases/.durability
};

// Layout must match the one the DbmsHandler ctor builds for itself (dbms_handler.cpp:215-219). `tag` must
// be unique per test: the root is remove_all'd below, so a shared tag would delete a sibling test's data.
SeededRoot MakeSeededRoot(std::string_view tag) {
  namespace fs = std::filesystem;
  SeededRoot sr;
  sr.root = fs::temp_directory_path() / (std::string{"MG_test_unit_dbms_handler_"} + std::string{tag});
  fs::remove_all(sr.root);
  sr.db_dir = sr.root / std::string{memgraph::dbms::kMultiTenantDir};
  sr.durability_dir = sr.db_dir / ".durability";
  fs::create_directories(sr.durability_dir);
  return sr;
}

// Exact shape of Durability::GenVal (dbms_handler.cpp:114): {"uuid": <uuid>, "rel_dir": <path>}, with
// rel_dir rooted at kMultiTenantDir/<uuid> as New_/UpdateDurability recompute it (dbms_handler.cpp:860).
struct SeededHotEntry {
  memgraph::utils::UUID uuid;
  // Bytes that SeedHotEntry wrote for this entry. Previously read back verbatim by
  // RenameMovesTenantDurabilityRecordVerbatim; that test now captures the live post-construction value
  // directly, so this field is no longer read but kept to avoid reshaping the return type.
  std::string json_str;
};

SeededHotEntry SeedHotEntry(memgraph::kvstore::KVStore &kv, std::string_view name) {
  const memgraph::utils::UUID uuid;
  nlohmann::json j;
  j["uuid"] = uuid;
  j["rel_dir"] = std::filesystem::path(std::string{memgraph::dbms::kMultiTenantDir}) / std::string{uuid};
  auto json_str = j.dump();
  kv.Put(std::string{kDBPrefixLiteral} + std::string{name}, json_str);
  return {uuid, std::move(json_str)};
}

// Counts actions applied via Transaction::Commit: Commit's early-return path (empty actions_) calls Abort(),
// never ApplyAction, so applied==1 vs 0 discriminates "AddAction<RenameDatabase> ran" from "skipped" -- a
// distinction CanReplicateInCommunity() can't make (false for both zero actions and one dbms action).
struct CountingReplicationPolicy {
  int *applied;

  auto ApplyAction(const memgraph::system::ISystemAction & /*action*/, const memgraph::system::Transaction & /*txn*/)
      -> memgraph::system::AllSyncReplicaStatus {
    ++*applied;
    return memgraph::system::AllSyncReplicaStatus::AllCommitsConfirmed;
  }

  auto FinalizeTransaction(const memgraph::system::Transaction & /*txn*/) -> memgraph::system::AllSyncReplicaStatus {
    return memgraph::system::AllSyncReplicaStatus::AllCommitsConfirmed;
  }
};

// Exact shape of Durability::GenColdVal (dbms_handler.cpp:150) minus `cold_stats`, which the restore loop
// reads under `json.contains("cold_stats")` (dbms_handler.cpp:239) -- omitting it is still faithful.
memgraph::utils::UUID SeedColdEntry(memgraph::kvstore::KVStore &kv, std::string_view name) {
  const memgraph::utils::UUID uuid;
  nlohmann::json j;
  j["uuid"] = uuid;
  j["rel_dir"] = std::filesystem::path(std::string{memgraph::dbms::kMultiTenantDir}) / std::string{uuid};
  j["cold"] = true;
  kv.Put(std::string{kDBPrefixLiteral} + std::string{name}, j.dump());
  return uuid;
}

// Exact shape of TenantProfiles::ProfileToJson (tenant_profiles.cpp:46): {"memory_limit": <int64>,
// "databases": [...]}, plus the kDbMappingPrefix rows AttachToDatabase writes (tenant_profiles.cpp:142).
void SeedProfile(memgraph::kvstore::KVStore &kv, std::string_view profile_name, int64_t memory_limit,
                 const std::set<std::string> &databases) {
  nlohmann::json j;
  j["memory_limit"] = memory_limit;
  j["databases"] = databases;
  kv.Put(std::string{memgraph::dbms::TenantProfiles::kPrefix} + std::string{profile_name}, j.dump());
  for (const auto &db : databases) {
    kv.Put(std::string{memgraph::dbms::TenantProfiles::kDbMappingPrefix} + db, std::string{profile_name});
  }
}

std::filesystem::path TenantDataDir(const SeededRoot &sr, const memgraph::utils::UUID &uuid) {
  return sr.db_dir / std::string{uuid};
}

memgraph::storage::Config MakeSeededConfig(const std::filesystem::path &root) {
  memgraph::storage::Config conf;
  memgraph::storage::UpdatePaths(conf, root);
  conf.durability.snapshot_wal_mode =
      memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  return conf;
}

// Unsigned-safe absolute difference for the memory-tracker snapshots below (int64_t IDs can be
// negative in theory; std::abs overload resolution on int64_t is platform-fiddly, so spell it out).
int64_t AbsDiff(int64_t lhs, int64_t rhs) { return lhs > rhs ? lhs - rhs : rhs - lhs; }

// Bounded poll: retries `pred` (checking wall-clock time only between retries, never spinning
// unbounded) until it returns true or `timeout` elapses. Every wait in the memory-attribution tests
// below is bounded like this, because what's being waited on is a background thread pool's progress,
// not a fixed-latency operation -- an unbounded wait would hang forever on a real regression, and a
// single fixed sleep would either flake (too short) or slow the suite down for nothing (too long).
template <typename Pred>
bool WaitUntil(std::chrono::milliseconds timeout, Pred &&pred) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  do {
    if (pred()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  } while (std::chrono::steady_clock::now() < deadline);
  return pred();
}

// Runs `f` on its own thread and waits up to `timeout`. Returns {true, f()'s result} if it finished in
// time. On timeout it detaches instead of joining -- the promise is heap-owned via shared_ptr, so a
// detached thread finishing later (or never, e.g. it is itself wedged) is safe, not a dangling
// reference -- and returns {false, std::nullopt}, so a hang FAILs only the calling assertion, never
// the whole binary. Mirrors hot_cold_gatekeeper.cpp's DtorOfDrainingGatekeeperReturnsPromptly.
template <typename F>
auto RunBounded(std::chrono::milliseconds timeout, F f) -> std::pair<bool, std::optional<std::invoke_result_t<F>>> {
  using T = std::invoke_result_t<F>;
  auto prom = std::make_shared<std::promise<T>>();
  auto fut = prom->get_future();
  std::thread worker([f = std::move(f), prom]() mutable { prom->set_value(f()); });
  const auto status = fut.wait_for(timeout);
  if (status == std::future_status::ready) {
    worker.join();
    return {true, fut.get()};
  }
  worker.detach();
  return {false, std::nullopt};
}

// Wedges a tenant's after-commit-trigger thread pool mid-task so a concurrent Delete()'s Phase 2
// (Database::StopAllBackgroundTasks() -> ThreadPool::ShutDown() -> its jthread vector's destructor;
// see dbms_handler.cpp's Delete_ Phase 2, database.cpp's StopAllBackgroundTasks, and
// thread_pool.cpp's ShutDown/~ThreadPool) blocks on that jthread join for a bounded, test-controlled
// window. This gives the drain window (between Delete_'s Phase 1 RecordDetached_ and the DeferDelete
// handoff that erases the gatekeeper from db_handler_) an actual, observable witness
// instead of inferring it from sleep durations. AddTask()/thread_pool() are public Database API
// (database.hpp), not a test-only seam.
class PhaseTwoStall {
 public:
  explicit PhaseTwoStall(memgraph::dbms::DatabaseAccess &acc) {
    acc->AddTask([this] {
      {
        std::lock_guard<std::mutex> set_running(mtx_);
        running_ = true;
      }
      running_cv_.notify_all();
      std::unique_lock<std::mutex> wait_lock(mtx_);
      // 10s is a safety net only, in case a test forgets to call Release() (e.g. an early ASSERT
      // return): every caller below releases well inside that bound.
      released_cv_.wait_for(wait_lock, std::chrono::seconds(10), [this] { return released_; });
    });
  }

  // False (not a hang) if the pool's single worker never dequeues the stalling task within `timeout`.
  bool WaitUntilRunning(std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    std::unique_lock<std::mutex> lock(mtx_);
    return running_cv_.wait_for(lock, timeout, [this] { return running_; });
  }

  void Release() {
    {
      std::lock_guard<std::mutex> lock(mtx_);
      released_ = true;
    }
    released_cv_.notify_all();
  }

 private:
  std::mutex mtx_;
  std::condition_variable running_cv_;
  std::condition_variable released_cv_;
  bool running_ = false;
  bool released_ = false;
};
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

// Pins PRE-EXISTING behavior: the ctor's post-restore "DATABASES CLEAN UP" pass (dbms_handler.cpp:345)
// keeps only dirs a surviving `database:` key names, so a lost physical delete is not a permanent leak.
TEST(DBMS_Handler, SweepReclaimsOrphanedTenantDirectoryButKeepsLiveOne) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;

  auto sr = MakeSeededRoot("sweep");

  memgraph::utils::UUID live_uuid;
  {
    memgraph::kvstore::KVStore seed_kv{sr.durability_dir};
    ASSERT_TRUE(seed_kv.Put("version", "V2"));
    live_uuid = SeedHotEntry(seed_kv, "live").uuid;
  }

  const auto live_dir = TenantDataDir(sr, live_uuid);
  const memgraph::utils::UUID orphan_uuid;  // deliberately has NO matching `database:` durability key
  const auto orphan_dir = TenantDataDir(sr, orphan_uuid);
  fs::create_directories(live_dir);
  fs::create_directories(orphan_dir);
  {
    std::ofstream marker{orphan_dir / "leftover.txt"};
    marker << "orphaned tenant data";
  }
  ASSERT_TRUE(fs::exists(orphan_dir / "leftover.txt")) << "sanity check: the orphan dir must be non-empty";

  auto conf = MakeSeededConfig(sr.root);
  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf));

  EXPECT_FALSE(fs::exists(orphan_dir))
      << "a directory whose durability key is already gone must be swept on boot, non-empty or not";
  EXPECT_TRUE(fs::exists(live_dir))
      << "a directory still referenced by a live durability key must survive the sweep untouched";
  const auto all = handler->All();
  EXPECT_TRUE(std::find(all.begin(), all.end(), "live") != all.end()) << "the live tenant must still restore HOT";

  handler.reset();
  fs::remove_all(sr.root);
}

// Positive case for the new startup reconciliation: the ctor runs PruneDatabases between constructing
// TenantProfiles and RestoreTenantProfiles_ (dbms_handler.cpp:368-385); rationale at its declaration.
TEST(DBMS_Handler, StaleTenantProfileMappingIsPrunedOnStartup) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;
  using memgraph::dbms::TenantProfiles;

  auto sr = MakeSeededRoot("prune_stale");

  memgraph::utils::UUID live_uuid;
  {
    memgraph::kvstore::KVStore seed_kv{sr.durability_dir};
    ASSERT_TRUE(seed_kv.Put("version", "V2"));
    live_uuid = SeedHotEntry(seed_kv, "live").uuid;
    // "gone" has NO `database:gone` durability key -- exactly the state a lost DetachFromDatabase leaves.
    SeedProfile(seed_kv, "p", /*memory_limit=*/1000, {"live", "gone"});
  }
  fs::create_directories(TenantDataDir(sr, live_uuid));

  auto conf = MakeSeededConfig(sr.root);
  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf));
  handler.reset();

  memgraph::kvstore::KVStore verify_kv{sr.durability_dir};
  EXPECT_FALSE(verify_kv.Get(std::string{TenantProfiles::kDbMappingPrefix} + "gone").has_value())
      << "a db_tenant_profile mapping for a database with no durability key must be pruned on startup";
  auto live_mapping = verify_kv.Get(std::string{TenantProfiles::kDbMappingPrefix} + "live");
  ASSERT_TRUE(live_mapping.has_value()) << "the mapping for a still-live database must survive the prune";
  EXPECT_EQ(*live_mapping, "p") << "the surviving mapping's target profile must be unchanged";

  auto profile_json = verify_kv.Get(std::string{TenantProfiles::kPrefix} + "p");
  ASSERT_TRUE(profile_json.has_value()) << "pruning a stale attachment must not delete the profile itself";
  const auto dbs = nlohmann::json::parse(*profile_json).at("databases").get<std::set<std::string>>();
  EXPECT_FALSE(dbs.contains("gone")) << "the stale name must be removed from the profile's databases set too";
  EXPECT_TRUE(dbs.contains("live")) << "pruning 'gone' must not disturb the profile's still-live attachment";

  fs::remove_all(sr.root);
}

// Negative control for PruneDatabases: a COLD tenant looks absent through the HOT-gated Get_/All lens, so
// pruning its attachment would be data loss. Pins the ctor's raw-`database:`-key live-set (dbms_handler.cpp:371).
TEST(DBMS_Handler, SuspendedTenantProfileMappingIsNotPrunedOnStartup) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;
  using memgraph::dbms::TenantProfiles;

  auto sr = MakeSeededRoot("prune_cold");

  memgraph::utils::UUID cold_uuid;
  {
    memgraph::kvstore::KVStore seed_kv{sr.durability_dir};
    ASSERT_TRUE(seed_kv.Put("version", "V2"));
    cold_uuid = SeedColdEntry(seed_kv, "cold");
    SeedProfile(seed_kv, "p", /*memory_limit=*/1000, {"cold"});
  }
  const auto cold_dir = TenantDataDir(sr, cold_uuid);
  fs::create_directories(cold_dir);

  auto conf = MakeSeededConfig(sr.root);
  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf));
  EXPECT_TRUE(handler->IsSuspended("cold"))
      << "the seeded entry must restore as a live COLD tenant -- otherwise this test would vacuously "
         "pass by testing an already-absent database instead of a suspended one";
  handler.reset();

  memgraph::kvstore::KVStore verify_kv{sr.durability_dir};
  auto mapping = verify_kv.Get(std::string{TenantProfiles::kDbMappingPrefix} + "cold");
  ASSERT_TRUE(mapping.has_value())
      << "a COLD tenant's profile attachment must survive startup -- pruning it would be unrecoverable";
  EXPECT_EQ(*mapping, "p");

  auto profile_json = verify_kv.Get(std::string{TenantProfiles::kPrefix} + "p");
  ASSERT_TRUE(profile_json.has_value());
  const auto dbs = nlohmann::json::parse(*profile_json).at("databases").get<std::set<std::string>>();
  EXPECT_TRUE(dbs.contains("cold")) << "the profile's databases set must still list the suspended tenant";

  EXPECT_TRUE(fs::exists(cold_dir)) << "a COLD tenant's data directory must also survive the unused-directory sweep";

  fs::remove_all(sr.root);
}

// This branch adds a startup reconciliation pass and reorders the DROP paths, but touches neither
// Durability::Migrate nor its version chain: a plain V2 store must still boot, stay V2, and keep its entry.
TEST(DBMS_Handler, ExistingV2DurabilityStoreBootsUnchanged) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;

  auto sr = MakeSeededRoot("v2_unchanged");

  memgraph::utils::UUID live_uuid;
  {
    memgraph::kvstore::KVStore seed_kv{sr.durability_dir};
    ASSERT_TRUE(seed_kv.Put("version", "V2"));
    live_uuid = SeedHotEntry(seed_kv, "live").uuid;
  }
  fs::create_directories(TenantDataDir(sr, live_uuid));

  auto conf = MakeSeededConfig(sr.root);
  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf))
      << "a plain, already-migrated V2 store must boot cleanly";

  const auto all = handler->All();
  EXPECT_TRUE(std::find(all.begin(), all.end(), "live") != all.end()) << "the V2 HOT entry must restore HOT";
  EXPECT_FALSE(handler->IsSuspended("live"));
  handler.reset();

  memgraph::kvstore::KVStore verify_kv{sr.durability_dir};
  auto version = verify_kv.Get("version");
  ASSERT_TRUE(version.has_value());
  EXPECT_EQ(*version, "V2") << "this branch must not introduce a durability version bump";

  auto entry = verify_kv.Get(std::string{"database:"} + "live");
  ASSERT_TRUE(entry.has_value());
  // Deliberately not a byte compare: New_ rewrites every restored HOT entry via UpdateDurability, which
  // recomputes rel_dir through std::filesystem::relative (dbms_handler.cpp:860) -- bytes would be flaky.
  const auto entry_json = nlohmann::json::parse(*entry);
  const auto expected_rel_dir =
      std::filesystem::path(std::string{memgraph::dbms::kMultiTenantDir}) / std::string{live_uuid};
  EXPECT_EQ(entry_json.at("uuid").get<memgraph::utils::UUID>(), live_uuid)
      << "the restored entry's uuid must be unchanged from what was seeded";
  EXPECT_EQ(entry_json.at("rel_dir").get<std::filesystem::path>(), expected_rel_dir)
      << "the restored entry's rel_dir must still point at the seeded tenant directory";
  EXPECT_FALSE(entry_json.value("cold", false)) << "a HOT entry must not acquire a cold marker on restart";

  fs::remove_all(sr.root);
}

// A corrupt attached profile record must not turn DROP into a half-done delete: DetachFromDatabase
// refuses it (DURABILITY_ERROR) and TryDelete must still retire the key + dir (dbms_handler.cpp:479-488).
TEST(DBMS_Handler, DropSucceedsDurablyWhenAttachedProfileJsonIsCorrupt) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;
  using memgraph::dbms::TenantProfiles;

  auto sr = MakeSeededRoot("drop_corrupt_profile");

  memgraph::utils::UUID victim_uuid;
  {
    memgraph::kvstore::KVStore seed_kv{sr.durability_dir};
    ASSERT_TRUE(seed_kv.Put("version", "V2"));
    victim_uuid = SeedHotEntry(seed_kv, "victim").uuid;
    // Point "victim" at profile "p", but write "p"'s durable record as deliberately broken JSON --
    // NOT via SeedProfile, which would also write a well-formed tenant_profile:p row.
    ASSERT_TRUE(seed_kv.Put(std::string{TenantProfiles::kDbMappingPrefix} + "victim", "p"));
    ASSERT_TRUE(seed_kv.Put(std::string{TenantProfiles::kPrefix} + "p", "{not json"));
  }
  fs::create_directories(TenantDataDir(sr, victim_uuid));

  auto conf = MakeSeededConfig(sr.root);
  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf));

  std::optional<DbmsHandler::DeleteResult> del;
  ASSERT_NO_THROW(del = handler->TryDelete("victim"))
      << "a corrupt attached tenant-profile record must not be able to throw a JSON exception out of DROP";
  ASSERT_TRUE(del.has_value());
  ASSERT_TRUE(del->has_value()) << "DROP must succeed even though the attached profile record is corrupt";

  const auto all = handler->All();
  EXPECT_TRUE(std::find(all.begin(), all.end(), "victim") == all.end())
      << "victim must be gone from All() immediately after a successful drop";
  handler.reset();

  {
    memgraph::kvstore::KVStore verify_kv{sr.durability_dir};
    EXPECT_FALSE(verify_kv.Get(std::string{kDBPrefixLiteral} + "victim").has_value())
        << "a corrupt attached profile record must not stop the tenant's durability key from being erased: a "
           "surviving key brings the tenant back on the next boot";
    EXPECT_FALSE(fs::exists(TenantDataDir(sr, victim_uuid)))
        << "the tenant's on-disk data directory must also be removed";

    auto mapping = verify_kv.Get(std::string{TenantProfiles::kDbMappingPrefix} + "victim");
    EXPECT_TRUE(mapping.has_value())
        << "DetachFromDatabase deliberately leaves the mapping in place when the profile it points at is "
           "corrupt -- PruneDatabases on the next boot is what collects it, not this call";
  }

  {
    std::unique_ptr<DbmsHandler> handler2;
    ASSERT_NO_THROW(handler2 = std::make_unique<DbmsHandler>(conf));
    handler2.reset();
  }

  memgraph::kvstore::KVStore verify_kv2{sr.durability_dir};
  EXPECT_FALSE(verify_kv2.Get(std::string{TenantProfiles::kDbMappingPrefix} + "victim").has_value())
      << "boot reconciliation (PruneDatabases) must collect the leftover mapping on the very next boot, "
         "even though the profile it names never parses";

  fs::remove_all(sr.root);
}

// A corrupt attached tenant-profile record must not turn RENAME into a false failure, nor skip
// txn->AddAction<RenameDatabase>. TenantProfiles::RenameDatabase used to parse the persisted profile record
// with an unguarded nlohmann::json::parse, reached *after* DbmsHandler::Rename had already committed the
// tenant rename both in memory and durably -- so the throw propagated out of Rename, telling the client the
// RENAME had failed for an operation that had fully succeeded, and skipping AddAction<RenameDatabase>
// (below the throw site), so the rename was never replicated. MAIN and the replica then permanently
// disagreed on the tenant's name. Fixed by c4479c893 (catch json::exception in TenantProfiles::RenameDatabase,
// return DURABILITY_ERROR instead) + 28b1e9046 (surface, but don't propagate, that error in Rename).
TEST(DBMS_Handler, RenameSucceedsAndReplicatesWhenAttachedProfileJsonIsCorrupt) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;
  using memgraph::dbms::TenantProfiles;

  auto sr = MakeSeededRoot("rename_corrupt_profile");

  memgraph::utils::UUID victim_uuid;
  {
    memgraph::kvstore::KVStore seed_kv{sr.durability_dir};
    ASSERT_TRUE(seed_kv.Put("version", "V2"));
    victim_uuid = SeedHotEntry(seed_kv, "victim").uuid;
    ASSERT_TRUE(seed_kv.Put(std::string{TenantProfiles::kDbMappingPrefix} + "victim", "p"));
    // Deliberately broken JSON -- NOT via SeedProfile, which would write a well-formed tenant_profile:p row.
    ASSERT_TRUE(seed_kv.Put(std::string{TenantProfiles::kPrefix} + "p", "{not json"));
  }
  fs::create_directories(TenantDataDir(sr, victim_uuid));

  auto conf = MakeSeededConfig(sr.root);
  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf));

  // Drive the rename through a real system transaction so replication (AddAction<RenameDatabase>) is
  // directly observable, rather than inferred.
  memgraph::system::System sys;  // default ctor: no storage, no recovery
  auto txn = sys.TryCreateTransaction();
  ASSERT_TRUE(txn.has_value());

  std::optional<DbmsHandler::RenameResult> res;
  ASSERT_NO_THROW(res = handler->Rename("victim", "renamed", &*txn))
      << "a corrupt attached tenant-profile record must not be able to throw a JSON exception out of RENAME";
  ASSERT_TRUE(res.has_value());
  ASSERT_TRUE(res->has_value())
      << "RENAME must report SUCCESS even though the attached profile record is corrupt -- reporting failure "
         "for an operation that fully succeeded is the exact bug";

  int applied = 0;
  txn->Commit(CountingReplicationPolicy{&applied});
  EXPECT_EQ(applied, 1)
      << "the RenameDatabase system action must have been recorded so the rename replicates -- pre-fix this "
         "was 0 because the throw skipped AddAction, which is what made MAIN and the replica disagree on the "
         "tenant name permanently";

  const auto all = handler->All();
  EXPECT_TRUE(std::find(all.begin(), all.end(), "renamed") != all.end()) << "'renamed' must be visible after RENAME";
  EXPECT_TRUE(std::find(all.begin(), all.end(), "victim") == all.end())
      << "'victim' must no longer be visible after RENAME";
  handler.reset();

  {
    memgraph::kvstore::KVStore verify_kv{sr.durability_dir};
    EXPECT_TRUE(verify_kv.Get(std::string{kDBPrefixLiteral} + "renamed").has_value())
        << "database:renamed must exist -- the durability record must have moved to the new name";
    EXPECT_FALSE(verify_kv.Get(std::string{kDBPrefixLiteral} + "victim").has_value())
        << "database:victim must no longer exist";

    auto mapping = verify_kv.Get(std::string{TenantProfiles::kDbMappingPrefix} + "victim");
    EXPECT_TRUE(mapping.has_value())
        << "the stale db_tenant_profile:victim mapping is deliberately left in place here -- the next boot's "
           "PruneDatabases collects it, not this call";
  }

  {
    std::unique_ptr<DbmsHandler> handler2;
    ASSERT_NO_THROW(handler2 = std::make_unique<DbmsHandler>(conf));
    handler2.reset();
  }

  memgraph::kvstore::KVStore verify_kv2{sr.durability_dir};
  EXPECT_FALSE(verify_kv2.Get(std::string{TenantProfiles::kDbMappingPrefix} + "victim").has_value())
      << "boot reconciliation (PruneDatabases) must collect the leftover db_tenant_profile:victim mapping on "
         "the very next boot, even though the profile it names never parses";

  fs::remove_all(sr.root);
}

// The tenant's own `database:` durability record must move VERBATIM on RENAME, not be parsed and rewritten.
// Pre-fix, DbmsHandler::Rename did `json = parse(old_val); json["name"] = new_name; Put(new_key, json.dump())`.
// Durability::GenVal never writes a "name" field, and nothing in the codebase ever reads one back -- the
// tenant's name lives in the KEY, and the restore loop derives it via key.substr(kDBPrefix.size()) -- so that
// write was pure litter added on every rename, and doubled as a second corrupt-record throw site (fixed
// alongside the profile one by 28b1e9046). This test doubles as the healthy-rename negative control: it
// proves the fix is not over-broad by checking a plain, non-corrupt rename still behaves.
TEST(DBMS_Handler, RenameMovesTenantDurabilityRecordVerbatim) {
  namespace fs = std::filesystem;
  using memgraph::dbms::DbmsHandler;

  auto sr = MakeSeededRoot("rename_verbatim");

  auto seeded = [&] {
    memgraph::kvstore::KVStore seed_kv{sr.durability_dir};
    seed_kv.Put("version", "V2");
    return SeedHotEntry(seed_kv, "before");
  }();
  fs::create_directories(TenantDataDir(sr, seeded.uuid));

  auto conf = MakeSeededConfig(sr.root);
  std::unique_ptr<DbmsHandler> handler;
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf));

  // Capture the live durable value the ctor (New_->UpdateDurability) wrote for "before". The handler
  // holds the KVStore lock (one-writer-at-a-time contract, see comment above SeedHotEntry), so reset
  // it first, read in a fresh KVStore scope, then reconstruct. UpdateDurability is deterministic for
  // the same (uuid, rel_dir) pair, so the second construction leaves "before"'s value unchanged and
  // live_before remains equal to whatever the rename will consume.
  std::string live_before;
  {
    handler.reset();
    memgraph::kvstore::KVStore snap_kv{sr.durability_dir};
    auto v = snap_kv.Get(std::string{kDBPrefixLiteral} + "before");
    ASSERT_TRUE(v.has_value()) << "database:before must exist after handler construction";
    live_before = *v;
  }
  ASSERT_NO_THROW(handler = std::make_unique<DbmsHandler>(conf));

  auto res = handler->Rename("before", "after");
  ASSERT_TRUE(res.has_value()) << "a plain, healthy rename must succeed";

  const auto all = handler->All();
  EXPECT_TRUE(std::find(all.begin(), all.end(), "after") != all.end());
  EXPECT_TRUE(std::find(all.begin(), all.end(), "before") == all.end());
  handler.reset();

  {
    memgraph::kvstore::KVStore verify_kv{sr.durability_dir};
    auto after_val = verify_kv.Get(std::string{kDBPrefixLiteral} + "after");
    ASSERT_TRUE(after_val.has_value()) << "database:after must exist";
    EXPECT_EQ(*after_val, live_before)
        << "the moved record must be byte-identical to the live value that was under database:before "
           "immediately before the rename; the pre-fix behavior (parse -> inject \"name\" field -> dump) "
           "would produce a different byte string and fail this comparison";
    EXPECT_FALSE(verify_kv.Get(std::string{kDBPrefixLiteral} + "before").has_value())
        << "database:before must no longer exist";

    const auto entry_json = nlohmann::json::parse(*after_val);
    EXPECT_FALSE(entry_json.contains("name"))
        << "the pre-fix code injected a \"name\" field on every rename; Durability::GenVal never writes one "
           "and nothing reads it back -- it was write-only litter";
    EXPECT_TRUE(entry_json.contains("uuid"));
    EXPECT_TRUE(entry_json.contains("rel_dir"));
  }

  {
    std::unique_ptr<DbmsHandler> handler2;
    ASSERT_NO_THROW(handler2 = std::make_unique<DbmsHandler>(conf));
    const auto all2 = handler2->All();
    EXPECT_TRUE(std::find(all2.begin(), all2.end(), "after") != all2.end())
        << "a fresh boot must restore the tenant under its renamed name";
    handler2.reset();
  }

  EXPECT_TRUE(fs::exists(TenantDataDir(sr, seeded.uuid))) << "the tenant's data directory must be untouched by RENAME";

  fs::remove_all(sr.root);
}

// --- Memory attribution for a force-deleted-while-held Database ---
//
// A Database force-deleted via DbmsHandler::Delete (NOT TryDelete, which would refuse with USING)
// while a DatabaseAccess is still held becomes invisible to DbmsHandler::Get/ForEach immediately:
// Handler::DeferDelete erases the entry from `items_` unconditionally, whether or not
// Gatekeeper::Accessor::try_delete() managed to delete synchronously. But the Database object stays
// ALIVE until its deferred destructor actually runs, which cannot happen until every outstanding
// accessor is released. Meanwhile its db_memory_tracker_ still parents into the global
// utils::graph_memory_tracker, so its bytes stay counted globally even though the tenant has
// vanished from the per-tenant reachable set. That is the "global total far exceeds the sum over
// tenants" gap.
//
// This test covers the part that made one stuck tenant expensive: each deferred destruction gets its
// own thread, so a tenant nobody can drain must not hold up an unrelated tenant's destruction (and
// its memory) behind it.
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

  // Captured while the accessors are alive: post_delete_func removes these directories, so their
  // disappearance is a direct, binary signal that a tenant's deferred destruction actually ran --
  // independent of any allocator or memory-tracker bookkeeping.
  const auto t1_dir = t1_acc->config().durability.storage_directory;
  const auto t2_dir = t2_acc->config().durability.storage_directory;

  constexpr size_t kNumVertices = 2000;
  constexpr size_t kPropertyBytes = 1024;
  const std::string blob(kPropertyBytes, 'y');
  auto write_payload = [&](memgraph::dbms::DatabaseAccess &acc) {
    // DbArenaScope required -- see the comment in the previous test for why.
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

  // Force-delete BOTH while both accessors are still held, so BOTH must go through the deferred
  // (not the immediate/synchronous) path in Handler::DeferDelete: try_delete()'s count_==1 check
  // fails for each, since each tenant's own accessor (t1_acc / t2_acc) is still outstanding.
  auto del1 = dbms.Delete("starve_orphan_t1");
  ASSERT_TRUE(del1.has_value()) << (int)del1.error();
  auto del2 = dbms.Delete("starve_orphan_t2");
  ASSERT_TRUE(del2.has_value()) << (int)del2.error();

  // Release t2's accessor. t2 now has nothing holding it, while t1 is still pinned and can never
  // drain. t2's destruction must complete anyway -- it has its own thread and cannot be queued
  // behind t1's. Three assertions, because each catches a different way this could go wrong.
  t2_acc.reset();

  // (1) Mechanism: t2's post_delete_func removed its storage directory, so t2's deferred task really
  //     did run to completion.
  const bool t2_destroyed = WaitUntil(std::chrono::seconds(10), [&] { return !std::filesystem::exists(t2_dir); });
  EXPECT_TRUE(t2_destroyed) << "t2's deferred destruction must complete even though t1 is still pinned; its storage "
                               "directory is still present: "
                            << t2_dir;

  // (2) t1 must NOT have been dragged along. Without this, the test would also pass if something had
  //     released t1 -- i.e. for the wrong reason, without the two tenants actually being decoupled.
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

  // Now release t1's accessor too. This finally lets t1's stuck task complete.
  t1_acc.reset();

#if USE_JEMALLOC
  const bool both_recovered = WaitUntil(std::chrono::seconds(10), [&] {
    return AbsDiff(memgraph::utils::graph_memory_tracker.Amount(), global_baseline) <= kTightToleranceBytes;
  });
  EXPECT_TRUE(both_recovered) << "both t1 and t2 must eventually be reclaimed once t1_acc is released; "
                                 "current amount: "
                              << memgraph::utils::graph_memory_tracker.Amount() << ", baseline: " << global_baseline;
  // Memory is freed when the Gatekeeper value is destroyed (in TryReserve), which happens before
  // post_delete_func removes the directory; wait for the directory too so the check below is race-free.
  WaitUntil(std::chrono::seconds(10), [&] { return !std::filesystem::exists(t1_dir); });
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
    // Unfalsifiable while DetachReason has only DROP; kept as the anchor for a future second reason.
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

// PINS: MakeDatabaseProtectorFactory's DRAIN GUARANTEE (database_handler.hpp) -- TTL (ttl.cpp) and the
// async indexer (async_indexer.cpp) both re-mint a DatabaseProtector via Storage::make_database_protector()
// per work item; if a draining tenant stayed reachable through that factory, either could hold a live
// DatabaseAccess indefinitely and the drain would never converge. Exercises the real seam
// (Storage::make_database_protector(), storage.hpp) rather than a hand-rolled lookup.
TEST(DBMS_Handler, DrainingTenantIsRefusedToTheProtectorFactory) {
  auto &dbms = *TestEnvironment::get();

  auto new_t = dbms.New("protector_seam");
  ASSERT_TRUE(new_t.has_value()) << (int)new_t.error();
  // Held for the whole test: `storage` (and the Database it belongs to) stays alive across the drop
  // below only because this accessor keeps DeferDelete's destruction deferred (count > 0).
  memgraph::dbms::DatabaseAccess acc = std::move(new_t.value());
  auto *storage = acc->storage();

  {
    auto protector = storage->make_database_protector();
    EXPECT_NE(protector, nullptr) << "a HOT tenant must still be protectable (TTL/async-indexer re-arm)";
  }

  PhaseTwoStall stall{acc};
  ASSERT_TRUE(stall.WaitUntilRunning()) << "the stalling task must start before the drop begins";

  auto del_prom = std::make_shared<std::promise<memgraph::dbms::DbmsHandler::DeleteResult>>();
  auto del_fut = del_prom->get_future();
  std::thread dropper([&dbms, del_prom] { del_prom->set_value(dbms.Delete("protector_seam")); });

  bool cleaned_up = false;
  auto cleanup = memgraph::utils::OnScopeExit{[&] {
    if (cleaned_up) return;
    stall.Release();
    if (del_fut.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
      dropper.join();
    } else {
      dropper.detach();
    }
  }};

  const bool draining_seen = WaitUntil(std::chrono::seconds(5), [&] {
    const auto statuses = dbms.AllWithHotColdStatus();
    return std::ranges::any_of(statuses,
                               [](auto const &kv) { return kv.first == "protector_seam" && kv.second == "DETACHED"; });
  });
  ASSERT_TRUE(draining_seen) << "the drop never reached the DRAINING window this test needs to probe";

  auto protector_while_draining = storage->make_database_protector();
  EXPECT_EQ(protector_while_draining, nullptr)
      << "a draining tenant must not be re-armable via the protector factory -- TTL/the async indexer "
         "would otherwise keep minting accessors and the drain would never converge";

  stall.Release();
  const auto del_status = del_fut.wait_for(std::chrono::seconds(5));
  if (del_status == std::future_status::ready) {
    dropper.join();
  } else {
    dropper.detach();
  }
  cleaned_up = true;
  ASSERT_EQ(del_status, std::future_status::ready) << "the drop must complete once the stall is released";
  EXPECT_TRUE(del_fut.get().has_value()) << "the drop itself must still succeed once its stall is released";

  acc.reset();
  const bool retired = WaitUntil(std::chrono::seconds(10), [&] {
    const auto all_detached = dbms.AllDetached();
    return std::ranges::none_of(all_detached, [](auto const &d) { return d.name == "protector_seam"; });
  });
  EXPECT_TRUE(retired) << "protector_seam's row must retire once its accessor is released";
}

// PINS constraint C8 -- Delete_'s Phase 2 (StopAllBackgroundTasks/streams()->DropAll()) must run with
// lock_ released, so an unrelated tenant's own exclusive-lock_ operation is never blocked behind this
// drop's teardown. This is the reason the three-phase split (dbms_handler.cpp's Delete_ doc comment)
// exists at all.
TEST(DBMS_Handler, DropDoesNotHoldTheHandlerLockDuringTeardown) {
  auto &dbms = *TestEnvironment::get();

  auto new_t = dbms.New("lock_teardown_target");
  ASSERT_TRUE(new_t.has_value()) << (int)new_t.error();
  memgraph::dbms::DatabaseAccess acc = std::move(new_t.value());

  PhaseTwoStall stall{acc};
  ASSERT_TRUE(stall.WaitUntilRunning()) << "the stalling task must start before the drop begins";
  acc.reset();  // not needed as an external holder; the stall alone parks Phase 2

  auto del_prom = std::make_shared<std::promise<memgraph::dbms::DbmsHandler::DeleteResult>>();
  auto del_fut = del_prom->get_future();
  std::thread dropper([&dbms, del_prom] { del_prom->set_value(dbms.Delete("lock_teardown_target")); });

  bool cleaned_up = false;
  auto cleanup = memgraph::utils::OnScopeExit{[&] {
    if (cleaned_up) return;
    stall.Release();
    if (del_fut.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
      dropper.join();
    } else {
      dropper.detach();
    }
  }};

  const bool draining_seen = WaitUntil(std::chrono::seconds(5), [&] {
    const auto statuses = dbms.AllWithHotColdStatus();
    return std::ranges::any_of(
        statuses, [](auto const &kv) { return kv.first == "lock_teardown_target" && kv.second == "DETACHED"; });
  });
  ASSERT_TRUE(draining_seen) << "the drop never reached the DRAINING window this test needs to probe";

  // Prove -- not infer from timing -- that lock_ is free: New() (std::lock_guard{lock_}, exclusive) for
  // a DIFFERENT tenant must complete well inside the stall's window while this drop sits in Phase 2.
  auto [other_ready, other_result] =
      RunBounded(std::chrono::seconds(2), [&] { return dbms.New("lock_teardown_other"); });
  EXPECT_TRUE(other_ready) << "a different tenant's New() must not block on lock_ while this drop's Phase 2 "
                              "(off-lock teardown) is in flight -- a regression re-holding lock_ across the "
                              "teardown would hang this call instead of returning";
  if (other_ready) {
    ASSERT_TRUE(other_result.has_value());
    EXPECT_TRUE(other_result->has_value()) << "the other tenant's creation must actually succeed";
  }

  stall.Release();
  const auto del_status = del_fut.wait_for(std::chrono::seconds(5));
  if (del_status == std::future_status::ready) {
    dropper.join();
  } else {
    dropper.detach();
  }
  cleaned_up = true;
  ASSERT_EQ(del_status, std::future_status::ready) << "the drop must complete once the stall is released";
  EXPECT_TRUE(del_fut.get().has_value()) << "the drop itself must still succeed once its stall is released";
}

// PINS the ad88a52fe class of regression against the new three-phase drop: a concurrent SUSPEND and
// DROP racing on the SAME tenant must never deadlock. Suspend_'s shared-lock_ phases and Delete_'s
// exclusive-lock_ Phase 1 are mutually exclusive under lock_, so exactly one side must observe the
// other's already-committed state and fail cleanly and retriably -- never block forever.
TEST(DBMS_Handler, ConcurrentSuspendAgainstADrainingDropDoesNotDeadlock) {
  auto &dbms = *TestEnvironment::get();

  auto new_t = dbms.New("suspend_drop_race");
  ASSERT_TRUE(new_t.has_value()) << (int)new_t.error();
  memgraph::dbms::DatabaseAccess acc = std::move(new_t.value());
  // Release: an outstanding accessor would make Suspend_'s try_begin_suspend() fail
  // ACTIVE_CONNECTIONS regardless of the drop, which would defeat the race this test wants to force.
  acc.reset();

  std::latch start_gate{2};
  auto del_prom = std::make_shared<std::promise<memgraph::dbms::DbmsHandler::DeleteResult>>();
  auto susp_prom = std::make_shared<std::promise<memgraph::dbms::DbmsHandler::SuspendResult>>();
  auto del_fut = del_prom->get_future();
  auto susp_fut = susp_prom->get_future();

  std::thread dropper([&dbms, &start_gate, del_prom] {
    start_gate.arrive_and_wait();
    del_prom->set_value(dbms.Delete("suspend_drop_race"));
  });
  std::thread suspender([&dbms, &start_gate, susp_prom] {
    start_gate.arrive_and_wait();
    susp_prom->set_value(dbms.Suspend("suspend_drop_race"));
  });

  constexpr auto kBound = std::chrono::seconds(5);
  const auto del_status = del_fut.wait_for(kBound);
  const auto susp_status = susp_fut.wait_for(kBound);

  if (del_status == std::future_status::ready) {
    dropper.join();
  } else {
    dropper.detach();
  }
  if (susp_status == std::future_status::ready) {
    suspender.join();
  } else {
    suspender.detach();
  }

  ASSERT_EQ(del_status, std::future_status::ready)
      << "DROP must return within " << kBound.count() << "s even racing a concurrent SUSPEND, not deadlock";
  ASSERT_EQ(susp_status, std::future_status::ready)
      << "SUSPEND must return within " << kBound.count() << "s even racing a concurrent DROP, not deadlock";

  const auto del_result = del_fut.get();
  const auto susp_result = susp_fut.get();
  const bool del_won = del_result.has_value();
  const bool susp_won = susp_result.has_value();

  EXPECT_TRUE(del_won != susp_won) << "exactly one of DROP/SUSPEND must win the race for the same tenant (del="
                                   << del_won << " susp=" << susp_won << ")";

  if (!del_won) {
    EXPECT_EQ(del_result.error(), memgraph::dbms::DeleteError::USING)
        << "a DROP that loses to a concurrent SUSPEND must be retriable USING, not a hard failure";
  }
  if (!susp_won) {
    EXPECT_EQ(susp_result.error(), memgraph::dbms::DbmsHandler::SuspendError::NON_EXISTENT)
        << "a SUSPEND that loses to a concurrent DROP must see the (now-draining) tenant as gone "
           "(NON_EXISTENT, drain-gated Get()), not deadlock or observe torn state";
  }

  // Whichever won, clean up so later tests are unaffected: a winning SUSPEND leaves a COLD shell
  // (drop it here); a winning DROP with no external holder already retired inline (nothing to do).
  if (susp_won) {
    auto cold_drop = dbms.Delete("suspend_drop_race");
    EXPECT_TRUE(cold_drop.has_value()) << "cleanup: dropping the now-COLD tenant must succeed";
  }
}

// PINS the visibility/accounting guarantee AllWithHotColdStatus/TenantMemorySum/AllDetached give while
// the tenant is still draining (Phase 2, draining_ set, gatekeeper still IN db_handler_) -- not just
// after db_handler_ has erased it, which
// DetachedTenantMemoryStaysAttributableWhileUnaddressable above already covers. A regression that let a
// draining tenant's plain access() succeed (see TenantMemorySum's comment, dbms_handler.hpp) would
// double-count it here (once HOT, once detached) or drop it from both totals.
TEST(DBMS_Handler, DrainingTenantIsVisibleAndCountedExactlyOnce) {
  auto &dbms = *TestEnvironment::get();

  auto new_t = dbms.New("draining_visibility_probe");
  ASSERT_TRUE(new_t.has_value()) << (int)new_t.error();
  memgraph::dbms::DatabaseAccess acc = std::move(new_t.value());
  const auto tenant_uuid = acc->uuid();

  constexpr size_t kNumVertices = 3000;
  constexpr size_t kPropertyBytes = 1024;
  const std::string blob(kPropertyBytes, 'd');
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

  PhaseTwoStall stall{acc};
  ASSERT_TRUE(stall.WaitUntilRunning()) << "the stalling task must start before the drop begins";
  acc.reset();

  auto del_prom = std::make_shared<std::promise<memgraph::dbms::DbmsHandler::DeleteResult>>();
  auto del_fut = del_prom->get_future();
  std::thread dropper([&dbms, del_prom] { del_prom->set_value(dbms.Delete("draining_visibility_probe")); });

  bool cleaned_up = false;
  auto cleanup = memgraph::utils::OnScopeExit{[&] {
    if (cleaned_up) return;
    stall.Release();
    if (del_fut.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
      dropper.join();
    } else {
      dropper.detach();
    }
  }};

  const bool draining_seen = WaitUntil(std::chrono::seconds(5), [&] {
    const auto all_detached = dbms.AllDetached();
    return std::ranges::any_of(all_detached, [&](auto const &d) { return d.uuid == tenant_uuid; });
  });
  ASSERT_TRUE(draining_seen) << "the drop never reached the DRAINING window this test needs to probe";

  {
    const auto statuses = dbms.AllWithHotColdStatus();
    const auto draining_count = std::ranges::count_if(
        statuses, [](auto const &kv) { return kv.first == "draining_visibility_probe" && kv.second == "DETACHED"; });
    EXPECT_EQ(draining_count, 1) << "a draining tenant must be listed exactly once, as DETACHED";
    EXPECT_TRUE(std::ranges::none_of(statuses, [](auto const &kv) {
      return kv.first == "draining_visibility_probe" && kv.second == "HOT";
    })) << "a draining tenant must not ALSO be reported HOT";
  }
#if USE_JEMALLOC
  {
    const auto during = dbms.TenantMemorySum();
    const int64_t tolerance = footprint / 10;
    EXPECT_LE(during.hot, before.hot - (footprint - tolerance))
        << "a draining tenant's plain access() must be refused, so it must NOT contribute to the HOT half";
    EXPECT_GE(during.detached, footprint - tolerance)
        << "its bytes must be attributed via the detached half while draining, or they vanish from every total";
  }
#endif
  {
    const auto all_detached = dbms.AllDetached();
    const auto matches = std::ranges::count_if(all_detached, [&](auto const &d) { return d.uuid == tenant_uuid; });
    ASSERT_EQ(matches, 1) << "exactly one detached row must exist for this tenant while it drains";
  }

  stall.Release();
  const auto del_status = del_fut.wait_for(std::chrono::seconds(5));
  if (del_status == std::future_status::ready) {
    dropper.join();
  } else {
    dropper.detach();
  }
  cleaned_up = true;
  ASSERT_EQ(del_status, std::future_status::ready) << "the drop must complete once the stall is released";
  EXPECT_TRUE(del_fut.get().has_value()) << "the drop itself must still succeed once its stall is released";

  const bool retired = WaitUntil(std::chrono::seconds(10), [&] {
    const auto all_detached = dbms.AllDetached();
    return std::ranges::none_of(all_detached, [&](auto const &d) { return d.uuid == tenant_uuid; });
  });
  EXPECT_TRUE(retired) << "draining_visibility_probe's row must retire once the drop completes";
}

// PINS holders_at_detach's documented meaning ("holders OTHER than the dropper", DetachedTenant's doc
// in dbms_handler.hpp) for the common idle case: an otherwise-unheld tenant's own drop must record 0,
// not 1 (the drop's own drain_bypass mint counted as if it were a foreign holder) and not UINT64_MAX
// (the saturating-clamp underflow the "holders" comment in Delete_, dbms_handler.cpp, guards against).
TEST(DBMS_Handler, IdleTenantDropReportsNoForeignHolders) {
  auto &dbms = *TestEnvironment::get();

  auto new_t = dbms.New("idle_holders_probe");
  ASSERT_TRUE(new_t.has_value()) << (int)new_t.error();
  memgraph::dbms::DatabaseAccess acc = std::move(new_t.value());
  const auto tenant_uuid = acc->uuid();

  PhaseTwoStall stall{acc};
  ASSERT_TRUE(stall.WaitUntilRunning()) << "the stalling task must start before the drop begins";
  // Idle: release our OWN accessor before dropping, so the only live accessor when Delete_'s Phase 1
  // reads holder_count() is its own drain_bypass mint -- the case the "holders" subtraction documents.
  acc.reset();

  auto del_prom = std::make_shared<std::promise<memgraph::dbms::DbmsHandler::DeleteResult>>();
  auto del_fut = del_prom->get_future();
  std::thread dropper([&dbms, del_prom] { del_prom->set_value(dbms.Delete("idle_holders_probe")); });

  bool cleaned_up = false;
  auto cleanup = memgraph::utils::OnScopeExit{[&] {
    if (cleaned_up) return;
    stall.Release();
    if (del_fut.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
      dropper.join();
    } else {
      dropper.detach();
    }
  }};

  std::optional<uint64_t> holders_at_detach;
  const bool draining_seen = WaitUntil(std::chrono::seconds(5), [&] {
    const auto all_detached = dbms.AllDetached();
    const auto it = std::ranges::find_if(all_detached, [&](auto const &d) { return d.uuid == tenant_uuid; });
    if (it == all_detached.end()) return false;
    holders_at_detach = it->holders_at_detach;
    return true;
  });
  ASSERT_TRUE(draining_seen) << "the drop never reached the DRAINING window this test needs to probe";
  ASSERT_TRUE(holders_at_detach.has_value()) << "the row must have been observed to record a value at all";
  EXPECT_EQ(*holders_at_detach, 0u)
      << "an idle tenant (no accessor besides the drop's own drain_bypass mint) must record zero foreign "
         "holders, not 1 (the dropper's own accessor left uncorrected) and not UINT64_MAX (a clamp underflow)";

  stall.Release();
  const auto del_status = del_fut.wait_for(std::chrono::seconds(5));
  if (del_status == std::future_status::ready) {
    dropper.join();
  } else {
    dropper.detach();
  }
  cleaned_up = true;
  ASSERT_EQ(del_status, std::future_status::ready) << "the drop must complete once the stall is released";
  EXPECT_TRUE(del_fut.get().has_value()) << "the drop itself must still succeed once its stall is released";
}

// PINS 1a82eb021: the FORCE-drop overload (DbmsHandler::Delete(std::string_view, system::Transaction *)
// -- the two-argument form DROP DATABASE ... FORCE calls, interpreter.cpp) must see a concurrently
// DRAINING tenant as retriable USING, not NON_EXISTENT. Deleting the is_draining() guard immediately
// above the GetConfig pre-check in that overload (dbms_handler.cpp) would make this call fall through
// to GetConfig's !conf branch and reintroduce the "does not exist" misreport for a tenant that plainly
// does.
TEST(DBMS_Handler, ForceDropOfADrainingTenantIsRetriableNotMissing) {
  auto &dbms = *TestEnvironment::get();

  auto new_t = dbms.New("force_drop_race");
  ASSERT_TRUE(new_t.has_value()) << (int)new_t.error();
  memgraph::dbms::DatabaseAccess acc = std::move(new_t.value());

  PhaseTwoStall stall{acc};
  ASSERT_TRUE(stall.WaitUntilRunning()) << "the stalling task must start before the first drop begins";
  acc.reset();  // not needed as an external holder; the stall alone parks Phase 2

  // First drop: any overload's Phase 1 (begin_drain()) puts the tenant into the same DRAINING state,
  // so the plain single-argument overload is enough to manufacture the race this test needs.
  auto del_prom = std::make_shared<std::promise<memgraph::dbms::DbmsHandler::DeleteResult>>();
  auto del_fut = del_prom->get_future();
  std::thread dropper([&dbms, del_prom] { del_prom->set_value(dbms.Delete("force_drop_race")); });

  bool cleaned_up = false;
  auto cleanup = memgraph::utils::OnScopeExit{[&] {
    if (cleaned_up) return;
    stall.Release();
    if (del_fut.wait_for(std::chrono::seconds(5)) == std::future_status::ready) {
      dropper.join();
    } else {
      dropper.detach();
    }
  }};

  const bool draining_seen = WaitUntil(std::chrono::seconds(5), [&] {
    const auto statuses = dbms.AllWithHotColdStatus();
    return std::ranges::any_of(statuses,
                               [](auto const &kv) { return kv.first == "force_drop_race" && kv.second == "DETACHED"; });
  });
  ASSERT_TRUE(draining_seen) << "the first drop never reached the DRAINING window this test needs to probe";

  // The call under test: the TWO-argument overload, Delete(std::string_view, system::Transaction *) --
  // the one DROP DATABASE ... FORCE binds to. The single-argument Delete(std::string_view) used for the
  // first drop above never had this bug (its Delete_ Phase 1 begin_drain() already returns USING), so
  // calling it here instead would pass this assertion for the wrong reason. The explicit
  // system::Transaction* cast on the second argument is belt-and-suspenders: Delete(utils::UUID) and
  // Delete(std::string_view) both take exactly one argument, so a plain `nullptr` would already bind
  // unambiguously to this two-argument overload -- the cast just makes that binding visible in the diff.
  auto [force_ready, force_result] = RunBounded(std::chrono::seconds(2), [&] {
    return dbms.Delete("force_drop_race", static_cast<memgraph::system::Transaction *>(nullptr));
  });
  ASSERT_TRUE(force_ready) << "a FORCE drop racing a DRAINING tenant must return promptly (the is_draining() "
                              "check runs before Phase 2's lock_-released teardown), not block";
  ASSERT_TRUE(force_result.has_value());
  ASSERT_FALSE(force_result->has_value())
      << "a FORCE drop racing a DRAINING tenant must fail, not silently succeed a second time";
  EXPECT_EQ(force_result->error(), memgraph::dbms::DeleteError::USING)
      << "regression: a DRAINING tenant reported via the FORCE-path Delete(name, transaction) overload must "
         "be USING (retriable), not NON_EXISTENT (the pre-1a82eb021 misreport)";

  stall.Release();
  const auto del_status = del_fut.wait_for(std::chrono::seconds(5));
  if (del_status == std::future_status::ready) {
    dropper.join();
  } else {
    dropper.detach();
  }
  cleaned_up = true;
  ASSERT_EQ(del_status, std::future_status::ready) << "the first drop must complete once the stall is released";
  EXPECT_TRUE(del_fut.get().has_value()) << "the first drop itself must still succeed once its stall is released";
}

// PINS: MakeDatabaseProtectorFactory (database_handler.hpp) must keep resolving its own tenant across a
// RENAME. If the factory captures the tenant's NAME at construction time and re-looks it up through
// Handler<T>::Get on every call, the lookup goes stale the moment the name changes -- items_ is now keyed
// under the NEW name -- and make_database_protector() starts returning nullptr forever. Both consumers
// (storage/v2/ttl.cpp and storage/v2/async_indexer.cpp) treat a nullptr protector as "the database was
// dropped, stop this worker", so this would silently and permanently kill TTL expiry and async index
// building for any tenant that is ever renamed, with no error, no log, and no recovery short of a
// process restart. Exercises the real seam (Storage::make_database_protector()), not a hand-rolled lookup.
TEST(DBMS_Handler, ProtectorFactorySurvivesRename) {
  auto &dbms = *TestEnvironment::get();

  auto new_t = dbms.New("factory_rename_src");
  ASSERT_TRUE(new_t.has_value()) << (int)new_t.error();
  // Held for the whole test, same reasoning as DrainingTenantIsRefusedToTheProtectorFactory above: this
  // accessor is what keeps `storage` (and the Database it belongs to) alive across the rename below.
  memgraph::dbms::DatabaseAccess acc = std::move(new_t.value());
  auto *storage = acc->storage();

  {
    auto p0 = storage->make_database_protector();
    EXPECT_NE(p0, nullptr) << "a HOT tenant must be protectable before the rename -- if this already "
                              "fails, the assertion below proves nothing about the rename itself";
  }

  auto rename_result = dbms.Rename("factory_rename_src", "factory_rename_dst");
  ASSERT_TRUE(rename_result.has_value()) << "the rename itself must succeed for this test to probe anything";

  auto p1 = storage->make_database_protector();
  EXPECT_NE(p1, nullptr) << "the protector factory must still resolve its own tenant after a RENAME -- a "
                            "factory that re-looks-up a captured NAME returns nullptr here, and both "
                            "consumers (storage/v2/ttl.cpp:315 and storage/v2/async_indexer.cpp:96) read "
                            "nullptr as 'database dropped, stop this worker', silently killing TTL expiry "
                            "and async index building until process restart";

  acc.reset();
  const bool retired = WaitUntil(std::chrono::seconds(10), [&] {
    const auto all_detached = dbms.AllDetached();
    return std::ranges::none_of(all_detached, [](auto const &d) { return d.name == "factory_rename_dst"; });
  });
  if (!retired) {
    // Best-effort: don't leave the shared TestEnvironment polluted, but don't hang the binary over it
    // either -- this cleanup path is not what this test is pinning.
    RunBounded(std::chrono::seconds(2), [&] { return dbms.Delete("factory_rename_dst"); });
  }
}

// PINS: Handler<T>::Get's items_.find (handler.hpp) has no synchronization of its own against
// structural mutation of items_ (insert/erase under DbmsHandler::lock_ elsewhere). If
// MakeDatabaseProtectorFactory re-resolves its tenant through that same unsynchronized map on every
// call, a lock-free reader racing New()/Delete() for an UNRELATED tenant is a data race on items_ --
// benign-looking under a normal build, but a real find under ThreadSanitizer. Under a non-TSan build
// this test is a smoke test only: it must not crash or hang. Its real purpose is to give TSan a window
// on that race; it does not (and cannot, without TSan) prove the race is absent.
TEST(DBMS_Handler, ProtectorFactoryConcurrentWithHandlerMapMutation) {
  auto &dbms = *TestEnvironment::get();

  auto new_t = dbms.New("factory_race_target");
  ASSERT_TRUE(new_t.has_value()) << (int)new_t.error();
  memgraph::dbms::DatabaseAccess acc = std::move(new_t.value());
  auto *storage = acc->storage();

  std::atomic<bool> stop{false};
  std::atomic<uint64_t> reader_iterations{0};
  std::atomic<uint64_t> non_null_results{0};

  std::thread reader([&] {
    constexpr uint64_t kMaxIterations = 20000;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!stop.load(std::memory_order_relaxed) &&
           reader_iterations.load(std::memory_order_relaxed) < kMaxIterations &&
           std::chrono::steady_clock::now() < deadline) {
      // A nullptr result is legitimate here (the target tenant is never itself mutated below), so this
      // loop intentionally does not assert on the returned protector -- only that calling this seam
      // concurrently with unrelated map mutation neither crashes nor hangs.
      auto protector = storage->make_database_protector();
      if (protector != nullptr) {
        non_null_results.fetch_add(1, std::memory_order_relaxed);
      }
      reader_iterations.fetch_add(1, std::memory_order_relaxed);
    }
  });

  // Cross-tenant churn only -- deliberately NOT renaming/dropping factory_race_target itself. A
  // same-tenant Rename loop would race Handler<T>::Rename's move-then-erase window (handler.hpp), during
  // which the in-map Gatekeeper::pimpl_ is transiently null; a concurrent lookup landing in that window
  // is a real finding (NULL-dereference/SIGSEGV) but would crash this whole shared test binary, not just
  // fail an assertion, so it is out of scope for this smoke test.
  const auto churn_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  uint64_t churn_iterations = 0;
  while (churn_iterations < 20000 && std::chrono::steady_clock::now() < churn_deadline) {
    auto other = dbms.New("factory_race_other");
    if (other.has_value()) {
      memgraph::dbms::DatabaseAccess other_acc = std::move(other.value());
      other_acc.reset();
      dbms.Delete("factory_race_other");  // best-effort; a USING/NON_EXISTENT race here is not this test's concern
    }
    ++churn_iterations;
  }
  stop.store(true, std::memory_order_relaxed);

  auto [reader_done, _] = RunBounded(std::chrono::seconds(5), [&] {
    reader.join();
    return true;
  });
  if (!reader_done) {
    // RunBounded already ran `reader.join()` on its own worker thread; if that worker itself is stuck,
    // detach rather than block this thread indefinitely (mirrors this file's established idiom for a
    // wedged background operation, e.g. DropDoesNotHoldTheHandlerLockDuringTeardown's dropper.detach()).
    ADD_FAILURE() << "the reader thread failed to join within the bound -- possible hang in the seam under test";
  }

  EXPECT_GT(reader_iterations.load(), 0u) << "the reader must have completed at least one iteration";

  // Best-effort cleanup: make sure this test doesn't leave factory_race_target behind for later tests.
  acc.reset();
  WaitUntil(std::chrono::seconds(10), [&] {
    const auto all_detached = dbms.AllDetached();
    return std::ranges::none_of(all_detached, [](auto const &d) { return d.name == "factory_race_target"; });
  });
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  // gtest takes ownership of the TestEnvironment ptr - we don't delete it.
  ::testing::AddGlobalTestEnvironment(new TestEnvironment);
  return RUN_ALL_TESTS();
}

#endif
