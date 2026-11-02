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

// Multi-threaded stress tests for the shared per-branch diff engines, meant to run under TSan/ASan (races, crashes and
// invariant violations, not throughput). Every thread owns its own Interpreter (one Bolt session) on ONE database.
// VERSIONING_STRESS_SCALE (double, default 1.0, clamped to [0.05, 100]) multiplies every per-thread op count;
// VERSIONING_STRESS_SEED (default 0x5eed5eed) seeds every RNG and is printed by every test.

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <typeinfo>
#include <utility>
#include <vector>

#include <unistd.h>

#include "gtest/gtest.h"

#include "communication/bolt/v1/value.hpp"
#include "flags/general.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "mg_procedure.h"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "query/procedure/mg_procedure_impl.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"
#include "versioning/version_store.hpp"

namespace {

using BoltValue = memgraph::communication::bolt::Value;
using BoltRows = std::vector<std::vector<BoltValue>>;

constexpr int64_t kSeedVertices = 2000;
constexpr int64_t kSeedEdges = 6000;
// Id ranges (ids are never reused, so "committed" bookkeeping is exact):
constexpr int64_t kBranchNativeBase = 1'000'000;  // branch writer t: kBranchNativeBase * (t + 1) + k
constexpr int64_t kMainNativeBase = 50'000'000;   // main writer w:   kMainNativeBase + w * 1'000'000 + k
// Random branch writers only touch ids below kRandomIds; ids above are phase-0 sentinels written once, single-threaded.
constexpr int64_t kRandomIds = 1800;
constexpr int64_t kSentinelDeleteFirst = 1990;   // [1990, 2000): DETACH DELETEd
constexpr int64_t kSentinelLabelFirst = 1800;    // [1800, 1810): SET :L, stays
constexpr int64_t kSentinelUnlabelFirst = 1810;  // [1810, 1820): SET :L then REMOVE :L
constexpr int64_t kSentinelEdgeFirst = 1820;     // [1820, 1830): ring of new edges with w = kSentinelEdgeW
constexpr int64_t kSentinelEdgeW = -7;
constexpr int64_t kSentinelEdgeDelFirst = 1830;  // [1830, 1835): one fork out-edge each deleted
// Values: branch writers set v in [1, kMainValueBase), main writers set v >= kMainValueBase. A main value visible on
// the branch is therefore a fork-isolation violation.
constexpr int64_t kMainValueBase = 1'000'000'000;

uint64_t Seed() {
  static const uint64_t seed = [] {
    const char *env = std::getenv("VERSIONING_STRESS_SEED");
    if (env == nullptr || *env == '\0') return uint64_t{0x5eed5eedULL};
    char *end = nullptr;
    const auto v = std::strtoull(env, &end, 0);
    return end == env ? uint64_t{0x5eed5eedULL} : static_cast<uint64_t>(v);
  }();
  return seed;
}

double Scale() {
  static const double scale = [] {
    const char *env = std::getenv("VERSIONING_STRESS_SCALE");
    if (env == nullptr || *env == '\0') return 1.0;
    char *end = nullptr;
    const double v = std::strtod(env, &end);
    if (end == env || !(v > 0.0)) return 1.0;
    return std::clamp(v, 0.05, 100.0);
  }();
  return scale;
}

int Scaled(int base) { return std::max(1, static_cast<int>(base * Scale())); }

// Threads released together by a start gate. A stuck worker cannot be joined, so Run() aborts the process (with the
// seed in the log) when they do not all finish in time instead of hanging the test.
class Workers {
 public:
  Workers() = default;
  Workers(const Workers &) = delete;
  Workers &operator=(const Workers &) = delete;

  // Reached without Run() (an earlier ASSERT): release the gate with the work cancelled, then join.
  ~Workers() {
    cancelled_.store(true, std::memory_order_relaxed);
    go_.store(true, std::memory_order_release);
    go_.notify_all();
    for (auto &t : threads_) t.join();
  }

  template <class F>
  void Spawn(F &&f) {
    threads_.emplace_back([this, fn = std::forward<F>(f)]() mutable {
      go_.wait(false, std::memory_order_acquire);
      if (!cancelled_.load(std::memory_order_relaxed)) fn();
      {
        std::lock_guard lock{mutex_};
        ++finished_;
      }
      done_.notify_all();
    });
  }

  void Run() {
    go_.store(true, std::memory_order_release);
    go_.notify_all();
    const auto limit = std::chrono::seconds(static_cast<int>(300 * std::max(1.0, Scale())));
    std::unique_lock lock{mutex_};
    if (!done_.wait_for(lock, limit, [&] { return finished_ == threads_.size(); })) {
      std::cerr << "[ STRESS ] seed=" << Seed() << " workers did not finish within " << limit.count()
                << " s (deadlock?)" << std::endl;
      std::abort();
    }
    lock.unlock();
    for (auto &t : threads_) t.join();
    threads_.clear();
  }

 private:
  std::vector<std::thread> threads_;
  std::mutex mutex_;
  std::condition_variable done_;
  size_t finished_{0};
  std::atomic<bool> go_{false};
  std::atomic<bool> cancelled_{false};
};

// Thread-safe sink for unexpected events; asserted empty from the main thread (no gtest assertions off-thread).
class FailureLog {
 public:
  void Add(std::string msg) {
    std::lock_guard lock{mutex_};
    ++count_;
    if (msgs_.size() < 40) msgs_.push_back(std::move(msg));
  }

  size_t Count() const {
    std::lock_guard lock{mutex_};
    return count_;
  }

  std::string Dump() const {
    std::lock_guard lock{mutex_};
    std::ostringstream os;
    os << "seed=" << Seed() << " " << count_ << " failure(s)";
    for (const auto &m : msgs_) os << "\n  - " << m;
    return os.str();
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::string> msgs_;
  size_t count_{0};
};

enum class Outcome { kOk, kConflict, kError };

struct Counters {
  std::atomic<uint64_t> ops{0};
  std::atomic<uint64_t> conflicts{0};
  std::atomic<uint64_t> errors{0};
  std::atomic<uint64_t> rollbacks{0};
};

// Runs one query on `s`; never throws. Only TransactionSerializationException is kConflict; any other error (including
// other RetryBasicException subtypes such as HintedAbortError) is kError and is logged.
Outcome Exec(InterpreterFaker &s, const std::string &query, FailureLog &log, Counters &c, std::string_view who,
             BoltRows *rows = nullptr) {
  c.ops.fetch_add(1, std::memory_order_relaxed);
  try {
    auto stream = s.Interpret(query);
    if (rows != nullptr) *rows = stream.GetResults();
    return Outcome::kOk;
  } catch (const memgraph::query::TransactionSerializationException &) {
    c.conflicts.fetch_add(1, std::memory_order_relaxed);
    return Outcome::kConflict;
  } catch (const std::exception &e) {
    c.errors.fetch_add(1, std::memory_order_relaxed);
    log.Add(std::string{who} + ": '" + query + "' threw " + typeid(e).name() + ": " + e.what());
    return Outcome::kError;
  }
}

void SwallowRollback(InterpreterFaker &s) {
  try {
    s.Interpret("ROLLBACK");
  } catch (...) {
  }
}

bool IsMarkerName(const std::string &name) { return name.rfind("__mg", 0) == 0; }

void CheckNames(const std::vector<BoltValue> &list, std::string_view what, const std::string &query, FailureLog &log) {
  for (const auto &v : list) {
    if (!v.IsString()) {
      log.Add(std::string{what} + " contains a non-string element for '" + query + "'");
    } else if (IsMarkerName(v.ValueString())) {
      log.Add("MARKER LEAK in " + std::string{what} + ": '" + v.ValueString() + "' for '" + query + "'");
    }
  }
}

// True when the EXPLAIN plan of `query` contains the operator named exactly `op` (not as a prefix of a longer name).
bool PlanUsesOperator(InterpreterFaker &s, const std::string &query, std::string_view op) {
  auto stream = s.Interpret("EXPLAIN " + query);
  auto const is_ident = [](char ch) { return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_'; };
  for (const auto &row : stream.GetResults()) {
    const std::string_view line = row[0].ValueString();
    for (auto pos = line.find(op); pos != std::string_view::npos; pos = line.find(op, pos + 1)) {
      const auto end = pos + op.size();
      if ((pos == 0 || !is_ident(line[pos - 1])) && (end == line.size() || !is_ident(line[end]))) return true;
    }
  }
  return false;
}

// One database plus its interpreter context, destroyed and rebuilt on the same directory to restart "the server".
class StressEngine {
 public:
  StressEngine(const std::filesystem::path &directory, bool narrowing)
      : config_{[&] {
          memgraph::storage::Config config{};
          config.durability.storage_directory = directory;
          config.experimental_versioning = true;
          config.experimental_commit_lock_narrowing = narrowing;
          config.durability.recover_on_startup = true;
          // Periodic GC must run while the workers do, so main's GC horizon is exercised against the branch's fork pin.
          config.gc.interval = std::chrono::milliseconds(10);
          config.durability.snapshot_wal_mode =
              memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
          config.disk.main_storage_directory = directory / "disk";
          config.salient.items.enable_schema_metadata = true;  // SHOW NODE_LABELS / EDGE_TYPES INFO
          return config;
        }()},
        repl_state_{memgraph::storage::ReplicationStateRootPath(config_)},
        db_gk_{config_},
        db_{[&] {
          auto db_acc_opt = db_gk_.access();
          MG_ASSERT(db_acc_opt, "Failed to access db");
          return std::move(*db_acc_opt);
        }()},
        interpreter_context_{{},
                             nullptr,
                             nullptr,
                             nullptr,
                             &repl_state_,
                             system_state_,
                             nullptr
#ifdef MG_ENTERPRISE
                             ,
                             nullptr,
                             nullptr
#endif
        } {
  }

  ~StressEngine() {
    interpreter_context_.auth_checker = &auth_checker_;
    sessions_.clear();
  }

  StressEngine(const StressEngine &) = delete;
  StressEngine &operator=(const StressEngine &) = delete;

  // Sessions are created on the calling thread (the faker rewrites interpreter_context.auth_checker, which is not
  // synchronized) and only used from workers.
  InterpreterFaker &NewSession() {
    sessions_.push_back(std::make_unique<InterpreterFaker>(&interpreter_context_, db_));
    return *sessions_.back();
  }

  InterpreterFaker &NewBranchSession(const std::string &branch) {
    auto &s = NewSession();
    s.Interpret("USE BRANCH '" + branch + "'");
    return s;
  }

  memgraph::dbms::DatabaseAccess &db() { return db_; }

 private:
  memgraph::storage::Config config_;
  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state_;
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk_;
  memgraph::dbms::DatabaseAccess db_;
  memgraph::system::System system_state_;
  memgraph::query::AllowEverythingAuthChecker auth_checker_;
  memgraph::query::InterpreterContext interpreter_context_;
  std::vector<std::unique_ptr<InterpreterFaker>> sessions_;
};

int64_t Scalar(InterpreterFaker &s, const std::string &query) {
  auto stream = s.Interpret(query);
  EXPECT_EQ(stream.GetResults().size(), 1U) << query;
  return stream.GetResults().at(0).at(0).ValueInt();
}

}  // namespace

// The parameter is main's experimental_commit_lock_narrowing: branches must behave the same under both commit modes.
// Restart() rebuilds the engine on the same directory; a throwing restart fails the test (a branch must always replay).
class VersioningStressTest : public ::testing::TestWithParam<bool> {
 protected:
  std::filesystem::path data_directory;
  std::unique_ptr<StressEngine> engine;

  void SetUp() override {
    memgraph::license::global_license_checker.EnableTesting();
    std::cerr << "[ STRESS ] seed=" << Seed() << std::endl;
    std::string test_name = ::testing::UnitTest::GetInstance()->current_test_info()->name();
    std::ranges::replace(test_name, '/', '_');
    data_directory = std::filesystem::temp_directory_path() /
                     ("MG_tests_unit_versioning_stress_" + std::to_string(::getpid()) + "_" + test_name);
    std::filesystem::remove_all(data_directory);
    engine = std::make_unique<StressEngine>(data_directory, GetParam());
  }

  void TearDown() override {
    engine.reset();
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  bool Restart() {
    engine.reset();
    try {
      engine = std::make_unique<StressEngine>(data_directory, GetParam());
    } catch (const std::exception &e) {
      ADD_FAILURE() << "the server refused to start after a restart: " << e.what();
      return false;
    }
    return true;
  }
};

namespace {

struct BranchPending {
  std::vector<int64_t> created;
  std::vector<int64_t> deleted;
};

struct BranchOp {
  std::string query;
  int64_t created{-1};
  int64_t deleted{-1};
};

struct CommittedBranchState {
  std::mutex mutex;
  std::set<int64_t> deleted;  // fork ids whose DETACH DELETE committed
  std::atomic<int64_t> created{0};

  void Publish(const BranchPending &p) {
    created.fetch_add(static_cast<int64_t>(p.created.size()), std::memory_order_relaxed);
    if (p.deleted.empty()) return;
    std::lock_guard lock{mutex};
    deleted.insert(p.deleted.begin(), p.deleted.end());
  }
};

BranchOp MakeBranchOp(std::mt19937_64 &rng, int tid, int64_t &next_native) {
  auto id = [&] { return static_cast<int64_t>(rng() % kRandomIds); };
  const int64_t x = id();
  const int64_t y = id();
  const int64_t val = 1 + static_cast<int64_t>(rng() % 900'000'000);
  const auto w = static_cast<int64_t>(rng() % 100'000);
  const int pick = static_cast<int>(rng() % 100);
  BranchOp op;
  if (pick < 22) {
    op.query = "MATCH (n:P {id:" + std::to_string(x) + "}) SET n.v = " + std::to_string(val);
  } else if (pick < 32) {
    op.query = "MATCH (n:P {id:" + std::to_string(x) + "}) REMOVE n.w";
  } else if (pick < 40) {
    op.query = "MATCH (n:P {id:" + std::to_string(x) + "}) SET n:L";
  } else if (pick < 48) {
    op.query = "MATCH (n:P {id:" + std::to_string(x) + "}) REMOVE n:L";
  } else if (pick < 60) {
    op.query = "MATCH (a:P {id:" + std::to_string(x) + "}), (b:P {id:" + std::to_string(y) +
               "}) CREATE (a)-[:R {w:" + std::to_string(w) + "}]->(b)";
  } else if (pick < 72) {
    op.query = "MATCH (:P {id:" + std::to_string(x) + "})-[r:R]->() WITH r LIMIT 1 SET r.w = " + std::to_string(w);
  } else if (pick < 80) {
    op.query = "MATCH (:P {id:" + std::to_string(x) + "})-[r:R]->() WITH r LIMIT 1 DELETE r";
  } else if (pick < 82) {
    op.query = "MATCH (n:P {id:" + std::to_string(x) + "}) DETACH DELETE n";
    op.deleted = x;
  } else if (pick < 91) {
    op.created = kBranchNativeBase * (tid + 1) + next_native++;
    op.query = "CREATE (:P {id:" + std::to_string(op.created) + ", v:" + std::to_string(val) +
               ", w:" + std::to_string(w) + "})";
  } else {
    // Native vertex + edge into the fork: the vertex is created even if the MATCHed fork vertex was deleted.
    op.created = kBranchNativeBase * (tid + 1) + next_native++;
    op.query = "CREATE (n:P {id:" + std::to_string(op.created) + ", v:" + std::to_string(val) +
               ", w:" + std::to_string(w) + "}) WITH n MATCH (a:P {id:" + std::to_string(x) +
               "}) CREATE (a)-[:R {w:" + std::to_string(w) + "}]->(n)";
  }
  return op;
}

void BranchWriter(InterpreterFaker &s, int tid, int ops, CommittedBranchState &committed, FailureLog &log,
                  Counters &c) {
  std::mt19937_64 rng{Seed() + 100 + static_cast<uint64_t>(tid)};
  int64_t next_native = 0;
  const std::string who = "branch-writer-" + std::to_string(tid);
  for (int i = 0; i < ops; ++i) {
    if (rng() % 3 != 0) {
      auto op = MakeBranchOp(rng, tid, next_native);
      if (Exec(s, op.query, log, c, who) == Outcome::kOk) {
        BranchPending p;
        if (op.created >= 0) p.created.push_back(op.created);
        if (op.deleted >= 0) p.deleted.push_back(op.deleted);
        committed.Publish(p);
      }
      continue;
    }
    if (Exec(s, "BEGIN", log, c, who) != Outcome::kOk) continue;
    BranchPending pending;
    bool failed = false;
    const int n = 1 + static_cast<int>(rng() % 3);
    for (int j = 0; j < n && !failed; ++j) {
      auto op = MakeBranchOp(rng, tid, next_native);
      if (Exec(s, op.query, log, c, who) != Outcome::kOk) {
        failed = true;
        break;
      }
      if (op.created >= 0) pending.created.push_back(op.created);
      if (op.deleted >= 0) pending.deleted.push_back(op.deleted);
    }
    if (failed) {
      SwallowRollback(s);
      continue;
    }
    if (rng() % 8 == 0) {
      c.rollbacks.fetch_add(1, std::memory_order_relaxed);
      if (Exec(s, "ROLLBACK", log, c, who) != Outcome::kOk) SwallowRollback(s);
      continue;
    }
    if (Exec(s, "COMMIT", log, c, who) == Outcome::kOk) {
      committed.Publish(pending);
    } else {
      SwallowRollback(s);
    }
  }
}

void BranchReader(InterpreterFaker &s, int tid, int iters, FailureLog &log, Counters &c) {
  std::mt19937_64 rng{Seed() + 200 + static_cast<uint64_t>(tid)};
  const std::string who = "branch-reader-" + std::to_string(tid);
  BoltRows rows;
  for (int i = 0; i < iters; ++i) {
    const int64_t x = static_cast<int64_t>(rng() % kSeedVertices);
    const int pick = static_cast<int>(rng() % 6);
    std::string q;
    rows.clear();
    switch (pick) {
      case 0: {
        q = "MATCH (n:P {id:" + std::to_string(x) + "}) RETURN n.id, labels(n), keys(n), properties(n)";
        if (Exec(s, q, log, c, who, &rows) != Outcome::kOk) break;
        if (rows.size() > 1) log.Add(who + ": id lookup returned " + std::to_string(rows.size()) + " rows: " + q);
        for (const auto &r : rows) {
          if (!r[0].IsInt() || r[0].ValueInt() != x) log.Add(who + ": wrong/missing id for " + q);
          CheckNames(r[1].ValueList(), "labels()", q, log);
          CheckNames(r[2].ValueList(), "keys()", q, log);
          bool has_id = false;
          for (const auto &[k, v] : r[3].ValueMap()) {
            if (IsMarkerName(k)) log.Add("MARKER LEAK in properties(): '" + k + "' for " + q);
            if (k == "id") has_id = true;
          }
          if (!has_id) log.Add(who + ": properties(n) lacks `id` for " + q);
        }
        break;
      }
      case 1: {
        q = "MATCH (n:L) RETURN n.id, labels(n) LIMIT 100";
        if (Exec(s, q, log, c, who, &rows) != Outcome::kOk) break;
        for (const auto &r : rows) {
          if (!r[0].IsInt()) log.Add(who + ": :L vertex without id: " + q);
          CheckNames(r[1].ValueList(), "labels()", q, log);
        }
        break;
      }
      case 2: {
        q = "MATCH (n:P {id:" + std::to_string(x) + "})-[r:R]->(m) RETURN n.id, m.id, r.w";
        if (Exec(s, q, log, c, who, &rows) != Outcome::kOk) break;
        for (const auto &r : rows) {
          if (!r[0].IsInt() || r[0].ValueInt() != x) log.Add(who + ": wrong source id for " + q);
          if (!r[1].IsInt()) log.Add(who + ": expand reached a vertex without `id`: " + q);
          if (!r[2].IsInt()) log.Add(who + ": expanded edge lost its `w`: " + q);
        }
        break;
      }
      case 3: {
        q = "MATCH (n:P {id:" + std::to_string(x) + "})-[:R]->()-[:R]->(m) RETURN count(m)";
        if (Exec(s, q, log, c, who, &rows) != Outcome::kOk) break;
        if (rows.size() != 1 || !rows[0][0].IsInt() || rows[0][0].ValueInt() < 0) log.Add(who + ": bad count: " + q);
        break;
      }
      case 4: {
        if (rng() % 8 != 0) {
          // One statement = one snapshot: the (edge-property indexed) filtered count must equal a full-scan count.
          const auto w = std::to_string(rng() % 100'000);
          q = "MATCH ()-[r:R]->() WHERE r.w = " + w +
              " WITH count(r) AS indexed MATCH ()-[r2:R]->() RETURN indexed, sum(CASE WHEN r2.w = " + w +
              " THEN 1 ELSE 0 END)";
          if (Exec(s, q, log, c, who, &rows) != Outcome::kOk) break;
          if (rows.size() != 1 || !rows[0][0].IsInt() || !rows[0][1].IsInt() ||
              rows[0][0].ValueInt() != rows[0][1].ValueInt()) {
            log.Add(who + ": indexed edge-property count disagrees with a full scan: " + q);
          }
          break;
        }
        q = "MATCH (n:P) RETURN count(*)";
        if (Exec(s, q, log, c, who, &rows) == Outcome::kOk && (rows.size() != 1 || rows[0][0].ValueInt() < 0)) {
          log.Add(who + ": negative/missing count(*) on the branch");
        }
        q = "MATCH (n:P) WHERE n.id IS NULL RETURN count(*)";
        if (Exec(s, q, log, c, who, &rows) == Outcome::kOk && (rows.size() != 1 || rows[0][0].ValueInt() != 0)) {
          log.Add(who + ": a branch vertex without `id` is visible");
        }
        break;
      }
      default: {  // fork isolation: no main value may ever be visible on the branch
        q = "MATCH (n:P {id:" + std::to_string(x) + "}) RETURN n.v";
        if (Exec(s, q, log, c, who, &rows) != Outcome::kOk) break;
        for (const auto &r : rows) {
          if (r[0].IsInt() && r[0].ValueInt() >= kMainValueBase) {
            log.Add(who + ": main-writer value leaked into the branch: " + q);
          }
        }
        break;
      }
    }
  }
}

struct MainBookkeeping {
  std::map<int64_t, int64_t> last_v;  // seeded id -> last committed v (single writer per id)
  int64_t created = 0;
  int64_t deleted = 0;
};

// Main writers own disjoint data: seeded ids with id % 2 == w for SET, and their own native (edge-less) vertices for
// CREATE/DELETE. Therefore ANY conflict or error here is caused by the branch machinery.
void MainWriter(InterpreterFaker &s, int w, int ops, MainBookkeeping &book, FailureLog &log, Counters &c) {
  std::mt19937_64 rng{Seed() + 300 + static_cast<uint64_t>(w)};
  const std::string who = "main-writer-" + std::to_string(w);
  std::vector<int64_t> live;
  int64_t next_native = 0;
  for (int i = 0; i < ops; ++i) {
    const int pick = static_cast<int>(rng() % 10);
    if (pick < 6) {
      const int64_t x = (static_cast<int64_t>(rng() % (kSeedVertices / 2))) * 2 + w;
      const int64_t v = kMainValueBase + static_cast<int64_t>(rng() % 900'000'000);
      const auto o = Exec(s, "MATCH (n:P {id:" + std::to_string(x) + "}) SET n.v = " + std::to_string(v), log, c, who);
      if (o == Outcome::kOk) book.last_v[x] = v;
      if (o == Outcome::kConflict)
        log.Add(who + ": branch-caused serialization conflict on SET (id " + std::to_string(x) + ")");
    } else if (pick < 9 || live.empty()) {
      const int64_t id = kMainNativeBase + w * 1'000'000 + next_native++;
      const auto o = Exec(s, "CREATE (:P {id:" + std::to_string(id) + ", v:0, w:0})", log, c, who);
      if (o == Outcome::kOk) {
        live.push_back(id);
        ++book.created;
      }
      if (o == Outcome::kConflict) log.Add(who + ": branch-caused serialization conflict on CREATE");
    } else {
      const size_t at = rng() % live.size();
      const int64_t id = live[at];
      const auto o = Exec(s, "MATCH (n:P {id:" + std::to_string(id) + "}) DELETE n", log, c, who);
      if (o == Outcome::kOk) {
        live.erase(live.begin() + static_cast<std::ptrdiff_t>(at));
        ++book.deleted;
      }
      if (o == Outcome::kConflict) log.Add(who + ": branch-caused serialization conflict on DELETE");
    }
  }
}

// CREATE BRANCH tmp; USE BRANCH tmp; write; USE BRANCH main; DROP BRANCH tmp. This session is the only one attached to
// tmp, so every step must succeed.
void Lifecycle(InterpreterFaker &s, int iters, FailureLog &log, Counters &c) {
  std::mt19937_64 rng{Seed() + 400};
  const std::string who = "lifecycle";
  auto step = [&](const std::string &q) { return Exec(s, q, log, c, who) == Outcome::kOk; };
  for (int i = 0; i < iters; ++i) {
    const std::string name = "tmp" + std::to_string(i);
    if (!step("CREATE BRANCH '" + name + "'")) return;
    if (!step("USE BRANCH '" + name + "'")) return;
    const int writes = 2 + static_cast<int>(rng() % 6);
    for (int j = 0; j < writes; ++j) {
      const auto x = static_cast<int64_t>(rng() % kSeedVertices);
      if (!step("MATCH (n:P {id:" + std::to_string(x) + "}) SET n.v = " + std::to_string(j + 1) + ", n:L")) return;
    }
    if (!step("CREATE (:P {id:-" + std::to_string(i + 1) + ", v:1, w:1})")) return;
    BoltRows rows;
    if (Exec(s, "MATCH (n:P) WHERE n.id < 0 RETURN count(*)", log, c, who, &rows) == Outcome::kOk &&
        (rows.size() != 1 || rows[0][0].ValueInt() != 1)) {
      log.Add(who + ": tmp branch native vertex count != 1 (another tmp's writes leaked in?)");
    }
    // tmp forks main, so branch b's natives must not be visible (main natives, >= kMainNativeBase, legitimately are).
    if (Exec(s,
             "MATCH (n:P) WHERE n.id >= " + std::to_string(kBranchNativeBase) + " AND n.id < " +
                 std::to_string(kMainNativeBase) + " RETURN count(*)",
             log,
             c,
             who,
             &rows) == Outcome::kOk &&
        (rows.size() != 1 || rows[0][0].ValueInt() != 0)) {
      log.Add(who + ": branch b's native vertices leaked into tmp");
    }
    if (!step("USE BRANCH 'main'")) return;
    if (!step("DROP BRANCH '" + name + "'")) return;
  }
}

void SeedMain(InterpreterFaker &s) {
  s.Interpret("CREATE INDEX ON :P");
  s.Interpret("CREATE INDEX ON :P(id)");
  s.Interpret("CREATE INDEX ON :L");
  s.Interpret("UNWIND range(0," + std::to_string(kSeedVertices - 1) + ") AS i CREATE (:P {id:i, v:0, w:i})");
  s.Interpret("UNWIND range(0," + std::to_string(kSeedEdges - 1) + ") AS i MATCH (a:P {id: i % " +
              std::to_string(kSeedVertices) + "}), (b:P {id: (i * 7 + 13) % " + std::to_string(kSeedVertices) +
              "}) CREATE (a)-[:R {w:i}]->(b)");
}

}  // namespace

TEST_P(VersioningStressTest, WritersReadersMainWritersAndLifecycleChurn) {
  auto &setup = engine->NewSession();
  SeedMain(setup);
  ASSERT_NO_THROW(setup.Interpret("CREATE EDGE INDEX ON :R(w)")) << "edge-property index must be supported";
  ASSERT_EQ(Scalar(setup, "MATCH (n:P) RETURN count(*)"), kSeedVertices);
  ASSERT_EQ(Scalar(setup, "MATCH ()-[r:R]->() RETURN count(r)"), kSeedEdges);
  setup.Interpret("CREATE BRANCH 'b'");

  constexpr int kBranchWriters = 4;
  constexpr int kBranchReaders = 2;
  constexpr int kMainWriters = 2;
  std::vector<InterpreterFaker *> bw, br, mw;
  for (int i = 0; i < kBranchWriters; ++i) {
    bw.push_back(&engine->NewBranchSession("b"));
  }
  for (int i = 0; i < kBranchReaders; ++i) {
    br.push_back(&engine->NewBranchSession("b"));
  }
  for (int i = 0; i < kMainWriters; ++i) mw.push_back(&engine->NewSession());
  auto &life = engine->NewSession();
  auto &prep = engine->NewBranchSession("b");

  FailureLog log;
  CommittedBranchState committed;
  std::vector<MainBookkeeping> main_books(kMainWriters);
  Counters bw_c, br_c, mw_c, life_c;
  const int writer_ops = Scaled(250);
  const int reader_ops = Scaled(300);
  const int main_ops = Scaled(300);
  const int life_iters = Scaled(8);

  // Phase 0 (single-threaded, on branch b): sentinel writes so the delete/label/edge invariants have data to check.
  const auto id_range = [](int64_t first, int64_t count) {
    return "n.id >= " + std::to_string(first) + " AND n.id < " + std::to_string(first + count);
  };
  const std::string del_edges_q =
      "MATCH (n:P)-[r:R]->() WHERE " + id_range(kSentinelEdgeDelFirst, 5) + " RETURN count(r)";
  const std::string sentinel_label_q = "MATCH (n:L) WHERE " + id_range(kSentinelLabelFirst, 10) + " RETURN count(*)";
  const std::string unlabel_q = "MATCH (n:L) WHERE " + id_range(kSentinelUnlabelFirst, 10) + " RETURN count(*)";
  const std::string sentinel_edge_q = "MATCH ()-[r:R {w:" + std::to_string(kSentinelEdgeW) + "}]->() RETURN count(r)";
  const int64_t del_edges_before = Scalar(prep, del_edges_q);
  ASSERT_EQ(del_edges_before, 15);
  {
    BranchPending phase0;
    for (int64_t i = 0; i < 10; ++i) {
      const int64_t d = kSentinelDeleteFirst + i;
      prep.Interpret("MATCH (n:P {id:" + std::to_string(d) + "}) DETACH DELETE n");
      phase0.deleted.push_back(d);
      prep.Interpret("MATCH (n:P {id:" + std::to_string(kSentinelLabelFirst + i) + "}) SET n:L");
      prep.Interpret("MATCH (n:P {id:" + std::to_string(kSentinelUnlabelFirst + i) + "}) SET n:L");
      prep.Interpret("MATCH (n:P {id:" + std::to_string(kSentinelUnlabelFirst + i) + "}) REMOVE n:L");
      prep.Interpret("MATCH (a:P {id:" + std::to_string(kSentinelEdgeFirst + i) +
                     "}), (b:P {id:" + std::to_string(kSentinelEdgeFirst + (i + 1) % 10) +
                     "}) CREATE (a)-[:R {w:" + std::to_string(kSentinelEdgeW) + "}]->(b)");
    }
    for (int64_t i = 0; i < 5; ++i) {
      prep.Interpret("MATCH (:P {id:" + std::to_string(kSentinelEdgeDelFirst + i) +
                     "})-[r:R]->() WITH r LIMIT 1 DELETE r");
    }
    committed.Publish(phase0);
  }
  ASSERT_EQ(Scalar(prep, del_edges_q), del_edges_before - 5);
  ASSERT_EQ(Scalar(prep, sentinel_label_q), 10);
  ASSERT_EQ(Scalar(prep, unlabel_q), 0);
  ASSERT_EQ(Scalar(prep, sentinel_edge_q), 10);
  const int64_t phase0_deleted = static_cast<int64_t>(committed.deleted.size());
  const bool full_scale = Scale() >= 0.5;  // scale-dependent minimums are waived below this

  Workers workers;
  for (int i = 0; i < kBranchWriters; ++i) {
    workers.Spawn([&, i] { BranchWriter(*bw[i], i, writer_ops, committed, log, bw_c); });
  }
  for (int i = 0; i < kBranchReaders; ++i) {
    workers.Spawn([&, i] { BranchReader(*br[i], i, reader_ops, log, br_c); });
  }
  for (int i = 0; i < kMainWriters; ++i) {
    workers.Spawn([&, i] { MainWriter(*mw[i], i, main_ops, main_books[i], log, mw_c); });
  }
  workers.Spawn([&] { Lifecycle(life, life_iters, log, life_c); });
  workers.Run();

  std::cerr << "[ STRESS ] scale=" << Scale() << " branch-writers: ops=" << bw_c.ops << " conflicts=" << bw_c.conflicts
            << " rollbacks=" << bw_c.rollbacks << " committed-created=" << committed.created
            << " committed-deleted=" << committed.deleted.size() << " | readers: ops=" << br_c.ops
            << " conflicts=" << br_c.conflicts << " | main: ops=" << mw_c.ops << " conflicts=" << mw_c.conflicts
            << " | lifecycle: ops=" << life_c.ops << std::endl;

  ASSERT_EQ(log.Count(), 0U) << log.Dump();
  ASSERT_EQ(mw_c.conflicts.load(), 0U) << "main writers (disjoint data) must never conflict because of a branch";
  ASSERT_EQ(mw_c.errors.load(), 0U);
  ASSERT_EQ(life_c.errors.load() + life_c.conflicts.load(), 0U) << "branch lifecycle churn must always succeed";
  ASSERT_EQ(br_c.conflicts.load(), 0U) << "branch readers must never conflict";
  ASSERT_EQ(br_c.errors.load(), 0U);
  ASSERT_GE(committed.deleted.size(), static_cast<size_t>(phase0_deleted));
  ASSERT_GE(committed.deleted.size(), 10U);
  if (full_scale) {
    ASSERT_GT(committed.created.load(), 0) << "no branch-native vertex was ever committed";
    ASSERT_GT(bw_c.ops.load(), 0U);
    ASSERT_GT(br_c.ops.load(), 0U);
    ASSERT_GT(life_c.ops.load(), 0U);
    for (size_t w = 0; w < main_books.size(); ++w) {
      ASSERT_FALSE(main_books[w].last_v.empty()) << "main writer " << w << " committed no SET";
      ASSERT_GT(main_books[w].created, 0) << "main writer " << w << " committed no CREATE";
    }
  }

  auto &chk = engine->NewBranchSession("b");
  ASSERT_EQ(chk.interpreter.current_db_.CurrentVersion(), "b");

  const int64_t deleted = static_cast<int64_t>(committed.deleted.size());
  EXPECT_EQ(Scalar(chk, "MATCH (n:P) RETURN count(*)"), kSeedVertices + committed.created - deleted)
      << "branch vertex count != seed + committed native creates - committed fork deletes";
  EXPECT_EQ(Scalar(chk, "MATCH (n) RETURN count(*)"), Scalar(chk, "MATCH (n:P) RETURN count(*)"))
      << "a vertex without :P is visible on the branch";
  EXPECT_EQ(Scalar(chk, "MATCH (n:P) WHERE n.id IS NULL RETURN count(*)"), 0) << "vertex without `id` on the branch";
  EXPECT_EQ(Scalar(chk, "MATCH (n:P) WHERE n.id >= " + std::to_string(kBranchNativeBase) + " RETURN count(*)"),
            committed.created)
      << "committed branch-native vertices missing/extra (or main natives leaked in)";
  EXPECT_EQ(Scalar(chk, "MATCH (n:P) WHERE n.v >= " + std::to_string(kMainValueBase) + " RETURN count(*)"), 0)
      << "main writes are visible on the branch";
  EXPECT_EQ(Scalar(chk, "MATCH (n) UNWIND labels(n) AS l WITH l WHERE l STARTS WITH '__mg' RETURN count(*)"), 0)
      << "marker label leaked through labels()";
  EXPECT_EQ(Scalar(chk, "MATCH (n) UNWIND keys(n) AS k WITH k WHERE k STARTS WITH '__mg' RETURN count(*)"), 0)
      << "marker key leaked through keys()";
  EXPECT_EQ(Scalar(chk, "MATCH ()-[r]->() UNWIND keys(r) AS k WITH k WHERE k STARTS WITH '__mg' RETURN count(*)"), 0)
      << "marker key leaked through keys(edge)";
  EXPECT_EQ(Scalar(chk, "MATCH (n:L) RETURN count(*)"),
            Scalar(chk, "MATCH (n) WITH n WHERE 'L' IN labels(n) RETURN count(*)"))
      << "label-index scan disagrees with a full scan on the branch";
  EXPECT_GT(Scalar(chk, "MATCH (n:L) RETURN count(*)"), 0) << "no :L vertex on the branch";
  EXPECT_EQ(Scalar(chk, sentinel_label_q), 10) << "sentinel SET :L lost";
  EXPECT_EQ(Scalar(chk, unlabel_q), 0) << "sentinel REMOVE :L lost";
  EXPECT_EQ(Scalar(chk, sentinel_edge_q), 10) << "sentinel branch-native edges lost/extra";
  {
    // Each sentinel source has 3 fork out-edges to one target; phase 0 deleted one, so 2 remain unless a random writer
    // later deleted the target vertex (which removes its edges).
    int64_t want = 0;
    for (int64_t i = 0; i < 5; ++i) {
      const int64_t target = ((kSentinelEdgeDelFirst + i) * 7 + 13) % kSeedVertices;
      if (!committed.deleted.contains(target)) want += 2;
    }
    EXPECT_EQ(Scalar(chk, del_edges_q), want) << "sentinel fork-edge deletes lost/extra";
  }

  {
    std::string ids;
    for (auto id : committed.deleted) ids += (ids.empty() ? "" : ",") + std::to_string(id);
    EXPECT_EQ(Scalar(chk, "MATCH (n:P) WHERE n.id IN [" + ids + "] RETURN count(*)"), 0)
        << "vertices DETACH DELETEd by a committed branch txn are still visible";
    // Start from SURVIVING endpoints (WITH pins the order): no edge may reach a deleted vertex.
    EXPECT_EQ(Scalar(chk,
                     "MATCH (n:P) WHERE NOT n.id IN [" + ids + "] WITH n MATCH (n)-[r:R]-(m) WHERE m.id IN [" + ids +
                         "] RETURN count(r)"),
              0)
        << "a surviving vertex still has an edge to a deleted branch vertex";
  }

  const auto edges_all = Scalar(chk, "MATCH ()-[r:R]->() RETURN count(r)");
  EXPECT_EQ(edges_all, Scalar(chk, "MATCH (n) OPTIONAL MATCH (n)-[r:R]->() RETURN count(r)"))
      << "edge count differs between an edge scan and per-vertex expansion";
  {
    const std::string indexed_q =
        "MATCH ()-[r:R]->() WHERE r.w >= " + std::to_string(kSentinelEdgeW) + " RETURN count(r)";
    ASSERT_TRUE(PlanUsesOperator(chk, indexed_q, "ScanAllByEdgeTypeProperty"))
        << "the planner does not use the :R(w) edge-property index on the branch";
    EXPECT_EQ(edges_all, Scalar(chk, indexed_q))
        << "edge-property index scan disagrees with a full edge scan on the branch";
  }
  EXPECT_EQ(Scalar(chk, "MATCH (a)-[:R]->(b) WHERE a.id IS NULL OR b.id IS NULL RETURN count(*)"), 0)
      << "a branch edge has a dangling/invalid endpoint";

  auto &mchk = engine->NewSession();
  int64_t created = 0, deleted_main = 0;
  std::map<int64_t, int64_t> expected_v;
  for (const auto &b : main_books) {
    created += b.created;
    deleted_main += b.deleted;
    expected_v.insert(b.last_v.begin(), b.last_v.end());
  }
  EXPECT_EQ(Scalar(mchk, "MATCH (n:P) RETURN count(*)"), kSeedVertices + created - deleted_main)
      << "main vertex count != bookkeeping of main creates/deletes";
  EXPECT_EQ(Scalar(mchk, "MATCH ()-[r:R]->() RETURN count(r)"), kSeedEdges) << "branch writes changed main's edges";
  EXPECT_EQ(Scalar(mchk, "MATCH (n:L) RETURN count(*)"), 0) << "a branch label leaked to main";
  EXPECT_EQ(Scalar(mchk,
                   "MATCH (n:P) WHERE n.id < " + std::to_string(kSeedVertices) +
                       " AND (n.w IS NULL OR n.w <> n.id) RETURN count(*)"),
            0)
      << "a branch REMOVE/SET of `w` leaked to main";
  EXPECT_EQ(Scalar(mchk, "MATCH (n) UNWIND labels(n) AS l WITH l WHERE l <> 'P' RETURN count(*)"), 0);
  EXPECT_EQ(Scalar(mchk, "MATCH (n) UNWIND keys(n) AS k WITH k WHERE NOT k IN ['id','v','w'] RETURN count(*)"), 0)
      << "unexpected key on main (marker leak?)";
  EXPECT_EQ(Scalar(mchk,
                   "MATCH (n:P) WHERE n.v >= 1 AND n.v < " + std::to_string(kMainValueBase) + " AND n.id < " +
                       std::to_string(kSeedVertices) + " RETURN count(*)"),
            0)
      << "a branch value of `v` leaked to main";
  {
    auto stream = mchk.Interpret("MATCH (n:P) WHERE n.id < " + std::to_string(kSeedVertices) + " RETURN n.id, n.v");
    EXPECT_EQ(stream.GetResults().size(), static_cast<size_t>(kSeedVertices));
    for (const auto &row : stream.GetResults()) {
      const auto id = row[0].ValueInt();
      const auto it = expected_v.find(id);
      const int64_t want = it == expected_v.end() ? 0 : it->second;
      if (row[1].ValueInt() != want) {
        ADD_FAILURE() << "main vertex " << id << ": v=" << row[1].ValueInt() << ", last committed main write=" << want;
        break;
      }
    }
  }

  for (int i = 0; i < life_iters; ++i) {
    EXPECT_FALSE(engine->db()->version_store()->Exists("tmp" + std::to_string(i)))
        << "tmp" << i << " survived DROP BRANCH (or churn aborted early)";
  }
}

namespace {

constexpr std::string_view kMarkerPrefix = "__mg";

// Order-insensitive for lists of strings (labels(), keys()), so two reads of one vertex compare equal.
std::string Canon(const BoltValue &v) {
  using Type = BoltValue::Type;
  switch (v.type()) {
    case Type::List: {
      std::vector<std::string> parts;
      bool all_strings = true;
      for (const auto &e : v.ValueList()) {
        all_strings = all_strings && e.IsString();
        parts.push_back(Canon(e));
      }
      if (all_strings) std::ranges::sort(parts);
      std::string out = "[";
      for (const auto &p : parts) out += p + ",";
      return out + "]";
    }
    case Type::Map: {
      std::string out = "{";
      for (const auto &[k, e] : v.ValueMap()) out += k + ":" + Canon(e) + ",";
      return out + "}";
    }
    case Type::Vertex: {
      auto labels = v.ValueVertex().labels;
      std::ranges::sort(labels);
      std::string out = "(";
      for (const auto &l : labels) out += ":" + l;
      out += " {";
      for (const auto &[k, e] : v.ValueVertex().properties) out += k + ":" + Canon(e) + ",";
      return out + "})";
    }
    case Type::Edge: {
      std::string out = "[:" + v.ValueEdge().type + " {";
      for (const auto &[k, e] : v.ValueEdge().properties) out += k + ":" + Canon(e) + ",";
      return out + "}]";
    }
    default: {
      std::ostringstream os;
      os << v;
      return os.str();
    }
  }
}

std::string Canon(const std::vector<BoltValue> &row) {
  std::string out;
  for (const auto &v : row) out += Canon(v) + "|";
  return out;
}

// Every vertex and edge of the session's current branch/main as sorted canonical strings: equal snapshots mean equal
// user-visible state (ignores internal ids).
std::vector<std::string> StateSnapshot(InterpreterFaker &s) {
  std::vector<std::string> out;
  for (const auto &row : s.Interpret("MATCH (n) RETURN labels(n), properties(n)").GetResults()) {
    out.push_back("V" + Canon(row));
  }
  for (const auto &row :
       s.Interpret("MATCH (a)-[r]->(b) RETURN properties(a), properties(b), type(r), properties(r)").GetResults()) {
    out.push_back("E" + Canon(row));
  }
  std::ranges::sort(out);
  return out;
}

std::string FirstDifference(const std::vector<std::string> &want, const std::vector<std::string> &got) {
  std::vector<std::string> missing, extra;
  std::ranges::set_difference(want, got, std::back_inserter(missing));
  std::ranges::set_difference(got, want, std::back_inserter(extra));
  std::ostringstream os;
  os << "want " << want.size() << " items, got " << got.size() << "; missing " << missing.size() << ", extra "
     << extra.size();
  if (!missing.empty()) os << "; first missing: " << missing.front();
  if (!extra.empty()) os << "; first extra: " << extra.front();
  return os.str();
}

// Marker names are internal; DUMP legitimately spells its own `__mg_vertex__` / `__mg_id__` helpers.
bool HasMarker(std::string text, bool allow_dump_helpers) {
  if (allow_dump_helpers) {
    for (const std::string_view helper : {"__mg_vertex__", "__mg_id__"}) {
      for (auto pos = text.find(helper); pos != std::string::npos; pos = text.find(helper))
        text.erase(pos, helper.size());
    }
  }
  return text.find(kMarkerPrefix) != std::string::npos;
}

// Runs `query` and fails the log for any marker name in any cell (vertices, edges, paths, maps, lists included).
BoltRows RunScanned(InterpreterFaker &s, const std::string &query, FailureLog &log, std::string_view when) {
  auto rows = s.Interpret(query).GetResults();
  for (const auto &row : rows) {
    for (const auto &cell : row) {
      std::ostringstream os;
      os << cell;
      if (HasMarker(os.str(), false)) {
        log.Add(std::string{when} + ": MARKER LEAK in '" + query + "': " + os.str().substr(0, 300));
        return rows;
      }
    }
  }
  return rows;
}

}  // namespace

// Hot fork-edge hammer: many sessions concurrently write the same few fork edges and their endpoints, and attach native
// edges to the same fork vertices while others DETACH DELETE them.

namespace {

constexpr int64_t kHotVertices = 12;
constexpr int64_t kHotStable = 8;  // ids [0, 8) are never deleted; [8, 12) may be DETACH DELETEd
constexpr int64_t kHotEdges = kHotVertices * 3;
constexpr int64_t kRemoved = -1;  // committed "w and z removed"

int64_t HotSrc(int64_t eid) { return eid / 3; }

int64_t HotDst(int64_t eid) { return (HotSrc(eid) + 1 + 2 * (eid % 3)) % kHotVertices; }

struct NativeEdge {
  int64_t nid, src, dst;
};

struct HotPending {
  std::vector<std::pair<int64_t, int64_t>> edge_values;    // (eid, w == z)
  std::vector<std::pair<int64_t, int64_t>> vertex_values;  // (vertex id, v)
  std::vector<int64_t> deleted_edges;
  std::vector<int64_t> deleted_vertices;
  std::vector<NativeEdge> natives;
};

// What committed. Values are unique per (session, phase, write), so membership proves a final value was never rolled
// back or invented.
struct HotCommitted {
  std::mutex mutex;
  std::array<std::set<int64_t>, kHotEdges> edge_values;
  std::array<std::set<int64_t>, kHotVertices> vertex_values;
  std::set<int64_t> deleted_edges;
  std::set<int64_t> deleted_vertices;
  std::vector<NativeEdge> natives;

  HotCommitted() {
    for (auto &s : edge_values) s.insert(0);
    for (auto &s : vertex_values) s.insert(0);
  }

  void Publish(const HotPending &p) {
    std::lock_guard lock{mutex};
    for (auto [eid, v] : p.edge_values) edge_values[static_cast<size_t>(eid)].insert(v);
    for (auto [id, v] : p.vertex_values) vertex_values[static_cast<size_t>(id)].insert(v);
    deleted_edges.insert(p.deleted_edges.begin(), p.deleted_edges.end());
    deleted_vertices.insert(p.deleted_vertices.begin(), p.deleted_vertices.end());
    natives.insert(natives.end(), p.natives.begin(), p.natives.end());
  }
};

enum class HotKind { kEdgeSet, kEdgeRemove, kEdgeDelete, kEndpoints, kNative, kVertexDelete, kReadOut, kReadIn };

struct HotOp {
  HotKind kind;
  std::string query;
  int64_t eid{-1};
  int64_t a{-1};
  int64_t b{-1};
  int64_t value{0};
};

struct HotRun {
  int phase{0};
  bool deletes{true};  // deleting edges (only eid % 3 == 2, so most edges stay) and DETACH DELETE of the victim
                       // vertices [kHotStable, kHotVertices)
};

void SetupHot(InterpreterFaker &s) {
  s.Interpret("CREATE INDEX ON :H");
  s.Interpret("CREATE INDEX ON :H(id)");
  s.Interpret("CREATE EDGE INDEX ON :E(eid)");
  s.Interpret("UNWIND range(0," + std::to_string(kHotVertices - 1) + ") AS i CREATE (:H {id:i, v:0})");
  for (int64_t e = 0; e < kHotEdges; ++e) {
    s.Interpret("MATCH (a:H {id:" + std::to_string(HotSrc(e)) + "}), (b:H {id:" + std::to_string(HotDst(e)) +
                "}) CREATE (a)-[:E {eid:" + std::to_string(e) + ", w:0, z:0}]->(b)");
  }
  s.Interpret("CREATE BRANCH 'hot'");
}

template <typename NextValue>
HotOp MakeHotOpOfAnyKind(std::mt19937_64 &rng, NextValue &&next_value) {
  const auto eid = static_cast<int64_t>(rng() % kHotEdges);
  const auto any_vertex = [&] { return static_cast<int64_t>(rng() % kHotVertices); };
  const auto eid_s = std::to_string(eid);
  const int pick = static_cast<int>(rng() % 100);
  HotOp op;
  op.eid = eid;
  if (pick < 26) {
    op.kind = HotKind::kEdgeSet;
    op.value = next_value();
    const auto v = std::to_string(op.value);
    op.query = "MATCH ()-[r:E {eid:" + eid_s + "}]->() SET r.w = " + v + ", r.z = " + v + " RETURN r.w, r.z";
  } else if (pick < 34) {
    op.kind = HotKind::kEdgeRemove;
    op.query = "MATCH ()-[r:E {eid:" + eid_s + "}]->() REMOVE r.w, r.z RETURN r.eid";
  } else if (pick < 43) {
    op.kind = HotKind::kEdgeDelete;
    op.eid = eid - eid % 3 + 2;
    op.query = "MATCH ()-[r:E {eid:" + std::to_string(op.eid) + "}]->() DELETE r RETURN 1";
  } else if (pick < 63) {
    op.kind = HotKind::kEndpoints;
    op.value = next_value();
    const auto v = std::to_string(op.value);
    op.a = HotSrc(eid);
    op.b = HotDst(eid);
    op.query = "MATCH (a:H {id:" + std::to_string(op.a) + "})-[r:E {eid:" + eid_s +
               "}]->(b:H {id:" + std::to_string(op.b) + "}) SET a.v = " + v + ", b.v = " + v + ", r.w = " + v +
               ", r.z = " + v + " RETURN a.v, b.v, r.w, r.z";
  } else if (pick < 77) {
    op.kind = HotKind::kNative;
    op.value = next_value();
    op.a = any_vertex();
    op.b = any_vertex();
    op.query = "MATCH (a:H {id:" + std::to_string(op.a) + "}), (b:H {id:" + std::to_string(op.b) +
               "}) CREATE (a)-[r:N {nid:" + std::to_string(op.value) + "}]->(b) RETURN r.nid";
  } else if (pick < 81) {
    op.kind = HotKind::kVertexDelete;
    op.a = kHotStable + static_cast<int64_t>(rng() % (kHotVertices - kHotStable));
    op.query = "MATCH (n:H {id:" + std::to_string(op.a) + "}) DETACH DELETE n RETURN 1";
  } else if (pick < 91) {
    op.kind = HotKind::kReadOut;
    op.a = any_vertex();
    op.query = "MATCH (a:H {id:" + std::to_string(op.a) + "})-[r:E]->(b) RETURN r.eid, r.w, r.z, b.id";
  } else {
    op.kind = HotKind::kReadIn;
    op.a = any_vertex();
    op.query = "MATCH (a:H {id:" + std::to_string(op.a) + "})<-[r:E]-(b) RETURN r.eid, r.w, r.z, b.id";
  }
  return op;
}

template <typename NextValue>
HotOp MakeHotOp(std::mt19937_64 &rng, const HotRun &run, NextValue &&next_value) {
  for (;;) {
    auto op = MakeHotOpOfAnyKind(rng, next_value);
    if ((op.kind == HotKind::kVertexDelete || op.kind == HotKind::kEdgeDelete) && !run.deletes) continue;
    return op;
  }
}

// Validates what one statement returned and records what it changed (only if it matched, i.e. returned a row).
void RecordHotOp(const HotOp &op, const BoltRows &rows, HotPending &p, FailureLog &log, std::string_view who) {
  const auto bad = [&](const std::string &what) { log.Add(std::string{who} + ": " + what + " in '" + op.query + "'"); };
  const auto is_value = [&](const BoltValue &v) { return v.IsInt() && v.ValueInt() == op.value; };
  switch (op.kind) {
    case HotKind::kEdgeSet:
    case HotKind::kEdgeRemove:
    case HotKind::kEdgeDelete:
    case HotKind::kEndpoints:
    case HotKind::kNative:
    case HotKind::kVertexDelete:
      if (rows.size() > 1) bad("statement matched " + std::to_string(rows.size()) + " rows (duplicated edge/vertex)");
      if (rows.size() != 1) return;
      break;
    default:
      break;
  }
  switch (op.kind) {
    case HotKind::kEdgeSet:
      if (!is_value(rows[0][0]) || !is_value(rows[0][1])) bad("read-your-writes violated");
      p.edge_values.emplace_back(op.eid, op.value);
      break;
    case HotKind::kEdgeRemove:
      p.edge_values.emplace_back(op.eid, kRemoved);
      break;
    case HotKind::kEdgeDelete:
      p.deleted_edges.push_back(op.eid);
      break;
    case HotKind::kEndpoints:
      for (const auto &cell : rows[0]) {
        if (!is_value(cell)) bad("read-your-writes violated");
      }
      p.vertex_values.emplace_back(op.a, op.value);
      p.vertex_values.emplace_back(op.b, op.value);
      p.edge_values.emplace_back(op.eid, op.value);
      break;
    case HotKind::kNative:
      if (!is_value(rows[0][0])) bad("native edge lost its nid");
      p.natives.push_back({op.value, op.a, op.b});
      break;
    case HotKind::kVertexDelete:
      p.deleted_vertices.push_back(op.a);
      break;
    case HotKind::kReadOut:
    case HotKind::kReadIn: {
      std::set<int64_t> seen;
      for (const auto &r : rows) {
        if (!r[0].IsInt() || !r[3].IsInt()) {
          bad("edge row without eid / neighbour id");
          continue;
        }
        const auto eid = r[0].ValueInt();
        if (eid < 0 || eid >= kHotEdges || !seen.insert(eid).second) {
          bad("bad or duplicated fork edge " + std::to_string(eid));
          continue;
        }
        const bool out = op.kind == HotKind::kReadOut;
        if ((out ? HotSrc(eid) : HotDst(eid)) != op.a || r[3].ValueInt() != (out ? HotDst(eid) : HotSrc(eid))) {
          bad("edge " + std::to_string(eid) + " reached through the wrong endpoints");
        }
        if (Canon(r[1]) != Canon(r[2])) bad("torn edge write (w != z) on edge " + std::to_string(eid));
        if (r[1].IsInt() && r[1].ValueInt() >= kMainValueBase) bad("main-writer value leaked into the branch");
      }
      break;
    }
  }
}

void HotWriter(InterpreterFaker &s, int tid, int ops, const HotRun &run, HotCommitted &committed, FailureLog &log,
               Counters &c) {
  std::mt19937_64 rng{Seed() + 600 + static_cast<uint64_t>(run.phase) * 100 + static_cast<uint64_t>(tid)};
  const std::string who = "hot-writer-" + std::to_string(tid);
  int64_t counter = 0;
  const auto next_value = [&] { return (static_cast<int64_t>(run.phase) * 16 + tid + 1) * 1'000'000 + counter++; };
  for (int i = 0; i < ops; ++i) {
    const bool explicit_txn = rng() % 3 == 0;
    const int n = explicit_txn ? 1 + static_cast<int>(rng() % 3) : 1;
    if (explicit_txn && Exec(s, "BEGIN", log, c, who) != Outcome::kOk) continue;
    HotPending pending;
    bool failed = false;
    for (int j = 0; j < n && !failed; ++j) {
      const auto op = MakeHotOp(rng, run, next_value);
      BoltRows rows;
      if (Exec(s, op.query, log, c, who, &rows) != Outcome::kOk) {
        failed = true;
        break;
      }
      RecordHotOp(op, rows, pending, log, who);
    }
    if (failed) {
      if (explicit_txn) SwallowRollback(s);
      continue;
    }
    if (explicit_txn) {
      if (rng() % 6 == 0) {
        c.rollbacks.fetch_add(1, std::memory_order_relaxed);
        if (Exec(s, "ROLLBACK", log, c, who) != Outcome::kOk) SwallowRollback(s);
        continue;
      }
      if (Exec(s, "COMMIT", log, c, who) != Outcome::kOk) {
        SwallowRollback(s);
        continue;
      }
    }
    committed.Publish(pending);
  }
}

// One snapshot-isolated read-only transaction reads the same aggregates twice with a pause in between: concurrent
// commits must not change the answer, and no edge may be counted twice.
void HotSnapshotReader(InterpreterFaker &s, int tid, int iters, const HotRun &run, FailureLog &log, Counters &c) {
  std::mt19937_64 rng{Seed() + 700 + static_cast<uint64_t>(run.phase) * 100 + static_cast<uint64_t>(tid)};
  const std::string who = "hot-snapshot-reader-" + std::to_string(tid);
  const std::string edge_q =
      "MATCH ()-[r]->() RETURN count(r), count(DISTINCT r.eid), count(DISTINCT r.nid), sum(coalesce(r.w, 0))";
  const std::string vertex_q = "MATCH (n:H) RETURN count(n), sum(coalesce(n.v, 0))";
  for (int i = 0; i < iters; ++i) {
    if (Exec(s, "BEGIN", log, c, who) != Outcome::kOk) continue;
    BoltRows e1, v1, e2, v2;
    if (Exec(s, edge_q, log, c, who, &e1) != Outcome::kOk || Exec(s, vertex_q, log, c, who, &v1) != Outcome::kOk) {
      SwallowRollback(s);
      continue;
    }
    std::this_thread::sleep_for(std::chrono::microseconds(rng() % 800));
    if (Exec(s, edge_q, log, c, who, &e2) != Outcome::kOk || Exec(s, vertex_q, log, c, who, &v2) != Outcome::kOk) {
      SwallowRollback(s);
      continue;
    }
    if (e1.size() != 1 || e2.size() != 1 || v1.size() != 1 || v2.size() != 1) {
      log.Add(who + ": aggregate returned no single row");
    } else {
      if (Canon(e1[0]) != Canon(e2[0])) {
        log.Add(who + ": edge aggregates changed inside one snapshot txn: " + Canon(e1[0]) + " -> " + Canon(e2[0]));
      }
      if (Canon(v1[0]) != Canon(v2[0])) {
        log.Add(who + ": vertex aggregates changed inside one snapshot txn: " + Canon(v1[0]) + " -> " + Canon(v2[0]));
      }
      if (e1[0][0].ValueInt() != e1[0][1].ValueInt() + e1[0][2].ValueInt()) {
        log.Add(who + ": duplicated edges visible: " + Canon(e1[0]));
      }
    }
    Exec(s, "COMMIT", log, c, who);
  }
}

// Main edges/vertices of the hot graph: written on main only, concurrently, never conflicting with the branch.
void HotMainWriter(InterpreterFaker &s, int ops, const HotRun &run, FailureLog &log, Counters &c) {
  std::mt19937_64 rng{Seed() + 800 + static_cast<uint64_t>(run.phase)};
  for (int i = 0; i < ops; ++i) {
    const auto value = std::to_string(kMainValueBase + static_cast<int64_t>(rng() % 900'000'000));
    const auto o = rng() % 2 == 0
                       ? Exec(s,
                              "MATCH ()-[r:E {eid:" + std::to_string(rng() % kHotEdges) + "}]->() SET r.w = " + value +
                                  ", r.z = " + value,
                              log,
                              c,
                              "hot-main-writer")
                       : Exec(s,
                              "MATCH (a:H {id:" + std::to_string(rng() % kHotVertices) + "}) SET a.v = " + value,
                              log,
                              c,
                              "hot-main-writer");
    if (o == Outcome::kConflict) log.Add("hot-main-writer: branch-caused serialization conflict on main");
  }
}

struct HotStats {
  Counters writers, readers, main;
};

// Sessions are prepared by the caller (on its thread): kHotWriters + kHotReaders on the branch, one on main.
constexpr int kHotWriters = 6;
constexpr int kHotReaders = 2;

void RunHotStress(const std::vector<InterpreterFaker *> &writers, const std::vector<InterpreterFaker *> &readers,
                  InterpreterFaker &main_writer, const HotRun &run, HotCommitted &committed, FailureLog &log,
                  HotStats &stats) {
  const int writer_ops = Scaled(800);
  Workers workers;
  for (size_t i = 0; i < writers.size(); ++i) {
    workers.Spawn(
        [&, i] { HotWriter(*writers[i], static_cast<int>(i), writer_ops, run, committed, log, stats.writers); });
  }
  for (size_t i = 0; i < readers.size(); ++i) {
    workers.Spawn(
        [&, i] { HotSnapshotReader(*readers[i], static_cast<int>(i), Scaled(150), run, log, stats.readers); });
  }
  workers.Spawn([&] { HotMainWriter(main_writer, Scaled(400), run, log, stats.main); });
  workers.Run();
}

// Quiescent checks against the committed bookkeeping: no lost/duplicated edge, every value committed, sane degrees.
void VerifyHotBranch(InterpreterFaker &chk, HotCommitted &c, std::string_view when) {
  SCOPED_TRACE(std::string{when});
  const auto rows_of = [&](const std::string &q) { return chk.Interpret(q).GetResults(); };
  const auto alive = [&](int64_t v) { return !c.deleted_vertices.contains(v); };

  const auto vertices = rows_of("MATCH (n:H) RETURN n.id, n.v");
  std::set<int64_t> seen_vertices;
  for (const auto &r : vertices) {
    const auto id = r[0].ValueInt();
    EXPECT_TRUE(seen_vertices.insert(id).second) << "vertex " << id << " visible twice";
    EXPECT_TRUE(alive(id)) << "DETACH DELETEd vertex " << id << " still visible";
    EXPECT_TRUE(r[1].IsInt() && c.vertex_values[static_cast<size_t>(id)].contains(r[1].ValueInt()))
        << "vertex " << id << " v=" << Canon(r[1]) << " was never committed";
  }
  EXPECT_EQ(vertices.size(), static_cast<size_t>(kHotVertices) - c.deleted_vertices.size());

  std::map<int64_t, std::pair<int64_t, int64_t>> fork_edges;  // eid -> (src, dst)
  for (const auto &r : rows_of("MATCH (a)-[r:E]->(b) RETURN r.eid, r.w, r.z, a.id, b.id")) {
    const auto eid = r[0].ValueInt();
    EXPECT_TRUE(fork_edges.emplace(eid, std::pair{r[3].ValueInt(), r[4].ValueInt()}).second)
        << "fork edge " << eid << " visible twice";
    EXPECT_EQ(Canon(r[1]), Canon(r[2])) << "torn edge " << eid;
    const int64_t w = r[1].IsInt() ? r[1].ValueInt() : kRemoved;
    EXPECT_TRUE(c.edge_values[static_cast<size_t>(eid)].contains(w))
        << "fork edge " << eid << " final w=" << w << " was never committed";
  }
  size_t want_fork = 0;
  for (int64_t e = 0; e < kHotEdges; ++e) {
    const bool want = !c.deleted_edges.contains(e) && alive(HotSrc(e)) && alive(HotDst(e));
    want_fork += want ? 1 : 0;
    const auto it = fork_edges.find(e);
    EXPECT_EQ(it != fork_edges.end(), want) << "fork edge " << e << (want ? " lost" : " resurrected");
    if (it != fork_edges.end()) {
      EXPECT_EQ(it->second, std::pair(HotSrc(e), HotDst(e))) << "fork edge " << e << " changed endpoints";
    }
  }

  std::map<int64_t, NativeEdge> want_natives;
  for (const auto &n : c.natives) {
    if (alive(n.src) && alive(n.dst)) want_natives.emplace(n.nid, n);
  }
  std::map<int64_t, std::pair<int64_t, int64_t>> got_natives;
  for (const auto &r : rows_of("MATCH (a)-[r:N]->(b) RETURN r.nid, a.id, b.id")) {
    EXPECT_TRUE(got_natives.emplace(r[0].ValueInt(), std::pair{r[1].ValueInt(), r[2].ValueInt()}).second)
        << "native edge " << r[0].ValueInt() << " visible twice";
  }
  EXPECT_EQ(got_natives.size(), want_natives.size()) << "native edges lost or kept after their endpoint was deleted";
  for (const auto &[nid, n] : want_natives) {
    const auto it = got_natives.find(nid);
    ASSERT_TRUE(it != got_natives.end()) << "committed native edge " << nid << " is missing";
    EXPECT_EQ(it->second, std::pair(n.src, n.dst)) << "native edge " << nid << " has wrong endpoints";
  }

  for (int64_t v = 0; v < kHotVertices; ++v) {
    if (!alive(v)) continue;
    int64_t want_out = 0, want_in = 0;
    for (const auto &[eid, ends] : fork_edges) {
      want_out += ends.first == v ? 1 : 0;
      want_in += ends.second == v ? 1 : 0;
    }
    for (const auto &[nid, n] : want_natives) {
      want_out += n.src == v ? 1 : 0;
      want_in += n.dst == v ? 1 : 0;
    }
    const auto id = std::to_string(v);
    EXPECT_EQ(Scalar(chk, "MATCH (n:H {id:" + id + "}) OPTIONAL MATCH (n)-[r]->() RETURN count(r)"), want_out)
        << "out-degree of fork vertex " << v;
    EXPECT_EQ(Scalar(chk, "MATCH (n:H {id:" + id + "}) OPTIONAL MATCH (n)<-[r]-() RETURN count(r)"), want_in)
        << "in-degree of fork vertex " << v;
  }
  EXPECT_EQ(Scalar(chk, "MATCH ()-[r]->() RETURN count(r)"), static_cast<int64_t>(want_fork + want_natives.size()))
      << "edge count differs from committed bookkeeping";
  EXPECT_EQ(Scalar(chk, "MATCH (n) RETURN count(*)"), static_cast<int64_t>(vertices.size()));
}

void VerifyHotMain(InterpreterFaker &mchk) {
  const auto rows = mchk.Interpret("MATCH (a)-[r:E]->(b) RETURN r.eid, r.w, r.z, a.id, b.id").GetResults();
  ASSERT_EQ(rows.size(), static_cast<size_t>(kHotEdges)) << "main lost or gained fork edges";
  for (const auto &r : rows) {
    const auto eid = r[0].ValueInt();
    EXPECT_EQ(Canon(r[1]), Canon(r[2])) << "main edge " << eid;
    EXPECT_TRUE(r[1].ValueInt() == 0 || r[1].ValueInt() >= kMainValueBase)
        << "branch value " << r[1].ValueInt() << " leaked onto main edge " << eid;
    EXPECT_EQ(r[3].ValueInt(), HotSrc(eid));
    EXPECT_EQ(r[4].ValueInt(), HotDst(eid));
  }
  EXPECT_EQ(mchk.Interpret("MATCH ()-[r:N]->() RETURN count(r)").GetResults().at(0).at(0).ValueInt(), 0)
      << "a branch-native edge leaked onto main";
  const auto vertices = mchk.Interpret("MATCH (n:H) RETURN n.id, n.v").GetResults();
  ASSERT_EQ(vertices.size(), static_cast<size_t>(kHotVertices)) << "main lost or gained hot vertices";
  for (const auto &r : vertices) {
    EXPECT_TRUE(r[1].ValueInt() == 0 || r[1].ValueInt() >= kMainValueBase)
        << "branch value leaked onto main vertex " << r[0].ValueInt();
  }
}

void ReportHot(std::string_view what, const HotStats &s) {
  std::cerr << "[ STRESS ] " << what << " seed=" << Seed() << " scale=" << Scale() << " writers: ops=" << s.writers.ops
            << " conflicts=" << s.writers.conflicts << " rollbacks=" << s.writers.rollbacks
            << " | snapshot readers: ops=" << s.readers.ops << " | main: ops=" << s.main.ops << std::endl;
}

void ExpectHotStatsClean(const FailureLog &log, const HotStats &s) {
  ASSERT_EQ(log.Count(), 0U) << log.Dump();
  ASSERT_EQ(s.main.conflicts.load(), 0U) << "main writer (disjoint from the branch) conflicted because of a branch";
  ASSERT_EQ(s.main.errors.load(), 0U);
  ASSERT_EQ(s.readers.conflicts.load(), 0U) << "read-only snapshot transactions never conflict";
  ASSERT_EQ(s.readers.errors.load(), 0U);
  ASSERT_EQ(s.writers.errors.load(), 0U);
}

}  // namespace

// Marker leak matrix: random removals/deletes on a branch checked against an exact model through every output surface.

namespace {

constexpr int64_t kLeakVertices = 40;
constexpr int64_t kLeakEdges = 120;
constexpr std::array<std::string_view, 4> kVertexKeys{"a", "b", "c", "d"};
constexpr std::array<std::string_view, 4> kEdgeKeys{"x", "y", "z", "u"};
constexpr std::array<std::string_view, 3> kLeakLabels{"A", "B", "C"};

using PropMap = std::map<std::string, int64_t>;

struct LeakVertex {
  std::set<std::string> labels;
  PropMap props;
};

struct LeakEdge {
  int64_t src, dst;
  PropMap props;
};

struct LeakModel {
  std::map<int64_t, LeakVertex> vertices;
  std::map<int64_t, LeakEdge> edges;
};

void SeedLeak(InterpreterFaker &s, LeakModel &m) {
  s.Interpret("CREATE INDEX ON :P");
  s.Interpret("CREATE INDEX ON :P(id)");
  for (const auto l : kLeakLabels) s.Interpret("CREATE INDEX ON :" + std::string{l});
  s.Interpret("CREATE EDGE INDEX ON :R(x)");
  for (int64_t i = 0; i < kLeakVertices; ++i) {
    LeakVertex v;
    v.labels = {"P", "A"};
    std::string labels = ":P:A";
    if (i % 2 == 0) {
      v.labels.insert("B");
      labels += ":B";
    }
    if (i % 3 == 0) {
      v.labels.insert("C");
      labels += ":C";
    }
    v.props = {{"id", i}, {"a", i}, {"b", i + 1}, {"c", i + 2}, {"d", i + 3}};
    m.vertices.emplace(i, std::move(v));
    const auto n = std::to_string(i);
    s.Interpret("CREATE (" + labels + " {id:" + n + ", a:" + n + ", b:" + std::to_string(i + 1) +
                ", c:" + std::to_string(i + 2) + ", d:" + std::to_string(i + 3) + "})");
  }
  for (int64_t e = 0; e < kLeakEdges; ++e) {
    const int64_t src = e % kLeakVertices;
    const int64_t dst = (e * 7 + 3) % kLeakVertices;
    m.edges.emplace(e, LeakEdge{src, dst, {{"eid", e}, {"x", e}, {"y", e + 1}, {"z", e + 2}, {"u", e + 3}}});
    const auto n = std::to_string(e);
    s.Interpret("MATCH (a:P {id:" + std::to_string(src) + "}), (b:P {id:" + std::to_string(dst) +
                "}) CREATE (a)-[:R {eid:" + n + ", x:" + n + ", y:" + std::to_string(e + 1) +
                ", z:" + std::to_string(e + 2) + ", u:" + std::to_string(e + 3) + "}]->(b)");
  }
}

// One random removal/overwrite/delete on the branch; the model follows. Runs single-threaded (no conflicts), so the
// model is exact.
void RandomLeakOp(InterpreterFaker &s, LeakModel &m, std::mt19937_64 &rng) {
  const auto val = static_cast<int64_t>(rng() % 100'000);
  const auto vs = std::to_string(val);
  const auto pick_key = [&](const auto &keys) { return std::string{keys[rng() % keys.size()]}; };
  if (rng() % 100 < 60 && !m.vertices.empty()) {
    auto it = m.vertices.begin();
    std::advance(it, static_cast<std::ptrdiff_t>(rng() % m.vertices.size()));
    const int64_t x = it->first;
    auto &v = it->second;
    const std::string match = "MATCH (n:P {id:" + std::to_string(x) + "}) ";
    const auto k = pick_key(kVertexKeys);
    auto k2 = pick_key(kVertexKeys);
    if (k2 == k) k2 = k == "a" ? "b" : "a";
    const auto label = std::string{kLeakLabels[rng() % kLeakLabels.size()]};
    switch (rng() % 12) {
      case 0:
      case 1:
        s.Interpret(match + "REMOVE n." + k);
        v.props.erase(k);
        break;
      case 2:
        s.Interpret(match + "SET n." + k + " = null");
        v.props.erase(k);
        break;
      case 3:
      case 4:
        s.Interpret(match + "SET n." + k + " = " + vs);
        v.props[k] = val;
        break;
      case 5:
        s.Interpret(match + "REMOVE n:" + label);
        v.labels.erase(label);
        break;
      case 6:
        s.Interpret(match + "SET n:" + label);
        v.labels.insert(label);
        break;
      case 7:
        s.Interpret(match + "SET n = {id:" + std::to_string(x) + ", " + k + ":" + vs + "}");
        v.props = {{"id", x}, {k, val}};
        break;
      case 8:
        s.Interpret(match + "SET n += {" + k + ":" + vs + ", " + k2 + ":null}");
        v.props[k] = val;
        v.props.erase(k2);
        break;
      case 9:
        s.Interpret(match + "REMOVE n." + k + ", n:" + label);
        v.props.erase(k);
        v.labels.erase(label);
        break;
      case 10:
        s.Interpret(match + "SET n:" + label + ", n." + k + " = " + vs);
        v.labels.insert(label);
        v.props[k] = val;
        break;
      default:
        if (rng() % 4 == 0) {
          s.Interpret(match + "DETACH DELETE n");
          std::erase_if(m.edges, [&](const auto &e) { return e.second.src == x || e.second.dst == x; });
          m.vertices.erase(it);
        } else {
          s.Interpret(match + "SET n." + k + " = " + vs);
          v.props[k] = val;
        }
        break;
    }
    return;
  }
  if (m.edges.empty()) return;
  auto it = m.edges.begin();
  std::advance(it, static_cast<std::ptrdiff_t>(rng() % m.edges.size()));
  const int64_t e = it->first;
  auto &r = it->second;
  const std::string match = "MATCH ()-[r:R {eid:" + std::to_string(e) + "}]->() ";
  const auto k = pick_key(kEdgeKeys);
  auto k2 = pick_key(kEdgeKeys);
  if (k2 == k) k2 = k == "x" ? "y" : "x";
  switch (rng() % 10) {
    case 0:
    case 1:
      s.Interpret(match + "SET r." + k + " = " + vs);
      r.props[k] = val;
      break;
    case 2:
    case 3:
      s.Interpret(match + "REMOVE r." + k);
      r.props.erase(k);
      break;
    case 4:
      s.Interpret(match + "SET r." + k + " = null");
      r.props.erase(k);
      break;
    case 5:
      s.Interpret(match + "SET r = {eid:" + std::to_string(e) + ", " + k + ":" + vs + "}");
      r.props = {{"eid", e}, {k, val}};
      break;
    case 6:
      s.Interpret(match + "SET r += {" + k + ":" + vs + ", " + k2 + ":null}");
      r.props[k] = val;
      r.props.erase(k2);
      break;
    case 7:
      s.Interpret(match + "REMOVE r." + k + ", r." + k2);
      r.props.erase(k);
      r.props.erase(k2);
      break;
    case 8:
      s.Interpret(match + "DELETE r");
      m.edges.erase(it);
      break;
    default:
      s.Interpret(match + "SET r." + k + " = " + vs + " REMOVE r." + k2);
      r.props[k] = val;
      r.props.erase(k2);
      break;
  }
}

std::vector<std::string> Sorted(std::vector<std::string> v) {
  std::ranges::sort(v);
  return v;
}

std::vector<std::string> SortedNames(const std::vector<BoltValue> &list) {
  std::vector<std::string> out;
  for (const auto &e : list) out.push_back(e.IsString() ? e.ValueString() : "<non-string>");
  return Sorted(std::move(out));
}

// nullopt (and a log entry) when a value is not an int.
std::optional<PropMap> IntProps(const memgraph::communication::bolt::map_t &props, FailureLog &log,
                                const std::string &what) {
  PropMap out;
  for (const auto &[k, v] : props) {
    if (!v.IsInt()) {
      log.Add(what + ": property '" + k + "' is not an int");
      return std::nullopt;
    }
    out.emplace(k, v.ValueInt());
  }
  return out;
}

std::string Describe(const PropMap &p) {
  std::string out = "{";
  for (const auto &[k, v] : p) out += k + ":" + std::to_string(v) + ",";
  return out + "}";
}

std::vector<std::string> ModelKeys(const PropMap &p) {
  std::vector<std::string> out;
  for (const auto &[k, v] : p) out.push_back(k);
  return out;
}

// Reads the whole branch through the procedure API and compares it with the model.
void CheckMgpAgainstModel(InterpreterFaker &s, const LeakModel &m, FailureLog &log, std::string_view when) {
  const auto fail = [&](const std::string &msg) { log.Add(std::string{when} + ": mgp: " + msg); };
  s.Interpret("BEGIN");
  auto &dba = *s.interpreter.current_db_.execution_db_accessor_;
  mgp_graph graph{&dba, memgraph::storage::View::NEW, nullptr, dba.GetStorageMode()};
  mgp_memory memory{memgraph::utils::NewDeleteResource()};

  // A failed call clears the cursor so the loops below stop instead of spinning on a stale pointer.
  const auto step = [&](mgp_error err, auto *&cursor) {
    if (err != mgp_error::MGP_ERROR_NO_ERROR) {
      fail("procedure API call failed");
      cursor = nullptr;
    }
  };
  const auto expect_ok = [&](mgp_error err) {
    if (err != mgp_error::MGP_ERROR_NO_ERROR) fail("procedure API call failed");
  };
  const auto read_props = [&](mgp_properties_iterator *it, PropMap &out) {
    mgp_property *p = nullptr;
    if (mgp_properties_iterator_get(it, &p) != mgp_error::MGP_ERROR_NO_ERROR)
      return fail("properties iterator get failed");
    for (; p != nullptr; step(mgp_properties_iterator_next(it, &p), p)) {
      int is_int = 0;
      int64_t value = 0;
      if (std::string_view{p->name}.starts_with(kMarkerPrefix)) fail(std::string{"MARKER LEAK key "} + p->name);
      if (mgp_value_is_int(p->value, &is_int) != mgp_error::MGP_ERROR_NO_ERROR || is_int == 0 ||
          mgp_value_get_int(p->value, &value) != mgp_error::MGP_ERROR_NO_ERROR) {
        fail(std::string{"non-int property "} + p->name);
        continue;
      }
      out.emplace(p->name, value);
    }
  };

  std::map<int64_t, LeakVertex> got_vertices;
  std::map<int64_t, LeakEdge> got_edges;
  size_t edge_visits = 0;
  mgp_vertices_iterator *vit = nullptr;
  if (mgp_graph_iter_vertices(&graph, &memory, &vit) != mgp_error::MGP_ERROR_NO_ERROR) {
    fail("mgp_graph_iter_vertices failed");
  } else {
    mgp_vertex *v = nullptr;
    step(mgp_vertices_iterator_get(vit, &v), v);
    for (; v != nullptr; step(mgp_vertices_iterator_next(vit, &v), v)) {
      LeakVertex lv;
      size_t n_labels = 0;
      expect_ok(mgp_vertex_labels_count(v, &n_labels));
      for (size_t i = 0; i < n_labels; ++i) {
        mgp_label label{};
        expect_ok(mgp_vertex_label_at(v, i, &label));
        if (std::string_view{label.name}.starts_with(kMarkerPrefix))
          fail(std::string{"MARKER LEAK label "} + label.name);
        lv.labels.insert(label.name);
      }
      mgp_properties_iterator *pit = nullptr;
      if (mgp_vertex_iter_properties(v, &memory, &pit) != mgp_error::MGP_ERROR_NO_ERROR) {
        fail("mgp_vertex_iter_properties failed");
        continue;
      }
      read_props(pit, lv.props);
      mgp_properties_iterator_destroy(pit);
      const auto id_it = lv.props.find("id");
      if (id_it == lv.props.end()) {
        fail("vertex without id");
        continue;
      }
      const auto id = id_it->second;

      mgp_edges_iterator *eit = nullptr;
      if (mgp_vertex_iter_out_edges(v, &memory, &eit) != mgp_error::MGP_ERROR_NO_ERROR) {
        fail("mgp_vertex_iter_out_edges failed");
      } else {
        mgp_edge *e = nullptr;
        step(mgp_edges_iterator_get(eit, &e), e);
        for (; e != nullptr; step(mgp_edges_iterator_next(eit, &e), e)) {
          ++edge_visits;
          mgp_edge_type type{};
          expect_ok(mgp_edge_get_type(e, &type));
          if (std::string_view{type.name} != "R") fail(std::string{"unexpected edge type "} + type.name);
          mgp_properties_iterator *epit = nullptr;
          if (mgp_edge_iter_properties(e, &memory, &epit) != mgp_error::MGP_ERROR_NO_ERROR) {
            fail("mgp_edge_iter_properties failed");
            continue;
          }
          LeakEdge le{id, -1, {}};
          read_props(epit, le.props);
          mgp_properties_iterator_destroy(epit);
          const auto eid_it = le.props.find("eid");
          if (eid_it == le.props.end()) {
            fail("edge without eid");
            continue;
          }
          got_edges.emplace(eid_it->second, std::move(le));
        }
        mgp_edges_iterator_destroy(eit);
      }
      got_vertices.emplace(id, std::move(lv));
    }
    mgp_vertices_iterator_destroy(vit);
  }
  s.Interpret("ROLLBACK");

  if (got_vertices.size() != m.vertices.size()) {
    fail("vertex count " + std::to_string(got_vertices.size()) + " != model " + std::to_string(m.vertices.size()));
  }
  for (const auto &[id, mv] : m.vertices) {
    const auto it = got_vertices.find(id);
    if (it == got_vertices.end()) {
      fail("vertex " + std::to_string(id) + " missing");
    } else if (it->second.labels != mv.labels || it->second.props != mv.props) {
      fail("vertex " + std::to_string(id) + " is " + Describe(it->second.props) + ", model " + Describe(mv.props));
    }
  }
  if (edge_visits != m.edges.size()) {
    fail("edge visits " + std::to_string(edge_visits) + " != model " + std::to_string(m.edges.size()));
  }
  for (const auto &[eid, me] : m.edges) {
    const auto it = got_edges.find(eid);
    if (it == got_edges.end()) {
      fail("edge " + std::to_string(eid) + " missing");
    } else if (it->second.src != me.src || it->second.props != me.props) {
      fail("edge " + std::to_string(eid) + " is " + Describe(it->second.props) + " from " +
           std::to_string(it->second.src) + ", model " + Describe(me.props) + " from " + std::to_string(me.src));
    }
  }
}

// Compares the branch with the model through every output surface and scans each for marker names.
void CheckLeakSurfaces(InterpreterFaker &s, const LeakModel &m, FailureLog &log, std::string_view when) {
  const auto fail = [&](const std::string &msg) { log.Add(std::string{when} + ": " + msg); };
  const auto run = [&](const std::string &q) { return RunScanned(s, q, log, when); };

  // Vertices: RETURN n, labels(), keys(), properties(), map projection and individual property reads.
  auto rows = run("MATCH (n:P) RETURN n, labels(n), keys(n), properties(n), n {.*}, n.a, n.b, n.c, n.d");
  if (rows.size() != m.vertices.size()) {
    fail("vertex rows " + std::to_string(rows.size()) + " != model " + std::to_string(m.vertices.size()));
  }
  std::set<int64_t> seen;
  for (const auto &row : rows) {
    const auto &vertex = row[0].ValueVertex();
    const auto props = IntProps(vertex.properties, log, "RETURN n");
    if (!props || !props->contains("id")) {
      fail("RETURN n: vertex without id");
      continue;
    }
    const int64_t id = props->at("id");
    if (!seen.insert(id).second) fail("vertex " + std::to_string(id) + " returned twice");
    const auto mit = m.vertices.find(id);
    if (mit == m.vertices.end()) {
      fail("deleted vertex " + std::to_string(id) + " is visible");
      continue;
    }
    const auto &mv = mit->second;
    const auto want_labels = Sorted({mv.labels.begin(), mv.labels.end()});
    const std::string tag = "vertex " + std::to_string(id);
    if (*props != mv.props) fail(tag + " RETURN n props " + Describe(*props) + " != model " + Describe(mv.props));
    if (Sorted(vertex.labels) != want_labels) fail(tag + " RETURN n labels differ from the model");
    if (SortedNames(row[1].ValueList()) != want_labels) fail(tag + " labels() differ from the model");
    if (SortedNames(row[2].ValueList()) != Sorted(ModelKeys(mv.props))) fail(tag + " keys() differ from the model");
    const auto fn_props = IntProps(row[3].ValueMap(), log, "properties()");
    if (!fn_props || *fn_props != mv.props) fail(tag + " properties() differ from the model");
    const auto proj = IntProps(row[4].ValueMap(), log, "n {.*}");
    if (!proj || *proj != mv.props) fail(tag + " n {.*} differs from the model");
    for (size_t i = 0; i < kVertexKeys.size(); ++i) {
      const auto it = mv.props.find(std::string{kVertexKeys[i]});
      const auto &cell = row[5 + i];
      if (it == mv.props.end() ? cell.type() != BoltValue::Type::Null
                               : !(cell.IsInt() && cell.ValueInt() == it->second)) {
        fail(tag + " n." + std::string{kVertexKeys[i]} + " = " + Canon(cell) + " differs from the model");
      }
    }
  }
  if (const auto all = run("MATCH (n) RETURN count(*)");
      all.at(0).at(0).ValueInt() != static_cast<int64_t>(m.vertices.size())) {
    fail("a vertex without :P is visible (or counts differ)");
  }

  rows = run("MATCH (a)-[r:R]->(b) RETURN r, type(r), keys(r), properties(r), r.x, r.y, r.z, r.u, a.id, b.id");
  if (rows.size() != m.edges.size()) {
    fail("edge rows " + std::to_string(rows.size()) + " != model " + std::to_string(m.edges.size()));
  }
  std::set<int64_t> seen_edges;
  for (const auto &row : rows) {
    const auto &edge = row[0].ValueEdge();
    const auto props = IntProps(edge.properties, log, "RETURN r");
    if (!props || !props->contains("eid")) {
      fail("RETURN r: edge without eid");
      continue;
    }
    const int64_t eid = props->at("eid");
    if (!seen_edges.insert(eid).second) fail("edge " + std::to_string(eid) + " returned twice");
    const auto mit = m.edges.find(eid);
    if (mit == m.edges.end()) {
      fail("deleted edge " + std::to_string(eid) + " is visible");
      continue;
    }
    const auto &me = mit->second;
    const std::string tag = "edge " + std::to_string(eid);
    if (*props != me.props) fail(tag + " RETURN r props " + Describe(*props) + " != model " + Describe(me.props));
    if (edge.type != "R" || row[1].ValueString() != "R") fail(tag + " wrong type");
    if (SortedNames(row[2].ValueList()) != Sorted(ModelKeys(me.props))) fail(tag + " keys() differ from the model");
    const auto fn_props = IntProps(row[3].ValueMap(), log, "properties()");
    if (!fn_props || *fn_props != me.props) fail(tag + " properties() differ from the model");
    for (size_t i = 0; i < kEdgeKeys.size(); ++i) {
      const auto it = me.props.find(std::string{kEdgeKeys[i]});
      const auto &cell = row[4 + i];
      if (it == me.props.end() ? cell.type() != BoltValue::Type::Null
                               : !(cell.IsInt() && cell.ValueInt() == it->second)) {
        fail(tag + " r." + std::string{kEdgeKeys[i]} + " = " + Canon(cell) + " differs from the model");
      }
    }
    if (row[8].ValueInt() != me.src || row[9].ValueInt() != me.dst) fail(tag + " has wrong endpoints");
  }

  // Degrees through both expansion directions.
  std::map<int64_t, int64_t> want_out, want_in;
  for (const auto &[eid, e] : m.edges) {
    ++want_out[e.src];
    ++want_in[e.dst];
  }
  for (const auto &[dir, want] : {std::pair{"(n)-[r]->()", &want_out}, std::pair{"(n)<-[r]-()", &want_in}}) {
    for (const auto &row : run(std::string{"MATCH (n:P) OPTIONAL MATCH "} + dir + " RETURN n.id, count(r)")) {
      const auto it = want->find(row[0].ValueInt());
      if (row[1].ValueInt() != (it == want->end() ? 0 : it->second)) {
        fail("degree of vertex " + std::to_string(row[0].ValueInt()) + " via " + dir + " differs from the model");
      }
    }
  }

  // Index and filter scans see the same set as the model.
  for (const auto label : kLeakLabels) {
    std::set<int64_t> want_ids, got_ids;
    for (const auto &[id, v] : m.vertices) {
      if (v.labels.contains(std::string{label})) want_ids.insert(id);
    }
    for (const auto &row : run("MATCH (n:" + std::string{label} + ") RETURN n.id")) got_ids.insert(row[0].ValueInt());
    if (want_ids != got_ids) fail("label scan :" + std::string{label} + " differs from the model");
  }
  for (const auto key : kVertexKeys) {
    std::set<int64_t> want_ids, got_ids;
    for (const auto &[id, v] : m.vertices) {
      if (!v.props.contains(std::string{key})) want_ids.insert(id);
    }
    for (const auto &row : run("MATCH (n:P) WHERE n." + std::string{key} + " IS NULL RETURN n.id")) {
      got_ids.insert(row[0].ValueInt());
    }
    if (want_ids != got_ids) fail("IS NULL filter on n." + std::string{key} + " differs from the model");
  }
  {
    int64_t want_x = 0;
    for (const auto &[eid, e] : m.edges) want_x += e.props.contains("x") ? 1 : 0;
    if (run("MATCH ()-[r:R]->() WHERE r.x IS NOT NULL RETURN count(r)").at(0).at(0).ValueInt() != want_x) {
      fail("edge-property index scan on :R(x) differs from the model");
    }
  }

  // Paths, aggregations of names and the schema/INFO surfaces.
  run("MATCH p = (a:P)-[r:R]->(b) RETURN p LIMIT 60");
  run("MATCH (n) RETURN DISTINCT labels(n)");
  run("MATCH ()-[r]->() RETURN DISTINCT type(r), keys(r)");
  run("MATCH (n:P) RETURN collect(properties(n))");
  for (const auto *info :
       {"SHOW NODE_LABELS INFO", "SHOW EDGE_TYPES INFO", "SHOW INDEX INFO", "SHOW CONSTRAINT INFO"}) {
    run(info);
  }
  for (const auto &row : run("SHOW NODE_LABELS INFO")) {
    if (!row[0].IsString() || !(row[0].ValueString() == "P" || row[0].ValueString() == "A" ||
                                row[0].ValueString() == "B" || row[0].ValueString() == "C")) {
      fail("SHOW NODE_LABELS INFO lists " + Canon(row[0]));
    }
  }
  for (const auto &row : run("SHOW EDGE_TYPES INFO")) {
    if (!row[0].IsString() || row[0].ValueString() != "R") fail("SHOW EDGE_TYPES INFO lists " + Canon(row[0]));
  }

  // DUMP: the branch's data, no markers other than DUMP's own helpers.
  size_t dump_vertices = 0, dump_edges = 0;
  for (const auto &row : s.Interpret("DUMP DATABASE").GetResults()) {
    const auto &line = row[0].ValueString();
    if (HasMarker(line, true)) fail("MARKER LEAK in DUMP: " + line.substr(0, 300));
    if (line.starts_with("CREATE (:__mg_vertex__")) ++dump_vertices;
    if (line.find("CREATE (u)-[") != std::string::npos) ++dump_edges;
  }
  if (dump_vertices != m.vertices.size()) {
    fail("DUMP has " + std::to_string(dump_vertices) + " vertices, model " + std::to_string(m.vertices.size()));
  }
  if (dump_edges != m.edges.size()) {
    fail("DUMP has " + std::to_string(dump_edges) + " edges, model " + std::to_string(m.edges.size()));
  }

  CheckMgpAgainstModel(s, m, log, when);
}

}  // namespace

TEST_P(VersioningStressTest, RandomRemovalsAndDeletesNeverLeakAMarkerOnAnyOutputSurface) {
  LeakModel model;
  SeedLeak(engine->NewSession(), model);
  engine->NewSession().Interpret("CREATE BRANCH 'leak'");

  const int total_ops = Scaled(400);
  constexpr int kCheckEvery = 25;
  std::mt19937_64 rng{Seed() + 900};
  FailureLog log;

  auto branch = &engine->NewBranchSession("leak");
  auto main_session = &engine->NewSession();
  const auto main_before = StateSnapshot(*main_session);
  ASSERT_EQ(main_before.size(), static_cast<size_t>(kLeakVertices + kLeakEdges));

  const auto check_all = [&](std::string_view when) {
    CheckLeakSurfaces(*branch, model, log, when);
    // Main never sees a branch write, and its own surfaces never show markers either.
    const auto main_now = StateSnapshot(*main_session);
    if (main_now != main_before)
      log.Add(std::string{when} + ": main changed: " + FirstDifference(main_before, main_now));
    RunScanned(*main_session, "MATCH (n:P) RETURN n, labels(n), keys(n), properties(n)", log, when);
    RunScanned(*main_session, "MATCH ()-[r:R]->() RETURN r, keys(r), properties(r)", log, when);
    for (const auto &row : main_session->Interpret("DUMP DATABASE").GetResults()) {
      if (HasMarker(row[0].ValueString(), true)) log.Add(std::string{when} + ": MARKER LEAK in main DUMP");
    }
    ASSERT_EQ(log.Count(), 0U) << when << ": " << log.Dump();
  };

  check_all("after the fork");
  for (int i = 0; i < total_ops; ++i) {
    RandomLeakOp(*branch, model, rng);
    if ((i + 1) % kCheckEvery == 0) {
      check_all("after op " + std::to_string(i + 1));
      if (::testing::Test::HasFatalFailure()) return;
    }
    if (i + 1 == total_ops / 2) {
      const auto branch_before = StateSnapshot(*branch);
      ASSERT_TRUE(Restart());
      branch = &engine->NewBranchSession("leak");
      main_session = &engine->NewSession();
      const auto branch_after = StateSnapshot(*branch);
      ASSERT_EQ(branch_before, branch_after)
          << "restart changed the branch: " << FirstDifference(branch_before, branch_after);
      check_all("after the restart");
      if (::testing::Test::HasFatalFailure()) return;
    }
  }
  check_all("at the end");
  std::cerr << "[ STRESS ] marker leak matrix seed=" << Seed() << " ops=" << total_ops
            << " live vertices=" << model.vertices.size() << " live edges=" << model.edges.size() << std::endl;
  ASSERT_GE(model.vertices.size(), 10U) << "the workload deleted nearly everything: the checks became vacuous";
}

TEST_P(VersioningStressTest, HotForkEdgesWithEndpointDeletesSurviveRestarts) {
  SetupHot(engine->NewSession());
  HotCommitted committed;
  FailureLog log;

  constexpr int kPhases = 3;
  for (int phase = 0; phase < kPhases; ++phase) {
    std::vector<InterpreterFaker *> writers, readers;
    for (int i = 0; i < kHotWriters; ++i) writers.push_back(&engine->NewBranchSession("hot"));
    for (int i = 0; i < kHotReaders; ++i) readers.push_back(&engine->NewBranchSession("hot"));
    auto &main_writer = engine->NewSession();

    // Deletes only in phase 0 (afterwards the victims are gone).
    const HotRun run{.phase = phase, .deletes = phase == 0};
    HotStats stats;
    RunHotStress(writers, readers, main_writer, run, committed, log, stats);
    ReportHot("hot fork edges phase " + std::to_string(phase), stats);
    ExpectHotStatsClean(log, stats);

    // Interrupted work: transactions still open (never committed) when the server goes down must not survive it.
    // Disjoint objects, so none of them conflicts with another.
    const auto dirty = [&](int i) { return std::to_string(9'000'000'000 + phase * 10 + i); };
    auto &open0 = engine->NewBranchSession("hot");
    open0.Interpret("BEGIN");
    open0.Interpret("MATCH ()-[r:E {eid:0}]->() SET r.w = " + dirty(0) + ", r.z = " + dirty(0));
    open0.Interpret("MATCH (a:H {id:6}) SET a.v = " + dirty(0));
    auto &open1 = engine->NewBranchSession("hot");
    open1.Interpret("BEGIN");
    open1.Interpret("MATCH ()-[r:E {eid:12}]->() SET r.w = " + dirty(1) + ", r.z = " + dirty(1));
    open1.Interpret("MATCH (a:H {id:7}) SET a.v = " + dirty(1));
    open1.Interpret("MATCH (a:H {id:2}), (b:H {id:3}) CREATE (a)-[:N {nid:" + dirty(1) + "}]->(b)");

    auto &chk = engine->NewBranchSession("hot");
    auto &mchk = engine->NewSession();
    VerifyHotBranch(chk, committed, "phase " + std::to_string(phase));
    VerifyHotMain(mchk);
    if (::testing::Test::HasFatalFailure()) return;
    if (phase + 1 == kPhases) {
      ASSERT_FALSE(committed.natives.empty()) << "no branch-native edge was ever committed: workload is vacuous";
      // Quiescent (interrupted transactions rolled back): one sequential write per surviving fork edge reads back.
      open0.Interpret("ROLLBACK");
      open1.Interpret("ROLLBACK");
      for (int64_t e = 0; e < kHotEdges; ++e) {
        const auto v = std::to_string(8'000'000'000 + e);
        chk.Interpret("MATCH ()-[r:E {eid:" + std::to_string(e) + "}]->() SET r.w = " + v + ", r.z = " + v);
      }
      const auto rows = chk.Interpret("MATCH ()-[r:E]->() RETURN r.eid, r.w, r.z").GetResults();
      ASSERT_FALSE(rows.empty()) << "sequential readback returned no edges";
      for (const auto &r : rows) {
        EXPECT_EQ(r[1].ValueInt(), 8'000'000'000 + r[0].ValueInt()) << "sequential edge write lost";
        EXPECT_EQ(r[2].ValueInt(), 8'000'000'000 + r[0].ValueInt()) << "sequential edge write lost";
      }
      break;
    }

    const auto branch_before = StateSnapshot(chk);
    const auto main_before = StateSnapshot(mchk);
    ASSERT_TRUE(Restart());
    auto &chk_after = engine->NewBranchSession("hot");
    auto &mchk_after = engine->NewSession();
    const auto branch_after = StateSnapshot(chk_after);
    ASSERT_EQ(branch_before, branch_after)
        << "restart changed the branch: " << FirstDifference(branch_before, branch_after);
    ASSERT_EQ(main_before, StateSnapshot(mchk_after)) << "restart changed main";
    for (const auto &line : branch_after) {
      ASSERT_EQ(line.find("9000000000"), std::string::npos)
          << "an uncommitted (interrupted) write survived the restart: " << line;
    }
    VerifyHotBranch(chk_after, committed, "after restart " + std::to_string(phase));
    if (::testing::Test::HasFatalFailure()) return;
  }
}

// Deterministic: a transaction started before an endpoint delete committed must not write the edge the delete hid (as
// on main), or the branch WAL records a write the replay rejects and the server refuses to start.
TEST_P(VersioningStressTest, EdgeWriteAfterACommittedEndpointDeleteConflicts) {
  auto &setup = engine->NewSession();
  setup.Interpret("CREATE INDEX ON :H(id)");
  setup.Interpret("CREATE (:H {id:1})-[:E {eid:1, w:0}]->(:H {id:2}), (:H {id:3})-[:E {eid:3, w:0}]->(:H {id:4})");
  setup.Interpret("CREATE BRANCH 'src'");
  setup.Interpret("CREATE BRANCH 'dst'");
  for (const auto &[branch, eid, deleted] : {std::tuple{"src", 1, 1}, std::tuple{"dst", 3, 4}}) {
    auto &older = engine->NewBranchSession(branch);
    auto &deleter = engine->NewBranchSession(branch);
    older.Interpret("BEGIN");
    older.Interpret("MATCH ()-[r:E {eid:" + std::to_string(eid) + "}]->() RETURN r.w");
    deleter.Interpret("MATCH (n:H {id:" + std::to_string(deleted) + "}) DETACH DELETE n");
    EXPECT_THROW(
        {
          older.Interpret("MATCH ()-[r:E {eid:" + std::to_string(eid) + "}]->() SET r.w = 7");
          older.Interpret("COMMIT");
        },
        memgraph::query::TransactionSerializationException)
        << "branch " << branch;
    SwallowRollback(older);
  }
  ASSERT_TRUE(Restart());
  for (const char *branch : {"src", "dst"}) {
    EXPECT_EQ(Scalar(engine->NewBranchSession(branch), "MATCH ()-[r:E]->() RETURN count(r)"), 1) << branch;
  }
}

INSTANTIATE_TEST_SUITE_P(CommitLock, VersioningStressTest, ::testing::Bool(),
                         [](const auto &info) { return info.param ? "Narrowing" : "FullLock"; });
