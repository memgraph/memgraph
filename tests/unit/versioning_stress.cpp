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

// Multi-threaded stress tests for the shared per-branch diff engines; meant to run under TSan/ASan (checks for races,
// crashes and invariant violations, not throughput). Every thread owns its own Interpreter (one Bolt session) on ONE
// database.
//
// VERSIONING_STRESS_SCALE (double, default 1.0, clamped to [0.05, 100]) multiplies every per-thread op count. Each
// phase also has a wall-clock deadline (12 s * max(1, scale)); a thread hitting it stops early. RNGs are seeded
// deterministically.

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <latch>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <typeinfo>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "communication/bolt/v1/value.hpp"
#include "flags/general.hpp"
#include "interpreter_faker.hpp"
#include "license/license.hpp"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/interpreter_context.hpp"
#include "replication/state.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/gatekeeper.hpp"
#include "utils/logging.hpp"
#include "utils/synchronized.hpp"
#include "versioning/version_store.hpp"

namespace {

using Clock = std::chrono::steady_clock;
using BoltValue = memgraph::communication::bolt::Value;
using BoltRows = std::vector<std::vector<BoltValue>>;

constexpr uint64_t kSeed = 0x5eed5eedULL;
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

Clock::time_point Deadline() {
  return Clock::now() + std::chrono::milliseconds(static_cast<int>(12000 * std::max(1.0, Scale())));
}

// Thread-safe sink for unexpected events; asserted empty from the main thread (gtest assertions off-thread are
// avoided).
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
    os << count_ << " failure(s)";
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

}  // namespace

class VersioningStressTest : public ::testing::Test {
 protected:
  std::filesystem::path data_directory = std::filesystem::temp_directory_path() / "MG_tests_unit_versioning_stress";

  memgraph::storage::Config config{[&] {
    memgraph::storage::Config config{};
    config.durability.storage_directory = data_directory;
    config.durability.snapshot_wal_mode =
        memgraph::storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
    config.disk.main_storage_directory = data_directory / "disk";
    return config;
  }()};

  memgraph::utils::Synchronized<memgraph::replication::ReplicationState, memgraph::utils::RWSpinLock> repl_state{
      memgraph::storage::ReplicationStateRootPath(config)};
  memgraph::utils::Gatekeeper<memgraph::dbms::Database> db_gk{config};
  memgraph::dbms::DatabaseAccess db{[&] {
    auto db_acc_opt = db_gk.access();
    MG_ASSERT(db_acc_opt, "Failed to access db");
    auto &db_acc = *db_acc_opt;
    MG_ASSERT(db_acc->GetStorageMode() == memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL,
              "Wrong storage mode!");
    return db_acc;
  }()};

  memgraph::system::System system_state;
  memgraph::query::AllowEverythingAuthChecker fixture_auth_checker;
  memgraph::query::InterpreterContext interpreter_context{{},
                                                          nullptr,
                                                          nullptr,
                                                          nullptr,
                                                          &repl_state,
                                                          system_state,
                                                          nullptr
#ifdef MG_ENTERPRISE
                                                          ,
                                                          nullptr,
                                                          nullptr
#endif
  };

  // All sessions are created on the main thread (InterpreterFaker's constructor rewrites
  // interpreter_context.auth_checker, which is not synchronized) and only USED from worker threads.
  std::vector<std::unique_ptr<InterpreterFaker>> sessions;

  void SetUp() override {
    FLAGS_versioning_enabled = true;
    memgraph::license::global_license_checker.EnableTesting();
  }

  void TearDown() override {
    interpreter_context.auth_checker = &fixture_auth_checker;
    sessions.clear();
    FLAGS_versioning_enabled = false;
    memgraph::license::global_license_checker.DisableTesting();
    std::filesystem::remove_all(data_directory);
  }

  InterpreterFaker &NewSession() {
    sessions.push_back(std::make_unique<InterpreterFaker>(&interpreter_context, db));
    return *sessions.back();
  }

  static int64_t Scalar(InterpreterFaker &s, const std::string &query) {
    auto stream = s.Interpret(query);
    EXPECT_EQ(stream.GetResults().size(), 1U) << query;
    return stream.GetResults().at(0).at(0).ValueInt();
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

void BranchWriter(InterpreterFaker &s, int tid, int ops, Clock::time_point deadline, CommittedBranchState &committed,
                  FailureLog &log, Counters &c) {
  std::mt19937_64 rng{kSeed + 100 + static_cast<uint64_t>(tid)};
  int64_t next_native = 0;
  const std::string who = "branch-writer-" + std::to_string(tid);
  for (int i = 0; i < ops && Clock::now() < deadline; ++i) {
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

void BranchReader(InterpreterFaker &s, int tid, int iters, Clock::time_point deadline, FailureLog &log, Counters &c) {
  std::mt19937_64 rng{kSeed + 200 + static_cast<uint64_t>(tid)};
  const std::string who = "branch-reader-" + std::to_string(tid);
  BoltRows rows;
  for (int i = 0; i < iters && Clock::now() < deadline; ++i) {
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
void MainWriter(InterpreterFaker &s, int w, int ops, Clock::time_point deadline, MainBookkeeping &book, FailureLog &log,
                Counters &c) {
  std::mt19937_64 rng{kSeed + 300 + static_cast<uint64_t>(w)};
  const std::string who = "main-writer-" + std::to_string(w);
  std::vector<int64_t> live;
  int64_t next_native = 0;
  for (int i = 0; i < ops && Clock::now() < deadline; ++i) {
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

// CREATE BRANCH tmp; CHECKOUT; write; CHECKOUT main; DROP BRANCH tmp. This session is the only one attached to tmp, so
// every step must succeed.
void Lifecycle(InterpreterFaker &s, int iters, Clock::time_point deadline, FailureLog &log, Counters &c) {
  std::mt19937_64 rng{kSeed + 400};
  const std::string who = "lifecycle";
  auto step = [&](const std::string &q) { return Exec(s, q, log, c, who) == Outcome::kOk; };
  for (int i = 0; i < iters && Clock::now() < deadline; ++i) {
    const std::string name = "tmp" + std::to_string(i);
    if (!step("CREATE BRANCH '" + name + "' FROM 'main'")) return;
    if (!step("CHECKOUT BRANCH '" + name + "'")) return;
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
    if (!step("CHECKOUT BRANCH 'main'")) return;
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

TEST_F(VersioningStressTest, WritersReadersMainWritersAndLifecycleChurn) {
  auto &setup = NewSession();
  SeedMain(setup);
  ASSERT_NO_THROW(setup.Interpret("CREATE EDGE INDEX ON :R(w)")) << "edge-property index must be supported";
  ASSERT_EQ(Scalar(setup, "MATCH (n:P) RETURN count(*)"), kSeedVertices);
  ASSERT_EQ(Scalar(setup, "MATCH ()-[r:R]->() RETURN count(r)"), kSeedEdges);
  setup.Interpret("CREATE BRANCH 'b' FROM 'main'");

  constexpr int kBranchWriters = 4;
  constexpr int kBranchReaders = 2;
  constexpr int kMainWriters = 2;
  std::vector<InterpreterFaker *> bw, br, mw;
  for (int i = 0; i < kBranchWriters; ++i) {
    bw.push_back(&NewSession());
    bw.back()->Interpret("CHECKOUT BRANCH 'b'");
  }
  for (int i = 0; i < kBranchReaders; ++i) {
    br.push_back(&NewSession());
    br.back()->Interpret("CHECKOUT BRANCH 'b'");
  }
  for (int i = 0; i < kMainWriters; ++i) mw.push_back(&NewSession());
  auto &life = NewSession();
  auto &prep = NewSession();
  prep.Interpret("CHECKOUT BRANCH 'b'");

  FailureLog log;
  CommittedBranchState committed;
  std::vector<MainBookkeeping> main_books(kMainWriters);
  Counters bw_c, br_c, mw_c, life_c;
  const auto deadline = Deadline();
  const int writer_ops = Scaled(250);
  const int reader_ops = Scaled(300);
  const int main_ops = Scaled(300);
  const int life_iters = Scaled(8);

  // Phase 0 (single-threaded, on branch b): sentinel writes so the delete/label/edge invariants always have something
  // to check.
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

  std::latch start{kBranchWriters + kBranchReaders + kMainWriters + 1};
  std::vector<std::thread> threads;
  for (int i = 0; i < kBranchWriters; ++i) {
    threads.emplace_back([&, i] {
      start.arrive_and_wait();
      BranchWriter(*bw[i], i, writer_ops, deadline, committed, log, bw_c);
    });
  }
  for (int i = 0; i < kBranchReaders; ++i) {
    threads.emplace_back([&, i] {
      start.arrive_and_wait();
      BranchReader(*br[i], i, reader_ops, deadline, log, br_c);
    });
  }
  for (int i = 0; i < kMainWriters; ++i) {
    threads.emplace_back([&, i] {
      start.arrive_and_wait();
      MainWriter(*mw[i], i, main_ops, deadline, main_books[i], log, mw_c);
    });
  }
  threads.emplace_back([&] {
    start.arrive_and_wait();
    Lifecycle(life, life_iters, deadline, log, life_c);
  });
  for (auto &t : threads) t.join();

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

  auto &chk = NewSession();
  chk.Interpret("CHECKOUT BRANCH 'b'");
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
  // Every visible edge must connect two visible vertices that carry `id`.
  EXPECT_EQ(Scalar(chk, "MATCH (a)-[:R]->(b) WHERE a.id IS NULL OR b.id IS NULL RETURN count(*)"), 0)
      << "a branch edge has a dangling/invalid endpoint";

  auto &mchk = NewSession();
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
    EXPECT_FALSE(db->version_store()->Exists("tmp" + std::to_string(i)))
        << "tmp" << i << " survived DROP BRANCH (or churn aborted early)";
  }
}

TEST_F(VersioningStressTest, EightSessionsHammerTheSameFiftyForkVertices) {
  constexpr int kSessions = 8;
  constexpr int64_t kHot = 50;

  auto &setup = NewSession();
  setup.Interpret("CREATE INDEX ON :K");
  setup.Interpret("CREATE INDEX ON :K(id)");
  setup.Interpret("UNWIND range(0," + std::to_string(kHot - 1) + ") AS i CREATE (:K {id:i, v:0, w:0})");
  setup.Interpret("CREATE BRANCH 'hot' FROM 'main'");

  std::vector<InterpreterFaker *> ss;
  for (int i = 0; i < kSessions; ++i) {
    ss.push_back(&NewSession());
    ss.back()->Interpret("CHECKOUT BRANCH 'hot'");
  }

  FailureLog log;
  Counters c;
  std::mutex committed_mutex;
  std::vector<std::set<int64_t>> committed(kHot);
  for (auto &set : committed) set.insert(0);
  std::atomic<uint64_t> commit_seq{0};  // diagnostic only: number of successful commits
  const auto deadline = Deadline();
  const int ops = Scaled(120);

  auto worker = [&](int tid) {
    std::mt19937_64 rng{kSeed + 500 + static_cast<uint64_t>(tid)};
    const std::string who = "hammer-" + std::to_string(tid);
    InterpreterFaker &s = *ss[tid];
    int64_t counter = 0;
    // Values are unique per (session, write): tid * 1e6 + counter.
    auto next_value = [&] { return static_cast<int64_t>(tid + 1) * 1'000'000 + counter++; };
    for (int i = 0; i < ops && Clock::now() < deadline; ++i) {
      std::vector<std::pair<int64_t, int64_t>> pending;  // (vertex id, value)
      const bool explicit_txn = rng() % 2 == 0;
      const int n = explicit_txn ? 1 + static_cast<int>(rng() % 3) : 1;
      if (explicit_txn && Exec(s, "BEGIN", log, c, who) != Outcome::kOk) continue;
      bool failed = false;
      for (int j = 0; j < n; ++j) {
        const auto id = static_cast<int64_t>(rng() % kHot);
        const auto value = next_value();
        BoltRows rows;
        const std::string q = "MATCH (n:K {id:" + std::to_string(id) + "}) SET n.v = " + std::to_string(value) +
                              ", n.w = " + std::to_string(value) + " RETURN n.v, n.w";
        if (Exec(s, q, log, c, who, &rows) != Outcome::kOk) {
          failed = true;
          break;
        }
        if (rows.size() != 1 || rows[0][0].ValueInt() != value || rows[0][1].ValueInt() != value) {
          log.Add(who + ": read-your-writes violated for vertex " + std::to_string(id));
        }
        pending.emplace_back(id, value);
        if (explicit_txn) {
          rows.clear();
          if (Exec(s, "MATCH (n:K {id:" + std::to_string(id) + "}) RETURN n.v, n.w", log, c, who, &rows) ==
                  Outcome::kOk &&
              (rows.size() != 1 || rows[0][0].ValueInt() != value || rows[0][1].ValueInt() != value)) {
            log.Add(who + ": own uncommitted write not visible on re-read for vertex " + std::to_string(id));
          }
        }
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
      commit_seq.fetch_add(1, std::memory_order_relaxed);
      std::lock_guard lock{committed_mutex};
      for (const auto &[id, value] : pending) committed[static_cast<size_t>(id)].insert(value);
    }
  };

  std::latch start{kSessions};
  std::vector<std::thread> threads;
  for (int i = 0; i < kSessions; ++i) {
    threads.emplace_back([&, i] {
      start.arrive_and_wait();
      worker(i);
    });
  }
  for (auto &t : threads) t.join();

  std::cerr << "[ STRESS ] scale=" << Scale() << " hammer: ops=" << c.ops << " conflicts=" << c.conflicts
            << " rollbacks=" << c.rollbacks << " commits=" << commit_seq << std::endl;
  ASSERT_EQ(log.Count(), 0U) << log.Dump();
  ASSERT_GT(commit_seq.load(), 0U) << "no hammer transaction ever committed";
  if (Scale() >= 1.0)
    ASSERT_GT(c.conflicts.load(), 0U) << "8 sessions on 50 vertices never conflicted: workload is vacuous";

  // Exact last-writer checking is not race-free (commit order is unobservable), so assert each final value was
  // committed by some session (never a rolled-back write) and that the two properties written by one statement are
  // equal.
  auto &chk = NewSession();
  chk.Interpret("CHECKOUT BRANCH 'hot'");
  auto stream = chk.Interpret("MATCH (n:K) RETURN n.id, n.v, n.w ORDER BY n.id");
  ASSERT_EQ(stream.GetResults().size(), static_cast<size_t>(kHot)) << "fork vertices lost/duplicated on the branch";
  int64_t expected_id = 0;
  for (const auto &row : stream.GetResults()) {
    const auto id = row[0].ValueInt();
    ASSERT_EQ(id, expected_id++) << "fork vertex ids are not exactly 0..49";
    ASSERT_TRUE(row[1].IsInt() && row[2].IsInt()) << "vertex " << id << " lost v or w";
    EXPECT_EQ(row[1].ValueInt(), row[2].ValueInt()) << "torn write on vertex " << id << " (v != w)";
    EXPECT_TRUE(committed[static_cast<size_t>(id)].contains(row[1].ValueInt()))
        << "vertex " << id << " final v=" << row[1].ValueInt() << " was never committed by any session";
  }

  // Quiescent engine: one sequential write per vertex must be readable back exactly (no lost writes).
  constexpr int64_t kFinalBase = 7'000'000'000;
  for (int64_t id = 0; id < kHot; ++id) {
    const auto v = std::to_string(kFinalBase + id);
    chk.Interpret("MATCH (n:K {id:" + std::to_string(id) + "}) SET n.v = " + v + ", n.w = " + v);
  }
  auto after = chk.Interpret("MATCH (n:K) RETURN n.id, n.v, n.w ORDER BY n.id");
  ASSERT_EQ(after.GetResults().size(), static_cast<size_t>(kHot));
  for (const auto &row : after.GetResults()) {
    const auto id = row[0].ValueInt();
    EXPECT_EQ(row[1].ValueInt(), kFinalBase + id) << "sequential write lost on vertex " << id;
    EXPECT_EQ(row[2].ValueInt(), kFinalBase + id) << "sequential write lost on vertex " << id;
  }

  auto &mchk = NewSession();
  EXPECT_EQ(Scalar(mchk, "MATCH (n:K) WHERE n.v <> 0 OR n.w <> 0 RETURN count(*)"), 0)
      << "hot-branch writes leaked to main";
  EXPECT_EQ(Scalar(mchk, "MATCH (n:K) RETURN count(*)"), kHot);
}
