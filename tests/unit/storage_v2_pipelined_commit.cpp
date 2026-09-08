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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <functional>
#include <latch>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

#include "flags/experimental.hpp"
#include "storage/v2/commit_order_gate.hpp"
#include "storage/v2/commit_probe.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/constraints/constraint_violation.hpp"
#include "storage/v2/durability/durability.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage_error.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"
#include "utils/crc_accumulator.hpp"
#include "utils/exceptions.hpp"

using memgraph::storage::BudgetRefuse;
using memgraph::storage::CommitOrderGate;
using memgraph::storage::CommitProbe;
using memgraph::storage::CommitTicket;
using memgraph::storage::Config;
using memgraph::storage::ConstraintViolation;
using memgraph::storage::Gid;
using memgraph::storage::InMemoryStorage;
using memgraph::storage::LabelId;
using memgraph::storage::PropertyId;
using memgraph::storage::PropertyValue;
using memgraph::storage::StorageManipulationError;
using memgraph::storage::View;

namespace {

template <class Pred>
bool WaitFor(Pred pred, std::chrono::milliseconds timeout) {
  auto const deadline = std::chrono::steady_clock::now() + timeout;
  while (!pred()) {
    if (std::chrono::steady_clock::now() > deadline) return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return true;
}

// Signal-safe watchdog: a hung parked-thread test exits 124 instead of hanging the runner.
extern "C" void WatchdogHandler(int) { _exit(124); }

void ArmWatchdog(unsigned seconds) {
  struct sigaction action{};
  action.sa_handler = WatchdogHandler;
  sigemptyset(&action.sa_mask);
  sigaction(SIGALRM, &action, nullptr);
  alarm(seconds);
}

// Enumerates the WAL files under `dir`, decodes each with the durability decoder, returns the timestamp of every
// DELTA_TRANSACTION_START in file order and verifies every transaction CRC on the way.
std::vector<uint64_t> ReadWalTransactionTimestamps(std::filesystem::path const &dir, bool *crcs_ok = nullptr) {
  using namespace memgraph::storage::durability;
  std::vector<uint64_t> timestamps;
  bool all_ok = true;
  auto wal_files = GetWalFiles(dir / kWalDirectory);
  if (!wal_files) return timestamps;
  std::ranges::sort(*wal_files, [](auto const &a, auto const &b) { return a.seq_num < b.seq_num; });
  for (auto const &wal_file : *wal_files) {
    auto const info = ReadWalInfo(wal_file.path);
    Decoder wal;
    wal.Initialize(wal_file.path, kWalMagic);
    wal.SetPosition(info.offset_deltas);
    wal.ResetCrcAcc();
    for (uint64_t i = 0; i < info.num_deltas; ++i) {
      auto const timestamp = ReadWalDeltaHeader(&wal);
      auto delta_data = ReadWalDeltaData(&wal);
      if (std::get_if<WalTransactionStart>(&delta_data.data_) != nullptr) timestamps.push_back(timestamp);
      if (std::get_if<WalTransactionEnd>(&delta_data.data_) != nullptr) {
        all_ok &= memgraph::utils::CrcAccumulator::Verify(wal.CrcAccValue());
        wal.ResetCrcAcc();
      }
    }
  }
  if (crcs_ok != nullptr) *crcs_ok = all_ok;
  return timestamps;
}

bool VerifyAllTransactionCrcs(std::filesystem::path const &dir) {
  bool ok = false;
  static_cast<void>(ReadWalTransactionTimestamps(dir, &ok));
  return ok;
}

// One-shot park with arrival handshakes and timed, idempotent release. Hooks fire for every commit, so the callback
// parks exactly one caller and records everyone else's arrival.
struct OneShotPark {
  std::mutex m;
  std::condition_variable cv;
  bool armed{true}, parked{false}, released{false};
  std::atomic<int> others{0};

  void operator()() {
    std::unique_lock l{m};
    if (!armed) {
      ++others;
      return;
    }
    armed = false;
    parked = true;
    cv.notify_all();
    cv.wait(l, [&] { return released; });
  }

  bool WaitParked(std::chrono::milliseconds t) {
    std::unique_lock l{m};
    return cv.wait_for(l, t, [&] { return parked; });
  }

  void Release() {
    {
      std::lock_guard l{m};
      released = true;
    }
    cv.notify_all();
  }
};

// Workers never call gtest ASSERT_*; they use REQUIRE_OK, which throws, and the exception is captured per worker.
#define REQUIRE_OK(expr)                                            \
  do {                                                              \
    if (!(expr)) throw std::runtime_error("worker failed: " #expr); \
  } while (0)

// Cleanup-before-wait for every threaded example. Declared right after the probe is installed, BEFORE any worker
// starts, wait, or assertion. The destructor releases the park, joins every worker, and clears the probe.
struct WorkerSet {
  OneShotPark &park;
  InMemoryStorage &storage;
  std::vector<std::thread> threads;
  std::vector<std::exception_ptr> errors;

  WorkerSet(OneShotPark &p, InMemoryStorage &s, size_t max_workers = 16) : park{p}, storage{s} {
    threads.reserve(max_workers);
    errors.resize(max_workers);
  }

  template <class F>
  void Start(F f) {
    auto *slot = &errors[threads.size()];
    threads.emplace_back([slot, f = std::move(f)] {
      try {
        f();
      } catch (...) {
        *slot = std::current_exception();
      }
    });
  }

  void JoinAll() {
    for (auto &t : threads) {
      if (t.joinable()) t.join();
    }
  }

  auto FirstError() const -> std::string {
    for (auto const &e : errors) {
      if (!e) continue;
      try {
        std::rethrow_exception(e);
      } catch (std::exception const &x) {
        return x.what();
      } catch (...) {
        return "non-std exception";
      }
    }
    return {};
  }

  auto ok() const -> bool { return FirstError().empty(); }

  ~WorkerSet() {
    park.Release();
    JoinAll();
    storage.SetCommitProbe(nullptr);
  }
};

Config MakeConfig(std::filesystem::path const &dir) {
  Config config;
  // UpdatePaths rebases every directory under `dir`; it must see the default storage directory as the old base.
  config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  config.durability.snapshot_interval = memgraph::utils::SchedulerInterval{std::chrono::hours(24)};
  config.durability.snapshot_on_exit = false;
  config.durability.wal_file_flush_every_n_tx = 1;
  config.gc.type = Config::Gc::Type::NONE;
  config.register_metrics = false;
  config.experimental_lockfree_read_snapshot = true;
  config.experimental_pipelined_commit = true;
  memgraph::storage::UpdatePaths(config, dir);
  return config;
}

}  // namespace

class PipelinedCommitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("mg_pipelined_" + std::to_string(getpid()) + "_" + std::to_string(counter_++));
    std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);
    config_ = MakeConfig(dir_);
    ArmWatchdog(120);
    Open();
    {
      auto acc = storage_->UniqueAccess();
      ASSERT_TRUE(acc->CreateUniqueConstraint(label_, {id_}).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    baseline_txns_ = 1;  // transactions committed by SetUp, excluded from WAL-order assertions
  }

  void Open() {
    storage_ = std::make_unique<InMemoryStorage>(config_);
    ResolveNames();
  }

  // Names are re-resolved after every reopen because recovery assigns ids from names in replay order.
  void ResolveNames() {
    label_ = storage_->NameToLabel("L");
    prop_ = storage_->NameToProperty("p");
    id_ = storage_->NameToProperty("id");
  }

  void Reopen(bool pipelined) {
    storage_.reset();
    config_.durability.recover_on_startup = true;
    config_.experimental_pipelined_commit = pipelined;
    Open();
  }

  void TearDown() override {
    storage_.reset();
    alarm(0);
    std::filesystem::remove_all(dir_);
  }

  Gid Seed(int id) {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    EXPECT_TRUE(v.AddLabel(label_).has_value());
    EXPECT_TRUE(v.SetProperty(id_, PropertyValue(id)).has_value());
    EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ++baseline_txns_;
    return v.Gid();
  }

  size_t CountVisibleVertices() {
    auto acc = storage_->Access(memgraph::storage::READ);
    size_t n = 0;
    for (auto v : acc->Vertices(View::OLD)) {
      static_cast<void>(v);
      ++n;
    }
    return n;
  }

  std::vector<uint64_t> MeasuredFrames() {
    auto frames = ReadWalTransactionTimestamps(dir_);
    EXPECT_GE(frames.size(), baseline_txns_);
    frames.erase(frames.begin(), frames.begin() + std::min(frames.size(), baseline_txns_));
    return frames;
  }

  static inline int counter_ = 0;
  std::filesystem::path dir_;
  Config config_;
  std::unique_ptr<InMemoryStorage> storage_;
  LabelId label_;
  PropertyId prop_, id_;
  size_t baseline_txns_{0};
};

// ---- Task 4: flags and quiescence --------------------------------------------------------------------------------

TEST(PipelinedCommitFlags, PipelinedCommitRequiresLockfreeReadSnapshot) {
  using memgraph::flags::Experiments;
  using memgraph::flags::ValidateExperimentDependencies;
  EXPECT_TRUE(ValidateExperimentDependencies(memgraph::flags::ReadExperimental("pipelined-commit")).has_value());
  EXPECT_FALSE(
      ValidateExperimentDependencies(memgraph::flags::ReadExperimental("lockfree-read-snapshot,pipelined-commit"))
          .has_value());
  EXPECT_FALSE(ValidateExperimentDependencies(Experiments::NONE).has_value());
  EXPECT_FALSE(ValidateExperimentDependencies(Experiments::LOCKFREE_READ_SNAPSHOT).has_value());
  // CLI `pipelined-commit` with environment `lockfree-read-snapshot`: validation runs on the combined mask.
  auto const combined =
      static_cast<Experiments>(std::to_underlying(memgraph::flags::ReadExperimental("pipelined-commit")) |
                               std::to_underlying(memgraph::flags::ReadExperimental("lockfree-read-snapshot")));
  EXPECT_FALSE(ValidateExperimentDependencies(combined).has_value());
}

TEST(PipelinedCommitFlags, StorageRejectsPipelinedCommitWithoutLockfreeReadSnapshot) {
  auto const dir = std::filesystem::temp_directory_path() / ("mg_pipelined_flags_" + std::to_string(getpid()));
  std::filesystem::remove_all(dir);
  auto config = MakeConfig(dir);
  config.experimental_lockfree_read_snapshot = false;
  EXPECT_THROW(InMemoryStorage{config}, memgraph::utils::BasicException);
  std::filesystem::remove_all(dir);
}

TEST_F(PipelinedCommitTest, SyntheticTicketQuiescesEpochChange) {
  auto &gate = storage_->commit_order_gate_for_tests();
  std::optional<CommitTicket> ticket;
  ticket.emplace(gate, 1'000'000);
  std::atomic<bool> epoch_done{false};
  std::thread epoch{[&] {
    // The InMemoryStorage override is private; the public entry point is Storage::PrepareForNewEpoch.
    static_cast<memgraph::storage::Storage &>(*storage_).PrepareForNewEpoch();
    epoch_done = true;
  }};
  EXPECT_FALSE(WaitFor([&] { return epoch_done.load(); }, std::chrono::milliseconds(200)));
  ticket->Enter();
  ticket->MarkAborted();
  ticket->Retire();
  EXPECT_TRUE(WaitFor([&] { return epoch_done.load(); }, std::chrono::seconds(5)));
  epoch.join();
  EXPECT_EQ(gate.Pending(), 0);
}

TEST_F(PipelinedCommitTest, EpochChangeWaitsForInFlightTicket) {
  auto const a = Seed(1);
  OneShotPark park;
  CommitProbe probe;
  probe.before_append = std::ref(park);
  storage_->SetCommitProbe(&probe);
  std::latch epoch_started{1};
  std::atomic<bool> epoch_done{false};  // captured state BEFORE the WorkerSet
  {
    WorkerSet w{park, *storage_};
    w.Start([&] {
      auto acc = storage_->Access(memgraph::storage::WRITE);
      REQUIRE_OK(acc->FindVertex(a, View::NEW)->SetProperty(prop_, PropertyValue(1)).has_value());
      REQUIRE_OK(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    });
    ASSERT_TRUE(park.WaitParked(std::chrono::seconds(5)));
    w.Start([&] {
      epoch_started.count_down();
      static_cast<memgraph::storage::Storage &>(*storage_).PrepareForNewEpoch();
      epoch_done = true;
    });
    epoch_started.wait();
    EXPECT_FALSE(WaitFor([&] { return epoch_done.load(); }, std::chrono::milliseconds(200)));  // must NOT complete
    park.Release();
    w.JoinAll();
    ASSERT_TRUE(w.ok()) << w.FirstError();
    EXPECT_TRUE(epoch_done.load());
  }  // WorkerSet scope ends before Reopen
  EXPECT_TRUE(VerifyAllTransactionCrcs(dir_));
  Reopen(false);
  EXPECT_EQ(
      storage_->Access(memgraph::storage::READ)->FindVertex(a, View::OLD)->GetProperty(prop_, View::OLD)->ValueInt(),
      1);
}

// ---- Task 5: ticketed legacy path ----------------------------------------------------------------------------------

// Serializer exclusion: a ticketed LEGACY writer parked at after_ticket still holds commit_mutex_, so a second
// committer cannot mint until it finishes. The first worker is metadata-only (DropIndex through Access(READ)) so it
// stays ineligible after Task 6; the successor is a data writer.
TEST_F(PipelinedCommitTest, LegacyWriterHoldsSerializerAcrossItsTicket) {
  {
    auto acc = storage_->UniqueAccess();
    ASSERT_TRUE(acc->CreateIndex(label_).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ++baseline_txns_;
  }
  auto const a = Seed(1);
  auto const finalizations_before = storage_->pipeline_test_counters().finalize_wal_calls.load();
  OneShotPark park;
  CommitProbe probe;
  probe.after_ticket = std::ref(park);
  std::atomic<int> mints{0};
  probe.after_mint = [&] { ++mints; };
  storage_->SetCommitProbe(&probe);
  bool first_ok = false, data_ok = false;
  std::atomic<bool> data_done{false};
  WorkerSet w{park, *storage_};
  w.Start([&] {
    auto acc = storage_->Access(memgraph::storage::READ);
    first_ok = acc->DropIndex(label_).has_value() &&
               acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
  });
  ASSERT_TRUE(park.WaitParked(std::chrono::seconds(5)));  // first: minted, ticketed, entered, serializer held
  w.Start([&] {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    data_ok = acc->FindVertex(a, View::NEW)->SetProperty(prop_, PropertyValue(1)).has_value() &&
              acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
    data_done = true;
  });
  EXPECT_FALSE(WaitFor([&] { return mints.load() >= 2; }, std::chrono::milliseconds(200)));  // serializer held
  EXPECT_FALSE(data_done.load());
  auto const watermark_before = storage_->LastCommittedMvccTimestamp();
  park.Release();
  w.JoinAll();
  ASSERT_TRUE(w.ok()) << w.FirstError();
  EXPECT_TRUE(first_ok);
  EXPECT_TRUE(data_ok);
  EXPECT_GT(storage_->LastCommittedMvccTimestamp(), watermark_before);
  // One WAL finalization per legacy transaction (the data writer is pipelined and finalizes once as well).
  EXPECT_EQ(storage_->pipeline_test_counters().finalize_wal_calls.load() - finalizations_before, 2);
  auto const frames = MeasuredFrames();
  ASSERT_EQ(frames.size(), 2);
  EXPECT_LT(frames[0], frames[1]);
}

// Failure protocol: an exception injected before append aborts in order and retires the ticket; the successor commits.
TEST_F(PipelinedCommitTest, ExceptionBeforeAppendAbortsInOrderAndReleasesSuccessor) {
  auto const a = Seed(1);
  auto const b = Seed(2);
  CommitProbe probe;
  std::atomic<bool> armed{true};
  probe.before_validate = [&] {
    if (armed.exchange(false)) throw std::runtime_error("injected");
  };
  storage_->SetCommitProbe(&probe);
  std::expected<void, StorageManipulationError> r_a;
  bool threw = false;
  OneShotPark unused_park;
  {
    WorkerSet w{unused_park, *storage_};
    w.Start([&] {
      auto acc = storage_->Access(memgraph::storage::WRITE);
      REQUIRE_OK(acc->FindVertex(a, View::NEW)->SetProperty(prop_, PropertyValue(1)).has_value());
      try {
        r_a = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
      } catch (std::runtime_error const &) {
        threw = true;
      }
    });
    w.JoinAll();
    ASSERT_TRUE(w.ok()) << w.FirstError();
  }
  EXPECT_TRUE(threw);
  {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    ASSERT_TRUE(acc->FindVertex(b, View::NEW)->SetProperty(prop_, PropertyValue(2)).has_value());
    EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());  // not blocked
  }
  storage_->SetCommitProbe(nullptr);
  EXPECT_EQ(storage_->commit_order_gate_for_tests().Pending(), 0);
  auto r = storage_->Access(memgraph::storage::READ);
  EXPECT_EQ(r->FindVertex(a, View::OLD)->GetProperty(prop_, View::OLD)->type(), PropertyValue::Type::Null);
  EXPECT_EQ(r->FindVertex(b, View::OLD)->GetProperty(prop_, View::OLD)->ValueInt(), 2);
}

// ---- Task 6: the pipelined branch ---------------------------------------------------------------------------------

// INV-UNIQUE: both committers create the same id; the first is parked between ticket and validation; exactly one wins.
TEST_F(PipelinedCommitTest, ConcurrentDuplicateCreatesAdmitExactlyOne) {
  OneShotPark park;
  CommitProbe probe;
  probe.after_ticket = std::ref(park);
  storage_->SetCommitProbe(&probe);
  std::expected<void, StorageManipulationError> first_result, second_result;
  WorkerSet w{park, *storage_};
  w.Start([&] {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    REQUIRE_OK(v.AddLabel(label_).has_value());
    REQUIRE_OK(v.SetProperty(id_, PropertyValue(7)).has_value());
    first_result = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  });
  ASSERT_TRUE(park.WaitParked(std::chrono::seconds(5)));
  w.Start([&] {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    REQUIRE_OK(v.AddLabel(label_).has_value());
    REQUIRE_OK(v.SetProperty(id_, PropertyValue(7)).has_value());
    second_result = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
  });
  ASSERT_TRUE(WaitFor([&] { return park.others.load() >= 1; }, std::chrono::seconds(5)));  // second minted
  park.Release();
  w.JoinAll();
  ASSERT_TRUE(w.ok()) << w.FirstError();
  EXPECT_TRUE(first_result.has_value());
  ASSERT_FALSE(second_result.has_value());
  ASSERT_TRUE(std::holds_alternative<ConstraintViolation>(second_result.error()));
  EXPECT_EQ(CountVisibleVertices(), 1);  // ApproximateVertexCount would count the aborted vertex until GC
  EXPECT_GE(storage_->pipeline_stats_for_tests().s2_encodes.load(), 2);
}

// Duplicate UPDATES: two writers set different vertices to the same unique value; exactly one may succeed.
TEST_F(PipelinedCommitTest, ConcurrentDuplicateUpdatesAdmitExactlyOne) {
  auto const a = Seed(1);
  auto const b = Seed(2);
  OneShotPark park;
  CommitProbe probe;
  probe.after_ticket = std::ref(park);
  storage_->SetCommitProbe(&probe);
  std::expected<void, StorageManipulationError> ra, rb;  // captured state BEFORE the WorkerSet
  {
    WorkerSet w{park, *storage_};
    w.Start([&] {
      auto acc = storage_->Access(memgraph::storage::WRITE);
      REQUIRE_OK(acc->FindVertex(a, View::NEW)->SetProperty(id_, PropertyValue(9)).has_value());
      ra = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    });
    ASSERT_TRUE(park.WaitParked(std::chrono::seconds(5)));
    w.Start([&] {
      auto acc = storage_->Access(memgraph::storage::WRITE);
      REQUIRE_OK(acc->FindVertex(b, View::NEW)->SetProperty(id_, PropertyValue(9)).has_value());
      rb = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    });
    ASSERT_TRUE(WaitFor([&] { return park.others.load() >= 1; }, std::chrono::seconds(5)));
    park.Release();
    w.JoinAll();
    ASSERT_TRUE(w.ok()) << w.FirstError();
  }  // WorkerSet scope ends before Reopen
  EXPECT_TRUE(ra.has_value());
  ASSERT_FALSE(rb.has_value());
  ASSERT_TRUE(std::holds_alternative<ConstraintViolation>(rb.error()));
  Reopen(false);
  auto acc = storage_->Access(memgraph::storage::READ);
  EXPECT_EQ(acc->FindVertex(a, View::OLD)->GetProperty(id_, View::OLD)->ValueInt(), 9);
  EXPECT_EQ(acc->FindVertex(b, View::OLD)->GetProperty(id_, View::OLD)->ValueInt(), 2);
}

// Selective refusal protocol, shared by the mixed tests: park the predecessor at before_validate (after its S2
// completed). The successor's after_mint hook (armed after the predecessor parked, so the second mint is the
// successor) reads minted_ticket and arms budget_refuse for that ticket at the site under test BEFORE returning, so
// refusal is armed before the successor's S2. The budget itself is large so every other allocation succeeds.
class PipelinedCommitRefusalTest : public PipelinedCommitTest,
                                   public ::testing::WithParamInterface<BudgetRefuse::Site> {
 protected:
  struct Outcome {
    std::expected<void, StorageManipulationError> first, second;
    std::atomic<uint64_t> retained_at_park{0};
    std::atomic<uint64_t> retained_before_fallback{0};
    std::atomic<bool> fallback_seen{false};
  };

  // Runs `first` then `second` as concurrent committers under the protocol; both operations receive an accessor.
  void RunProtocol(Outcome &outcome, std::function<void(memgraph::storage::Storage::Accessor &)> first,
                   std::function<void(memgraph::storage::Storage::Accessor &)> second) {
    auto const in_flight_before = storage_->pipeline_budget_for_tests().InFlightBytes();
    auto const fallbacks_before = storage_->pipeline_stats_for_tests().budget_fallbacks.load();
    OneShotPark park;
    CommitProbe probe;
    std::atomic<bool> arm_successor{false};
    probe.before_validate = std::ref(park);
    probe.after_mint = [&] {
      if (!arm_successor.load()) return;
      probe.budget_refuse.ticket = probe.minted_ticket.load();
      probe.budget_refuse.site = GetParam();
    };
    probe.before_legacy_fallback = [&] {
      outcome.retained_before_fallback = storage_->pipeline_budget_for_tests().InFlightBytes();
      outcome.fallback_seen = true;
    };
    storage_->SetCommitProbe(&probe);
    WorkerSet w{park, *storage_};
    w.Start([&] {
      auto acc = storage_->Access(memgraph::storage::WRITE);
      first(*acc);
      outcome.first = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    });
    ASSERT_TRUE(park.WaitParked(std::chrono::seconds(5)));
    // The successor has not started: what is in flight now is exactly what the parked predecessor retains.
    outcome.retained_at_park = storage_->pipeline_budget_for_tests().InFlightBytes();
    EXPECT_GT(outcome.retained_at_park.load(), 0);
    arm_successor = true;
    w.Start([&] {
      auto acc = storage_->Access(memgraph::storage::WRITE);
      second(*acc);
      outcome.second = acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs());
    });
    ASSERT_TRUE(WaitFor([&] { return outcome.fallback_seen.load(); }, std::chrono::seconds(5)));
    // Release-BEFORE-fallback: the successor's charges were released while the predecessor is still parked.
    EXPECT_EQ(outcome.retained_before_fallback.load(), outcome.retained_at_park.load());
    park.Release();
    w.JoinAll();
    ASSERT_TRUE(w.ok()) << w.FirstError();
    EXPECT_TRUE(probe.budget_refuse.fired.load());
    EXPECT_EQ(storage_->pipeline_stats_for_tests().budget_fallbacks.load() - fallbacks_before, 1);
    EXPECT_EQ(storage_->pipeline_budget_for_tests().InFlightBytes(), in_flight_before);  // eventual release
  }
};

INSTANTIATE_TEST_SUITE_P(Sites, PipelinedCommitRefusalTest,
                         ::testing::Values(BudgetRefuse::kMaterializer, BudgetRefuse::kEncoder));

TEST_P(PipelinedCommitRefusalTest, DuplicateCreateAgainstOverBudgetFallbackAdmitsExactlyOne) {
  Outcome outcome;
  auto const create = [&](memgraph::storage::Storage::Accessor &acc) {
    auto v = acc.CreateVertex();
    REQUIRE_OK(v.AddLabel(label_).has_value());
    REQUIRE_OK(v.SetProperty(id_, PropertyValue(7)).has_value());
  };
  RunProtocol(outcome, create, create);
  EXPECT_TRUE(outcome.first.has_value());
  ASSERT_FALSE(outcome.second.has_value());
  ASSERT_TRUE(std::holds_alternative<ConstraintViolation>(outcome.second.error()));
  EXPECT_EQ(CountVisibleVertices(), 1);
  Reopen(false);
  EXPECT_EQ(CountVisibleVertices(), 1);
}

TEST_P(PipelinedCommitRefusalTest, ConcurrentDuplicateUpdateAgainstOverBudgetFallbackAdmitsExactlyOne) {
  auto const a = Seed(1);
  auto const b = Seed(2);
  Outcome outcome;
  RunProtocol(
      outcome,
      [&](memgraph::storage::Storage::Accessor &acc) {
        REQUIRE_OK(acc.FindVertex(a, View::NEW)->SetProperty(id_, PropertyValue(9)).has_value());
      },
      [&](memgraph::storage::Storage::Accessor &acc) {
        REQUIRE_OK(acc.FindVertex(b, View::NEW)->SetProperty(id_, PropertyValue(9)).has_value());
      });
  EXPECT_TRUE(outcome.first.has_value());
  ASSERT_FALSE(outcome.second.has_value());
  ASSERT_TRUE(std::holds_alternative<ConstraintViolation>(outcome.second.error()));
  Reopen(false);
  auto acc = storage_->Access(memgraph::storage::READ);
  EXPECT_EQ(acc->FindVertex(a, View::OLD)->GetProperty(id_, View::OLD)->ValueInt(), 9);
  EXPECT_EQ(acc->FindVertex(b, View::OLD)->GetProperty(id_, View::OLD)->ValueInt(), 2);
}

// INV-ONE-DOMAIN across paths: pipelined predecessor parked after the serializer release, legacy successor (DDL) must
// wait in the gate.
TEST_F(PipelinedCommitTest, LegacyCommitDoesNotOvertakePipelinedTicket) {
  {
    auto acc = storage_->UniqueAccess();
    ASSERT_TRUE(acc->CreateIndex(label_).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    ++baseline_txns_;
  }
  auto const a = Seed(1);
  OneShotPark park;
  CommitProbe probe;
  probe.after_ticket = std::ref(park);
  std::atomic<int> mints{0};
  probe.after_mint = [&] { ++mints; };
  storage_->SetCommitProbe(&probe);  // both hooks installed BEFORE either worker starts
  bool first_ok = false, ddl_ok = false;
  std::atomic<bool> ddl_done{false};
  WorkerSet w{park, *storage_};
  w.Start([&] {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    first_ok = acc->FindVertex(a, View::NEW)->SetProperty(prop_, PropertyValue(1)).has_value() &&
               acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
  });
  ASSERT_TRUE(park.WaitParked(std::chrono::seconds(5)));  // eligible writer: minted, ticketed, serializer released
  w.Start([&] {
    auto acc = storage_->Access(memgraph::storage::READ);
    ddl_ok = acc->DropIndex(label_).has_value() &&
             acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value();
    ddl_done = true;
  });
  ASSERT_TRUE(WaitFor([&] { return mints.load() >= 2; }, std::chrono::seconds(5)));        // the DDL minted
  EXPECT_FALSE(WaitFor([&] { return ddl_done.load(); }, std::chrono::milliseconds(200)));  // waiting in the gate
  auto const watermark_before = storage_->LastCommittedMvccTimestamp();
  park.Release();
  w.JoinAll();
  ASSERT_TRUE(w.ok()) << w.FirstError();
  EXPECT_TRUE(first_ok);
  EXPECT_TRUE(ddl_ok);
  EXPECT_GT(storage_->LastCommittedMvccTimestamp(), watermark_before);
  auto const frames = MeasuredFrames();
  ASSERT_EQ(frames.size(), 2);
  EXPECT_LT(frames[0], frames[1]);
}

// INV-ORDER: a later-minted committer with a tiny encode must not append before an earlier one with a large encode.
TEST_F(PipelinedCommitTest, WalFramesStayInCommitTimestampOrder) {
  auto const a = Seed(1);
  auto const b = Seed(2);
  OneShotPark park;
  CommitProbe probe;
  probe.after_ticket = std::ref(park);
  storage_->SetCommitProbe(&probe);
  WorkerSet w{park, *storage_};
  w.Start([&] {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    auto v = acc->FindVertex(a, View::NEW);
    for (int i = 0; i < 2000; ++i) REQUIRE_OK(v->SetProperty(prop_, PropertyValue(i)).has_value());
    REQUIRE_OK(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  });
  ASSERT_TRUE(park.WaitParked(std::chrono::seconds(5)));
  w.Start([&] {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    REQUIRE_OK(acc->FindVertex(b, View::NEW)->SetProperty(prop_, PropertyValue(1)).has_value());
    REQUIRE_OK(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  });
  ASSERT_TRUE(WaitFor([&] { return park.others.load() >= 1; }, std::chrono::seconds(5)));
  park.Release();
  w.JoinAll();
  ASSERT_TRUE(w.ok()) << w.FirstError();
  auto const frames = MeasuredFrames();
  ASSERT_EQ(frames.size(), 2);
  EXPECT_LT(frames[0], frames[1]);
}

TEST_F(PipelinedCommitTest, RotationBetweenTicketAndAppendIsSafe) {
  config_.durability.wal_file_size_kibibytes = 1;
  Reopen(true);
  std::vector<Gid> gids;
  for (int i = 0; i < 400; ++i) gids.push_back(Seed(100 + i));
  OneShotPark unused_park;  // no park in this test; WorkerSet still owns cleanup
  {
    WorkerSet w{unused_park, *storage_};
    for (int k = 0; k < 8; ++k) {
      w.Start([&, k] {
        for (int r = 0; r < 10; ++r) {
          auto acc = storage_->Access(memgraph::storage::WRITE);
          for (int i = k; i < 400; i += 8) {
            REQUIRE_OK(acc->FindVertex(gids[i], View::NEW)->SetProperty(prop_, PropertyValue(r)).has_value());
          }
          REQUIRE_OK(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
        }
      });
    }
    w.JoinAll();
    ASSERT_TRUE(w.ok()) << w.FirstError();
  }
  EXPECT_GE(storage_->pipeline_stats_for_tests().s2_encodes.load(), 80);
  Reopen(false);  // recovers on a flag-off instance
  auto acc = storage_->Access(memgraph::storage::READ);
  for (auto gid : gids) EXPECT_EQ(acc->FindVertex(gid, View::OLD)->GetProperty(prop_, View::OLD)->ValueInt(), 9);
}

// 9d: with a 1-byte budget every measured data commit takes the legacy fallback; one fallback and one finalization
// per measured transaction, rotation really happens, and recovery succeeds.
TEST_F(PipelinedCommitTest, LegacyForcedRotationFinalizesOncePerTransaction) {
  config_.durability.wal_file_size_kibibytes = 1;
  config_.pipelined_commit_max_bytes = 1;
  Reopen(true);
  std::vector<Gid> gids;
  for (int i = 0; i < 64; ++i) gids.push_back(Seed(200 + i));
  auto const fallbacks_before = storage_->pipeline_stats_for_tests().budget_fallbacks.load();
  auto const finalizations_before = storage_->pipeline_test_counters().finalize_wal_calls.load();
  constexpr int kMeasured = 32;
  for (int r = 0; r < kMeasured; ++r) {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    for (auto gid : gids)
      ASSERT_TRUE(acc->FindVertex(gid, View::NEW)->SetProperty(prop_, PropertyValue(r)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(storage_->pipeline_stats_for_tests().budget_fallbacks.load() - fallbacks_before, kMeasured);
  EXPECT_EQ(storage_->pipeline_test_counters().finalize_wal_calls.load() - finalizations_before, kMeasured);
  auto wal_files = memgraph::storage::durability::GetWalFiles(dir_ / memgraph::storage::durability::kWalDirectory);
  ASSERT_TRUE(wal_files.has_value());
  EXPECT_GT(wal_files->size(), 1);
  Reopen(false);
  auto acc = storage_->Access(memgraph::storage::READ);
  for (auto gid : gids) {
    EXPECT_EQ(acc->FindVertex(gid, View::OLD)->GetProperty(prop_, View::OLD)->ValueInt(), kMeasured - 1);
  }
}

TEST_F(PipelinedCommitTest, WalDisabledMainIsNotEligible) {
  storage_.reset();
  config_.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
  config_.durability.recover_on_startup = true;
  Open();
  auto const encodes_before = storage_->pipeline_stats_for_tests().s2_encodes.load();
  for (int i = 0; i < 5; ++i) {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    ASSERT_TRUE(v.AddLabel(label_).has_value());
    ASSERT_TRUE(v.SetProperty(id_, PropertyValue(300 + i)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  EXPECT_EQ(storage_->pipeline_stats_for_tests().s2_encodes.load(), encodes_before);
  EXPECT_EQ(storage_->commit_order_gate_for_tests().Pending(), 0);
  EXPECT_EQ(CountVisibleVertices(), 5);
}

// WAL-disabled companions: a before_publish exception aborts and releases its successor.
TEST_F(PipelinedCommitTest, WalDisabledExceptionBeforePublishAbortsInOrder) {
  storage_.reset();
  config_.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
  config_.durability.recover_on_startup = true;
  Open();
  auto const a = Seed(1);
  auto const b = Seed(2);
  CommitProbe probe;
  std::atomic<bool> armed{true};
  probe.before_publish = [&] {
    if (armed.exchange(false)) throw std::runtime_error("injected");
  };
  storage_->SetCommitProbe(&probe);
  bool threw = false;
  {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    ASSERT_TRUE(acc->FindVertex(a, View::NEW)->SetProperty(prop_, PropertyValue(1)).has_value());
    try {
      static_cast<void>(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    } catch (std::runtime_error const &) {
      threw = true;
    }
  }
  EXPECT_TRUE(threw);
  {
    auto acc = storage_->Access(memgraph::storage::WRITE);
    ASSERT_TRUE(acc->FindVertex(b, View::NEW)->SetProperty(prop_, PropertyValue(2)).has_value());
    EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  storage_->SetCommitProbe(nullptr);
  EXPECT_EQ(storage_->commit_order_gate_for_tests().Pending(), 0);
  auto r = storage_->Access(memgraph::storage::READ);
  EXPECT_EQ(r->FindVertex(a, View::OLD)->GetProperty(prop_, View::OLD)->type(), PropertyValue::Type::Null);
  EXPECT_EQ(r->FindVertex(b, View::OLD)->GetProperty(prop_, View::OLD)->ValueInt(), 2);
}

// ---- Task 6 lifecycle faults (nonfatal) --------------------------------------------------------------------------

class PipelinedCommitLifecycleTest : public PipelinedCommitTest {
 protected:
  // Injects one exception through `arm`, expects the faulting commit to abort in order, then a successor commits and
  // the gate is empty.
  void ExpectOrderedAbortThenSuccessor(std::function<void(CommitProbe &)> arm) {
    auto const a = Seed(1);
    auto const b = Seed(2);
    CommitProbe probe;
    arm(probe);
    storage_->SetCommitProbe(&probe);
    auto const watermark_before = storage_->LastCommittedMvccTimestamp();
    bool threw = false;
    {
      auto acc = storage_->Access(memgraph::storage::WRITE);
      ASSERT_TRUE(acc->FindVertex(a, View::NEW)->SetProperty(prop_, PropertyValue(1)).has_value());
      try {
        static_cast<void>(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
      } catch (std::runtime_error const &) {
        threw = true;
      }
    }
    EXPECT_TRUE(threw);
    EXPECT_EQ(storage_->LastCommittedMvccTimestamp(), watermark_before);
    EXPECT_EQ(storage_->commit_order_gate_for_tests().Pending(), 0);
    {
      auto acc = storage_->Access(memgraph::storage::WRITE);
      ASSERT_TRUE(acc->FindVertex(b, View::NEW)->SetProperty(prop_, PropertyValue(2)).has_value());
      EXPECT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }
    storage_->SetCommitProbe(nullptr);
    EXPECT_EQ(storage_->commit_order_gate_for_tests().Pending(), 0);
    EXPECT_TRUE(VerifyAllTransactionCrcs(dir_));
    Reopen(false);
    auto r = storage_->Access(memgraph::storage::READ);
    EXPECT_EQ(r->FindVertex(a, View::OLD)->GetProperty(prop_, View::OLD)->type(), PropertyValue::Type::Null);
    EXPECT_EQ(r->FindVertex(b, View::OLD)->GetProperty(prop_, View::OLD)->ValueInt(), 2);
  }
};

TEST_F(PipelinedCommitLifecycleTest, AfterMintThrowsAbortsInOrder) {
  ExpectOrderedAbortThenSuccessor([](CommitProbe &probe) {
    auto armed = std::make_shared<std::atomic<bool>>(true);
    probe.after_mint = [armed] {
      if (armed->exchange(false)) throw std::runtime_error("injected after_mint");
    };
  });
}

TEST_F(PipelinedCommitLifecycleTest, EligibleAfterTicketThrowsAbortsInOrder) {
  ExpectOrderedAbortThenSuccessor([](CommitProbe &probe) {
    auto armed = std::make_shared<std::atomic<bool>>(true);
    probe.after_ticket = [armed] {
      if (armed->exchange(false)) throw std::runtime_error("injected after_ticket");
    };
  });
}

TEST_F(PipelinedCommitLifecycleTest, MaterializationFaultAbortsInOrder) {
  ExpectOrderedAbortThenSuccessor([](CommitProbe &probe) {
    probe.budget_refuse.mode = BudgetRefuse::kThrowRuntimeError;
    probe.budget_refuse.site = BudgetRefuse::kMaterializer;
    // The first ticketed commit after installation is the faulting one.
    probe.after_mint = [&probe] {
      if (probe.budget_refuse.ticket.load() == 0) probe.budget_refuse.ticket = probe.minted_ticket.load();
    };
  });
}

// 10: first eligible commit on an empty WAL, then rotation through the real InitializeWalFile call.
class PipelinedCommitEmptyWalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() / ("mg_pipelined_empty_" + std::to_string(getpid()));
    std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);
    ArmWatchdog(120);
  }

  void TearDown() override {
    alarm(0);
    std::filesystem::remove_all(dir_);
  }

  std::filesystem::path dir_;
};

TEST_F(PipelinedCommitEmptyWalTest, FirstEligibleCommitInitializesTheWalAndRotationReinitializes) {
  auto config = MakeConfig(dir_);
  Gid gid;
  {
    // NO preliminary commit: the first write is pipeline-eligible and initializes the WAL itself.
    InMemoryStorage storage{config};
    auto const prop = storage.NameToProperty("p");
    auto acc = storage.Access(memgraph::storage::WRITE);
    auto v = acc->CreateVertex();
    gid = v.Gid();
    ASSERT_TRUE(v.SetProperty(prop, PropertyValue(1)).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    EXPECT_EQ(storage.pipeline_stats_for_tests().s2_encodes.load(), 1);
  }
  {
    // The storage copies its config at construction, so the rotation threshold is set in the config used to open.
    config.durability.recover_on_startup = true;
    config.durability.wal_file_size_kibibytes = 1;
    InMemoryStorage storage{config};
    auto const prop = storage.NameToProperty("p");
    {
      auto acc = storage.Access(memgraph::storage::WRITE);
      auto v = acc->FindVertex(gid, View::NEW);
      ASSERT_TRUE(v.has_value());
      for (int i = 0; i < 300; ++i)
        ASSERT_TRUE(v->SetProperty(storage.NameToProperty("q" + std::to_string(i)), PropertyValue(i)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());  // forces rotation
    }
    {
      auto acc = storage.Access(memgraph::storage::WRITE);
      ASSERT_TRUE(acc->FindVertex(gid, View::NEW)->SetProperty(prop, PropertyValue(2)).has_value());
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());  // new WAL file
    }
    EXPECT_EQ(storage.pipeline_stats_for_tests().s2_encodes.load(), 2);
    auto wal_files = memgraph::storage::durability::GetWalFiles(dir_ / memgraph::storage::durability::kWalDirectory);
    ASSERT_TRUE(wal_files.has_value());
    EXPECT_GE(wal_files->size(), 2);
  }
  {
    config.durability.wal_file_size_kibibytes = 20 * 1024;
    config.experimental_pipelined_commit = false;
    InMemoryStorage storage{config};
    auto const prop = storage.NameToProperty("p");
    auto acc = storage.Access(memgraph::storage::READ);
    EXPECT_EQ(acc->FindVertex(gid, View::OLD)->GetProperty(prop, View::OLD)->ValueInt(), 2);
  }
}

// ---- Task 6 lifecycle faults (fatal) --------------------------------------------------------------------------------

// Opens NO storage in the parent SetUp and installs no alarm: the death statement arms the watchdog and constructs the
// storage in a directory taken from MG_DEATH_DIR (inherited across gtest's re-exec), so the parent recovers exactly
// that directory afterwards. A child hang exits 124 and fails the KilledBySignal(SIGABRT) matcher.
class PipelinedCommitDeathTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (auto const *inherited = std::getenv("MG_DEATH_DIR"); inherited != nullptr && *inherited != '\0') {
      dir_ = inherited;
    } else {
      dir_ = std::filesystem::temp_directory_path() /
             ("mg_pipelined_death_" + std::to_string(getpid()) + "_" + std::to_string(counter_++));
      std::filesystem::remove_all(dir_);
      std::filesystem::create_directories(dir_);
      setenv("MG_DEATH_DIR", dir_.c_str(), 1);
    }
  }

  void TearDown() override {
    unsetenv("MG_DEATH_DIR");
    std::filesystem::remove_all(dir_);
  }

  // The child: warm-up commits, then the faulting one; never returns normally when the fault fires.
  void RunChild(std::function<void(CommitProbe &)> arm, bool wal_disabled = false) {
    ArmWatchdog(60);
    auto config = MakeConfig(dir_);
    if (wal_disabled) config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
    InMemoryStorage storage{config};
    auto const prop = storage.NameToProperty("p");
    std::vector<Gid> gids;
    for (int i = 0; i < kWarmUp; ++i) {
      auto acc = storage.Access(memgraph::storage::WRITE);
      auto v = acc->CreateVertex();
      gids.push_back(v.Gid());
      if (!v.SetProperty(prop, PropertyValue(i)).has_value()) _exit(3);
      if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) _exit(3);
    }
    CommitProbe probe;
    arm(probe);
    storage.SetCommitProbe(&probe);
    auto acc = storage.Access(memgraph::storage::WRITE);
    if (!acc->FindVertex(gids.front(), View::NEW)->SetProperty(prop, PropertyValue(1000)).has_value()) _exit(3);
    static_cast<void>(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));
    _exit(5);  // the fault did not fire
  }

  // The parent: recovers the child's directory and reports how many warm-up vertices came back and the value of
  // the faulting transaction's target.
  struct Recovered {
    size_t vertices{0};
    std::optional<int64_t> faulted_value;
    bool crcs_ok{false};
  };

  Recovered Recover(bool wal_disabled = false) {
    Recovered recovered;
    recovered.crcs_ok = wal_disabled || VerifyAllTransactionCrcs(dir_);
    auto config = MakeConfig(dir_);
    config.experimental_pipelined_commit = false;
    config.durability.recover_on_startup = true;
    if (wal_disabled) config.durability.snapshot_wal_mode = Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT;
    InMemoryStorage storage{config};
    auto const prop = storage.NameToProperty("p");
    auto acc = storage.Access(memgraph::storage::READ);
    for (auto v : acc->Vertices(View::OLD)) {
      ++recovered.vertices;
      auto const value = v.GetProperty(prop, View::OLD);
      if (value.has_value() && value->IsInt() && value->ValueInt() == 1000) recovered.faulted_value = 1000;
    }
    return recovered;
  }

  static constexpr int kWarmUp = 20;
  static inline int counter_ = 0;
  std::filesystem::path dir_;
};

TEST_F(PipelinedCommitDeathTest, AfterAppendThrowsTerminatesAndWarmUpRecovers) {
  EXPECT_EXIT(RunChild([](CommitProbe &probe) { probe.after_append = [] { throw std::runtime_error("injected"); }; }),
              ::testing::KilledBySignal(SIGABRT),
              "");
  auto const recovered = Recover();
  EXPECT_TRUE(recovered.crcs_ok);
  EXPECT_EQ(recovered.vertices, kWarmUp);  // loss of the faulting transaction's user-space tail is allowed
}

TEST_F(PipelinedCommitDeathTest, AfterFinalizeWalThrowsTerminatesAndTheTransactionRecoversCommitted) {
  EXPECT_EXIT(
      RunChild([](CommitProbe &probe) { probe.after_finalize_wal = [] { throw std::runtime_error("injected"); }; }),
      ::testing::KilledBySignal(SIGABRT),
      "");
  auto const recovered = Recover();
  EXPECT_TRUE(recovered.crcs_ok);
  EXPECT_EQ(recovered.vertices, kWarmUp);
  EXPECT_TRUE(recovered.faulted_value.has_value());  // FinalizeWalFile synced it (flush every transaction)
}

TEST_F(PipelinedCommitDeathTest, AfterPublishThrowsTerminatesAndTheTransactionRecoversCommitted) {
  EXPECT_EXIT(RunChild([](CommitProbe &probe) { probe.after_publish = [] { throw std::runtime_error("injected"); }; }),
              ::testing::KilledBySignal(SIGABRT),
              "");
  auto const recovered = Recover();
  EXPECT_TRUE(recovered.crcs_ok);
  EXPECT_EQ(recovered.vertices, kWarmUp);
  EXPECT_TRUE(recovered.faulted_value.has_value());
}

TEST_F(PipelinedCommitDeathTest, WalDisabledAfterPublishThrowsTerminates) {
  EXPECT_EXIT(RunChild([](CommitProbe &probe) { probe.after_publish = [] { throw std::runtime_error("injected"); }; },
                       /*wal_disabled=*/true),
              ::testing::KilledBySignal(SIGABRT),
              "");
}

// 7: a failed abort (AbortAndResetCommitTs throwing while a validation failure aborts) terminates, never retried.
TEST_F(PipelinedCommitDeathTest, FailedAbortTerminates) {
  EXPECT_EXIT(
      {
        ArmWatchdog(60);
        auto config = MakeConfig(dir_);
        InMemoryStorage storage{config};
        auto const label = storage.NameToLabel("L");
        auto const id = storage.NameToProperty("id");
        {
          auto acc = storage.UniqueAccess();
          if (!acc->CreateUniqueConstraint(label, {id}).has_value()) _exit(3);
          if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) _exit(3);
        }
        {
          auto acc = storage.Access(memgraph::storage::WRITE);
          auto v = acc->CreateVertex();
          if (!v.AddLabel(label).has_value() || !v.SetProperty(id, PropertyValue(1)).has_value()) _exit(3);
          if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) _exit(3);
        }
        CommitProbe probe;
        probe.abort_throws_once = true;
        storage.SetCommitProbe(&probe);
        auto acc = storage.Access(memgraph::storage::WRITE);
        auto v = acc->CreateVertex();
        if (!v.AddLabel(label).has_value() || !v.SetProperty(id, PropertyValue(1)).has_value()) _exit(3);
        static_cast<void>(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()));  // duplicate: aborts
        _exit(5);
      },
      ::testing::KilledBySignal(SIGABRT),
      "");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  return RUN_ALL_TESTS();
}
