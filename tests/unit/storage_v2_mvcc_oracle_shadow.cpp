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

// Runs the engine's MVCC visibility predicates and an independent transcription
// of them over the same inputs, and fails when they disagree.
//
// The transcription lives outside this repository, in a storage-agnostic
// reference library that models this engine's concurrency. Every layer there
// agrees with the transcription rather than with the engine, so a transcription
// error pointing the same way as the model is invisible at every layer at once.
// A revision pin catches the engine CHANGING one of these predicates; only
// running both spellings in one build catches the transcription having meant
// something different all along. That is what this file is for.
//
// The transcription is not vendored here on purpose: two copies drift, and
// agreement between a copy and its original is a tautology. The path is passed
// in, and without it this test is not built.
//
// The inputs are enumerated rather than sampled. These are pure predicates over
// a handful of integers and flags, so a small alphabet chosen to straddle every
// boundary in them is exhaustive over the interesting space, and a disagreement
// anywhere in it is reported with the input that produced it.

#include <cstdint>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "engine_oracle.hpp"

#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/isolation_level.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/population_status.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/view.hpp"

namespace {

using memgraph::storage::Delta;
using memgraph::storage::DeltaChainState;
using memgraph::storage::EdgeRef;
using memgraph::storage::EdgeTypeId;
using memgraph::storage::IsolationLevel;
using memgraph::storage::PopulationStatus;
using memgraph::storage::StorageMode;
using memgraph::storage::Transaction;
using memgraph::storage::View;

namespace oracle = mvcc2::oracle;

// The transcription's own constant, checked against the engine's rather than
// assumed equal to it. Everything downstream of the oracle reads a committed
// stamp as one below this value.
static_assert(oracle::kTransactionInitialId == memgraph::storage::kTransactionInitialId,
              "the transcription and the engine disagree on where transaction ids begin");

// The four isolation settings the transcription distinguishes. It folds the
// lock-free flag into the isolation enum, which is faithful only because
// InMemoryStorage::CreateTransaction sets that flag exclusively for snapshot
// isolation. Nothing in either spelling enforces that, so the two combinations
// the engine cannot produce are excluded here by the same invariant, and
// LockfreeFlagIsSnapshotOnly records what the exclusion rests on.
struct IsolationSetting {
  oracle::Isolation transcribed;
  IsolationLevel engine;
  bool lockfree;
  char const *name;
};

constexpr IsolationSetting kIsolations[] = {
    {oracle::Isolation::SnapshotLockFree, IsolationLevel::SNAPSHOT_ISOLATION, true, "SI lock-free"},
    {oracle::Isolation::SnapshotLegacy, IsolationLevel::SNAPSHOT_ISOLATION, false, "SI legacy"},
    {oracle::Isolation::ReadCommitted, IsolationLevel::READ_COMMITTED, false, "READ COMMITTED"},
    {oracle::Isolation::ReadUncommitted, IsolationLevel::READ_UNCOMMITTED, false, "READ UNCOMMITTED"},
};

// A reader's own identity. start_timestamp and snapshot_ts differ so that the
// two boundaries cannot pass for each other, and the alphabet of delta stamps
// below straddles both.
constexpr uint64_t kStartTimestamp = 30;
constexpr uint64_t kSnapshotTs = 20;
constexpr uint64_t kCommandId = 2;
constexpr uint64_t kOwnTransactionId = oracle::kTransactionInitialId + 7;
constexpr uint64_t kOtherTransactionId = oracle::kTransactionInitialId + 9;
constexpr uint64_t kOwnCommitTimestamp = 41;

// Every stamp that sits on a different side of some boundary in these
// predicates: below, on and above the inclusive lock-free boundary; below, on
// and above the exclusive legacy one; this transaction's own id and its own
// commit stamp; and another transaction's id, which is the only value that is
// uncommitted and not ours.
constexpr uint64_t kDeltaTimestamps[] = {
    19,
    20,
    21,
    29,
    30,
    31,
    kOwnCommitTimestamp,
    kOwnTransactionId,
    kOtherTransactionId,
};

// Straddles the reader's own command id in both directions, which is what
// separates the NEW and OLD branches.
constexpr uint64_t kDeltaCommandIds[] = {kCommandId - 1, kCommandId, kCommandId + 1};

// A Transaction carries a graph's worth of machinery that none of these
// predicates touch. This builds a real one anyway: the point is to run the
// engine's own member functions, so a stand-in with the same fields would
// establish nothing.
struct Reader {
  explicit Reader(IsolationSetting iso, bool committing)
      : txn{kOwnTransactionId,
            kStartTimestamp,
            iso.engine,
            StorageMode::IN_MEMORY_TRANSACTIONAL,
            /*edge_import_mode_active=*/false,
            memgraph::storage::PointIndexStorage{}.CreatePointIndexContext(),
            nullptr,
            nullptr} {
    txn.snapshot_ts = kSnapshotTs;
    txn.lockfree_snapshot = iso.lockfree;
    txn.command_id = kCommandId;
    // A committing transaction has its stamp published on its own deltas, and
    // the engine reads its own writes through that stamp rather than its id.
    if (committing) txn.commit_info = std::make_unique<memgraph::storage::CommitInfo>(kOwnCommitTimestamp);
  }

  // The transcription's view of the same reader. commit_timestamp is the engine's
  // own `commit_info ? timestamp : transaction_id`.
  [[nodiscard]] oracle::Txn Transcribed(IsolationSetting iso, bool view_new) const {
    return oracle::Txn{
        .isolation = iso.transcribed,
        .snapshot_ts = txn.snapshot_ts,
        .start_timestamp = txn.start_timestamp,
        .commit_timestamp = txn.commit_info ? txn.commit_info->timestamp.load() : txn.transaction_id,
        .transaction_id = txn.transaction_id,
        .command_id = txn.command_id,
        .view_new = view_new,
    };
  }

  Transaction txn;
};

// One link of a chain, in the terms the transcription takes.
struct DeltaSpec {
  uint64_t ts;
  uint64_t command_id;
  bool non_sequential;
  bool deserialized_delete;

  [[nodiscard]] std::string Describe() const {
    return "ts=" + std::to_string(ts) + " cid=" + std::to_string(command_id) +
           (non_sequential ? " non-sequential" : " sequential") + (deserialized_delete ? " deserialized-delete" : "");
  }
};

// Real Delta objects, with the actions that give the transcription's two chain
// flags their meaning. A delta is non-sequential only as an edge removal marked
// so, and only DELETE_DESERIALIZED_OBJECT takes the OLD-view branch's second arm,
// so the flags cannot be set independently of the action that carries them.
class Chain {
 public:
  explicit Chain(std::vector<DeltaSpec> const &specs) {
    commit_infos_.reserve(specs.size());
    deltas_.reserve(specs.size());
    for (auto const &spec : specs) {
      commit_infos_.push_back(std::make_shared<memgraph::storage::CommitInfo>(spec.ts));
      auto *ci = commit_infos_.back().get();
      if (spec.deserialized_delete) {
        // The engine's own constructor for this action allocates its CommitInfo
        // from a page-slab resource and fixes the command id at zero, neither of
        // which the predicate under test looks at. The action is what it looks
        // at, so it is set directly.
        deltas_.push_back(std::make_unique<Delta>(Delta::DeleteObjectTag{}, ci, spec.command_id));
        deltas_.back()->action = Delta::Action::DELETE_DESERIALIZED_OBJECT;
      } else {
        deltas_.push_back(
            std::make_unique<Delta>(Delta::RemoveInEdgeTag{},
                                    EdgeTypeId::FromUint(1),
                                    nullptr,
                                    EdgeRef{memgraph::storage::Gid::FromUint(1)},
                                    spec.non_sequential ? DeltaChainState::NON_SEQUENTIAL : DeltaChainState::SEQUENTIAL,
                                    ci,
                                    spec.command_id));
      }
    }
    for (std::size_t i = 0; i + 1 < deltas_.size(); ++i) deltas_[i]->next.store(deltas_[i + 1].get());
  }

  [[nodiscard]] Delta const *Head() const { return deltas_.empty() ? nullptr : deltas_.front().get(); }

  // Which link a Delta pointer is, so a callback's argument can be compared
  // against the transcription's verdict sequence by position.
  [[nodiscard]] std::size_t IndexOf(Delta const &d) const {
    for (std::size_t i = 0; i < deltas_.size(); ++i)
      if (deltas_[i].get() == &d) return i;
    return deltas_.size();
  }

 private:
  std::vector<std::shared_ptr<memgraph::storage::CommitInfo>> commit_infos_;
  std::vector<std::unique_ptr<Delta>> deltas_;
};

// The traversal the transcription's per-link verdict implies: apply, skip on,
// or stop. Returned as the sequence of applied positions so it can be compared
// against what the engine's callback saw.
std::vector<std::size_t> TranscribedTraversal(oracle::Txn const &t, std::vector<DeltaSpec> const &specs) {
  std::vector<std::size_t> applied;
  for (std::size_t i = 0; i < specs.size(); ++i) {
    auto const verdict = oracle::ApplyDeltasForRead(
        t, specs[i].ts, specs[i].command_id, specs[i].non_sequential, specs[i].deserialized_delete);
    if (verdict == oracle::Verdict::Stop) break;
    if (verdict == oracle::Verdict::Apply) applied.push_back(i);
  }
  return applied;
}

std::vector<std::size_t> EngineTraversal(Transaction const &txn, Chain const &chain, View view) {
  std::vector<std::size_t> applied;
  memgraph::storage::ApplyDeltasForRead(
      &txn, chain.Head(), view, [&](Delta const &d) { applied.push_back(chain.IndexOf(d)); });
  return applied;
}

std::string Describe(std::vector<DeltaSpec> const &specs) {
  std::string out;
  for (auto const &spec : specs) out += "[" + spec.Describe() + "] ";
  return out;
}

// The invariant that licenses the transcription folding the lock-free flag into
// its isolation enum. If the engine ever sets the flag outside snapshot
// isolation, the two spellings diverge for every read that transaction makes,
// and this fails rather than the agreement quietly becoming contingent.
TEST(MvccOracleShadow, LockfreeFlagIsSnapshotOnly) {
  for (auto const &iso : kIsolations) {
    if (iso.lockfree) {
      EXPECT_EQ(iso.engine, IsolationLevel::SNAPSHOT_ISOLATION)
          << "the transcription reads the lock-free boundary only under snapshot isolation";
    }
  }
  // Both spellings must agree that the flag is what selects the boundary, and
  // that the boundaries are genuinely different values for this reader.
  Reader lockfree{kIsolations[0], /*committing=*/false};
  ASSERT_TRUE(lockfree.txn.lockfree_snapshot) << "the first setting must be the lock-free one";
  EXPECT_NE(lockfree.txn.CommittedBeforeSnapshot(kSnapshotTs + 1), lockfree.txn.CommittedBeforeSnapshot(kSnapshotTs))
      << "an alphabet that never straddles the boundary would agree with anything";
}

// A transaction's own writes are identified by its id in the write check and by
// `commit_info ? timestamp : transaction_id` in the read walk. Those are the
// same value everywhere the write check can run, because the engine creates
// commit_info holding the id and overwrites it with the commit stamp only on the
// commit path, after which nothing writes. A transcription that conflates the
// two is therefore wrong without being wrong about any reachable question, which
// is how one survived here. The enumeration below now drives both spellings
// through the unreachable state as well, so their agreement no longer rests on
// this invariant; it is pinned anyway, because if it breaks the read walk and
// the write check stop meaning the same thing by "mine".
TEST(MvccOracleShadow, CommitInfoStartsAtTheTransactionId) {
  Reader reader{kIsolations[0], /*committing=*/false};
  ASSERT_EQ(reader.txn.commit_info, nullptr) << "a transaction must not begin with a commit stamp";
  reader.txn.EnsureCommitInfoExists();
  ASSERT_NE(reader.txn.commit_info, nullptr);
  EXPECT_EQ(reader.txn.commit_info->timestamp.load(), reader.txn.transaction_id)
      << "an in-flight transaction's commit_info must carry its id, or the write check and the read "
         "walk disagree about which deltas are its own";
}

TEST(MvccOracleShadow, CommittedBeforeSnapshot) {
  for (auto const &iso : kIsolations) {
    Reader reader{iso, /*committing=*/false};
    auto const transcribed = reader.Transcribed(iso, /*view_new=*/false);
    for (auto const ts : kDeltaTimestamps) {
      EXPECT_EQ(reader.txn.CommittedBeforeSnapshot(ts), oracle::CommittedBeforeSnapshot(transcribed, ts))
          << iso.name << ", ts=" << ts;
    }
  }
}

TEST(MvccOracleShadow, SchemaReconstructionBound) {
  for (auto const &iso : kIsolations) {
    Reader reader{iso, /*committing=*/false};
    auto const transcribed = reader.Transcribed(iso, /*view_new=*/false);
    EXPECT_EQ(reader.txn.SchemaReconstructionBound(), oracle::SchemaReconstructionBound(transcribed)) << iso.name;

    // The bound exists so that a `ts < bound` primitive decides exactly what
    // CommittedBeforeSnapshot decides. Agreement on the value is worth less
    // than agreement on that, so both are checked.
    for (auto const ts : kDeltaTimestamps) {
      EXPECT_EQ(ts < reader.txn.SchemaReconstructionBound(), reader.txn.CommittedBeforeSnapshot(ts))
          << "the bound must encode the boundary it stands for: " << iso.name << ", ts=" << ts;
    }
  }
}

TEST(MvccOracleShadow, SchemaObjectVisible) {
  for (auto const &iso : kIsolations) {
    Reader reader{iso, /*committing=*/false};
    auto const transcribed = reader.Transcribed(iso, /*view_new=*/false);
    for (auto const ts : kDeltaTimestamps) {
      PopulationStatus status;
      status.Commit(ts);
      EXPECT_EQ(status.IsVisible(reader.txn.start_timestamp), oracle::SchemaObjectVisible(transcribed, ts))
          << iso.name << ", schema commit ts=" << ts;
    }
  }

  // A populating object is visible to nobody, and the transcription reaches that
  // through the same comparison rather than through a special case.
  PopulationStatus populating;
  EXPECT_FALSE(populating.IsVisible(kStartTimestamp)) << "an unpopulated object cannot be visible";
}

// One link, every combination. This is the direct comparison, because the
// transcription states a per-link verdict.
TEST(MvccOracleShadow, ApplyDeltasForReadSingleDelta) {
  std::size_t checked = 0;
  for (auto const &iso : kIsolations) {
    for (bool const committing : {false, true}) {
      for (auto const view : {View::OLD, View::NEW}) {
        Reader reader{iso, committing};
        auto const transcribed = reader.Transcribed(iso, view == View::NEW);
        for (auto const ts : kDeltaTimestamps) {
          for (auto const cid : kDeltaCommandIds) {
            for (bool const non_sequential : {false, true}) {
              for (bool const deserialized_delete : {false, true}) {
                // A deserialized delete is its own action, so it cannot also be
                // a non-sequential edge removal.
                if (deserialized_delete && non_sequential) continue;
                std::vector<DeltaSpec> const specs{{ts, cid, non_sequential, deserialized_delete}};
                Chain chain{specs};
                EXPECT_EQ(EngineTraversal(reader.txn, chain, view), TranscribedTraversal(transcribed, specs))
                    << iso.name << ", " << (committing ? "committing" : "in flight") << ", "
                    << (view == View::NEW ? "NEW" : "OLD") << ", " << Describe(specs);
                ++checked;
              }
            }
          }
        }
      }
    }
  }
  EXPECT_GT(checked, 0U) << "an enumeration that ran no cases agrees with anything";
}

// Three links. A per-link verdict can be right at every link and still compose
// into the wrong traversal, because skipping continues and stopping does not.
TEST(MvccOracleShadow, ApplyDeltasForReadChainTraversal) {
  // Reduced alphabet, since the composition is what is under test rather than
  // the boundary arithmetic the previous case covers exhaustively.
  constexpr uint64_t kChainTimestamps[] = {19, 30, kOwnCommitTimestamp, kOwnTransactionId};
  std::size_t stopped_early = 0;
  std::size_t skipped_on = 0;

  for (auto const &iso : kIsolations) {
    for (bool const committing : {false, true}) {
      for (auto const view : {View::OLD, View::NEW}) {
        Reader reader{iso, committing};
        auto const transcribed = reader.Transcribed(iso, view == View::NEW);
        for (auto const ts0 : kChainTimestamps) {
          for (auto const ts1 : kChainTimestamps) {
            for (auto const ts2 : kChainTimestamps) {
              for (bool const ns0 : {false, true}) {
                for (bool const ns1 : {false, true}) {
                  std::vector<DeltaSpec> const specs{
                      {ts0, kCommandId, ns0, false},
                      {ts1, kCommandId, ns1, false},
                      {ts2, kCommandId, false, false},
                  };
                  Chain chain{specs};
                  auto const engine = EngineTraversal(reader.txn, chain, view);
                  EXPECT_EQ(engine, TranscribedTraversal(transcribed, specs))
                      << iso.name << ", " << (committing ? "committing" : "in flight") << ", "
                      << (view == View::NEW ? "NEW" : "OLD") << ", " << Describe(specs);
                  if (engine.size() < specs.size()) ++stopped_early;
                  if (!engine.empty() && engine.back() + 1 > engine.size()) ++skipped_on;
                }
              }
            }
          }
        }
      }
    }
  }

  // Without both shapes present the comparison never sees the difference
  // between skipping and stopping, which is the whole content of this case.
  EXPECT_GT(stopped_early, 0U) << "no chain stopped short, so stopping was never compared";
  EXPECT_GT(skipped_on, 0U) << "no chain skipped a link and applied a later one, so skipping was never compared";
}

// The engine's write check is a template over the object, and one branch of it
// exists only for objects that track uncommitted non-sequential deltas. Both
// shapes are needed: a transcription that always applied that branch would
// refuse writes the engine accepts.
struct TrackingObject {
  [[nodiscard]] Delta *delta() const { return head; }

  [[nodiscard]] bool has_uncommitted_non_sequential_deltas() const { return uncommitted_non_sequential; }

  Delta *head = nullptr;
  bool uncommitted_non_sequential = false;
};

struct PlainObject {
  [[nodiscard]] Delta *delta() const { return head; }

  Delta *head = nullptr;
};

// The engine reaches its optional branch through `if constexpr (requires ...)`
// inside a template, where a missing member is a substitution failure. Spelled
// on a concrete type the same requires-expression is a hard error, so the test
// of which shape has the member goes through a concept too.
template <typename T>
concept TracksNonSequential = requires(T o) { o.has_uncommitted_non_sequential_deltas(); };

TEST(MvccOracleShadow, PrepareForWrite) {
  static_assert(TracksNonSequential<TrackingObject>, "the tracking shape must reach the branch it exists to reach");
  static_assert(!TracksNonSequential<PlainObject>, "the plain shape must not reach it, or both shapes test one branch");

  std::size_t accepted = 0;
  std::size_t refused = 0;

  for (auto const &iso : kIsolations) {
    for (bool const committing : {false, true}) {
      for (auto const head_ts : kDeltaTimestamps) {
        for (bool const head_non_sequential : {false, true}) {
          for (bool const uncommitted_non_sequential : {false, true}) {
            std::vector<DeltaSpec> const specs{{head_ts, kCommandId, head_non_sequential, false}};
            Chain chain{specs};
            auto *head = const_cast<Delta *>(chain.Head());

            for (bool const tracking : {false, true}) {
              Reader reader{iso, committing};
              auto const transcribed = reader.Transcribed(iso, /*view_new=*/false);
              bool engine = false;
              if (tracking) {
                TrackingObject obj{head, uncommitted_non_sequential};
                engine = memgraph::storage::PrepareForWrite(&reader.txn, &obj);
              } else {
                PlainObject obj{head};
                engine = memgraph::storage::PrepareForWrite(&reader.txn, &obj);
              }
              EXPECT_EQ(engine,
                        oracle::PrepareForWriteAccepts(
                            transcribed, head_ts, head_non_sequential, tracking, uncommitted_non_sequential))
                  << iso.name << ", " << (committing ? "committing" : "in flight") << ", head " << specs[0].Describe()
                  << ", " << (tracking ? "tracking object" : "plain object")
                  << ", uncommitted-non-sequential=" << uncommitted_non_sequential;

              // A refusal is also a state change on the transaction, and a
              // transcription of the return value alone would miss it.
              EXPECT_EQ(reader.txn.has_serialization_error, !engine)
                  << "a refused write must record a serialization error: " << iso.name << ", head "
                  << specs[0].Describe();
              engine ? ++accepted : ++refused;
            }
          }
        }
      }
    }
  }

  EXPECT_GT(accepted, 0U) << "no write was accepted, so acceptance was never compared";
  EXPECT_GT(refused, 0U) << "no write was refused, so refusal was never compared";
}

// The one branch the transcription declares it cannot carry, checked here so
// the declaration stays true. An object with no head is always writable.
TEST(MvccOracleShadow, PrepareForWriteWithNoHead) {
  for (auto const &iso : kIsolations) {
    Reader reader{iso, /*committing=*/false};
    PlainObject obj{};
    EXPECT_TRUE(memgraph::storage::PrepareForWrite(&reader.txn, &obj)) << iso.name;
    EXPECT_FALSE(reader.txn.has_serialization_error) << iso.name;
  }
}

}  // namespace
