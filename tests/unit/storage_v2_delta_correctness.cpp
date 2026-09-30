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

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "storage/v2/inmemory/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace ms = memgraph::storage;
using ::testing::UnorderedElementsAreArray;

// =============================================================================
// Data-Driven Test Framework for Sequential/NonSequential Delta Correctness
// =============================================================================

// Transaction identifier (0-based index)
using TxId = int;

// Vertex identifier for the test (we use v1, v2 as the main test vertices)
enum class VId { V1, V2 };

// Expected result of an operation
enum class Expect { OK, SERIALIZATION_ERROR };

// Operation types
struct OpStart {
  TxId tx;
};

struct OpCommit {
  TxId tx;
};

struct OpAbort {
  TxId tx;
};

struct OpAddLabel {
  TxId tx;
  VId vertex;
  std::string label;
  Expect expect{Expect::OK};
};

struct OpCreateEdge {
  TxId tx;
  VId from;
  VId to;
  std::string edge_type;
  Expect expect{Expect::OK};
};

struct OpCheckEdges {
  TxId tx;
  VId vertex;
  ms::View view;
  std::vector<std::string> expected_edge_types;
};

struct OpCheckLabels {
  TxId tx;
  VId vertex;
  ms::View view;
  std::vector<std::string> expected_labels;
};

struct OpCheckVertexFlag {
  VId vertex;
  bool expected_has_uncommitted_non_sequential_deltas;
};

// GC control via sentinel transaction - holds deltas from being collected
struct OpHoldGc {};  // Start sentinel transaction to prevent GC/fast-discard

struct OpReleaseGc {};  // Release sentinel, allowing GC to proceed

struct OpGc {};  // Trigger manual garbage collection

// A single step in the test sequence
using Op = std::variant<OpStart, OpCommit, OpAbort, OpAddLabel, OpCreateEdge, OpCheckEdges, OpCheckLabels, OpHoldGc,
                        OpReleaseGc, OpGc, OpCheckVertexFlag>;

// A complete test case
struct TestCase {
  std::string name;
  std::vector<Op> operations;
};

// =============================================================================
// Test Executor
// =============================================================================

class DeltaCorrectnessTest : public ::testing::TestWithParam<TestCase> {
 protected:
  void SetUp() override {
    // Disable periodic GC so we can control it manually during tests
    ms::Config config{};
    config.gc.type = ms::Config::Gc::Type::NONE;
    storage_ = std::make_unique<ms::InMemoryStorage>(config);
  }

  void TearDown() override {
    // Release sentinel transaction if held
    gc_sentinel_.reset();
    // Abort any remaining transactions
    transactions_.clear();
    // Run GC to clean up stale deltas between tests
    if (storage_) {
      storage_->FreeMemory();
    }
    storage_.reset();
  }

  void RunTestCase(const TestCase &tc) {
    // Setup: Create two vertices
    {
      auto acc = storage_->Access(ms::WRITE);
      auto v1 = acc->CreateVertex();
      auto v2 = acc->CreateVertex();
      v1_gid_ = v1.Gid();
      v2_gid_ = v2.Gid();
      ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
    }

    // Execute operations
    for (size_t i = 0; i < tc.operations.size(); ++i) {
      SCOPED_TRACE("Operation " + std::to_string(i));
      std::visit([this](const auto &op) { Execute(op); }, tc.operations[i]);
    }
  }

 private:
  void Execute(const OpStart &op) {
    ASSERT_EQ(transactions_.count(op.tx), 0) << "Transaction " << op.tx << " already started";
    transactions_[op.tx] = storage_->Access(ms::WRITE);
  }

  void Execute(const OpCommit &op) {
    auto it = transactions_.find(op.tx);
    ASSERT_NE(it, transactions_.end()) << "Transaction " << op.tx << " not found";
    ASSERT_TRUE(it->second->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value())
        << "Commit failed for tx" << op.tx;
    transactions_.erase(it);
  }

  void Execute(const OpAbort &op) {
    auto it = transactions_.find(op.tx);
    ASSERT_NE(it, transactions_.end()) << "Transaction " << op.tx << " not found";
    it->second->Abort();
    transactions_.erase(it);
  }

  void Execute(const OpAddLabel &op) {
    auto &acc = GetTx(op.tx);
    auto vertex = acc->FindVertex(GetGid(op.vertex), ms::View::NEW);
    ASSERT_TRUE(vertex.has_value()) << "Vertex not found";

    auto label = acc->NameToLabel(op.label);
    auto result = vertex->AddLabel(label);

    if (op.expect == Expect::OK) {
      ASSERT_TRUE(result.has_value()) << "AddLabel(" << op.label << ") expected to succeed but failed";
    } else {
      ASSERT_FALSE(result.has_value()) << "AddLabel(" << op.label << ") expected SERIALIZATION_ERROR but succeeded";
      EXPECT_EQ(result.error(), ms::Error::SERIALIZATION_ERROR);
    }
  }

  void Execute(const OpCreateEdge &op) {
    auto &acc = GetTx(op.tx);
    auto from = acc->FindVertex(GetGid(op.from), ms::View::NEW);
    auto to = acc->FindVertex(GetGid(op.to), ms::View::NEW);
    ASSERT_TRUE(from.has_value() && to.has_value()) << "Vertices not found";

    auto et = acc->NameToEdgeType(op.edge_type);
    auto result = acc->CreateEdge(&*from, &*to, et);

    if (op.expect == Expect::OK) {
      ASSERT_TRUE(result.has_value()) << "CreateEdge(" << op.edge_type << ") expected to succeed but failed";
    } else {
      ASSERT_FALSE(result.has_value()) << "CreateEdge(" << op.edge_type
                                       << ") expected SERIALIZATION_ERROR but succeeded";
      EXPECT_EQ(result.error(), ms::Error::SERIALIZATION_ERROR);
    }
  }

  void Execute(const OpCheckEdges &op) {
    auto &acc = GetTx(op.tx);
    auto vertex = acc->FindVertex(GetGid(op.vertex), ms::View::NEW);
    ASSERT_TRUE(vertex.has_value()) << "Vertex not found";

    auto edges_result = vertex->OutEdges(op.view);
    ASSERT_TRUE(edges_result.has_value()) << "OutEdges failed";

    std::vector<std::string> actual_types;
    for (const auto &edge : edges_result->edges) {
      actual_types.push_back(acc->EdgeTypeToName(edge.EdgeType()));
    }

    EXPECT_THAT(actual_types, UnorderedElementsAreArray(op.expected_edge_types))
        << "Edge check failed for tx" << op.tx << " on " << (op.vertex == VId::V1 ? "V1" : "V2") << " with "
        << (op.view == ms::View::OLD ? "OLD" : "NEW") << " view";
  }

  void Execute(const OpCheckLabels &op) {
    auto &acc = GetTx(op.tx);
    auto vertex = acc->FindVertex(GetGid(op.vertex), ms::View::NEW);
    ASSERT_TRUE(vertex.has_value()) << "Vertex not found";

    auto labels_result = vertex->Labels(op.view);
    ASSERT_TRUE(labels_result.has_value()) << "Labels failed";

    std::vector<std::string> actual_labels;
    for (const auto &label : labels_result.value()) {
      actual_labels.push_back(acc->LabelToName(label));
    }

    EXPECT_THAT(actual_labels, UnorderedElementsAreArray(op.expected_labels))
        << "Label check failed for tx" << op.tx << " on " << (op.vertex == VId::V1 ? "V1" : "V2") << " with "
        << (op.view == ms::View::OLD ? "OLD" : "NEW") << " view";
  }

  void Execute(const OpHoldGc & /*op*/) {
    ASSERT_FALSE(gc_sentinel_) << "GC sentinel already held";
    // Start a read transaction that will prevent GC from cleaning up deltas
    // with timestamps newer than this transaction's start timestamp
    gc_sentinel_ = storage_->Access(ms::READ);
  }

  void Execute(const OpReleaseGc & /*op*/) {
    ASSERT_TRUE(gc_sentinel_) << "GC sentinel not held";
    gc_sentinel_.reset();
  }

  void Execute(const OpGc & /*op*/) { storage_->FreeMemory(); }

  void Execute(OpCheckVertexFlag const &op) {
    auto acc = storage_->Access(ms::READ);
    auto vertex = acc->FindVertex(GetGid(op.vertex), ms::View::OLD);
    ASSERT_TRUE(vertex.has_value()) << "Vertex not found";
    auto const guard = std::shared_lock{vertex->vertex_->lock};
    EXPECT_EQ(vertex->vertex_->has_uncommitted_non_sequential_deltas(),
              op.expected_has_uncommitted_non_sequential_deltas)
        << "Flag check failed for " << (op.vertex == VId::V1 ? "V1" : "V2");
  }

  std::unique_ptr<ms::Storage::Accessor> &GetTx(TxId tx) {
    auto it = transactions_.find(tx);
    if (it == transactions_.end()) {
      ADD_FAILURE() << "Transaction " << tx << " not found";
      static std::unique_ptr<ms::Storage::Accessor> dummy;
      return dummy;
    }
    return it->second;
  }

  ms::Gid GetGid(VId v) const { return v == VId::V1 ? v1_gid_ : v2_gid_; }

  std::unique_ptr<ms::Storage> storage_;
  std::map<TxId, std::unique_ptr<ms::Storage::Accessor>> transactions_;
  std::unique_ptr<ms::Storage::Accessor> gc_sentinel_;  // Holds deltas from being GC'd
  ms::Gid v1_gid_;
  ms::Gid v2_gid_;
};

// =============================================================================
// Test Cases
// =============================================================================

// clang-format off
std::vector<TestCase> GetTestCases() {
  using enum VId;
  using enum Expect;
  using enum ms::View;

  return {
    // =========================================================================
    // SECTION 1: Single Transaction - Sequential Delta Tests
    // =========================================================================
            {
              "Experiment",
              {
                OpHoldGc{},
                OpStart{0},
                OpAddLabel{0, V1, "Label1", OK},
                OpCreateEdge{0, V1, V2, "E1", OK},
                OpCheckLabels{0, V1, NEW, {"Label1"}},
                OpCommit{0},
                //
                OpStart{1},
                OpCreateEdge{1, V1, V2, "E1", OK},
                //
                OpStart{2},
                OpCreateEdge{2, V1, V2, "E1", OK},
              }
            },

{
  "Experiment2",
  {
    OpHoldGc{},
    OpStart{0},
    OpAddLabel{0, V1, "Label1", OK}, // SEQ
    OpCreateEdge{0, V1, V2, "E1", OK}, //Forced SEQ
    OpCheckLabels{0, V1, NEW, {"Label1"}}, // SEQ
    OpCommit{0}, // COMMITTED
    //
    OpStart{1},
    OpCreateEdge{1, V1, V2, "E1", OK},
    //
    OpStart{2},
    OpCreateEdge{2, V1, V2, "E1", OK},
    OpCommit{2},
    //
    OpStart{3},
  OpCreateEdge{3, V1, V2, "E1", OK},
  }
},

    {
      "SingleTx_AddLabel_Sequential",
      {
        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCheckLabels{0, V1, NEW, {"Label1"}},
        OpCommit{0},
        // Verify persisted
        OpStart{1},
        OpCheckLabels{1, V1, OLD, {"Label1"}},
        OpCommit{1},
      }
    },

    {
      "SingleTx_CreateEdge_Sequential",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpCheckEdges{0, V1, NEW, {"Edge1"}},
        OpCommit{0},
        // Verify persisted
        OpStart{1},
        OpCheckEdges{1, V1, OLD, {"Edge1"}},
        OpCommit{1},
      }
    },

    {
      "SingleTx_MultipleAddLabel_AllSequential",
      {
        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpAddLabel{0, V1, "Label2", OK},
        OpAddLabel{0, V1, "Label3", OK},
        OpCheckLabels{0, V1, NEW, {"Label1", "Label2", "Label3"}},
        OpCommit{0},
      }
    },

    {
      "SingleTx_MultipleCreateEdge_AllSequential",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpCreateEdge{0, V1, V2, "Edge2", OK},
        OpCreateEdge{0, V1, V2, "Edge3", OK},
        OpCheckEdges{0, V1, NEW, {"Edge1", "Edge2", "Edge3"}},
        OpCommit{0},
      }
    },

    {
      "SingleTx_AddLabelThenCreateEdge_BothSequential",
      {
        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpCheckLabels{0, V1, NEW, {"Label1"}},
        OpCheckEdges{0, V1, NEW, {"Edge1"}},
        OpCommit{0},
      }
    },

    {
      "SingleTx_CreateEdgeThenAddLabel_BothSequential",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpAddLabel{0, V1, "Label1", OK},
        OpCheckLabels{0, V1, NEW, {"Label1"}},
        OpCheckEdges{0, V1, NEW, {"Edge1"}},
        OpCommit{0},
      }
    },

    // =========================================================================
    // SECTION 2: Two Transactions - Non-Sequential Delta Tests
    // =========================================================================

    {
      "TwoTx_BothCreateEdge_NonSequential_BothSucceed",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", OK},  // Non-sequential, should succeed
        // Each sees only their own edge
        OpCheckEdges{0, V1, NEW, {"Edge1"}},
        OpCheckEdges{1, V1, NEW, {"Edge2"}},
        OpCommit{0},
        OpCommit{1},
        // New transaction sees both
        OpStart{2},
        OpCheckEdges{2, V1, OLD, {"Edge1", "Edge2"}},
        OpCommit{2},
      }
    },

    {
      "TwoTx_AddLabel_BlocksCreateEdge",
      {
        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},  // Blocking operation
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", SERIALIZATION_ERROR},  // Blocked by uncommitted label
        OpAbort{0},
        OpAbort{1},
      }
    },

    {
      "TwoTx_CreateEdge_BlocksAddLabel",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpStart{1},
        OpAddLabel{1, V1, "Label1", SERIALIZATION_ERROR},  // Blocked - can't add blocking after uncommitted edge
        OpAbort{0},
        OpAbort{1},
      }
    },

    {
      "TwoTx_AddLabelAndEdge_BlocksOtherEdge",
      {
        // Tx0: AddLabel then CreateEdge - edge has blocker upstream flag set
        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        // Tx1: Try CreateEdge - should fail due to FORCED_SEQUENTIAL state
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", SERIALIZATION_ERROR},
        OpAbort{0},
        OpAbort{1},
      }
    },

    // =========================================================================
    // SECTION 3: Commit Ordering and Visibility Tests
    // =========================================================================

    {
      "CommitOrder_Tx0First_Tx1SeesOnlyOwn",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", OK},
        // Tx0 commits first
        OpCommit{0},
        // Tx1 still sees only its own edge (snapshot isolation)
        OpCheckEdges{1, V1, NEW, {"Edge2"}},
        // Tx2 starts after Tx0 committed, sees Edge1 but not uncommitted Edge2
        OpStart{2},
        OpCheckEdges{2, V1, OLD, {"Edge1"}},
        // Tx1 commits
        OpCommit{1},
        // Tx2 still sees only Edge1 (started before Tx1 committed)
        OpCheckEdges{2, V1, OLD, {"Edge1"}},
        // Tx3 sees both
        OpStart{3},
        OpCheckEdges{3, V1, OLD, {"Edge1", "Edge2"}},
        OpCommit{2},
        OpCommit{3},
      }
    },

    {
      "CommitOrder_Tx1First_OutOfOrder",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", OK},
        // Tx1 commits first (out of order)
        OpCommit{1},
        // Tx0 still sees only its own edge
        OpCheckEdges{0, V1, NEW, {"Edge1"}},
        // Tx2 sees Tx1's edge but not uncommitted Tx0
        OpStart{2},
        OpCheckEdges{2, V1, OLD, {"Edge2"}},
        // Tx0 commits
        OpCommit{0},
        // Tx2 still sees only Tx1's edge
        OpCheckEdges{2, V1, OLD, {"Edge2"}},
        // Tx3 sees both
        OpStart{3},
        OpCheckEdges{3, V1, OLD, {"Edge1", "Edge2"}},
        OpCommit{2},
        OpCommit{3},
      }
    },

    {
      "AbortedTx_NonSequentialEdgeStillCommits",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", OK},  // Non-sequential
        // Tx0 aborts
        OpAbort{0},
        // Tx1 commits
        OpCommit{1},
        // Only Edge2 visible
        OpStart{2},
        OpCheckEdges{2, V1, OLD, {"Edge2"}},
        OpCommit{2},
      }
    },

    // =========================================================================
    // SECTION 4: Three-way Concurrent Transaction Tests
    // =========================================================================

    {
      "ThreeTx_AllCreateEdge_AllNonSequential",
      {
        OpStart{0},
        OpStart{1},
        OpStart{2},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpCreateEdge{1, V1, V2, "Edge2", OK},  // Non-sequential
        OpCreateEdge{2, V1, V2, "Edge3", OK},  // Non-sequential
        // Each sees only their own
        OpCheckEdges{0, V1, NEW, {"Edge1"}},
        OpCheckEdges{1, V1, NEW, {"Edge2"}},
        OpCheckEdges{2, V1, NEW, {"Edge3"}},
        OpCommit{0},
        OpCommit{1},
        OpCommit{2},
        // All visible
        OpStart{3},
        OpCheckEdges{3, V1, OLD, {"Edge1", "Edge2", "Edge3"}},
        OpCommit{3},
      }
    },

    {
      "ThreeTx_InterleavedEdgeCreations",
      {
        OpStart{0},
        OpStart{1},
        OpStart{2},
        // Interleaved: 0, 1, 2, 0, 1, 2
        OpCreateEdge{0, V1, V2, "Edge0a", OK},
        OpCreateEdge{1, V1, V2, "Edge1a", OK},
        OpCreateEdge{2, V1, V2, "Edge2a", OK},
        OpCreateEdge{0, V1, V2, "Edge0b", OK},
        OpCreateEdge{1, V1, V2, "Edge1b", OK},
        OpCreateEdge{2, V1, V2, "Edge2b", OK},
        // Each sees only their own
        OpCheckEdges{0, V1, NEW, {"Edge0a", "Edge0b"}},
        OpCheckEdges{1, V1, NEW, {"Edge1a", "Edge1b"}},
        OpCheckEdges{2, V1, NEW, {"Edge2a", "Edge2b"}},
        OpCommit{0},
        OpCommit{1},
        OpCommit{2},
        // All visible
        OpStart{3},
        OpCheckEdges{3, V1, OLD, {"Edge0a", "Edge0b", "Edge1a", "Edge1b", "Edge2a", "Edge2b"}},
        OpCommit{3},
      }
    },

    // =========================================================================
    // SECTION 5: After Abort - Blocking Operations Should Succeed
    // =========================================================================

    {
      "AfterAbort_AddLabelSucceeds",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpAbort{0},
        // Now AddLabel should succeed
        OpStart{1},
        OpAddLabel{1, V1, "Label1", OK},
        OpCommit{1},
        // Verify
        OpStart{2},
        OpCheckLabels{2, V1, OLD, {"Label1"}},
        OpCheckEdges{2, V1, OLD, {}},  // Edge1 was aborted
        OpCommit{2},
      }
    },

    {
      "AfterAbort_CreateEdgeSucceeds",
      {
        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpAbort{0},
        // Now CreateEdge should succeed
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},
        OpCommit{1},
        // Verify
        OpStart{2},
        OpCheckLabels{2, V1, OLD, {}},  // Label1 was aborted
        OpCheckEdges{2, V1, OLD, {"Edge1"}},
        OpCommit{2},
      }
    },

    // =========================================================================
    // SECTION 6: Same Transaction - Non-Sequential Then Blocking Fails
    // =========================================================================

    {
      "SameTx_NonSequentialThenBlocking_Fails",
      {
        // Tx0 creates edge (sequential)
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        // Tx1 creates edge (becomes non-sequential)
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", OK},
        // Tx1 tries to add label - should fail (own head is non-sequential)
        OpAddLabel{1, V1, "Label1", SERIALIZATION_ERROR},
        OpAbort{0},
        OpAbort{1},
      }
    },

    {
      "SameTx_MultipleNonSequentialEdges_ThenBlocking_Fails",
      {
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", OK},  // Non-sequential
        OpCreateEdge{1, V1, V2, "Edge3", OK},  // Still non-sequential
        OpAddLabel{1, V1, "Label1", SERIALIZATION_ERROR},  // Still fails
        OpAbort{0},
        OpAbort{1},
      }
    },

    // =========================================================================
    // SECTION 7: Edge Cases and Boundary Conditions
    // =========================================================================

    {
      "CommittedHead_UncommittedDownstream_BlocksAddLabel",
      {
        // Tx0 creates edge
        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        // Tx1 creates edge (non-sequential)
        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", OK},
        // Tx0 commits - head is now committed
        OpCommit{0},
        // Tx2 creates edge
        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge3", OK},
        OpCommit{2},
        // Tx3 tries AddLabel - should fail (uncommitted Tx1 downstream)
        OpStart{3},
        OpAddLabel{3, V1, "Label1", SERIALIZATION_ERROR},
        OpAbort{1},
        OpAbort{3},
      }
    },

    {
      "MultipleBlockingOps_AllFail",
      {
        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpStart{1},
        // All these should fail
        OpAddLabel{1, V1, "Label2", SERIALIZATION_ERROR},
        OpCreateEdge{1, V1, V2, "Edge1", SERIALIZATION_ERROR},
        OpAbort{0},
        OpAbort{1},
      }
    },

    {
      "AfterCommit_NewTxCanDoAnything",
      {
        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpCommit{0},
        // New transaction can do anything
        OpStart{1},
        OpAddLabel{1, V1, "Label2", OK},
        OpCreateEdge{1, V1, V2, "Edge2", OK},
        OpCommit{1},
        // Verify all persisted
        OpStart{2},
        OpCheckLabels{2, V1, OLD, {"Label1", "Label2"}},
        OpCheckEdges{2, V1, OLD, {"Edge1", "Edge2"}},
        OpCommit{2},
      }
    },


    // =========================================================================
    // SECTION 8: Commit/Abort ordering
    // =========================================================================


    {
      "TransactionOrdering_2Commits1Commits",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpCommit{2},
        OpCommit{1},

        // Should succeed as has_uncommitted_non_sequential_deltas flag is cleared
        OpStart{3},
        OpAddLabel{3, V1, "Label2", OK},
        OpCommit{3}
      }
    },

    {
      "TransactionOrdering_2Commits1Aborts",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpCommit{2},
        OpAbort{1},

        // Should succeed as has_uncommitted_non_sequential_deltas flag is cleared
        OpStart{3},
        OpAddLabel{3, V1, "Label2", OK},
        OpCommit{3}
      }
    },

    {
      "TransactionOrdering_2Aborts1Commits",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpAbort{2},
        OpCommit{1},

        // Should succeed as has_uncommitted_non_sequential_deltas flag is cleared
        OpStart{3},
        OpAddLabel{3, V1, "Label2", OK},
        OpCommit{3}
      }
    },

    {
      "TransactionOrdering_2Aborts1Aborts",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpAbort{2},
        OpAbort{1},

        // Should succeed as has_uncommitted_non_sequential_deltas flag is cleared
        OpStart{3},
        OpAddLabel{3, V1, "Label2", OK},
        OpCommit{3}
      }
    },

    {
      "TransactionOrdering_1Commits2Commits",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpCommit{1},
        OpCommit{2},

        // Should succeed as has_uncommitted_non_sequential_deltas flag is cleared
        OpStart{3},
        OpAddLabel{3, V1, "Label2", OK},
        OpCommit{3}
      }
    },

    {
      "TransactionOrdering_1Commits2Aborts",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpCommit{1},
        OpAbort{2},

        // Should succeed as has_uncommitted_non_sequential_deltas flag is cleared
        OpStart{3},
        OpAddLabel{3, V1, "Label2", OK},
        OpCommit{3}
      }
    },

    {
      "TransactionOrdering_1Aborts2Commits",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpAbort{1},
        OpCommit{2},

        // Should succeed as has_uncommitted_non_sequential_deltas flag is cleared
        OpStart{3},
        OpAddLabel{3, V1, "Label2", OK},
        OpCommit{3}
      }
    },

    {
      "TransactionOrdering_1Aborts2Aborts",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpAbort{1},
        OpAbort{2},

        // Should succeed as has_uncommitted_non_sequential_deltas flag is cleared
        OpStart{3},
        OpAddLabel{3, V1, "Label2", OK},
        OpCommit{3}
      }
    },

    // =========================================================================
    // SECTION 9: Propagated (upgraded) delta flag persistence
    // =========================================================================

    {
      "PropagatedDelta_UpstreamCommitsFirst_FlagRemains",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpCommit{2},

        // Flag should remain
        OpStart{3},
        OpAddLabel{3, V1, "Label2", SERIALIZATION_ERROR},

        // Flag should finally be cleared
        OpCommit{1},

        OpStart{4},
        OpAddLabel{4, V1, "Label3", OK},
        OpCommit{4}
      }
    },

    {
      "PropagatedDelta_UpstreamAbortsFirst_FlagRemains",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpAbort{2},

        // Flag should remain
        OpStart{3},
        OpAddLabel{3, V1, "Label2", SERIALIZATION_ERROR},

        // Flag should finally be cleared
        OpCommit{1},

        OpStart{4},
        OpAddLabel{4, V1, "Label3", OK},
        OpCommit{4}
      }
    },

    {
      "PropagatedDelta_UpstreamCommitsFirst_DownstreamAborts",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpCommit{2},

        // Flag should remain
        OpStart{3},
        OpAddLabel{3, V1, "Label2", SERIALIZATION_ERROR},

        /// Flag should finally be cleared
        OpAbort{1},

        OpStart{4},
        OpAddLabel{4, V1, "Label3", OK},
        OpCommit{4}
      }
    },

    {
      "PropagatedDelta_UpstreamAbortsFirst_DownstreamAborts",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpAbort{2},

        // Flag should remain
        OpStart{3},
        OpAddLabel{3, V1, "Label2", SERIALIZATION_ERROR},

         // Flag should finally be cleared
        OpAbort{1},

        OpStart{4},
        OpAddLabel{4, V1, "Label3", OK},
        OpCommit{4}
      }
    },

    {
      "PropagatedDelta_ThreeTransactions_MiddleCommitsFirst",
      {
        OpHoldGc{},

        OpStart{0},
        OpAddLabel{0, V1, "Label1", OK},
        OpCommit{0},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge1", OK},

        OpStart{2},
        OpCreateEdge{2, V1, V2, "Edge2", OK},

        OpStart{3},
        OpCreateEdge{3, V1, V2, "Edge3", OK},

        OpCommit{2},

        OpStart{4},
        OpAddLabel{4, V1, "Label2", SERIALIZATION_ERROR},

        // Flag should remain
        OpCommit{3},

        OpStart{5},
        OpAddLabel{5, V1, "Label3", SERIALIZATION_ERROR},

        // Flag should finally be cleared
        OpCommit{1},

        OpStart{6},
        OpAddLabel{6, V1, "Label4", OK},
        OpCommit{6}
      }
    },

    {
      "PropagatedDelta_MultipleVerticesAllGetFlag",
      {
        OpHoldGc{},

        OpStart{0},
        OpCreateEdge{0, V1, V2, "Edge1", OK},
        OpCheckVertexFlag{V1, false},
        OpCheckVertexFlag{V2, false},

        OpStart{1},
        OpCreateEdge{1, V1, V2, "Edge2", OK},
        OpCheckVertexFlag{V1, true},
        OpCheckVertexFlag{V2, true},

        OpCommit{1},
        OpCheckVertexFlag{V1, true},
        OpCheckVertexFlag{V2, true},

        OpCommit{0},
        OpCheckVertexFlag{V1, false},
        OpCheckVertexFlag{V2, false},
      }
    },

  };
}

// clang-format on

TEST_P(DeltaCorrectnessTest, ExecuteTestCase) { RunTestCase(GetParam()); }

INSTANTIATE_TEST_SUITE_P(DeltaCorrectness, DeltaCorrectnessTest, ::testing::ValuesIn(GetTestCases()),
                         [](const ::testing::TestParamInfo<TestCase> &info) { return info.param.name; });
