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

// What the `%` label wildcard costs per row. The label set is a small vector with room for two labels
// inline, so reading it to test only emptiness starts allocating at the third label; the range below spans
// that point. Each case runs over committed vertices whose deltas have been reclaimed, which is the state a
// scan over loaded data reads.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/eval.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"
#include "tests/test_commit_args_helper.hpp"

namespace {

constexpr int64_t kVertices = 20000;
constexpr size_t kFrameMemoryBlockSize = 1UL * 1024UL * 1024UL;

/// Committed vertices carrying @p labels_per_vertex labels each, with the deltas of the loading transaction
/// already reclaimed.
struct LoadedGraph {
  std::unique_ptr<memgraph::storage::Storage> db{new memgraph::storage::InMemoryStorage()};

  explicit LoadedGraph(int64_t labels_per_vertex) {
    {
      auto acc = db->Access(memgraph::storage::WRITE);
      std::vector<memgraph::storage::LabelId> labels;
      labels.reserve(labels_per_vertex);
      for (int64_t i = 0; i < labels_per_vertex; ++i) {
        labels.push_back(acc->NameToLabel("LABEL" + std::to_string(i)));
      }
      for (int64_t v = 0; v < kVertices; ++v) {
        auto vertex = acc->CreateVertex();
        for (auto label : labels) {
          auto added = vertex.AddLabel(label);
          if (!added.has_value()) std::abort();
        }
      }
      if (!acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value()) std::abort();
    }
    // Until the loading transaction's deltas are reclaimed every read applies them, which is not the state a
    // scan over loaded data reads.
    db->FreeMemory();
  }
};

/// Whether every vertex carries a label, read through @p answer. The return value is consumed so the read
/// cannot be folded away.
template <typename Answer>
void CountLabelled(benchmark::State &state, const Answer &answer) {
  LoadedGraph graph{state.range(0)};
  auto storage_dba = graph.db->Access(memgraph::storage::READ);
  memgraph::query::DbAccessor dba(storage_dba.get());

  int64_t rows = 0;
  while (state.KeepRunningBatch(kVertices)) {
    for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
      benchmark::DoNotOptimize(answer(vertex));
      ++rows;
    }
  }
  state.SetItemsProcessed(rows);
}

/// The label set, read to test only that it is not empty.
void WildcardViaLabelSet(benchmark::State &state) {
  CountLabelled(state, [](const memgraph::query::VertexAccessor &vertex) {
    auto labels = vertex.Labels(memgraph::storage::View::OLD);
    return labels.has_value() && !labels->empty();
  });
}

/// The same question asked of the vertex directly.
void WildcardViaAnyLabel(benchmark::State &state) {
  CountLabelled(state, [](const memgraph::query::VertexAccessor &vertex) {
    auto any = vertex.HasAnyLabel(memgraph::storage::View::OLD);
    return any.has_value() && *any;
  });
}

/// `n:%` as a query evaluates it, so the per-row cost includes reaching the vertex through the evaluator.
void WildcardExpression(benchmark::State &state) {
  LoadedGraph graph{state.range(0)};
  auto storage_dba = graph.db->Access(memgraph::storage::READ);
  memgraph::query::DbAccessor dba(storage_dba.get());

  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;
  auto *identifier = ast.Create<memgraph::query::Identifier>("n", true);
  auto node_symbol = symbol_table.CreateSymbol("n", true);
  identifier->MapTo(node_symbol);
  auto *test = MakeLabelsTest(ast, identifier, memgraph::query::LabelTerm{memgraph::query::LabelTerm::Wildcard{}});

  memgraph::utils::MonotonicBufferResource memory{kFrameMemoryBlockSize};
  memgraph::query::Frame frame(symbol_table.max_position(), &memory);
  memgraph::query::ExecutionContext ctx;
  ctx.db_accessor = &dba;
  ctx.symbol_table = symbol_table;
  ctx.evaluation_context = memgraph::query::EvaluationContext{&memory};
  ctx.evaluation_context.labels = memgraph::query::NamesToLabels(ast.labels_, &dba);
  memgraph::query::ExpressionEvaluator evaluator(&frame, ctx, memgraph::storage::View::OLD);
  auto frame_writer = frame.GetFrameWriter(nullptr, &memory);

  int64_t rows = 0;
  while (state.KeepRunningBatch(kVertices)) {
    for (auto vertex : dba.Vertices(memgraph::storage::View::OLD)) {
      frame_writer.Write(node_symbol, memgraph::query::TypedValue(vertex, &memory));
      benchmark::DoNotOptimize(test->Accept(evaluator));
      ++rows;
    }
  }
  state.SetItemsProcessed(rows);
}

// One label and two fit inline; three and beyond do not.
void LabelCounts(benchmark::internal::Benchmark *bench) {
  for (int64_t labels : {1, 2, 3, 4, 8}) bench->Arg(labels);
  bench->Unit(benchmark::kMillisecond);
}

}  // namespace

BENCHMARK(WildcardViaLabelSet)->Apply(LabelCounts);
BENCHMARK(WildcardViaAnyLabel)->Apply(LabelCounts);
BENCHMARK(WildcardExpression)->Apply(LabelCounts);

BENCHMARK_MAIN();
