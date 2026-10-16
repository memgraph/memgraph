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

// `reduce(acc = "", x IN list | acc + x)` copies the whole accumulator several times per element,
// making the work quadratic in the list length.
//
// `ns_per_byte_of_result` stays flat while the cost is linear in the result and rises with the
// list length when it is not. `pmr_bytes_per_element` divided by the average accumulator size
// gives the number of copies the evaluation resource sees; it is a lower bound, because the
// temporaries inside `operator+` are `std::allocator` strings that never reach it.

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/eval.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "utils/memory.hpp"

namespace {

constexpr size_t kElementSize = 64;

// Reports allocation volume alongside time.
class CountingResource final : public memgraph::utils::MemoryResource {
 public:
  size_t bytes_allocated() const { return bytes_allocated_; }

  void Reset() { bytes_allocated_ = 0; }

 private:
  void *do_allocate(size_t bytes, size_t alignment) override {
    bytes_allocated_ += bytes;
    return upstream_->allocate(bytes, alignment);
  }

  void do_deallocate(void *p, size_t bytes, size_t alignment) override { upstream_->deallocate(p, bytes, alignment); }

  bool do_is_equal(memgraph::utils::MemoryResource const &other) const noexcept override { return this == &other; }

  memgraph::utils::MemoryResource *upstream_{memgraph::utils::NewDeleteResource()};
  size_t bytes_allocated_{0};
};

std::string ElementText(size_t i) {
  auto text = std::to_string(i);
  text.resize(kElementSize, 'x');
  return text;
}

// NOLINTNEXTLINE(google-runtime-references)
void ReduceStringAccumulate(benchmark::State &state) {
  auto const list_length = static_cast<size_t>(state.range(0));

  memgraph::query::AstStorage ast;
  memgraph::query::SymbolTable symbol_table;

  auto *ident_acc = ast.Create<memgraph::query::Identifier>("acc");
  auto *ident_x = ast.Create<memgraph::query::Identifier>("x");

  std::vector<memgraph::query::Expression *> elements;
  elements.reserve(list_length);
  for (size_t i = 0; i < list_length; ++i) {
    elements.push_back(
        ast.Create<memgraph::query::PrimitiveLiteral>(memgraph::storage::ExternalPropertyValue(ElementText(i))));
  }

  auto *reduce = ast.Create<memgraph::query::Reduce>(
      ast.Create<memgraph::query::Identifier>("acc"),
      ast.Create<memgraph::query::PrimitiveLiteral>(memgraph::storage::ExternalPropertyValue(std::string{})),
      ast.Create<memgraph::query::Identifier>("x"),
      ast.Create<memgraph::query::ListLiteral>(elements),
      ast.Create<memgraph::query::AdditionOperator>(ident_acc, ident_x));

  auto const acc_sym = symbol_table.CreateSymbol("acc", true);
  reduce->accumulator_->MapTo(acc_sym);
  ident_acc->MapTo(acc_sym);
  auto const x_sym = symbol_table.CreateSymbol("x", true);
  reduce->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);

  CountingResource memory;
  // Symbols exist by now, so the frame is sized for both of them.
  memgraph::query::Frame frame(symbol_table.max_position(), &memory);

  std::unique_ptr<memgraph::storage::Storage> db(new memgraph::storage::InMemoryStorage());
  auto storage_dba = db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  memgraph::query::ExecutionContext ctx;
  ctx.db_accessor = &dba;
  ctx.symbol_table = symbol_table;
  ctx.evaluation_context = memgraph::query::EvaluationContext{&memory};
  memgraph::query::ExpressionEvaluator evaluator(&frame, ctx, memgraph::storage::View::NEW);

  size_t result_size = 0;
  memory.Reset();
  for (auto _ : state) {
    auto value = reduce->Accept(evaluator);
    result_size = value.ValueString().size();
    benchmark::DoNotOptimize(result_size);
  }

  auto const iterations = static_cast<double>(state.iterations());
  state.counters["result_bytes"] = static_cast<double>(result_size);
  state.counters["pmr_bytes_per_element"] =
      static_cast<double>(memory.bytes_allocated()) / iterations / static_cast<double>(list_length);
  state.counters["ns_per_byte_of_result"] = benchmark::Counter(
      static_cast<double>(result_size), benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

BENCHMARK(ReduceStringAccumulate)->RangeMultiplier(2)->Range(256, 4096)->Unit(benchmark::kMillisecond);

// One `acc + element` step, with `acc` already grown to `state.range(0)` bytes.
// NOLINTNEXTLINE(google-runtime-references)
void StringConcatOperator(benchmark::State &state) {
  auto const acc_size = static_cast<size_t>(state.range(0));
  CountingResource memory;
  memgraph::query::TypedValue const acc(std::string(acc_size, 'a'), &memory);
  memgraph::query::TypedValue const element(ElementText(0), &memory);

  memory.Reset();
  for (auto _ : state) {
    auto joined = acc + element;
    benchmark::DoNotOptimize(joined);
  }

  state.counters["pmr_bytes_per_step"] =
      static_cast<double>(memory.bytes_allocated()) / static_cast<double>(state.iterations());
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(acc_size));
}

BENCHMARK(StringConcatOperator)->RangeMultiplier(8)->Range(1024, 1U << 19U)->Unit(benchmark::kMicrosecond);

// The floor the step above is measured against: one allocation, one pass over each input.
// NOLINTNEXTLINE(google-runtime-references)
void StringConcatIdeal(benchmark::State &state) {
  auto const acc_size = static_cast<size_t>(state.range(0));
  std::string const acc(acc_size, 'a');
  std::string const element = ElementText(0);

  for (auto _ : state) {
    std::string joined;
    joined.reserve(acc.size() + element.size());
    joined.append(acc);
    joined.append(element);
    benchmark::DoNotOptimize(joined);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(acc_size));
}

BENCHMARK(StringConcatIdeal)->RangeMultiplier(8)->Range(1024, 1U << 19U)->Unit(benchmark::kMicrosecond);

}  // namespace

BENCHMARK_MAIN();
