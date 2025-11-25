// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <memory>
#include <set>
#include <unordered_map>
#include <vector>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frame_change.hpp"
#include "query/frame_change_caching.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/view.hpp"
#include "utils/frame_change_id.hpp"

#include <gtest/gtest.h>

namespace memgraph::query::test {

// RAII guard for temporarily setting spdlog logging level
// Captures current level on construction, restores it on destruction
struct ScopedLogLevel {
  explicit ScopedLogLevel(const spdlog::level::level_enum level = spdlog::level::off)
      : previous_level_(spdlog::default_logger()->level()) {
    // Set the desired logging level
    spdlog::set_level(level);
  }

  ~ScopedLogLevel() {
    // Restore the previous logging level
    spdlog::set_level(previous_level_);
  }

 private:
  spdlog::level::level_enum previous_level_;
};

// User-defined literal for creating TypedValue from integer literals
inline auto operator""_tv(const unsigned long long value) -> TypedValue {
  return TypedValue(static_cast<int64_t>(value));
}

// User-defined literal for creating TypedValue from floating-point literals
inline auto operator""_tv(const long double value) -> TypedValue { return TypedValue(static_cast<double>(value)); }

// Helper to create a list of integers [start..end] for testing
inline auto CreateIntList(const int64_t start, const int64_t end) -> TypedValue {
  TypedValue::TVector list_values{};
  for (int64_t i = start; i <= end; ++i) {
    list_values.emplace_back(TypedValue(i));
  }
  return TypedValue(std::move(list_values));
}

// Minimal storage component for unit tests
// Provides database setup and accessor management
struct StorageComponent {
  storage::Config config;
  std::unique_ptr<storage::Storage> db;
  std::unique_ptr<storage::Storage::Accessor> storage_dba;
  DbAccessor dba;

  StorageComponent()
      : db(std::make_unique<storage::InMemoryStorage>(config)), storage_dba(db->Access()), dba(storage_dba.get()) {}
};

// Query building component for unit tests
// Provides AST storage and helpers to create query expressions
struct QueryBuildingComponent {
  AstStorage ast_storage;
  SymbolTable symbol_table;

  // Helper to create an identifier (returns both the AST node and symbol)
  auto CreateIdentifier(const std::string &name) -> std::pair<Identifier *, Symbol> {
    auto *id = ast_storage.Create<Identifier>(name, true);
    const auto symbol = symbol_table.CreateSymbol(name, true);
    id->MapTo(symbol);
    return {id, symbol};
  }

  // Helper to create range(start, end) function call
  auto CreateRangeFunction(int64_t start, int64_t end) -> Function * {
    return ast_storage.Create<Function>("range", std::vector<Expression *>{ast_storage.Create<PrimitiveLiteral>(start),
                                                                           ast_storage.Create<PrimitiveLiteral>(end)});
  }

  // Helper to create a function with given name and arguments
  auto CreateFunction(const std::string &name, std::vector<Expression *> args = {}) -> Function * {
    return ast_storage.Create<Function>(name, args);
  }

  // Helper to create a list literal with given expressions
  template <typename... Args>
  // should all be pointers derived from Expression
  auto CreateListLiteral(Args &&...args) -> ListLiteral * {
    return ast_storage.Create<ListLiteral>(std::vector<Expression *>{std::forward<Args>(args)...});
  }

  // Helper to create a primitive literal
  template <typename T>
  auto CreateLiteral(T value) -> PrimitiveLiteral * {
    return ast_storage.Create<PrimitiveLiteral>(value);
  }

  // Helper to create an InList operator
  auto CreateInListOperator(Expression *lhs, Expression *rhs) -> InListOperator * {
    return ast_storage.Create<InListOperator>(lhs, rhs);
  }

  // Helper to create a RegexMatch operator
  auto CreateRegexMatch(Expression *string_expr, Expression *regex_expr) -> RegexMatch * {
    return ast_storage.Create<RegexMatch>(string_expr, regex_expr);
  }
};

// Query evaluation component for unit tests
// Provides frame, context, and evaluation helpers
struct QueryEvaluationComponent {
  QueryBuildingComponent &builder;
  StorageComponent &storage_component;

  memgraph::utils::MonotonicBufferResource mem{1024 * 1024};
  EvaluationContext ctx{.memory = &mem, .timestamp = QueryTimestamp()};
  Frame frame{128};
  FrameChangeCollector frame_change_collector{memgraph::utils::NewDeleteResource()};

  QueryEvaluationComponent(QueryBuildingComponent &builder, StorageComponent &storage_component)
      : builder(builder), storage_component(storage_component) {}

  // Helper to populate a symbol with a value in the frame
  void SetSymbolValue(const Symbol &symbol, const TypedValue &value) {
    FrameWriter frame_writer(frame, &frame_change_collector, ctx.memory);
    frame_writer.Write(symbol, value);
  }

  // Helper to prepare caching for all InList and RegexMatch operators in the AST
  void PrepareCaching() { ::memgraph::query::PrepareCaching(builder.ast_storage, &frame_change_collector); }

  // Helper to evaluate expression with frame change collector
  // Optionally accepts a map of symbols to values to populate just before evaluation
  auto Eval(Expression *expr, const std::unordered_map<Symbol, TypedValue> &symbol_values = {}) -> TypedValue {
    // Populate frame with provided symbol values just before evaluation
    if (!symbol_values.empty()) {
      FrameWriter frame_writer(frame, &frame_change_collector, ctx.memory);
      for (const auto &[symbol, value] : symbol_values) {
        frame_writer.Write(symbol, value);
      }
    }

    ctx.properties = NamesToProperties(builder.ast_storage.properties_, &storage_component.dba);
    ctx.labels = NamesToLabels(builder.ast_storage.labels_, &storage_component.dba);
    ExpressionEvaluator eval(&frame, builder.symbol_table, ctx, &storage_component.dba, storage::View::OLD,
                             &frame_change_collector);
    return expr->Accept(eval);
  }

  // Helper to verify an InList is tracked for caching
  void ExpectTrackedForCaching(const InListOperator *in_list, const bool should_be_tracked = true) const {
    const auto cached_id = memgraph::utils::GetFrameChangeId(*in_list);
    if (should_be_tracked) {
      EXPECT_TRUE(frame_change_collector.IsInlistKeyTracked(cached_id));
    } else {
      EXPECT_FALSE(frame_change_collector.IsInlistKeyTracked(cached_id));
    }
  }

  // Helper to verify a RegexMatch is tracked for caching
  void ExpectTrackedForCaching(const RegexMatch *regex_match, const bool should_be_tracked = true) const {
    const auto cached_id = memgraph::utils::GetFrameChangeId(*regex_match);
    if (should_be_tracked) {
      EXPECT_TRUE(frame_change_collector.IsRegexKeyTracked(cached_id));
    } else {
      EXPECT_FALSE(frame_change_collector.IsRegexKeyTracked(cached_id));
    }
  }

  // Helper to get cached value reference for InList
  auto GetInlistCachedValue(const memgraph::utils::FrameChangeId &cached_id) const
      -> std::optional<std::reference_wrapper<const CachedSet>> {
    return frame_change_collector.TryGetInlistCachedValue(cached_id);
  }

  // Helper to verify InList cache is populated
  void ExpectInListCachePopulated(const InListOperator *in_list) const {
    const auto cached_id = memgraph::utils::GetFrameChangeId(*in_list);
    const auto cached_value_ref = GetInlistCachedValue(cached_id);
    ASSERT_TRUE(cached_value_ref.has_value()) << "Cache should be populated";
  }

  // Helper to verify InList cache is NOT populated
  void ExpectInListCacheNotPopulated(const InListOperator *in_list,
                                     const std::string &reason = "Cache should not be populated") const {
    const auto cached_id = memgraph::utils::GetFrameChangeId(*in_list);
    const auto cached_value_ref = GetInlistCachedValue(cached_id);
    EXPECT_FALSE(cached_value_ref.has_value()) << reason;
  }

  // Helper to verify InList cache contains specific values
  auto ExpectInListCacheContains(const InListOperator *in_list, const std::vector<TypedValue> &should_contain,
                                 const std::vector<TypedValue> &should_not_contain = {}) const -> void {
    const auto cached_id = memgraph::utils::GetFrameChangeId(*in_list);

    const auto cached_value_ref = GetInlistCachedValue(cached_id);
    ASSERT_TRUE(cached_value_ref.has_value()) << "Cache should be populated";

    const auto &cached_value = cached_value_ref->get();
    for (const auto &value : should_contain) {
      EXPECT_TRUE(cached_value.Contains(value)) << "Cache missing expected value";
    }
    for (const auto &value : should_not_contain) {
      EXPECT_FALSE(cached_value.Contains(value)) << "Cache contains unexpected value";
    }
  }

  // Helper to verify RegexMatch cache is populated
  void ExpectCachePopulated(const RegexMatch *regex_match) const {
    const auto cached_id = memgraph::utils::GetFrameChangeId(*regex_match);
    const auto cached_value_ref = frame_change_collector.TryGetRegexCachedValue(cached_id);
    ASSERT_TRUE(cached_value_ref.has_value()) << "Regex cache should be populated";
  }

  // Helper to verify RegexMatch cache is NOT populated
  void ExpectCacheNotPopulated(const RegexMatch *regex_match,
                               const std::string &reason = "Regex cache should not be populated") const {
    const auto cached_id = memgraph::utils::GetFrameChangeId(*regex_match);
    const auto cached_value_ref = frame_change_collector.TryGetRegexCachedValue(cached_id);
    EXPECT_FALSE(cached_value_ref.has_value()) << reason;
  }
};

}  // namespace memgraph::query::test
