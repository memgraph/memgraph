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

#include <gtest/gtest.h>

#include "query_plan_common.hpp"

#include "query/db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/hint_provider.hpp"
#include "storage/v2/inmemory/storage.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using namespace memgraph::storage;

class HintProviderSuite : public ::testing::Test {
 protected:
  std::unique_ptr<Storage> db = std::make_unique<InMemoryStorage>();
  std::optional<std::unique_ptr<Storage::Accessor>> storage_dba;
  std::optional<memgraph::query::DbAccessor> dba;
  LabelId label = db->NameToLabel("label");
  PropertyId property = db->NameToProperty("property");
  PropertyId other_property = db->NameToProperty("other_property");

  std::vector<std::shared_ptr<LogicalOperator>> pattern_filters_{};

  AstStorage storage;
  SymbolTable symbol_table;
  View view = View::OLD;
  int symbol_count = 0;

  void SetUp() {
    storage_dba.emplace(db->Access(memgraph::storage::WRITE));
    dba.emplace(storage_dba->get());
  }

  Symbol NextSymbol() { return symbol_table.CreateSymbol("Symbol" + std::to_string(symbol_count++), true); }

  void VerifyHintMessages(LogicalOperator *plan, const std::vector<std::string> &expected_messages,
                          bool expected_has_unindexed_scan) {
    auto const result = ProvidePlanHints(plan, symbol_table);
    auto const &messages = result.hints;

    ASSERT_EQ(expected_messages.size(), messages.size());
    ASSERT_EQ(expected_has_unindexed_scan, result.has_unindexed_scan);

    for (size_t i = 0; i < messages.size(); i++) {
      const auto &expected_message = expected_messages[i];
      const auto &actual_message = messages[i];

      ASSERT_EQ(expected_message, actual_message);
    }
  }

  std::vector<LabelIx> GetLabelIx(std::vector<LabelId> labels) {
    std::vector<LabelIx> label_ixs{};
    for (const auto &label : labels) {
      label_ixs.emplace_back(storage.GetLabelIx(db->LabelToName(label)));
    }

    return label_ixs;
  }
};

TEST_F(HintProviderSuite, HintWhenFilteringByLabel) {
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto *filter_expr = storage.template Create<LabelsTest>(scan_all.node_->identifier_, GetLabelIx({label}));

  auto filter = std::make_shared<Filter>(scan_all.op_, pattern_filters_, filter_expr);

  const std::vector<std::string> expected_messages{
      "Sequential scan will be used on symbol `n` although there is a filter on labels :label. Consider "
      "creating a label index."};

  VerifyHintMessages(filter.get(), expected_messages, true);
}

TEST_F(HintProviderSuite, DontHintWhenLabelOperatorPresent) {
  auto scan_all_by_label = MakeScanAllByLabel(storage, symbol_table, "n", label);
  auto produce = MakeProduce(scan_all_by_label.op_, nullptr);

  const std::vector<std::string> expected_messages{};

  VerifyHintMessages(produce.get(), expected_messages, false);
}

TEST_F(HintProviderSuite, HintWhenFilteringByLabelAndProperty) {
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto *filter_expr = storage.template Create<AndOperator>(
      storage.template Create<LabelsTest>(scan_all.node_->identifier_, GetLabelIx({label})),
      EQ(PROPERTY_LOOKUP(*dba, scan_all.node_->identifier_, property), LITERAL(42)));

  auto filter = std::make_shared<Filter>(scan_all.op_, pattern_filters_, filter_expr);

  const std::vector<std::string> expected_messages{
      "Sequential scan will be used on symbol `n` although there is a filter on labels :label and properties property. "
      "Consider "
      "creating a label-property index."};

  VerifyHintMessages(filter.get(), expected_messages, true);
}

TEST_F(HintProviderSuite, DontHintWhenLabelPropertyOperatorPresent) {
  auto scan_all_by_label_prop_value = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, property, storage.template Create<Identifier>("n"));
  auto produce = MakeProduce(scan_all_by_label_prop_value.op_, nullptr);

  const std::vector<std::string> expected_messages{};

  VerifyHintMessages(produce.get(), expected_messages, false);
}

TEST_F(HintProviderSuite, DontHintWhenLabelPropertyOperatorPresentAndAdditionalPropertyFilterPresent) {
  auto scan_all_by_label_prop_value = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, property, storage.template Create<Identifier>("n"));

  auto *filter_expr =
      EQ(PROPERTY_LOOKUP(*dba, scan_all_by_label_prop_value.node_->identifier_, other_property), LITERAL(42));
  auto filter = std::make_shared<Filter>(scan_all_by_label_prop_value.op_, pattern_filters_, filter_expr);
  const std::vector<std::string> expected_messages{};

  VerifyHintMessages(filter.get(), expected_messages, false);
}

TEST_F(HintProviderSuite, HintWhenLabelOperatorPresentButFilteringAlsoByProperty) {
  auto scan_all_by_label = MakeScanAllByLabel(storage, symbol_table, "n", label);
  auto *filter_expr = EQ(PROPERTY_LOOKUP(*dba, scan_all_by_label.node_->identifier_, property), LITERAL(42));

  auto filter = std::make_shared<Filter>(scan_all_by_label.op_, pattern_filters_, filter_expr);

  const std::vector<std::string> expected_messages{
      "Label index will be used on symbol `n` although there is also a filter on properties property. "
      "Consider "
      "creating a label-property index."};

  VerifyHintMessages(filter.get(), expected_messages, false);
}

TEST_F(HintProviderSuite, DoubleHintWhenCartesianInFilters) {
  auto first_scan_all = MakeScanAll(storage, symbol_table, "n");
  auto *first_filter_expr = storage.template Create<LabelsTest>(first_scan_all.node_->identifier_, GetLabelIx({label}));
  auto first_filter = std::make_shared<Filter>(first_scan_all.op_, pattern_filters_, first_filter_expr);

  auto second_scan_all = MakeScanAll(storage, symbol_table, "m");
  auto *second_filter_expr =
      storage.template Create<LabelsTest>(second_scan_all.node_->identifier_, GetLabelIx({label}));
  auto second_filter = std::make_shared<Filter>(second_scan_all.op_, pattern_filters_, second_filter_expr);

  const std::vector<Symbol> empty_symbols{};

  auto cartesian = std::make_shared<Cartesian>(first_filter, empty_symbols, second_filter, empty_symbols);

  const std::vector<std::string> expected_messages{
      "Sequential scan will be used on symbol `n` although there is a filter on labels :label. Consider "
      "creating a label index.",
      "Sequential scan will be used on symbol `m` although there is a filter on labels :label. Consider "
      "creating a label index."};

  VerifyHintMessages(cartesian.get(), expected_messages, true);
}

TEST_F(HintProviderSuite, DontHintOrCountWhenFilteringByPropertyOnlyOnSequentialScan) {
  // A label-less property filter cannot be served by any node index (node indices require a
  // label), so there is nothing to suggest and nothing to count.
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto *filter_expr = EQ(PROPERTY_LOOKUP(*dba, scan_all.node_->identifier_, property), LITERAL(42));
  auto filter = std::make_shared<Filter>(scan_all.op_, pattern_filters_, filter_expr);

  const std::vector<std::string> expected_messages{};

  VerifyHintMessages(filter.get(), expected_messages, false);
}

// Builds the operator chain produced by the parallel-execution rewrite for a scan whose
// output is `node_symbol`: Filter -> ScanChunk -> ParallelMerge -> `parallel_scan`.
std::shared_ptr<LogicalOperator> WrapInParallelChunk(std::shared_ptr<ScanParallel> parallel_scan, Symbol node_symbol,
                                                     Symbol state_symbol, View view) {
  auto parallel_merge = std::make_shared<ParallelMerge>(std::move(parallel_scan));
  return std::make_shared<ScanChunk>(parallel_merge, node_symbol, view, state_symbol);
}

TEST_F(HintProviderSuite, HintWhenFilteringByLabelOnParallelScan) {
  // Parallel-execution rewrite of an unindexed `ScanAll`: the counter must still fire.
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto state_symbol = NextSymbol();
  auto parallel_scan = std::make_shared<ScanParallel>(std::make_shared<Once>(), view, 2, state_symbol);
  auto scan_chunk = WrapInParallelChunk(parallel_scan, scan_all.sym_, state_symbol, view);

  auto *filter_expr = storage.template Create<LabelsTest>(scan_all.node_->identifier_, GetLabelIx({label}));
  auto filter = std::make_shared<Filter>(scan_chunk, pattern_filters_, filter_expr);

  const std::vector<std::string> expected_messages{
      "Sequential scan will be used on symbol `n` although there is a filter on labels :label. Consider "
      "creating a label index."};

  VerifyHintMessages(filter.get(), expected_messages, true);
}

TEST_F(HintProviderSuite, DontCountParallelLabelScanWithPropertyFilter) {
  // Parallel-execution rewrite of a `ScanAllByLabel`: a label index is already used, so this
  // is a suboptimal-index hint, not a missing-index scan -> no count.
  auto scan_all = MakeScanAll(storage, symbol_table, "n");
  auto state_symbol = NextSymbol();
  auto parallel_scan = std::make_shared<ScanParallelByLabel>(std::make_shared<Once>(), view, 2, state_symbol, label);
  auto scan_chunk = WrapInParallelChunk(parallel_scan, scan_all.sym_, state_symbol, view);

  auto *filter_expr = EQ(PROPERTY_LOOKUP(*dba, scan_all.node_->identifier_, property), LITERAL(42));
  auto filter = std::make_shared<Filter>(scan_chunk, pattern_filters_, filter_expr);

  const std::vector<std::string> expected_messages{
      "Label index will be used on symbol `n` although there is also a filter on properties property. "
      "Consider "
      "creating a label-property index."};

  VerifyHintMessages(filter.get(), expected_messages, false);
}
