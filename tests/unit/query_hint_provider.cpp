// Copyright 2023 Memgraph Ltd.
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
    ASSERT_FALSE(db->CreateIndex(label).HasError());
    ASSERT_FALSE(db->CreateIndex(label, property).HasError());
    storage_dba.emplace(db->Access());
    dba.emplace(storage_dba->get());
  }

  Symbol NextSymbol() { return symbol_table.CreateSymbol("Symbol" + std::to_string(symbol_count++), true); }

  void VerifyHintMessages(LogicalOperator *plan, const std::vector<std::string> &expected_messages) {
    auto messages = ProvidePlanHints(plan, symbol_table);

    ASSERT_EQ(expected_messages.size(), messages.size());

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

  VerifyHintMessages(filter.get(), expected_messages);
}

TEST_F(HintProviderSuite, DontHintWhenLabelOperatorPresent) {
  auto scan_all_by_label = MakeScanAllByLabel(storage, symbol_table, "n", label);
  auto produce = MakeProduce(scan_all_by_label.op_, nullptr);

  const std::vector<std::string> expected_messages{};

  VerifyHintMessages(produce.get(), expected_messages);
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
      "creating a label property index."};

  VerifyHintMessages(filter.get(), expected_messages);
}

TEST_F(HintProviderSuite, DontHintWhenLabelPropertyOperatorPresent) {
  auto scan_all_by_label_prop_value = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, property, "property", storage.template Create<Identifier>("n"));
  auto produce = MakeProduce(scan_all_by_label_prop_value.op_, nullptr);

  const std::vector<std::string> expected_messages{};

  VerifyHintMessages(produce.get(), expected_messages);
}

TEST_F(HintProviderSuite, DontHintWhenLabelPropertyOperatorPresentAndAdditionalPropertyFilterPresent) {
  auto scan_all_by_label_prop_value = MakeScanAllByLabelPropertyValue(
      storage, symbol_table, "n", label, property, "property", storage.template Create<Identifier>("n"));

  auto *filter_expr =
      EQ(PROPERTY_LOOKUP(*dba, scan_all_by_label_prop_value.node_->identifier_, other_property), LITERAL(42));
  auto filter = std::make_shared<Filter>(scan_all_by_label_prop_value.op_, pattern_filters_, filter_expr);
  const std::vector<std::string> expected_messages{};

  VerifyHintMessages(filter.get(), expected_messages);
}

TEST_F(HintProviderSuite, HintWhenLabelOperatorPresentButFilteringAlsoByProperty) {
  auto scan_all_by_label = MakeScanAllByLabel(storage, symbol_table, "n", label);
  auto *filter_expr = EQ(PROPERTY_LOOKUP(*dba, scan_all_by_label.node_->identifier_, property), LITERAL(42));

  auto filter = std::make_shared<Filter>(scan_all_by_label.op_, pattern_filters_, filter_expr);

  const std::vector<std::string> expected_messages{
      "Label index will be used on symbol `n` although there is also a filter on properties property. "
      "Consider "
      "creating a label property index."};

  VerifyHintMessages(filter.get(), expected_messages);
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

  VerifyHintMessages(cartesian.get(), expected_messages);
}
