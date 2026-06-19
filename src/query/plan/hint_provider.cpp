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

#include "hint_provider.hpp"

namespace memgraph::query::plan {

namespace {
// Maps a scan to the sequential scan type whose hint logic applies. Parallel-execution plans
// rewrite `Filter -> Scan*` into `Filter -> ScanChunk -> ParallelMerge -> ScanParallel*`, so the
// real scan semantics live in the `ScanParallel*` below the chunk (see parallel_rewrite.hpp).
const utils::TypeInfo &EffectiveScanType(const ScanAll &scan) {
  const auto &type = scan.GetTypeInfo();
  // Sequential plans (incl. ScanChunkByEdge, whose type differs) use their own scan type directly.
  if (type != ScanChunk::kType) {
    return type;
  }
  const auto *parallel_merge = scan.input().get();
  if (parallel_merge == nullptr) return type;
  const auto *parallel_scan = parallel_merge->input().get();
  if (parallel_scan == nullptr) return type;

  const auto &parallel_type = parallel_scan->GetTypeInfo();
  if (parallel_type == ScanParallel::kType) return ScanAll::kType;
  if (parallel_type == ScanParallelByLabel::kType) return ScanAllByLabel::kType;
  // Anything else (index-backed scans like ScanParallelByLabelProperties, and full edge scans
  // like ScanParallelByEdge whose hints are out of scope) keeps its own type, so the logic below
  // emits no missing-index hint or count for it.
  return parallel_type;
}
}  // namespace

void PlanHintsProvider::HintIndexUsage(Filter &op) {
  auto *scan_operator = dynamic_cast<ScanAll *>(op.input().get());
  if (scan_operator == nullptr) {
    return;
  }

  auto const scan_symbol = scan_operator->output_symbol_;
  auto const &scan_type = EffectiveScanType(*scan_operator);

  Filters filters;
  filters.CollectFilterExpression(op.expression_, symbol_table_);
  const std::string filtered_labels = ExtractAndJoin(filters.FilteredLabels(scan_symbol),
                                                     [](const auto &item) { return fmt::format(":{0}", item.name); });
  const std::string filtered_properties =
      ExtractAndJoin(filters.FilteredProperties(scan_symbol), std::mem_fn(&PropertyIxPath::AsPathString));
  if (filtered_labels.empty() && filtered_properties.empty()) {
    return;
  }

  if (scan_type == ScanAll::kType) {
    if (!filtered_labels.empty() && !filtered_properties.empty()) {
      hints_.push_back(
          fmt::format("Sequential scan will be used on symbol `{0}` although there is a filter on labels {1} and "
                      "properties {2}. Consider "
                      "creating a label-property index.",
                      scan_symbol.name(),
                      filtered_labels,
                      filtered_properties));
      has_unindexed_scan_ = true;
      return;
    }

    if (!filtered_labels.empty()) {
      hints_.push_back(
          fmt::format("Sequential scan will be used on symbol `{0}` although there is a filter on labels {1}. Consider "
                      "creating a label index.",
                      scan_symbol.name(),
                      filtered_labels));
      has_unindexed_scan_ = true;
      return;
    }
    // Property-only filter with no label: no node index can serve a label-less filter,
    // so there is nothing to suggest and nothing to count.
    return;
  }

  // Label index already used; suboptimal-index hint, not a missing-index scan -> does not
  // increment the unindexed-scan counter.
  if (scan_type == ScanAllByLabel::kType && !filtered_properties.empty()) {
    hints_.push_back(fmt::format(
        "Label index will be used on symbol `{0}` although there is also a filter on properties {1}. Consider "
        "creating a label-property index.",
        scan_symbol.name(),
        filtered_properties));
    return;
  }
}

PlanHintsResult ProvidePlanHints(const LogicalOperator *plan_root, const SymbolTable &symbol_table) {
  PlanHintsProvider plan_hinter(symbol_table);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
  const_cast<LogicalOperator *>(plan_root)->Accept(plan_hinter);

  return PlanHintsResult{
      .hints = plan_hinter.take_hints(),
      .has_unindexed_scan = plan_hinter.has_unindexed_scan(),
  };
}

}  // namespace memgraph::query::plan
