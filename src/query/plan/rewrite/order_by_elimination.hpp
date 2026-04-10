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

/// @file
/// ORDER BY elimination helper.  Shared between IndexLookupRewriter and
/// EdgeIndexRewriter so that both vertex-property and edge-property index
/// scans can satisfy an ORDER BY without an explicit sort.

#pragma once

#include <algorithm>
#include <ranges>
#include <type_traits>
#include <variant>
#include <vector>

#include "query/plan/operator.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/property_path.hpp"

namespace memgraph::query::plan {

/// Helper that tracks ORDER BY operators during a plan-tree visitor walk and
/// eliminates them when an index scan already provides the required order.
///
/// @tparam TDbAccessor  The database accessor type (provides NameToProperty).
///
/// Usage: the rewriter owns an instance and calls the On* hooks from the
/// appropriate Pre/PostVisit methods.  The helper reads (but does not own)
/// the rewriter's `prev_ops_` stack.
template <class TDbAccessor>
class OrderByEliminator {
 public:
  // ---------- types --------------------------------------------------------

  struct OrderByEntry {
    storage::PropertyPath resolved;          // filled once the property path is known
    Symbol::Position_t source_position{-1};  // symbol table position -- used for all matching

    [[nodiscard]] bool has_path() const { return !resolved.empty(); }

    // could this entry's property overlap with the given property?
    // unresolved entries conservatively return true
    [[nodiscard]] bool may_overlap(storage::PropertyId prop) const { return !has_path() || resolved[0] == prop; }
  };

  using ProvidedScan = std::variant<const ScanAllByLabelProperties *, const ScanAllByEdgeTypePropertyRange *,
                                    const ScanAllByEdgePropertyRange *, const ScanAllByEdgeTypePropertyValue *,
                                    const ScanAllByEdgePropertyValue *>;

  struct OrderByInfo {
    OrderBy *op{nullptr};
    std::vector<OrderByEntry> entries;
    std::vector<ProvidedScan> provided_scans;  // accumulated bottom-up (outermost first)
    bool well_formed{false};
    bool order_preserving_path{true};  // set false if walk from first scan to OrderBy fails

    [[nodiscard]] bool has_pending_entries() const {
      return std::ranges::any_of(entries, [](const auto &e) { return !e.has_path(); });
    }
  };

  // ---------- construction -------------------------------------------------

  OrderByEliminator(TDbAccessor *db, const std::vector<LogicalOperator *> &prev_ops) : db_(db), prev_ops_(prev_ops) {}

  // ---------- hooks called by the rewriter ---------------------------------

  void OnPreVisitOrderBy(OrderBy &op) { order_by_stack_.emplace_back(ExtractOrderByInfo(op)); }

  bool OnPostVisitOrderBy(OrderBy & /*op*/) {
    DMG_ASSERT(!order_by_stack_.empty(), "OrderBy stack underflow in PostVisit");
    const auto &ctx = order_by_stack_.back();
    const bool eliminate = CanEliminate(ctx);
    order_by_stack_.pop_back();
    return eliminate;
  }

  void OnPreVisitProduce(Produce *produce) {
    if (auto *ctx = ActiveContext()) {
      ResolveDownward(produce, *ctx);
    }
  }

  void RecordVertexScan(const ScanAllByLabelProperties *scan) {
    if (auto *ctx = ActiveContext()) {
      if (ctx->provided_scans.empty()) WalkAndCheckOrderPreserving(*ctx);
      ctx->provided_scans.emplace_back(scan);
    }
  }

  template <typename EdgeScan>
  void RecordEdgeScan(const EdgeScan *scan) {
    static_assert(std::is_same_v<EdgeScan, ScanAllByEdgeTypePropertyRange> ||
                      std::is_same_v<EdgeScan, ScanAllByEdgePropertyRange> ||
                      std::is_same_v<EdgeScan, ScanAllByEdgeTypePropertyValue> ||
                      std::is_same_v<EdgeScan, ScanAllByEdgePropertyValue>,
                  "Only range and exact-value scans provide ordered iteration");
    if (auto *ctx = ActiveContext()) {
      if (ctx->provided_scans.empty()) WalkAndCheckOrderPreserving(*ctx);
      ctx->provided_scans.emplace_back(scan);
    }
  }

  /// Try to record an edge scan for ORDER BY elimination.
  /// Dispatches to RecordEdgeScan for scan types that provide ordered iteration.
  void TryRecordEdgeScan(const LogicalOperator *op) {
    if (const auto *etr = dynamic_cast<const ScanAllByEdgeTypePropertyRange *>(op)) {
      RecordEdgeScan(etr);
    } else if (const auto *epr = dynamic_cast<const ScanAllByEdgePropertyRange *>(op)) {
      RecordEdgeScan(epr);
    } else if (const auto *etv = dynamic_cast<const ScanAllByEdgeTypePropertyValue *>(op)) {
      RecordEdgeScan(etv);
    } else if (const auto *epv = dynamic_cast<const ScanAllByEdgePropertyValue *>(op)) {
      RecordEdgeScan(epv);
    }
  }

  void MarkFirstScanUnrecorded() {
    if (auto *ctx = ActiveContext()) {
      if (ctx->provided_scans.empty()) ctx->order_preserving_path = false;
    }
  }

 private:
  TDbAccessor *db_;
  const std::vector<LogicalOperator *> &prev_ops_;
  std::vector<OrderByInfo> order_by_stack_;

  storage::PropertyId GetProperty(const PropertyIx &prop) const { return db_->NameToProperty(prop.name); }

  // resolve a property path from AST PropertyIx nodes to storage PropertyIds
  storage::PropertyPath ResolvePropertyPath(const auto &path) const {
    auto to_id = [this](const auto &pix) -> storage::PropertyId { return GetProperty(pix); };
    return storage::PropertyPath{std::ranges::to<std::vector<storage::PropertyId>>(std::views::transform(path, to_id))};
  }

  OrderByInfo *ActiveContext() {
    if (order_by_stack_.empty() || !order_by_stack_.back().well_formed) return nullptr;
    return &order_by_stack_.back();
  }

  OrderByInfo ExtractOrderByInfo(OrderBy &op) const {
    OrderByInfo info;
    info.op = &op;  // non-const: SetOnParent needs op.input() later

    const auto &orderings = op.compare_.orderings();
    const auto &order_by_exprs = op.order_by_;

    if (orderings.empty()) return info;

    // TODO(ivan): support DESC -- SkipList indexes can be iterated in reverse
    const bool all_asc =
        std::ranges::all_of(orderings, [](const auto &ord) { return ord.ordering() == Ordering::ASC; });
    if (!all_asc) return info;

    for (const auto *expr : order_by_exprs) {
      if (const auto *prop_lookup = dynamic_cast<const PropertyLookup *>(expr)) {
        const auto *ident = dynamic_cast<const Identifier *>(prop_lookup->expression_);
        if (!ident) return info;

        info.entries.emplace_back(ResolvePropertyPath(prop_lookup->property_path_), ident->symbol_pos_);
      } else if (const auto *ident = dynamic_cast<const Identifier *>(expr)) {
        info.entries.emplace_back(storage::PropertyPath{}, ident->symbol_pos_);
      } else {
        return info;
      }
    }

    info.well_formed = true;
    return info;
  }

  // -- order-preserving classification --------------------------------------

  static bool IsOrderPreserving(const utils::TypeInfo &type_info) {
    return type_info == Filter::kType || type_info == ConstructNamedPath::kType ||
           type_info == EdgeUniquenessFilter::kType || type_info == Limit::kType || type_info == Skip::kType ||
           type_info == Expand::kType || type_info == ExpandVariable::kType || type_info == Optional::kType ||
           type_info == Distinct::kType || type_info == EvaluatePatternFilter::kType ||
           type_info == RollUpApply::kType || type_info == Apply::kType || type_info == Unwind::kType ||
           type_info == Produce::kType || type_info == Accumulate::kType || type_info == SetLabels::kType ||
           type_info == RemoveLabels::kType;
  }

  static bool IsScanAllVariant(const utils::TypeInfo &type_info) {
    return type_info == ScanAll::kType || type_info == ScanAllByLabel::kType ||
           type_info == ScanAllByLabelProperties::kType || type_info == ScanAllById::kType ||
           type_info == ScanAllByPointDistance::kType || type_info == ScanAllByPointWithinbbox::kType ||
           type_info == ScanAllByEdge::kType || type_info == ScanAllByEdgeType::kType ||
           type_info == ScanAllByEdgeTypeProperty::kType || type_info == ScanAllByEdgeTypePropertyValue::kType ||
           type_info == ScanAllByEdgeTypePropertyRange::kType || type_info == ScanAllByEdgeProperty::kType ||
           type_info == ScanAllByEdgePropertyValue::kType || type_info == ScanAllByEdgePropertyRange::kType ||
           type_info == ScanAllByEdgeId::kType;
  }

  /// Check if a mutation operator (SetProperty, RemoveProperty) modifies a property
  /// that does not overlap with the ORDER BY columns. Returns true only for mutations
  /// that provably leave the ordering intact.
  static bool IsMutationOrderPreserving(const LogicalOperator &op, const OrderByInfo &ctx) {
    // SetProperty/RemoveProperty have a statically-known PropertyId. SetProperties
    // (SET n = expr / SET n += expr) is not handled because the map expression is
    // opaque at plan time and may touch any property, so it falls through to false.
    // TODO: SetProperties with a MapLiteral RHS could be handled -- the map keys are
    // literal PropertyIx values, so we could check overlap. Needs db_ access to resolve.
    const auto property_overlaps_order_by = [&ctx](storage::PropertyId prop) {
      return std::ranges::any_of(ctx.entries, [prop](const auto &entry) { return entry.may_overlap(prop); });
    };

    if (const auto *sp = dynamic_cast<const SetProperty *>(&op)) {
      return !property_overlaps_order_by(sp->property_);
    }
    if (const auto *rp = dynamic_cast<const RemoveProperty *>(&op)) {
      return !property_overlaps_order_by(rp->property_);
    }
    return false;
  }

  /// Would this operator break the row order established by an index scan?
  static bool BreaksOrder(const LogicalOperator &op, const OrderByInfo &ctx) {
    const auto &ti = op.GetTypeInfo();
    return !IsOrderPreserving(ti) && !IsScanAllVariant(ti) && !IsMutationOrderPreserving(op, ctx);
  }

  void WalkAndCheckOrderPreserving(OrderByInfo &ctx) const {
    for (auto it = prev_ops_.rbegin(); it != prev_ops_.rend() && *it != ctx.op; ++it) {
      if (BreaksOrder(**it, ctx)) {
        ctx.order_preserving_path = false;
        break;
      }
    }
  }

  // -- alias resolution -----------------------------------------------------

  void ResolveDownward(Produce *produce, OrderByInfo &info) const {
    // step 1: track resolved entries through renames (e.g. Produce `n AS m` -> follow to inner symbol)
    for (auto &entry : info.entries) {
      DMG_ASSERT(entry.source_position != -1, "ORDER BY entry has unmapped symbol");
      const auto it = std::ranges::find_if(produce->named_expressions_,
                                           [&](const auto *ne) { return ne->symbol_pos_ == entry.source_position; });
      if (it != produce->named_expressions_.end()) {
        if (const auto *ident = dynamic_cast<const Identifier *>((*it)->expression_)) {
          entry.source_position = ident->symbol_pos_;
        }
      }
    }

    // step 2: resolve unresolved aliases (e.g. n.prop AS a -> fill in resolved path)
    for (auto &entry : info.entries) {
      if (entry.has_path()) continue;

      const auto it = std::ranges::find_if(produce->named_expressions_,
                                           [&](const auto *ne) { return ne->symbol_pos_ == entry.source_position; });
      if (it == produce->named_expressions_.end()) continue;
      const auto *matched = *it;

      if (const auto *prop = dynamic_cast<const PropertyLookup *>(matched->expression_)) {
        const auto *inner = dynamic_cast<const Identifier *>(prop->expression_);
        if (!inner) {
          info.well_formed = false;
          return;
        }
        entry.source_position = inner->symbol_pos_;
        entry.resolved = ResolvePropertyPath(prop->property_path_);
      } else if (const auto *ident = dynamic_cast<const Identifier *>(matched->expression_)) {
        entry.source_position = ident->symbol_pos_;
      } else {
        info.well_formed = false;
        return;
      }
    }
  }

  // -- scan matching --------------------------------------------------------

  static bool MatchGroupAgainstScan(const ProvidedScan &scan, const std::vector<OrderByEntry> &entries,
                                    size_t group_start, size_t group_size) {
    return std::visit(
        [&](const auto *s) -> bool {
          using T = std::remove_cvref_t<decltype(*s)>;
          if constexpr (std::is_same_v<T, ScanAllByLabelProperties>) {
            size_t ob_ptr = 0;
            for (size_t i = 0; i < s->properties_.size() && ob_ptr < group_size; ++i) {
              if (s->properties_[i] == entries[group_start + ob_ptr].resolved) {
                ++ob_ptr;
              } else if (i < s->expression_ranges_.size() &&
                         s->expression_ranges_[i].type_ == PropertyFilter::Type::EQUAL) {
                continue;
              } else {
                return false;
              }
            }
            return ob_ptr == group_size;
          } else {
            // edge scans have a single property -- ORDER BY must reference exactly that one
            return group_size == 1 && storage::PropertyPath{s->property_} == entries[group_start].resolved;
          }
        },
        scan);
  }

  /// Can the provided scans satisfy all ORDER BY entries without an explicit sort?
  [[nodiscard]] static bool CanEliminate(const OrderByInfo &ctx) {
    if (!ctx.well_formed || !ctx.order_preserving_path || ctx.has_pending_entries()) return false;
    if (ctx.provided_scans.empty()) return false;

    size_t scan_idx = 0;
    size_t entry_idx = 0;

    // entries must form contiguous groups by source_position, matching provided scans in order
    while (entry_idx < ctx.entries.size() && scan_idx < ctx.provided_scans.size()) {
      const auto sym_pos =
          std::visit([](const auto *s) { return s->output_symbol_.position(); }, ctx.provided_scans[scan_idx]);

      if (ctx.entries[entry_idx].source_position != sym_pos) return false;

      const size_t group_start = entry_idx;
      while (entry_idx < ctx.entries.size() && ctx.entries[entry_idx].source_position == sym_pos) {
        ++entry_idx;
      }
      const size_t group_size = entry_idx - group_start;

      if (!MatchGroupAgainstScan(ctx.provided_scans[scan_idx], ctx.entries, group_start, group_size)) return false;

      ++scan_idx;
    }

    return entry_idx == ctx.entries.size();
  }
};

}  // namespace memgraph::query::plan
