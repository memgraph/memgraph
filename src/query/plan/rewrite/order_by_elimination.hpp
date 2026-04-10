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
    storage::PropertyPath resolved;  // filled if PropertyLookup, or after alias resolution
    // views into AST node name strings -- valid for the lifetime of the AstStorage
    std::string_view alias;                  // non-empty if bare Identifier (unresolved)
    std::string_view source_name;            // which scan symbol this entry references
    Symbol::Position_t source_position{-1};  // symbol table position -- disambiguates same-name symbols

    [[nodiscard]] bool is_resolved() const { return alias.empty(); }

    // could this entry's property overlap with the given property?
    // unresolved entries conservatively return true
    [[nodiscard]] bool may_overlap(storage::PropertyId prop) const {
      return !is_resolved() || (!resolved.empty() && resolved[0] == prop);
    }
  };

  using ProvidedScan = std::variant<const ScanAllByLabelProperties *, const ScanAllByEdgeTypePropertyRange *,
                                    const ScanAllByEdgePropertyRange *, const ScanAllByEdgeTypePropertyValue *,
                                    const ScanAllByEdgePropertyValue *>;

  struct OrderByInfo {
    OrderBy *op{nullptr};
    std::vector<OrderByEntry> entries;
    std::vector<ProvidedScan> provided_scans;  // accumulated bottom-up (outermost first)
    bool valid{false};
    bool should_eliminate{false};
    bool order_preserving_path{true};  // set false if walk from first scan to OrderBy fails

    [[nodiscard]] bool has_unresolved() const {
      return std::ranges::any_of(entries, [](const auto &e) { return !e.is_resolved(); });
    }
  };

  // ---------- construction -------------------------------------------------

  OrderByEliminator(TDbAccessor *db, const std::vector<LogicalOperator *> &prev_ops) : db_(db), prev_ops_(prev_ops) {}

  // ---------- hooks called by the rewriter ---------------------------------

  void OnPreVisitOrderBy(OrderBy &op) { order_by_stack_.emplace_back(ExtractOrderByInfo(op)); }

  bool OnPostVisitOrderBy(OrderBy & /*op*/) {
    DMG_ASSERT(!order_by_stack_.empty(), "OrderBy stack underflow in PostVisit");
    auto &ctx = order_by_stack_.back();
    CheckOrderByElimination(ctx);
    const bool eliminate = ctx.should_eliminate;
    order_by_stack_.pop_back();
    return eliminate;
  }

  void OnPreVisitProduce(Produce *produce) {
    if (auto *ctx = ActiveContext()) {
      ResolveDownward(produce, *ctx);
    }
  }

  void RecordVertexScan(ScanAllByLabelProperties *scan) {
    if (auto *ctx = ActiveContext()) {
      if (ctx->provided_scans.empty()) WalkAndCheckOrderPreserving(*ctx);
      ctx->provided_scans.emplace_back(scan);
    }
  }

  template <typename EdgeScan>
  void RecordEdgeScan(EdgeScan *scan) {
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

  void MarkFirstScanUnrecorded() {
    if (auto *ctx = ActiveContext()) {
      if (ctx->provided_scans.empty()) ctx->order_preserving_path = false;
    }
  }

  // ---------- static helpers -----------------------------------------------

  static bool IsOrderPreserving(const utils::TypeInfo &type_info) {
    return type_info == Filter::kType || type_info == ConstructNamedPath::kType ||
           type_info == EdgeUniquenessFilter::kType || type_info == Limit::kType || type_info == Skip::kType ||
           type_info == Expand::kType || type_info == ExpandVariable::kType || type_info == Optional::kType ||
           type_info == Distinct::kType || type_info == EvaluatePatternFilter::kType ||
           type_info == RollUpApply::kType || type_info == Apply::kType || type_info == Unwind::kType ||
           type_info == Produce::kType || type_info == Accumulate::kType || type_info == SetLabels::kType ||
           type_info == RemoveLabels::kType;
  }

  static bool IsMutationOrderPreserving(const LogicalOperator &op, const OrderByInfo &ctx) {
    // SetProperty/RemoveProperty have a statically-known PropertyId. The property name
    // comes from `propertyLookup : '.' propertyKeyName` where `propertyKeyName` is a
    // `symbolicName` (literal identifier) -- the atom before the dot could be a parameter
    // (e.g. SET $param.prop = 1) but the property name itself cannot. SetProperties
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
    if (order_by_stack_.empty() || !order_by_stack_.back().valid) return nullptr;
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

        info.entries.emplace_back(
            ResolvePropertyPath(prop_lookup->property_path_), std::string_view{}, ident->name_, ident->symbol_pos_);
      } else if (const auto *ident = dynamic_cast<const Identifier *>(expr)) {
        info.entries.emplace_back(storage::PropertyPath{}, ident->name_, std::string_view{}, ident->symbol_pos_);
      } else {
        return info;
      }
    }

    info.valid = true;
    return info;
  }

  void WalkAndCheckOrderPreserving(OrderByInfo &ctx) const {
    for (auto it = prev_ops_.rbegin(); it != prev_ops_.rend() && *it != ctx.op; ++it) {
      const auto &ti = (*it)->GetTypeInfo();
      if (!IsOrderPreserving(ti) && !IsScanAllVariant(ti) && !IsMutationOrderPreserving(**it, ctx)) {
        ctx.order_preserving_path = false;
        break;
      }
    }
  }

  void ResolveDownward(Produce *produce, OrderByInfo &info) const {
    // step 1: track source_name through renames (e.g. Produce has `n AS m` -> source_name "m" becomes "n")
    for (auto &entry : info.entries) {
      if (entry.source_name.empty()) continue;
      const auto it = std::ranges::find_if(produce->named_expressions_,
                                           [&](const auto *ne) { return ne->name_ == entry.source_name; });
      if (it != produce->named_expressions_.end()) {
        if (const auto *ident = dynamic_cast<const Identifier *>((*it)->expression_)) {
          entry.source_name = ident->name_;
          entry.source_position = ident->symbol_pos_;
        }
      }
    }

    // step 2: resolve unresolved aliases (e.g. n.prop AS a -> fill in resolved path)
    for (auto &entry : info.entries) {
      if (entry.is_resolved()) continue;

      const auto it =
          std::ranges::find_if(produce->named_expressions_, [&](const auto *ne) { return ne->name_ == entry.alias; });
      if (it == produce->named_expressions_.end()) continue;
      const auto *matched = *it;

      if (const auto *prop = dynamic_cast<const PropertyLookup *>(matched->expression_)) {
        const auto *inner = dynamic_cast<const Identifier *>(prop->expression_);
        if (!inner) {
          info.valid = false;
          return;
        }
        entry.source_name = inner->name_;
        entry.source_position = inner->symbol_pos_;
        entry.resolved = ResolvePropertyPath(prop->property_path_);
        entry.alias = {};
      } else if (const auto *ident = dynamic_cast<const Identifier *>(matched->expression_)) {
        entry.alias = ident->name_;
        entry.source_position = ident->symbol_pos_;
      } else {
        info.valid = false;
        return;
      }
    }
  }

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

  static void CheckOrderByElimination(OrderByInfo &ctx) {
    if (!ctx.valid || !ctx.order_preserving_path || ctx.has_unresolved()) return;
    if (ctx.provided_scans.empty()) return;

    size_t scan_idx = 0;
    size_t entry_idx = 0;

    // entries must form contiguous groups by source_position, matching provided scans in order
    while (entry_idx < ctx.entries.size() && scan_idx < ctx.provided_scans.size()) {
      const auto sym_pos =
          std::visit([](const auto *s) { return s->output_symbol_.position(); }, ctx.provided_scans[scan_idx]);

      if (ctx.entries[entry_idx].source_position != sym_pos) return;

      const size_t group_start = entry_idx;
      while (entry_idx < ctx.entries.size() && ctx.entries[entry_idx].source_position == sym_pos) {
        ++entry_idx;
      }
      const size_t group_size = entry_idx - group_start;

      if (!MatchGroupAgainstScan(ctx.provided_scans[scan_idx], ctx.entries, group_start, group_size)) return;

      ++scan_idx;
    }

    if (entry_idx != ctx.entries.size()) return;

    ctx.should_eliminate = true;
  }
};

}  // namespace memgraph::query::plan
