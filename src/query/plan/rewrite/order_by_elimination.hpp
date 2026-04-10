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
    storage::PropertyPath resolved;          // filled if PropertyLookup, or after alias resolution
    std::string_view alias;                  // non-empty if bare Identifier (unresolved)
    std::string_view source_name;            // which scan symbol this entry references
    Symbol::Position_t source_position{-1};  // symbol table position — disambiguates same-name symbols

    bool is_resolved() const { return alias.empty(); }
  };

  using ProvidedScan = std::variant<const ScanAllByLabelProperties *, const ScanAllByEdgeTypePropertyRange *,
                                    const ScanAllByEdgePropertyRange *>;

  struct OrderByInfo {
    OrderBy *op{nullptr};
    std::vector<OrderByEntry> entries;
    std::vector<ProvidedScan> provided_scans;  // accumulated bottom-up (outermost first)
    bool valid{false};
    bool should_eliminate{false};
    bool order_preserving_path{true};  // set false if walk from first scan to OrderBy fails

    bool has_unresolved() const {
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
    bool eliminate = ctx.should_eliminate;
    order_by_stack_.pop_back();
    return eliminate;
  }

  void OnPreVisitProduce(Produce *produce) {
    if (!order_by_stack_.empty() && order_by_stack_.back().valid) {
      ResolveDownward(produce, order_by_stack_.back());
    }
  }

  void RecordVertexScan(ScanAllByLabelProperties *scan) {
    if (order_by_stack_.empty() || !order_by_stack_.back().valid) return;
    auto &ctx = order_by_stack_.back();
    if (ctx.provided_scans.empty()) {
      WalkAndCheckOrderPreserving(ctx);
    }
    ctx.provided_scans.emplace_back(scan);
  }

  template <typename EdgeScan>
  void RecordEdgeScan(EdgeScan *scan) {
    static_assert(std::is_same_v<EdgeScan, ScanAllByEdgeTypePropertyRange> ||
                      std::is_same_v<EdgeScan, ScanAllByEdgePropertyRange>,
                  "Only range scans provide ordered iteration");
    if (order_by_stack_.empty() || !order_by_stack_.back().valid) return;
    auto &ctx = order_by_stack_.back();
    if (ctx.provided_scans.empty()) {
      WalkAndCheckOrderPreserving(ctx);
    }
    ctx.provided_scans.emplace_back(scan);
  }

  void MarkFirstScanUnrecorded() {
    if (!order_by_stack_.empty() && order_by_stack_.back().valid && order_by_stack_.back().provided_scans.empty()) {
      order_by_stack_.back().order_preserving_path = false;
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
    // `symbolicName` (literal identifier) — the atom before the dot could be a parameter
    // (e.g. SET $param.prop = 1) but the property name itself cannot. SetProperties
    // (SET n = expr / SET n += expr) is not handled because the map expression is
    // opaque at plan time and may touch any property, so it falls through to false.
    // TODO: SetProperties with a MapLiteral RHS could be handled — the map keys are
    // literal PropertyIx values, so we could check overlap. Needs db_ access to resolve.
    auto property_overlaps_order_by = [&ctx](storage::PropertyId prop) -> bool {
      for (const auto &entry : ctx.entries) {
        if (!entry.is_resolved()) return true;
        if (!entry.resolved.empty() && entry.resolved[0] == prop) return true;
      }
      return false;
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

  OrderByInfo ExtractOrderByInfo(OrderBy &op) const {
    OrderByInfo info;
    info.op = &op;

    const auto &orderings = op.compare_.orderings();
    const auto &order_by_exprs = op.order_by_;

    if (orderings.empty()) return info;

    for (const auto &ord : orderings) {
      if (ord.ordering() != Ordering::ASC) return info;
    }

    for (auto *expr : order_by_exprs) {
      if (auto *prop_lookup = dynamic_cast<PropertyLookup *>(expr)) {
        auto *ident = dynamic_cast<Identifier *>(prop_lookup->expression_);
        if (!ident) return info;

        std::vector<storage::PropertyId> prop_ids;
        prop_ids.reserve(prop_lookup->property_path_.size());
        for (const auto &pix : prop_lookup->property_path_) {
          prop_ids.emplace_back(GetProperty(pix));
        }
        info.entries.emplace_back(
            storage::PropertyPath{std::move(prop_ids)}, std::string_view{}, ident->name_, ident->symbol_pos_);
      } else if (auto *ident = dynamic_cast<Identifier *>(expr)) {
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
    for (auto &entry : info.entries) {
      if (entry.source_name.empty()) continue;
      for (const auto *ne : produce->named_expressions_) {
        if (ne->name_ != entry.source_name) continue;
        auto *ident = dynamic_cast<Identifier *>(ne->expression_);
        if (ident) {
          entry.source_name = ident->name_;
          entry.source_position = ident->symbol_pos_;
        }
        break;
      }
    }

    for (auto &entry : info.entries) {
      if (entry.is_resolved()) continue;

      const NamedExpression *matched = nullptr;
      for (const auto *ne : produce->named_expressions_) {
        if (ne->name_ == entry.alias) {
          matched = ne;
          break;
        }
      }
      if (!matched) continue;

      if (auto *prop = dynamic_cast<PropertyLookup *>(matched->expression_)) {
        auto *inner = dynamic_cast<Identifier *>(prop->expression_);
        if (!inner) {
          info.valid = false;
          return;
        }
        entry.source_name = inner->name_;
        entry.source_position = inner->symbol_pos_;
        std::vector<storage::PropertyId> prop_ids;
        prop_ids.reserve(prop->property_path_.size());
        for (const auto &pix : prop->property_path_) {
          prop_ids.emplace_back(GetProperty(pix));
        }
        entry.resolved = storage::PropertyPath{std::move(prop_ids)};
        entry.alias = {};
      } else if (auto *ident = dynamic_cast<Identifier *>(matched->expression_)) {
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
          using T = std::decay_t<decltype(*s)>;
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
