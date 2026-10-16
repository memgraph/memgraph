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

#include "query/plan/read_write_set.hpp"

#include <algorithm>
#include <array>
#include <string_view>
#include <variant>

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/preprocess.hpp"
#include "utils/string.hpp"

namespace memgraph::query::plan {

namespace {

bool Intersects(const std::set<std::string> &lhs, const std::set<std::string> &rhs) {
  return std::ranges::any_of(lhs, [&rhs](const auto &key) { return rhs.contains(key); });
}

bool KeysConflict(const std::set<std::string> &read_keys, bool read_any, const std::set<std::string> &write_keys,
                  bool write_any) {
  if (read_any) return write_any || !write_keys.empty();
  if (write_any) return !read_keys.empty();
  return Intersects(read_keys, write_keys);
}

/// Functions that read what the planner cannot name: every label, key or edge of an entity.
bool ReadsTheWholeEntity(std::string_view function_name) {
  static constexpr auto kNames =
      std::array<std::string_view, 6>{"LABELS", "KEYS", "PROPERTIES", "DEGREE", "INDEGREE", "OUTDEGREE"};
  return std::ranges::contains(kNames, utils::ToUpperCase(function_name));
}

class ReadWriteCollector final : public HierarchicalTreeVisitor {
 public:
  ReadWriteCollector(const SymbolTable &symbol_table, const std::unordered_set<Symbol> &bound,
                     std::unordered_set<Symbol> non_graph)
      : symbol_table_(symbol_table), bound_(bound), non_graph_(std::move(non_graph)) {}

  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;

  bool Visit(Identifier & /*ident*/) override { return true; }

  bool Visit(PrimitiveLiteral & /*literal*/) override { return true; }

  bool Visit(ParameterLookup & /*lookup*/) override { return true; }

  bool Visit(EnumValueAccess & /*access*/) override { return true; }

  // ---------------------------------------------------------------- reads

  bool PreVisit(PropertyLookup &lookup) override {
    if (ReadsGraph(lookup.expression_)) result.reads.properties.insert(lookup.property_path_.front().name);
    return true;
  }

  bool PreVisit(AllPropertiesLookup &lookup) override {
    if (ReadsGraph(lookup.expression_)) result.reads.any_property = true;
    return true;
  }

  bool PreVisit(SubscriptOperator &op) override {
    if (!ReadsGraph(op.expression1_) || utils::Downcast<ListLiteral>(op.expression1_)) return true;
    if (auto *key = utils::Downcast<PrimitiveLiteral>(op.expression2_)) {
      if (key->value_.IsString()) result.reads.properties.insert(std::string{key->value_.ValueString()});
      return true;
    }
    result.reads.any_property = true;
    return true;
  }

  bool PreVisit(LabelsTest &test) override {
    if (const auto *cnf = test.Cnf()) {
      for (const auto &label : cnf->labels) result.reads.labels.insert(label.name);
      for (const auto &group : cnf->or_labels) {
        for (const auto &label : group) result.reads.labels.insert(label.name);
      }
    } else if (const auto *term = test.Term()) {
      ReadLabelTerm(*term);
    }
    return true;
  }

  bool PreVisit(EdgeTypesTest &test) override {
    for (const auto &type : test.valid_edgetypes_) result.reads.edge_types.insert(type.name);
    return true;
  }

  bool PreVisit(Function &function) override {
    if (ReadsTheWholeEntity(function.function_name_)) result.reads.everything = true;
    return true;
  }

  bool PreVisit(NodeAtom &node) override {
    result.reads.symbols.insert(symbol_table_.at(*node.identifier_));
    if (node.label_term_) {
      ReadLabelTerm(*node.label_term_);
    } else {
      result.reads.nodes = true;
    }
    ReadPropertyKeys(node.properties_);
    return true;
  }

  bool PreVisit(EdgeAtom &edge) override {
    result.reads.symbols.insert(symbol_table_.at(*edge.identifier_));
    if (edge.edge_types_.empty()) result.reads.any_edge_type = true;
    for (const auto &type : edge.edge_types_) {
      if (const auto *name = std::get_if<EdgeTypeIx>(&type)) {
        result.reads.edge_types.insert(name->name);
      } else {
        result.reads.any_edge_type = true;
      }
    }
    ReadPropertyKeys(edge.properties_);
    // EdgeAtom::Accept does not walk the lambdas.
    for (auto *lambda : {&edge.filter_lambda_, &edge.weight_lambda_}) {
      if (lambda->expression) lambda->expression->Accept(*this);
    }
    return true;
  }

  bool PreVisit(CallProcedure &call) override {
    result.reads.everything = true;
    if (call.graph_access_ == GraphAccess::Write) result.writes.everything = true;
    return true;
  }

  bool PreVisit(LoadCsv &load) override {
    non_graph_.insert(symbol_table_.at(*load.row_var_));
    return true;
  }

  bool PreVisit(LoadParquet &load) override {
    non_graph_.insert(symbol_table_.at(*load.row_var_));
    return true;
  }

  bool PreVisit(LoadJsonl &load) override {
    non_graph_.insert(symbol_table_.at(*load.row_var_));
    return true;
  }

  // ---------------------------------------------------------------- writes

  bool PreVisit(Create &create) override {
    for (auto *pattern : create.patterns_) WritePattern(*pattern);
    return false;
  }

  bool PreVisit(Merge &merge) override {
    // MERGE first matches its pattern, then may create it.
    merge.pattern_->Accept(*this);
    WritePattern(*merge.pattern_);
    for (auto *clause : merge.on_match_) clause->Accept(*this);
    for (auto *clause : merge.on_create_) clause->Accept(*this);
    return false;
  }

  bool PreVisit(Delete &del) override {
    result.writes.everything = true;
    for (auto *expression : del.expressions_) expression->Accept(*this);
    return false;
  }

  bool PreVisit(SetProperty &set) override {
    result.writes.properties.insert(set.property_lookup_->property_path_.front().name);
    set.expression_->Accept(*this);
    return false;
  }

  bool PreVisit(SetProperties &set) override {
    result.writes.any_property = true;
    if (auto *ident = utils::Downcast<Identifier>(set.expression_); ident && ReadsGraph(ident)) {
      // `SET n = m` copies every property of an entity.
      result.reads.any_property = true;
    }
    set.expression_->Accept(*this);
    return false;
  }

  bool PreVisit(SetLabels &set) override {
    WriteLabels(set.labels_);
    return false;
  }

  bool PreVisit(RemoveProperty &remove) override {
    result.writes.properties.insert(remove.property_lookup_->property_path_.front().name);
    return false;
  }

  bool PreVisit(RemoveLabels &remove) override {
    WriteLabels(remove.labels_);
    return false;
  }

  ReadsAndWrites result;

 private:
  /// A lookup on a map value reads nothing from the graph; one on an entity, or a value of unknown type, does.
  bool ReadsGraph(Expression *object) const {
    if (utils::Downcast<MapLiteral>(object)) return false;
    auto *ident = utils::Downcast<Identifier>(object);
    if (!ident) return true;
    auto const &symbol = symbol_table_.at(*ident);
    if (non_graph_.contains(symbol)) return false;
    auto const type = symbol.type();
    return type == Symbol::Type::ANY || type == Symbol::Type::VERTEX || type == Symbol::Type::EDGE;
  }

  void ReadLabelTerm(const LabelTerm &term) {
    std::visit(
        [this](const auto &node) {
          using T = std::decay_t<decltype(node)>;
          if constexpr (std::is_same_v<T, LabelTerm::Label>) {
            result.reads.labels.insert(node.label.name);
          } else if constexpr (std::is_same_v<T, LabelTerm::And> || std::is_same_v<T, LabelTerm::Or>) {
            for (const auto &operand : node.operands) ReadLabelTerm(operand);
          } else {
            result.reads.any_label = true;
            if constexpr (std::is_same_v<T, LabelTerm::Dynamic>) {
              if (node.expression) node.expression->Accept(*this);
            }
          }
        },
        term.node);
  }

  template <class TProperties>
  void ReadPropertyKeys(const TProperties &properties) {
    if (const auto *map = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&properties)) {
      for (const auto &[key, value] : *map) result.reads.properties.insert(key.name);
    } else {
      result.reads.any_property = true;
    }
  }

  template <class TProperties>
  void WritePropertyKeys(const TProperties &properties) {
    if (const auto *map = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&properties)) {
      for (const auto &[key, value] : *map) {
        result.writes.properties.insert(key.name);
        value->Accept(*this);
      }
    } else {
      result.writes.any_property = true;
    }
  }

  void WriteLabels(const std::vector<QueryLabelType> &labels) {
    for (const auto &label : labels) {
      if (const auto *name = std::get_if<LabelIx>(&label)) {
        result.writes.labels.insert(name->name);
      } else {
        result.writes.any_label = true;
        std::get<Expression *>(label)->Accept(*this);
      }
    }
#ifdef MG_ENTERPRISE
    // The fine-grained access check reads the labels the node already has.
    result.reads.any_label = true;
#endif
  }

  void WritePattern(Pattern &pattern) {
    for (auto *atom : pattern.atoms_) {
      auto const &symbol = symbol_table_.at(*atom->identifier_);
      if (bound_.contains(symbol)) continue;
      result.writes.symbols.insert(symbol);
      if (auto *node = utils::Downcast<NodeAtom>(atom)) {
        result.writes.nodes = true;
        if (auto labels = node->LabelConjunction()) WriteLabels(*labels);
        WritePropertyKeys(node->properties_);
      } else if (auto *edge = utils::Downcast<EdgeAtom>(atom)) {
        for (const auto &type : edge->edge_types_) {
          if (const auto *name = std::get_if<EdgeTypeIx>(&type)) {
            result.writes.edge_types.insert(name->name);
          } else {
            result.writes.any_edge_type = true;
            std::get<Expression *>(type)->Accept(*this);
          }
        }
        WritePropertyKeys(edge->properties_);
      }
    }
  }

  const SymbolTable &symbol_table_;
  const std::unordered_set<Symbol> &bound_;
  std::unordered_set<Symbol> non_graph_;
};

}  // namespace

bool ReadWriteSet::Empty() const {
  return properties.empty() && !any_property && labels.empty() && !any_label && edge_types.empty() && !any_edge_type &&
         !nodes && !everything && symbols.empty();
}

void ReadWriteSet::Merge(const ReadWriteSet &other) {
  properties.insert(other.properties.begin(), other.properties.end());
  any_property |= other.any_property;
  labels.insert(other.labels.begin(), other.labels.end());
  any_label |= other.any_label;
  edge_types.insert(other.edge_types.begin(), other.edge_types.end());
  any_edge_type |= other.any_edge_type;
  nodes |= other.nodes;
  everything |= other.everything;
  symbols.insert(other.symbols.begin(), other.symbols.end());
}

bool Conflicts(const ReadWriteSet &reads, const ReadWriteSet &writes) {
  if (reads.Empty() || writes.Empty()) return false;
  if (reads.everything || writes.everything) return true;
  // A pattern through a node this command created: View::OLD cannot see it at all.
  if (std::ranges::any_of(reads.symbols, [&writes](const auto &sym) { return writes.symbols.contains(sym); })) {
    return true;
  }
  return KeysConflict(reads.properties, reads.any_property, writes.properties, writes.any_property) ||
         KeysConflict(reads.labels, reads.any_label, writes.labels, writes.any_label) ||
         KeysConflict(reads.edge_types, reads.any_edge_type, writes.edge_types, writes.any_edge_type) ||
         (reads.nodes && writes.nodes);
}

ReadsAndWrites CollectReadsAndWrites(utils::Visitable<HierarchicalTreeVisitor> &tree, const SymbolTable &symbol_table,
                                     const std::unordered_set<Symbol> &bound,
                                     const std::unordered_set<Symbol> &non_graph) {
  ReadWriteCollector collector(symbol_table, bound, non_graph);
  tree.Accept(collector);
  return std::move(collector.result);
}

ReadWriteSet CollectReads(const Matching &matching, const SymbolTable &symbol_table,
                          const std::unordered_set<Symbol> &non_graph) {
  const std::unordered_set<Symbol> no_bound;
  ReadWriteCollector collector(symbol_table, no_bound, non_graph);
  for (const auto &expansion : matching.expansions) {
    if (expansion.node1) expansion.node1->Accept(collector);
    if (expansion.edge) expansion.edge->Accept(collector);
    if (expansion.node2) expansion.node2->Accept(collector);
  }
  for (const auto &filter : matching.filters) {
    if (filter.expression) filter.expression->Accept(collector);
  }
  return std::move(collector.result.reads);
}

}  // namespace memgraph::query::plan
