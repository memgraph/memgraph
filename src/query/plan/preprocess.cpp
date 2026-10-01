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

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <stack>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>

#include "frontend/ast/query/identifier.hpp"
#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/frontend/semantic/rw_checker.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/plan/preprocess.hpp"
#include "query/plan/rewrite/range.hpp"
#include "utils/bound.hpp"
#include "utils/logging.hpp"
#include "utils/typeinfo.hpp"

using namespace std::string_view_literals;

namespace memgraph::query::plan {

namespace {

void ForEachPattern(Pattern &pattern, std::function<void(NodeAtom *)> base,
                    std::function<void(NodeAtom *, EdgeAtom *, NodeAtom *)> collect) {
  DMG_ASSERT(!pattern.atoms_.empty(), "Missing atoms in pattern");
  auto atoms_it = pattern.atoms_.begin();
  auto current_node = utils::Downcast<NodeAtom>(*atoms_it++);
  DMG_ASSERT(current_node, "First pattern atom is not a node");
  base(current_node);
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  while (atoms_it != pattern.atoms_.end()) {
    auto edge = utils::Downcast<EdgeAtom>(*atoms_it++);
    DMG_ASSERT(edge, "Expected an edge atom in pattern.");
    DMG_ASSERT(atoms_it != pattern.atoms_.end(), "Edge atom should not end the pattern.");
    auto prev_node = current_node;
    current_node = utils::Downcast<NodeAtom>(*atoms_it++);
    DMG_ASSERT(current_node, "Expected a node atom in pattern.");
    collect(prev_node, edge, current_node);
  }
}

// Converts multiple Patterns to Expansions. Each Pattern can contain an
// arbitrarily long chain of nodes and edges. The conversion to an Expansion is
// done by splitting a pattern into triplets (node1, edge, node2). The triplets
// conserve the semantics of the pattern. For example, in a pattern:
// (m) -[e]- (n) -[f]- (o) the same can be achieved with:
// (m) -[e]- (n), (n) -[f]- (o).
// This representation makes it easier to permute from which node or edge we
// want to start expanding.
std::vector<Expansion> NormalizePatterns(const SymbolTable &symbol_table, const std::vector<Pattern *> &patterns) {
  std::vector<Expansion> expansions;
  ExpansionGroupId unknown_expansion_group_id = ExpansionGroupId::FromInt(-1);
  auto ignore_node = [&](auto *) {};
  for (const auto &pattern : patterns) {
    if (pattern->atoms_.size() == 1U) {
      auto *node = utils::Downcast<NodeAtom>(pattern->atoms_[0]);
      DMG_ASSERT(node, "First pattern atom is not a node");
      expansions.emplace_back(Expansion{.node1 = node, .expansion_group_id = unknown_expansion_group_id});
    } else {
      auto collect_expansion = [&](auto *prev_node, auto *edge, auto *current_node) {
        UsedSymbolsCollector collector(symbol_table);
        if (edge->IsVariable()) {
          if (edge->lower_bound_) edge->lower_bound_->Accept(collector);
          if (edge->upper_bound_) edge->upper_bound_->Accept(collector);
          if (edge->filter_lambda_.expression) edge->filter_lambda_.expression->Accept(collector);
          // Remove symbols which are bound by lambda arguments.
          collector.symbols_.erase(symbol_table.at(*edge->filter_lambda_.inner_edge));
          collector.symbols_.erase(symbol_table.at(*edge->filter_lambda_.inner_node));
          if (edge->filter_lambda_.accumulated_path) {
            collector.symbols_.erase(symbol_table.at(*edge->filter_lambda_.accumulated_path));

            if (edge->filter_lambda_.accumulated_weight) {
              collector.symbols_.erase(symbol_table.at(*edge->filter_lambda_.accumulated_weight));
            }
          }
          if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH ||
              edge->type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS) {
            collector.symbols_.erase(symbol_table.at(*edge->weight_lambda_.inner_edge));
            collector.symbols_.erase(symbol_table.at(*edge->weight_lambda_.inner_node));
          }
        }
        expansions.emplace_back(Expansion{
            prev_node, edge, edge->direction_, false, collector.symbols_, current_node, unknown_expansion_group_id});
      };
      ForEachPattern(*pattern, ignore_node, collect_expansion);
    }
  }
  return expansions;
}

void AssignExpansionGroupIds(std::vector<Expansion> &expansions, Matching &matching, const SymbolTable &symbol_table) {
  ExpansionGroupId next_expansion_group_id = ExpansionGroupId::FromUint(matching.number_of_expansion_groups + 1);

  auto assign_expansion_group_id = [&matching, &next_expansion_group_id](const Symbol &symbol, Expansion &expansion) {
    ExpansionGroupId expansion_group_id_to_assign = next_expansion_group_id;
    if (matching.node_symbol_to_expansion_group_id.contains(symbol)) {
      expansion_group_id_to_assign = matching.node_symbol_to_expansion_group_id[symbol];
    }

    if (expansion.expansion_group_id.AsInt() == -1 ||
        expansion_group_id_to_assign.AsInt() < expansion.expansion_group_id.AsInt()) {
      expansion.expansion_group_id = expansion_group_id_to_assign;
    }

    matching.node_symbol_to_expansion_group_id[symbol] = expansion.expansion_group_id;
  };

  for (auto &expansion : expansions) {
    const auto &node1_sym = symbol_table.at(*expansion.node1->identifier_);
    assign_expansion_group_id(node1_sym, expansion);

    if (expansion.edge) {
      const auto &edge_sym = symbol_table.at(*expansion.edge->identifier_);
      const auto &node2_sym = symbol_table.at(*expansion.node2->identifier_);

      assign_expansion_group_id(edge_sym, expansion);
      assign_expansion_group_id(node2_sym, expansion);
    }

    matching.number_of_expansion_groups = matching.number_of_expansion_groups < expansion.expansion_group_id.AsUint()
                                              ? expansion.expansion_group_id.AsUint()
                                              : matching.number_of_expansion_groups;
    next_expansion_group_id = ExpansionGroupId::FromUint(matching.number_of_expansion_groups + 1);
  }

  // By the time we finished assigning expansions, no expansion should have its expansion group ID unassigned
  for (const auto &expansion : matching.expansions) {
    MG_ASSERT(expansion.expansion_group_id.AsInt() != -1, "Expansion group ID is not assigned to the pattern!");
  }
}

void CollectEdgeSymbols(std::vector<Expansion> &expansions, Matching &matching, const SymbolTable &symbol_table) {
  std::unordered_set<Symbol> edge_symbols;
  for (auto &expansion : expansions) {
    if (expansion.edge) {
      const auto &edge_sym = symbol_table.at(*expansion.edge->identifier_);
      // Fill edge symbols for Cyphermorphism.
      edge_symbols.insert(edge_sym);
    }
  }

  if (!edge_symbols.empty()) {
    matching.edge_symbols.emplace_back(edge_symbols);
  }
}

void CollectExpansionSymbols(std::vector<Expansion> &expansions, Matching &matching, const SymbolTable &symbol_table) {
  for (auto &expansion : expansions) {
    // Map node1 symbol to expansion
    const auto &node1_sym = symbol_table.at(*expansion.node1->identifier_);
    matching.expansion_symbols.insert(node1_sym);

    if (expansion.edge) {
      const auto &edge_sym = symbol_table.at(*expansion.edge->identifier_);
      matching.expansion_symbols.insert(edge_sym);

      const auto &node2_sym = symbol_table.at(*expansion.node2->identifier_);
      matching.expansion_symbols.insert(node2_sym);
    }
  }
}

void AddExpansionsToMatching(std::vector<Expansion> &expansions, Matching &matching, const SymbolTable &symbol_table) {
  for (auto &expansion : expansions) {
    // Matching may already have some expansions, so offset our index.
    const size_t expansion_ix = matching.expansions.size();
    const auto &node1_sym = symbol_table.at(*expansion.node1->identifier_);
    matching.atom_symbol_to_expansions[node1_sym].insert(expansion_ix);

    if (expansion.edge) {
      const auto &node2_sym = symbol_table.at(*expansion.node2->identifier_);
      matching.atom_symbol_to_expansions[node2_sym].insert(expansion_ix);
      const auto &edge_sym = symbol_table.at(*expansion.edge->identifier_);
      matching.atom_symbol_to_expansions[edge_sym].insert(expansion_ix);
    }

    matching.expansions.push_back(expansion);
  }
}

auto MatchesIdentifier(Identifier *identifier) {
  return [identifier](FilterInfo const &existing) {
    // Only a label filter accumulates labels. A node test carries a `LabelsTest` too, and merging into it would
    // make it demand labels the pattern it came from never stated.
    if (existing.type != FilterInfo::Type::Label) return false;

    auto *existing_label_test = dynamic_cast<LabelsTest *>(existing.expression);
    if (!existing_label_test) return false;

    auto *exisiting_identifier = dynamic_cast<Identifier *>(existing_label_test->expression_);
    if (!exisiting_identifier) return false;

    // If are the same symbol position then they must be referring to the same identifer
    return identifier->symbol_pos_ == exisiting_identifier->symbol_pos_;
  };
};

/// Adds `group` as one more OR group of a node's label test. A group is implied by an AND label it names or by
/// a group whose labels it contains. An implied group is not added, and the groups `group` implies are removed,
/// so the filter tests no redundant group and index selection cannot pick one.
void AddOrLabelGroup(std::vector<std::vector<LabelIx>> &or_labels, const std::vector<LabelIx> &and_labels,
                     std::vector<LabelIx> group) {
  auto contains_all = [](const std::vector<LabelIx> &super, const std::vector<LabelIx> &sub) {
    return std::ranges::all_of(sub, [&](const auto &label) { return std::ranges::contains(super, label); });
  };
  if (std::ranges::any_of(group, [&](const auto &label) { return std::ranges::contains(and_labels, label); }) ||
      std::ranges::any_of(or_labels, [&](const auto &existing) { return contains_all(group, existing); })) {
    return;
  }
  std::erase_if(or_labels, [&](const auto &existing) { return contains_all(existing, group); });
  or_labels.push_back(std::move(group));
}

/// Adds `labels` to a node's AND labels. A group that names one of them is then implied and removed, as
/// AddOrLabelGroup refuses a group that arrives after such a label.
void AddAndLabels(std::vector<LabelIx> &and_labels, std::vector<std::vector<LabelIx>> &or_labels,
                  const std::vector<LabelIx> &labels) {
  for (const auto &label : labels) {
    if (!std::ranges::contains(and_labels, label)) and_labels.push_back(label);
  }
  std::erase_if(or_labels, [&](const auto &group) {
    return std::ranges::any_of(group, [&](const auto &label) { return std::ranges::contains(and_labels, label); });
  });
}

/// The labels a test contributes to a disjunction over one variable -- a single label, or a single OR group as
/// `n:A|B` has -- or nothing if it demands anything else of the variable.
auto DisjunctLabels(const LabelsTest &labels_test) -> const std::vector<LabelIx> * {
  const auto *cnf = labels_test.Cnf();
  if (!cnf) return nullptr;
  if (cnf->labels.size() == 1 && cnf->or_labels.empty()) return &cnf->labels;
  if (cnf->labels.empty() && cnf->or_labels.size() == 1) return &cnf->or_labels.front();
  return nullptr;
}

/// The plain labels a label filter's test holds. A whole term is always a generic filter, so a label filter
/// never holds one.
LabelCnf &FilterLabels(const FilterInfo &filter) {
  auto *labels_test = utils::Downcast<LabelsTest>(filter.expression);
  MG_ASSERT(labels_test && labels_test->Cnf(), "A label filter tests plain labels");
  return *labels_test->Cnf();
}

/// Records what index selection may use of a label filter: whatever its test demands. The two part company
/// only where a scan absorbs a label, which every collection then re-derives from the test.
void DeriveLabelView(FilterInfo &filter) {
  const auto &cnf = FilterLabels(filter);
  filter.labels = cnf.labels;
  filter.or_labels = cnf.or_labels;
}

/// Adds `group` as one more OR group of the filter's label test.
void AddOrLabelGroup(FilterInfo &filter, std::vector<LabelIx> group) {
  auto &cnf = FilterLabels(filter);
  AddOrLabelGroup(cnf.or_labels, cnf.labels, std::move(group));
  DeriveLabelView(filter);
}

/// Adds `labels` to the AND labels of the filter's label test.
void AddAndLabels(FilterInfo &filter, const std::vector<LabelIx> &labels) {
  auto &cnf = FilterLabels(filter);
  AddAndLabels(cnf.labels, cnf.or_labels, labels);
  DeriveLabelView(filter);
}

}  // namespace

PropertyFilter::PropertyFilter(const SymbolTable &symbol_table, const Symbol &symbol, PropertyIx property,
                               Expression *value, Type type)
    : symbol_(symbol), property_ids_({std::move(property)}), type_(type), value_(value) {
  MG_ASSERT(type != Type::RANGE);
  UsedSymbolsCollector collector(symbol_table);
  value->Accept(collector);
  is_symbol_in_value_ = collector.symbols_.contains(symbol);
}

PropertyFilter::PropertyFilter(const SymbolTable &symbol_table, const Symbol &symbol, PropertyIx property,
                               const std::optional<PropertyFilter::Bound> &lower_bound,
                               const std::optional<PropertyFilter::Bound> &upper_bound)
    : symbol_(symbol),
      property_ids_({std::move(property)}),
      type_(Type::RANGE),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound) {
  UsedSymbolsCollector collector(symbol_table);
  if (lower_bound) {
    lower_bound->value()->Accept(collector);
  }
  if (upper_bound) {
    upper_bound->value()->Accept(collector);
  }
  is_symbol_in_value_ = collector.symbols_.contains(symbol);
}

PropertyFilter::PropertyFilter(const SymbolTable &symbol_table, const Symbol &symbol, PropertyIxPath properties,
                               Expression *value, Type type)
    : symbol_(symbol), property_ids_(std::move(properties)), type_(type), value_(value) {
  MG_ASSERT(type != Type::RANGE);
  UsedSymbolsCollector collector(symbol_table);
  value->Accept(collector);
  is_symbol_in_value_ = collector.symbols_.contains(symbol);
}

PropertyFilter::PropertyFilter(const SymbolTable &symbol_table, const Symbol &symbol, PropertyIxPath properties,
                               const std::optional<PropertyFilter::Bound> &lower_bound,
                               const std::optional<PropertyFilter::Bound> &upper_bound)
    : symbol_(symbol),
      property_ids_(std::move(properties)),
      type_(Type::RANGE),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound) {
  UsedSymbolsCollector collector(symbol_table);
  if (lower_bound) {
    lower_bound->value()->Accept(collector);
  }
  if (upper_bound) {
    upper_bound->value()->Accept(collector);
  }
  is_symbol_in_value_ = collector.symbols_.contains(symbol);
}

PropertyFilter::PropertyFilter(Symbol symbol, PropertyIx property, Type type)
    : symbol_(std::move(symbol)), property_ids_({std::move(property)}), type_(type) {
  // As this constructor is used for property filters where
  // we don't have to evaluate the filter expression, we set
  // the is_symbol_in_value_ to false, although the filter
  // expression may actually contain the symbol whose property
  // we may be looking up.
}

PropertyFilter::PropertyFilter(Symbol symbol, PropertyIxPath properties, Type type)
    : symbol_(std::move(symbol)), property_ids_(std::move(properties)), type_(type) {}

IdFilter::IdFilter(const SymbolTable &symbol_table, const Symbol &symbol, Expression *value, bool expects_string_id,
                   Type type)
    : symbol_(symbol), value_(value), expects_string_id_(expects_string_id), type_(type) {
  MG_ASSERT(value);
  UsedSymbolsCollector collector(symbol_table);
  value->Accept(collector);
  is_symbol_in_value_ = collector.symbols_.contains(symbol);
}

void Filters::EraseFilter(const FilterInfo &filter) {
  // TODO: Ideally, we want to determine the equality of both expression trees,
  // instead of a simple pointer compare.
  std::erase_if(all_filters_, [&filter](const auto &f) { return f.expression == filter.expression; });
}

// Tries to erase the filter which contains a label of a symbol
// Filtered label can refer to a node and its label, or an edge and its edge type
void Filters::EraseLabelFilter(const Symbol &symbol, const LabelIx &label, std::vector<Expression *> *removed_filters) {
  for (auto filter_it = all_filters_.begin(); filter_it != all_filters_.end();) {
    if (filter_it->type != FilterInfo::Type::Label) {
      ++filter_it;
      continue;
    }
    if (!filter_it->used_symbols.contains(symbol)) {
      ++filter_it;
      continue;
    }
    auto label_it = std::find(filter_it->labels.begin(), filter_it->labels.end(), label);
    if (label_it == filter_it->labels.end()) {
      ++filter_it;
      continue;
    }
    filter_it->labels.erase(label_it);
    DMG_ASSERT(!std::ranges::contains(filter_it->labels, label), "Didn't expect duplicated labels");
    if (filter_it->labels.empty() && filter_it->or_labels.empty()) {
      // If there are no labels to filter, then erase the whole FilterInfo.
      if (removed_filters) {
        removed_filters->push_back(filter_it->expression);
      }
      filter_it = all_filters_.erase(filter_it);
    } else {
      ++filter_it;
    }
  }
}

// Tries to erase the filter which contains a vector of labels of a symbol
void Filters::EraseOrLabelFilter(const Symbol &symbol, const std::vector<LabelIx> &labels,
                                 std::vector<Expression *> *removed_filters) {
  for (auto filter_it = all_filters_.begin(); filter_it != all_filters_.end();) {
    if (filter_it->type != FilterInfo::Type::Label) {
      ++filter_it;
      continue;
    }
    if (!filter_it->used_symbols.contains(symbol)) {
      ++filter_it;
      continue;
    }
    auto label_vec_it = std::find(filter_it->or_labels.begin(), filter_it->or_labels.end(), labels);
    if (label_vec_it == filter_it->or_labels.end()) {
      ++filter_it;
      continue;
    }
    filter_it->or_labels.erase(label_vec_it);
    DMG_ASSERT(!std::ranges::contains(filter_it->or_labels, labels), "Didn't expect duplicated labels");
    if (filter_it->or_labels.empty() && filter_it->labels.empty()) {
      // If there are no labels to filter, then erase the whole FilterInfo.
      if (removed_filters) {
        removed_filters->push_back(filter_it->expression);
      }
      filter_it = all_filters_.erase(filter_it);
    } else {
      ++filter_it;
    }
  }
}

void Filters::CollectPatternFilters(Pattern &pattern, SymbolTable &symbol_table, AstStorage &storage) {
  UsedSymbolsCollector collector(symbol_table);
  auto add_properties_variable = [&](EdgeAtom *atom) {
    const auto &symbol = symbol_table.at(*atom->identifier_);
    if (auto *properties = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&atom->properties_)) {
      for (auto &prop_pair : *properties) {
        // We need to store two property-lookup filters in all_filters. One is
        // used for inlining property filters into variable expansion, and
        // utilizes the inner_edge symbol. The other is used for post-expansion
        // filtering and does not use the inner_edge symbol, but the edge symbol
        // (a list of edges).
        {
          collector.symbols_.clear();
          prop_pair.second->Accept(collector);
          collector.symbols_.emplace(symbol_table.at(*atom->filter_lambda_.inner_node));
          collector.symbols_.emplace(symbol_table.at(*atom->filter_lambda_.inner_edge));
          if (atom->filter_lambda_.accumulated_path) {
            collector.symbols_.emplace(symbol_table.at(*atom->filter_lambda_.accumulated_path));

            if (atom->filter_lambda_.accumulated_weight) {
              collector.symbols_.emplace(symbol_table.at(*atom->filter_lambda_.accumulated_weight));
            }
          }
          // First handle the inline property filter.
          auto *property_lookup = storage.Create<PropertyLookup>(atom->filter_lambda_.inner_edge, prop_pair.first);
          auto *prop_equal = storage.Create<EqualOperator>(property_lookup, prop_pair.second);
          // Currently, variable expand has no gains if we set PropertyFilter.
          all_filters_.emplace_back(FilterInfo::Type::Generic, prop_equal, collector.symbols_);
        }
        {
          collector.symbols_.clear();
          prop_pair.second->Accept(collector);
          collector.symbols_.insert(symbol);  // PropertyLookup uses the symbol.
          // Now handle the post-expansion filter.
          // Create a new identifier and a symbol which will be filled in All.
          auto *identifier = storage.Create<Identifier>(atom->identifier_->name_, atom->identifier_->user_declared_)
                                 ->MapTo(symbol_table.CreateSymbol(atom->identifier_->name_, false));
          // Create an equality expression and store it in all_filters_.
          auto *property_lookup = storage.Create<PropertyLookup>(identifier, prop_pair.first);
          auto *prop_equal = storage.Create<EqualOperator>(property_lookup, prop_pair.second);
          // Currently, variable expand has no gains if we set PropertyFilter.
          all_filters_.emplace_back(
              FilterInfo::Type::Generic,
              storage.Create<All>(identifier, atom->identifier_, storage.Create<Where>(prop_equal)),
              collector.symbols_);
        }
      }
      return;
    }
    throw SemanticException("Property map matching not supported in MATCH/MERGE clause!");
  };
  auto add_properties = [&](auto *atom) {
    const auto &symbol = symbol_table.at(*atom->identifier_);
    if (auto *properties = std::get_if<std::unordered_map<PropertyIx, Expression *>>(&atom->properties_)) {
      for (auto &prop_pair : *properties) {
        // Create an equality expression and store it in all_filters_.
        auto *property_lookup = storage.Create<PropertyLookup>(atom->identifier_, prop_pair.first);
        auto *prop_equal = storage.Create<EqualOperator>(property_lookup, prop_pair.second);
        collector.symbols_.clear();
        prop_equal->Accept(collector);
        FilterInfo filter_info{FilterInfo::Type::Property, prop_equal, collector.symbols_};
        // Store a PropertyFilter on the value of the property.
        filter_info.property_filter.emplace(
            symbol_table, symbol, prop_pair.first, prop_pair.second, PropertyFilter::Type::EQUAL);
        all_filters_.emplace_back(filter_info);
      }
      return;
    }
    throw SemanticException("Property map matching not supported in MATCH/MERGE clause!");
  };
  auto add_node_filter = [&](NodeAtom *node) {
    const auto &node_symbol = symbol_table.at(*node->identifier_);
    const auto conjunction = node->LabelConjunction();
    if (!conjunction) {
      // Pattern position and WHERE position have to yield the same filters, so the term goes through the
      // very analysis a WHERE expression gets.
      CollectFilterExpression(LabelsTest::Make(storage, node->identifier_, *node->label_term_),
                              symbol_table,
                              storage,
                              LabelTestMerging::kAllowed);
      add_properties(node);
      return;
    }
    std::vector<LabelIx> labels;
    for (const auto &label : *conjunction) {
      if (std::get_if<Expression *>(&label)) {
        throw SemanticException("Property lookup not supported in MATCH/MERGE clause!");
      }
      labels.push_back(std::get<LabelIx>(label));
    }
    if (!labels.empty()) {
      // find existing LabelsTest that matched identifier
      auto it = std::ranges::find_if(all_filters_, MatchesIdentifier(node->identifier_));
      if (it == all_filters_.end()) {
        // No existing LabelTest for this identifier
        auto *labels_test = storage.Create<LabelsTest>(node->identifier_, labels);
        auto label_filter = FilterInfo{FilterInfo::Type::Label, labels_test, std::unordered_set<Symbol>{node_symbol}};
        DeriveLabelView(label_filter);
        all_filters_.emplace_back(label_filter);
      } else {
        // Add these labels to the existing LabelsTest.
        AddAndLabels(*it, labels);
      }
    }
    add_properties(node);
  };
  auto add_expand_filter = [&](NodeAtom *, EdgeAtom *edge, NodeAtom *node) {
    for (auto edge_type : edge->edge_types_) {
      if (std::get_if<Expression *>(&edge_type)) {
        throw SemanticException("Property lookup not supported in MATCH/MERGE clause!");
      }
    }

    if (edge->IsVariable())
      add_properties_variable(edge);
    else
      add_properties(edge);
    add_node_filter(node);
  };
  ForEachPattern(pattern, add_node_filter, add_expand_filter);
}

// Adds the where filter expression to `all_filters_` and collects additional
// information for potential property and label indexing.
void Filters::CollectWhereFilter(Where &where, const SymbolTable &symbol_table, AstStorage &storage) {
  CollectFilterExpression(where.expression_, symbol_table, storage, LabelTestMerging::kAllowed);
}

auto Filters::FromExpression(Expression *expr, const SymbolTable &symbol_table, AstStorage &storage) -> Filters {
  Filters filters;
  filters.CollectFilterExpression(expr, symbol_table, storage, LabelTestMerging::kAllowed);
  return filters;
}

void Filters::AddOperatorFilters(Expression *expr, const SymbolTable &symbol_table, AstStorage &storage) {
  CollectFilterExpression(expr, symbol_table, storage, LabelTestMerging::kForbidden);
}

// Adds the expression to `all_filters_` and collects additional
// information for potential property and label indexing.
void Filters::CollectFilterExpression(Expression *expr, const SymbolTable &symbol_table, AstStorage &storage,
                                      LabelTestMerging merging) {
  auto filters = SplitExpression(expr);
  for (const auto &filter : filters) {
    AnalyzeAndStoreFilter(filter, symbol_table, storage, merging);
  }
}

// Analyzes the filter expression by collecting information on filtering labels
// and properties to be used with indexing.
void Filters::AnalyzeAndStoreFilter(Expression *expr, const SymbolTable &symbol_table, AstStorage &storage,
                                    LabelTestMerging merging) {
  using Bound = PropertyFilter::Bound;
  UsedSymbolsCollector collector(symbol_table);
  expr->Accept(collector);
  auto make_filter = [&collector, &expr](FilterInfo::Type type) { return FilterInfo{type, expr, collector.symbols_}; };
  auto get_property_lookup = [](auto *maybe_lookup, auto *&prop_lookup, auto *&ident) -> bool {
    return (prop_lookup = utils::Downcast<PropertyLookup>(maybe_lookup)) &&
           (ident = utils::Downcast<Identifier>(prop_lookup->expression_));
  };
  auto is_nested_property_lookup = [](auto *maybe_lookup) {
    return utils::Downcast<PropertyLookup>(maybe_lookup) &&
           utils::Downcast<PropertyLookup>(utils::Downcast<PropertyLookup>(maybe_lookup)->expression_);
  };
  auto extract_nested_property_lookup = [&](auto *maybe_lookup) {
    auto *expr = utils::Downcast<PropertyLookup>(maybe_lookup);
    auto *prev_expr = expr;
    auto nested_properties = std::vector<memgraph::query::PropertyIx>{};
    while (expr) {
      nested_properties.emplace_back(expr->property_);
      prev_expr = expr;
      expr = utils::Downcast<PropertyLookup>(expr->expression_);
    }

    // Properties are reversed because the AST walk produces them in reverse
    // order. This returns them to the order as specified in the grammar.
    std::ranges::reverse(nested_properties);

    return std::pair<Identifier *, PropertyIxPath>{utils::Downcast<Identifier>(prev_expr->expression_),
                                                   std::move(nested_properties)};
  };
  // Checks if maybe_lookup is a property lookup, stores it as a
  // PropertyFilter and returns true. If it isn't, returns false.
  auto try_add_prop_filter = [&](auto *maybe_lookup, auto *val_expr, PropertyFilter::Type type) -> bool {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    if (is_nested_property_lookup(maybe_lookup)) {
      auto [ident, nested_properties] = extract_nested_property_lookup(maybe_lookup);
      if (!ident) {
        return false;
      }
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter =
          PropertyFilter(symbol_table, symbol_table.at(*ident), std::move(nested_properties), val_expr, type);
      all_filters_.emplace_back(filter);
      return true;
    }

    if (get_property_lookup(maybe_lookup, prop_lookup, ident)) {
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter =
          PropertyFilter(symbol_table, symbol_table.at(*ident), prop_lookup->property_, val_expr, type);
      all_filters_.emplace_back(filter);
      return true;
    }
    return false;
  };

  // Helper
  auto commutative_apply = [](auto &&func, auto &&arg1, auto &&arg2) {
    if (func(arg1, arg2)) return true;
    if (func(arg2, arg1)) return true;
    return false;
  };

  auto is_independant = [&symbol_table](Symbol const &sym, Expression *expression) {
    UsedSymbolsCollector collector{symbol_table};
    expression->Accept(collector);
    return !collector.symbols_.contains(sym);
  };

  auto get_point_distance_function =
      [&](Expression *expr, PropertyLookup *&propertyLookup, Identifier *&ident, Expression *&other) -> bool {
    auto *func = utils::Downcast<Function>(expr);
    auto isPointDistance = func && utils::ToUpperCase(func->function_name_) == "POINT.DISTANCE"sv;
    if (!isPointDistance) return false;
    if (func->arguments_.size() != 2) {
      throw SemanticException("point.distance function requires 2 arguments");
    }

    auto extract_prop_lookup_and_identifers = [&](Expression *lhs, Expression *rhs) -> bool {
      if (get_property_lookup(lhs, propertyLookup, ident)) {
        auto const &sym = symbol_table.at(*ident);
        if (!is_independant(sym, rhs)) return false;
        other = rhs;
        return true;
      }
      return false;
    };

    return commutative_apply(extract_prop_lookup_and_identifers, func->arguments_[0], func->arguments_[1]);
  };

  auto add_point_distance_filter = [&](auto *expr1, auto *expr2, PointDistanceCondition kind) {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    Expression *other = nullptr;
    if (get_point_distance_function(expr1, prop_lookup, ident, other)) {
      // point.distance(n.prop, other) > expr2
      auto const &sym = symbol_table.at(*ident);
      // if expr2 is also dependant on the same symbol then not possible to subsitute with a point index
      if (!is_independant(sym, expr2)) return false;
      auto filter = make_filter(FilterInfo::Type::Point);
      filter.point_filter.emplace(sym, prop_lookup->property_, other, kind, expr2);
      all_filters_.emplace_back(filter);
      return true;
    }
    return false;
  };

  auto get_point_withinbbox_function = [&](Expression *expr,
                                           PropertyLookup *&propertyLookup,
                                           Identifier *&ident,
                                           Expression *&bottom_left,
                                           Expression *&top_right) -> bool {
    auto *func = utils::Downcast<Function>(expr);
    auto isPointWithinbbox = func && utils::ToUpperCase(func->function_name_) == "POINT.WITHINBBOX"sv;
    if (!isPointWithinbbox) return false;
    if (func->arguments_.size() != 3) {
      throw SemanticException("point.withinbbox function requires 3 arguments");
    }

    auto extract_prop_lookup_and_identifers =
        [&](Expression *point, Expression *bottom_left_expr, Expression *top_right_expr) -> bool {
      if (get_property_lookup(point, propertyLookup, ident)) {
        auto const &scan_symbol = symbol_table.at(*ident);
        if (!is_independant(scan_symbol, bottom_left_expr)) return false;
        if (!is_independant(scan_symbol, top_right_expr)) return false;
        bottom_left = bottom_left_expr;
        top_right = top_right_expr;
        return true;
      }
      return false;
    };

    return extract_prop_lookup_and_identifers(func->arguments_[0], func->arguments_[1], func->arguments_[2]);
  };

  auto add_point_withinbbox_filter_binary = [&](auto *expr1, auto *expr2) {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    Expression *bottom_left = nullptr;
    Expression *top_right = nullptr;
    if (get_point_withinbbox_function(expr1, prop_lookup, ident, bottom_left, top_right)) {
      // point.withinbbox(n.prop, bottom_left, top_right) = expr (true/false)
      auto const &sym = symbol_table.at(*ident);
      // if expr2 is also dependant on the same symbol then not possible to subsitute with a point index
      // theoretically possible but in practice never
      auto filter = make_filter(FilterInfo::Type::Point);
      if (!is_independant(sym, expr2)) return false;
      // have to figure out WithinbboxCondition from expr2
      filter.point_filter.emplace(sym, prop_lookup->property_, bottom_left, top_right, expr2);
      all_filters_.emplace_back(filter);
      return true;
    }
    return false;
  };

  auto add_point_withinbbox_filter_unary = [&](auto *expr1, WithinBBoxCondition condition) {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    Expression *bottom_left = nullptr;
    Expression *top_right = nullptr;
    if (get_point_withinbbox_function(expr1, prop_lookup, ident, bottom_left, top_right)) {
      // point.withinbbox(n.prop, bottom_left, top_right)
      auto filter = make_filter(FilterInfo::Type::Point);
      filter.point_filter.emplace(symbol_table.at(*ident), prop_lookup->property_, bottom_left, top_right, condition);
      all_filters_.emplace_back(filter);
      return true;
    }
    return false;
  };

  // Checks if either the expr1 and expr2 are property lookups, adds them as
  // PropertyFilter and returns true. Otherwise, returns false.
  auto add_prop_greater = [&](auto *expr1, auto *expr2, auto bound_type) -> bool {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    bool is_prop_filter = false;

    // We need to get both possible lookups `n.prop > thing` and `thing > n.prop`
    // Only one can be used in the rewrite, so even when we add both into all_filters_ only one can be used
    // example where two can be inserted `n0.propA > n1.propB`, the rewritten scan could be for n0 or n1
    if (is_nested_property_lookup(expr1)) {
      auto [ident, nested_properties] = extract_nested_property_lookup(expr1);
      if (ident) {
        auto filter = make_filter(FilterInfo::Type::Property);
        filter.property_filter = PropertyFilter(
            symbol_table, symbol_table.at(*ident), nested_properties, Bound(expr2, bound_type), std::nullopt);
        all_filters_.emplace_back(filter);
      }
    } else if (get_property_lookup(expr1, prop_lookup, ident)) {
      // n.prop > value
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter.emplace(
          symbol_table, symbol_table.at(*ident), prop_lookup->property_, Bound(expr2, bound_type), std::nullopt);
      all_filters_.emplace_back(filter);
      is_prop_filter = true;
    }
    if (is_nested_property_lookup(expr2)) {
      auto [ident, nested_properties] = extract_nested_property_lookup(expr2);
      if (ident) {
        auto filter = make_filter(FilterInfo::Type::Property);
        filter.property_filter = PropertyFilter(symbol_table,
                                                symbol_table.at(*ident),
                                                std::move(nested_properties),
                                                std::nullopt,
                                                Bound(expr1, bound_type));
        all_filters_.emplace_back(filter);
      }
    } else if (get_property_lookup(expr2, prop_lookup, ident)) {
      // value > n.prop
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter.emplace(
          symbol_table, symbol_table.at(*ident), prop_lookup->property_, std::nullopt, Bound(expr1, bound_type));
      all_filters_.emplace_back(filter);
      is_prop_filter = true;
    }
    return is_prop_filter;
  };
  auto add_prop_range = [&](RangeOperator *range) {
    auto filter = RangeOpToFilter(range, symbol_table);
    all_filters_.emplace_back(filter);
    return;
  };
  // Returns the identifier argument of id(ident)/elementId(ident) with whether
  // it was elementId (a string id), or a null identifier if expr is neither.
  auto as_id_function = [](auto *expr) -> std::pair<Identifier *, bool> {
    auto *id_fun = utils::Downcast<Function>(expr);
    if (!id_fun) return {nullptr, false};
    const bool is_element_id = id_fun->function_name_ == kElementId;
    if (id_fun->function_name_ != kId && !is_element_id) return {nullptr, false};
    if (id_fun->arguments_.size() != 1U) return {nullptr, false};
    return {utils::Downcast<Identifier>(id_fun->arguments_.front()), is_element_id};
  };
  // Adds `id(n) = val_expr` (or elementId) as an equality IdFilter.
  auto add_id_equal = [&](auto *maybe_id_fun, auto *val_expr) -> bool {
    auto [ident, is_element_id] = as_id_function(maybe_id_fun);
    if (!ident) return false;
    auto filter = make_filter(FilterInfo::Type::Id);
    filter.id_filter.emplace(symbol_table, symbol_table.at(*ident), val_expr, is_element_id);
    all_filters_.emplace_back(filter);
    return true;
  };
  // A statically-known non-list literal (e.g. `IN 'a'`) can never be a
  // membership list, so leave it to the generic Filter: rewriting it would
  // throw even on empty input, where the Filter yields no rows. Parameters and
  // other expressions are admitted; their list-ness is only known at runtime.
  auto is_non_list_literal = [](Expression *expr) {
    return utils::Downcast<BaseLiteral>(expr) && !utils::Downcast<ListLiteral>(expr);
  };
  // Classifies `id(n) IN val_expr` as an IN-flavoured IdFilter. elementId() is
  // excluded; string ids are deferred.
  auto add_id_in_list = [&](auto *maybe_id_fun, auto *val_expr) -> bool {
    if (is_non_list_literal(val_expr)) return false;
    auto [ident, is_element_id] = as_id_function(maybe_id_fun);
    if (!ident || is_element_id) return false;
    auto filter = make_filter(FilterInfo::Type::Id);
    filter.id_filter.emplace(
        symbol_table, symbol_table.at(*ident), val_expr, /*expects_string_id=*/false, IdFilter::Type::IN);
    all_filters_.emplace_back(filter);
    return true;
  };
  // Checks if maybe_lookup is a property lookup, stores it as a
  // PropertyFilter and returns true. If it isn't, returns false.
  auto add_prop_in_list = [&](auto *maybe_lookup, auto *val_expr) -> bool {
    if (is_non_list_literal(val_expr)) return false;
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    if (is_nested_property_lookup(maybe_lookup)) {
      auto [ident, nested_properties] = extract_nested_property_lookup(maybe_lookup);
      if (!ident) {
        return false;
      }
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter = PropertyFilter(
          symbol_table, symbol_table.at(*ident), std::move(nested_properties), val_expr, PropertyFilter::Type::IN);
      all_filters_.emplace_back(filter);
      return true;
    }
    if (get_property_lookup(maybe_lookup, prop_lookup, ident)) {
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter = PropertyFilter(
          symbol_table, symbol_table.at(*ident), prop_lookup->property_, val_expr, PropertyFilter::Type::IN);
      all_filters_.emplace_back(filter);
      return true;
    }
    return false;
  };

  // Checks whether maybe_prop_not_null_check is the null check on a property,
  // ("prop IS NOT NULL"), stores it as a PropertyFilter if it is, and returns
  // true. If it isn't returns false.
  auto add_prop_is_not_null_check = [&](auto *maybe_is_not_null_check) -> bool {
    // Strip away the outer NOT operator, and figure out
    // whether the inner expression is of the form "prop IS NULL"
    if (!maybe_is_not_null_check) {
      return false;
    }

    auto *maybe_is_null_check = utils::Downcast<IsNullOperator>(maybe_is_not_null_check->expression_);
    if (!maybe_is_null_check) {
      return false;
    }
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;

    if (get_property_lookup(maybe_is_null_check->expression_, prop_lookup, ident)) {
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter =
          PropertyFilter(symbol_table.at(*ident), prop_lookup->property_, PropertyFilter::Type::IS_NOT_NULL);
      all_filters_.emplace_back(filter);
      return true;
    }
    if (is_nested_property_lookup(maybe_is_null_check->expression_)) {
      auto [ident, nested_properties] = extract_nested_property_lookup(maybe_is_null_check->expression_);
      if (!ident) {
        return false;
      }
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter =
          PropertyFilter(symbol_table.at(*ident), std::move(nested_properties), PropertyFilter::Type::IS_NOT_NULL);
      all_filters_.emplace_back(filter);
      return true;
    }
    return false;
  };
  // We are only interested to see the insides of And, because Or prevents
  // indexing since any labels and properties found there may be optional.
  DMG_ASSERT(!utils::IsSubtype(*expr, AndOperator::kType), "Expected AndOperators have been split.");

  if (auto *edgetype_test = utils::Downcast<EdgeTypesTest>(expr)) {
    if (auto *identifier = utils::Downcast<Identifier>(edgetype_test->expression_)) {
      if (auto it = std::ranges::find_if(all_filters_, MatchesIdentifier(identifier)); it == all_filters_.end()) {
        // No existing EdgeTypesTest for this identifier
        auto filter = make_filter(FilterInfo::Type::EdgeType);
        filter.edgetypes = edgetype_test->valid_edgetypes_;
        all_filters_.emplace_back(filter);
      } else {
        // We could intersect the two OR lists, but for now we don't do that processing and keep it as  separate filter
        all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
      }
    } else {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *labels_test = utils::Downcast<LabelsTest>(expr)) {
    // The conjuncts of a label expression that index selection can use become tests of their own. In reverse,
    // as SplitExpression hands over the operands of an AND.
    if (auto pieces = LabelsTest::Split(storage, *labels_test); !pieces.empty()) {
      for (auto *piece : pieces | std::views::reverse) {
        AnalyzeAndStoreFilter(piece, symbol_table, storage, merging);
      }
      return;
    }
    // Since LabelsTest may contain any expression, we can only use the
    // simplest test on an identifier.
    if (auto *identifier = utils::Downcast<Identifier>(labels_test->expression_)) {
      if (labels_test->IsNodeTest()) {
        all_filters_.emplace_back(make_filter(FilterInfo::Type::Node));
        return;
      }
      // Index selection reads only the labels a test demands outright. A whole term demands none: an index scan
      // must never absorb it, `%` included, which a node scan implies but an edge scan does not.
      const auto *cnf = labels_test->Cnf();
      if (!cnf) {
        all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
        return;
      }
      auto it = std::ranges::find_if(all_filters_, MatchesIdentifier(identifier));
      if (it == all_filters_.end() || merging == LabelTestMerging::kForbidden) {
        // Either no label test of this identifier was collected yet, or the one collected belongs to another
        // operator and says nothing about the rows this expression is asked about.
        auto filter = make_filter(FilterInfo::Type::Label);
        DeriveLabelView(filter);
        all_filters_.emplace_back(filter);
      } else {
        // Each disjunction this test names is a conjunct of its own, so it is kept whole; the labels it
        // requires outright are added to those the test already requires.
        for (const auto &group : cnf->or_labels) {
          AddOrLabelGroup(*it, group);
        }
        AddAndLabels(*it, cnf->labels);
      }
    } else {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *eq = utils::Downcast<EqualOperator>(expr)) {
    // Try to get property equality test from the top expressions.
    // Unfortunately, we cannot go deeper inside Equal, because chained equals
    // need not correspond to And. For example, `(n.prop = value) = false)`:
    //         EQ
    //       /    \
    //      EQ   false  -- top expressions
    //    /    \
    // n.prop  value
    // Here the `prop` may be different than `value` resulting in `false`. This
    // would compare with the top level `false`, producing `true`. Therefore, it
    // is incorrect to pick up `n.prop = value` for scanning by property index.
    bool is_prop_filter = try_add_prop_filter(eq->expression1_, eq->expression2_, PropertyFilter::Type::EQUAL);
    // And reversed.
    is_prop_filter |= try_add_prop_filter(eq->expression2_, eq->expression1_, PropertyFilter::Type::EQUAL);
    // Try to get ID equality filter.
    bool is_id_filter = add_id_equal(eq->expression1_, eq->expression2_);
    is_id_filter |= add_id_equal(eq->expression2_, eq->expression1_);

    // WHERE point.withinbbox() = true/
    if (commutative_apply(add_point_withinbbox_filter_binary, eq->expression1_, eq->expression2_)) {
      return;
    }

    if (!is_prop_filter && !is_id_filter) {
      // No special filter was added, so just store a generic filter.
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *regex_match = utils::Downcast<RegexMatch>(expr)) {
    if (!try_add_prop_filter(regex_match->string_expr_, regex_match->regex_, PropertyFilter::Type::REGEX_MATCH)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *range = utils::Downcast<RangeOperator>(expr)) {
    // We only support property type ranges for now
    add_prop_range(range);
  } else if (auto *gt = utils::Downcast<GreaterOperator>(expr)) {
    if (
        // look for point.distance(n.prop, other) > expr2
        !add_point_distance_filter(gt->expression1_, gt->expression2_, PointDistanceCondition::OUTSIDE) &&
        // look for expr2 > point.distance(n.prop, other)
        !add_point_distance_filter(gt->expression2_, gt->expression1_, PointDistanceCondition::INSIDE) &&
        !add_prop_greater(gt->expression1_, gt->expression2_, Bound::Type::EXCLUSIVE)) {
      // fallback generic
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *ge = utils::Downcast<GreaterEqualOperator>(expr)) {
    if (
        // look for point.distance(n.prop, other) >= expr2
        !add_point_distance_filter(ge->expression1_, ge->expression2_, PointDistanceCondition::OUTSIDE_AND_BOUNDARY) &&
        // look for expr2 >= point.distance(n.prop, other)
        !add_point_distance_filter(ge->expression2_, ge->expression1_, PointDistanceCondition::INSIDE_AND_BOUNDARY) &&
        !add_prop_greater(ge->expression1_, ge->expression2_, Bound::Type::INCLUSIVE)) {
      // fallback generic
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *lt = utils::Downcast<LessOperator>(expr)) {
    if (
        // look for point.distance(n.prop, other) < expr2
        !add_point_distance_filter(lt->expression1_, lt->expression2_, PointDistanceCondition::INSIDE) &&
        // look for expr2 < point.distance(n.prop, other)
        !add_point_distance_filter(lt->expression2_, lt->expression1_, PointDistanceCondition::OUTSIDE) &&
        // Like greater, but in reverse.
        !add_prop_greater(lt->expression2_, lt->expression1_, Bound::Type::EXCLUSIVE)) {
      // fallback generic
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *le = utils::Downcast<LessEqualOperator>(expr)) {
    if (
        // look for point.distance(n.prop, other) <= expr2
        !add_point_distance_filter(le->expression1_, le->expression2_, PointDistanceCondition::INSIDE_AND_BOUNDARY) &&
        // look for expr2 <= point.distance(n.prop, other)
        !add_point_distance_filter(le->expression2_, le->expression1_, PointDistanceCondition::OUTSIDE_AND_BOUNDARY) &&
        // Like greater equal, but in reverse.
        !add_prop_greater(le->expression2_, le->expression1_, Bound::Type::INCLUSIVE)) {
      // fallback generic
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *in = utils::Downcast<InListOperator>(expr)) {
    // IN isn't equivalent to Equal because IN isn't a symmetric operator. The
    // IN filter is captured here only if the property lookup or id() occurs on
    // the left side of the operator. In that case, it's valid to do the IN list
    // optimization during the index lookup rewrite phase.
    if (!add_prop_in_list(in->expression1_, in->expression2_) && !add_id_in_list(in->expression1_, in->expression2_)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *is_not = utils::Downcast<NotOperator>(expr)) {
    // Negating twice leaves a predicate unchanged, `null` included, so analyse
    // what the pair wraps. Left as it is, the outer negation would only ever
    // yield a generic filter and hide a comparison an index could answer.
    // Re-entering CollectFilterExpression also splits any conjunction inside.
    if (auto *inner_not = utils::Downcast<NotOperator>(is_not->expression_)) {
      CollectFilterExpression(inner_not->expression_, symbol_table, storage, merging);
      return;
    }
    // WHERE NOT point.withinbbox()
    if (!add_point_withinbbox_filter_unary(is_not->expression_, WithinBBoxCondition::OUTSIDE) &&
        !add_prop_is_not_null_check(is_not)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (utils::Downcast<SubqueryExpression>(expr)) {
    all_filters_.emplace_back(make_filter(FilterInfo::Type::Pattern));
  } else if (auto *func = utils::Downcast<Function>(expr)) {
    auto const &fn = func->function_name_;
    // WHERE point.withinbbox()
    if (add_point_withinbbox_filter_unary(expr, WithinBBoxCondition::INSIDE)) {
      // handled
    } else if ((fn == kContains || fn == kStartsWith || fn == kEndsWith) && func->arguments_.size() == 2) {
      auto const type = [&] {
        if (fn == kStartsWith) return PropertyFilter::Type::STARTS_WITH;
        if (fn == kContains) return PropertyFilter::Type::CONTAINS;
        return PropertyFilter::Type::ENDS_WITH;
      }();
      if (!try_add_prop_filter(func->arguments_[0], func->arguments_[1], type)) {
        all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
      }
    } else {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *or_operator = utils::Downcast<OrOperator>(expr)) {
    auto filters = SplitExpression(or_operator, SplitExpressionMode::OR);
    // Fold an OR of label choices on one variable into one OR group of a LabelsTest. A choice is what
    // DisjunctLabels accepts. Any other OR stays generic: `n:A OR m:B` is not a group of either variable.
    std::optional<int32_t> symbol_pos;
    auto is_each_labels_test = std::ranges::all_of(filters, [&](auto &filter) {
      auto *labels_test = utils::Downcast<LabelsTest>(filter);
      if (!labels_test || !DisjunctLabels(*labels_test)) {
        return false;
      }
      auto *identifier = utils::Downcast<Identifier>(labels_test->expression_);
      if (!identifier) {
        return false;
      }
      if (!symbol_pos) {
        symbol_pos = identifier->symbol_pos_;
      }
      return identifier->symbol_pos_ == *symbol_pos;
    });
    if (is_each_labels_test) {
      // The disjuncts' labels are one group; a label stated twice is named once.
      std::vector<LabelIx> group;
      for (auto *filter : filters) {
        for (const auto &label : *DisjunctLabels(*utils::Downcast<LabelsTest>(filter))) {
          if (!std::ranges::contains(group, label)) {
            group.push_back(label);
          }
        }
      }
      auto *labels_test = utils::Downcast<LabelsTest>(filters.front());
      auto *identifier = utils::Downcast<Identifier>(labels_test->expression_);
      auto it = std::ranges::find_if(all_filters_, MatchesIdentifier(identifier));
      if (it == all_filters_.end() || merging == LabelTestMerging::kForbidden) {
        // The group gets a test of its own. Rewriting a disjunct's test would leave the query holding a test
        // it was not parsed with, and a later collection of the same query reading something else.
        auto *group_test = storage.Create<LabelsTest>(identifier, std::move(group), /*or_group=*/true);
        auto filter_info = FilterInfo{FilterInfo::Type::Label, group_test, collector.symbols_};
        DeriveLabelView(filter_info);
        all_filters_.emplace_back(std::move(filter_info));
      } else {
        // One more OR group of the test already collected, kept whole, as in the pattern merge.
        AddOrLabelGroup(*it, std::move(group));
      }
    } else {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else {
    all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
  }
  // TODO: Collect comparisons like `expr1 < n.prop < expr2` for potential
  // indexing by range. Note, that the generated Ast uses AND for chained
  // relation operators. Therefore, `expr1 < n.prop < expr2` will be represented
  // as `expr1 < n.prop AND n.prop < expr2`.
}

// Fills the given Matching, by converting the Match patterns to normalized
// representation as Expansions. Filters used in the Match are also collected,
// as well as edge symbols which determine Cyphermorphism. Collecting filters
// will lift them out of a pattern and generate new expressions (just like they
// were in a Where clause).
void AddMatching(const std::vector<Pattern *> &patterns, Where *where, SymbolTable &symbol_table, AstStorage &storage,
                 Matching &matching) {
  std::vector<Expansion> expansions = NormalizePatterns(symbol_table, patterns);

  // At this point, all of the expansions have the expansion group id of -1
  // By the time the assigning is done, all the expansions should have their expansion group id adjusted
  AssignExpansionGroupIds(expansions, matching, symbol_table);

  // Add edge symbols for every expansion to ensure edge uniqueness
  CollectEdgeSymbols(expansions, matching, symbol_table);

  // Add all the symbols found in these expansions
  CollectExpansionSymbols(expansions, matching, symbol_table);

  // Matching is of reference type and needs to append the expansions
  AddExpansionsToMatching(expansions, matching, symbol_table);

  for (auto *const pattern : patterns) {
    matching.filters.CollectPatternFilters(*pattern, symbol_table, storage);
    if (pattern->identifier_->user_declared_) {
      std::vector<Symbol> path_elements;
      for (auto *const pattern_atom : pattern->atoms_)
        path_elements.push_back(symbol_table.at(*pattern_atom->identifier_));
      matching.named_paths.emplace(symbol_table.at(*pattern->identifier_), std::move(path_elements));
    }
  }
  if (where) {
    matching.filters.CollectWhereFilter(*where, symbol_table, storage);
  }
}

void AddMatching(const Match &match, SymbolTable &symbol_table, AstStorage &storage, Matching &matching) {
  AddMatching(match.patterns_, match.where_, symbol_table, storage, matching);

  // If there are any pattern filters, we add those as well
  for (auto &filter : matching.filters) {
    SubqueryMatchingCollector collector(symbol_table, storage);
    filter.expression->Accept(collector);
    filter.subquery_matchings = collector.getSubqueryMatchings();
    filter.pattern_comprehension_matchings = collector.getPatternComprehensionMatchings();
  }
}

// SubqueryMatchingCollector implementation
SubqueryMatchingCollector::SubqueryMatchingCollector(SymbolTable &symbol_table, AstStorage &storage)
    : symbol_table_(symbol_table), storage_(storage) {}

SubqueryMatchingCollector::~SubqueryMatchingCollector() = default;

bool SubqueryMatchingCollector::PreVisit(PatternComprehension &op) {
  PatternComprehensionMatching matching;
  AddMatching({op.pattern_}, op.filter_, symbol_table_, storage_, matching);

  // Handle named path variable (e.g., [path = (a)-[r]->(b) | ...])
  // Unlike MATCH patterns where the path name is on Pattern::identifier_,
  // pattern comprehensions store the path name in PatternComprehension::variable_.
  if (op.variable_ && op.variable_->user_declared_) {
    std::vector<Symbol> path_elements;
    path_elements.reserve(op.pattern_->atoms_.size());
    for (auto *const pattern_atom : op.pattern_->atoms_) {
      path_elements.push_back(symbol_table_.at(*pattern_atom->identifier_));
    }
    matching.named_paths.emplace(symbol_table_.at(*op.variable_), std::move(path_elements));
    // Also add the path symbol to expansion_symbols so it's recognized as bound
    matching.expansion_symbols.insert(symbol_table_.at(*op.variable_));
  }

  // Process nested pattern comprehensions in filters
  for (auto &filter : matching.filters) {
    SubqueryMatchingCollector nested_collector(symbol_table_, storage_);
    filter.expression->Accept(nested_collector);
    filter.subquery_matchings = nested_collector.getSubqueryMatchings();
    filter.pattern_comprehension_matchings = nested_collector.getPatternComprehensionMatchings();
  }

  // Process nested pattern comprehensions in result expression
  SubqueryMatchingCollector result_collector(symbol_table_, storage_);
  op.resultExpr_->Accept(result_collector);
  matching.nested_pattern_comprehensions = result_collector.getPatternComprehensionMatchings();

  // Create the result expression wrapper
  matching.result_expr = storage_.Create<NamedExpression>(symbol_table_.at(op).name(), op.resultExpr_);
  matching.result_expr->MapTo(symbol_table_.at(op));
  matching.result_symbol = symbol_table_.at(op);

  // Every symbol the comprehension reads that is NOT bound within it, e.g. the FOREACH variable `x` in
  // `[(a)-[r]->(b) WHERE a.id = x | b]`. `DepsSatisfied` decides from this when a comprehension may drain, so it must
  // cover every position an outer symbol can appear in, not just the two obvious expressions: a miss splices the
  // RollUpApply below the operator that writes the symbol, where it reads an unwritten frame slot.
  std::unordered_set<Symbol> used_symbols;

  // Collect symbols from filter expression
  if (op.filter_) {
    UsedSymbolsCollector filter_collector(symbol_table_);
    op.filter_->expression_->Accept(filter_collector);
    used_symbols.insert(filter_collector.symbols_.begin(), filter_collector.symbols_.end());
  }

  // Collect symbols from result expression
  UsedSymbolsCollector result_symbol_collector(symbol_table_);
  op.resultExpr_->Accept(result_symbol_collector);
  used_symbols.insert(result_symbol_collector.symbols_.begin(), result_symbol_collector.symbols_.end());

  // The collectors above skip the comprehension's own pattern, missing `[(p {name: name})-->(q) | q]`. `AddMatching`
  // already routed those into the property filters' `used_symbols`.
  for (const auto &filter : matching.filters) {
    used_symbols.insert(filter.used_symbols.begin(), filter.used_symbols.end());
  }

  // `UsedSymbolsCollector` visits only a nested comprehension's pattern, missing what its filter reads. A nested
  // matching's `external_symbols` comes from this same block, so it is already complete.
  auto collect_nested_external = [&used_symbols](const PatternComprehensionMatchings &nested) {
    for (const auto &nested_pc : nested) {
      used_symbols.insert(nested_pc.external_symbols.begin(), nested_pc.external_symbols.end());
    }
  };
  collect_nested_external(matching.nested_pattern_comprehensions);
  for (const auto &filter : matching.filters) {
    collect_nested_external(filter.pattern_comprehension_matchings);
  }

  // Collect symbols bound by nested pattern comprehensions.
  // These should NOT be treated as external symbols - they are bound within their respective nested PCs.
  std::unordered_set<Symbol> nested_pc_symbols;
  for (const auto &nested_pc : matching.nested_pattern_comprehensions) {
    nested_pc_symbols.insert(nested_pc.expansion_symbols.begin(), nested_pc.expansion_symbols.end());
  }

  // External symbols = used symbols - expansion symbols - nested PC symbols
  for (const auto &sym : used_symbols) {
    if (!matching.expansion_symbols.contains(sym) && !nested_pc_symbols.contains(sym)) {
      matching.external_symbols.insert(sym);
    }
  }

  pattern_comprehension_matchings_.push_back(std::move(matching));

  return false;  // Don't auto-traverse, we handled it manually
}

bool SubqueryMatchingCollector::PreVisit(SubqueryExpression &op) {
  SubqueryMatching subquery_matching;
  subquery_matching.symbol = std::make_optional<Symbol>(symbol_table_.at(op));
  subquery_matching.fold = op.fold_;

  if (op.HasPattern()) {
    std::vector<Pattern *> patterns;
    patterns.push_back(op.GetPattern());
    AddMatching(patterns, nullptr, symbol_table_, storage_, subquery_matching);
    subquery_matching.type = SubqueryKind::kPattern;
  } else if (op.HasSubquery()) {
    subquery_matching.type = SubqueryKind::kSubquery;
    subquery_matching.subquery =
        std::make_shared<QueryParts>(CollectQueryParts(symbol_table_, storage_, op.GetSubquery(), true));
  } else {
    throw SemanticException(
        "{} semantic is neither of type pattern, or subquery! Please contact Memgraph support as this scenario "
        "should not happen!",
        op.FoldName());
  }

  subquery_matchings_.push_back(std::move(subquery_matching));

  return false;  // Don't auto-traverse, we handled it manually
}

std::vector<SubqueryMatching> SubqueryMatchingCollector::getSubqueryMatchings() { return subquery_matchings_; }

PatternComprehensionMatchings SubqueryMatchingCollector::getPatternComprehensionMatchings() {
  return pattern_comprehension_matchings_;
}

namespace {

// Collect MERGE matchings from FOREACH and its nested FOREACH clauses
// Note: Pattern comprehensions are now collected via SubqueryMatchingCollector
void CollectForeachMergeMatchings(Foreach &foreach, SingleQueryPart &query_part, AstStorage &storage,
                                  SymbolTable &symbol_table) {
  for (auto *clause : foreach.clauses_) {
    if (auto *merge = utils::Downcast<Merge>(clause)) {
      query_part.merge_matching.emplace_back(Matching{});
      AddMatching({merge->pattern_}, nullptr, symbol_table, storage, query_part.merge_matching.back());
    } else if (auto *nested = utils::Downcast<Foreach>(clause)) {
      CollectForeachMergeMatchings(*nested, query_part, storage, symbol_table);
    }
  }
}

}  // namespace

// Converts a Query to multiple QueryParts. In the process new Ast nodes may be
// created, e.g. filter expressions.
std::vector<SingleQueryPart> CollectSingleQueryParts(SymbolTable &symbol_table, AstStorage &storage,
                                                     SingleQuery *single_query) {
  std::vector<SingleQueryPart> query_parts(1);
  auto *query_part = &query_parts.back();
  for (auto *clause : single_query->clauses_) {
    if (auto *match = utils::Downcast<Match>(clause)) {
      if (!query_part->remaining_clauses.empty()) {
        // New match started
        // This query part is done, continue with a new one.
        query_parts.emplace_back(SingleQueryPart{});
        query_part = &query_parts.back();
      }
      if (match->optional_) {
        query_part->optional_matching.emplace_back();
        AddMatching(*match, symbol_table, storage, query_part->optional_matching.back());
      } else {
        DMG_ASSERT(query_part->optional_matching.empty(), "Match clause cannot follow optional match.");
        AddMatching(*match, symbol_table, storage, query_part->matching);
      }
    } else {
      query_part->remaining_clauses.push_back(clause);

      // Handle MERGE matching (direct or nested in FOREACH)
      if (auto *merge = utils::Downcast<Merge>(clause)) {
        query_part->merge_matching.emplace_back(Matching{});
        AddMatching({merge->pattern_}, nullptr, symbol_table, storage, query_part->merge_matching.back());
      } else if (auto *foreach = utils::Downcast<Foreach>(clause)) {
        CollectForeachMergeMatchings(*foreach, *query_part, storage, symbol_table);
      } else if (auto *call_subquery = utils::Downcast<CallSubquery>(clause)) {
        query_part->subqueries.emplace_back(
            std::make_shared<QueryParts>(CollectQueryParts(symbol_table, storage, call_subquery->cypher_query_, true)));
      }

      // Collect pattern comprehensions from ALL non-MATCH clauses using HierarchicalTreeVisitor.
      // This automatically traverses all expressions in all clause types including:
      // - CREATE/MERGE pattern properties (NodeAtom/EdgeAtom)
      // - SET/DELETE expressions
      // - FOREACH list expression and nested clauses
      // - WITH/RETURN named_expressions, order_by, skip, limit, where
      // - UNWIND expression
      // - EdgeAtom lower_bound, upper_bound, total_weight, limit
      // Not traversed, so a comprehension there is collected by nobody and fails loudly at evaluation: EdgeAtom's
      // filter/weight lambdas, dynamic label expressions, and LOAD CSV/PARQUET/JSONL file and config expressions.
      SubqueryMatchingCollector collector(symbol_table, storage);
      clause->Accept(collector);
      // Record the originating clause, so a drain can tell "can this be planned yet?" from "should it be?".
      auto matchings = collector.getPatternComprehensionMatchings();
      for (auto &matching : matchings) {
        matching.origin_clause = clause;
        query_part->pattern_comprehension_matchings.push_back(std::move(matching));
      }
      // Keep the EXISTS matchings too - a WITH/RETURN body plans them on demand, keyed by result symbol.
      query_part->subquery_matchings.append_range(collector.getSubqueryMatchings());

      // Handle query part boundaries
      if (utils::Downcast<With>(clause) || utils::Downcast<Unwind>(clause) ||
          utils::IsSubtype(*clause, CallProcedure::kType) || utils::IsSubtype(*clause, LoadCsv::kType) ||
          utils::IsSubtype(*clause, LoadParquet::kType) || utils::IsSubtype(*clause, LoadJsonl::kType)) {
        query_parts.emplace_back(SingleQueryPart{});
        query_part = &query_parts.back();
      } else if (utils::Downcast<Return>(clause)) {
        return query_parts;
      }
    }
  }
  return query_parts;
}

namespace {

QueryParts CollectQueryParts(SymbolTable &symbol_table, AstStorage &storage, CypherQuery *query, bool is_subquery,
                             Expression *commit_frequency);

/// One UNION leg. A WHEN body becomes its branches, its predicates, and per predicate the filters holding its
/// subqueries; it has no query parts of its own.
QueryPart CollectQueryPart(SymbolTable &symbol_table, AstStorage &storage, SingleQuery *single_query, Tree *combinator,
                           bool is_subquery, Expression *commit_frequency) {
  auto *branches =
      single_query->clauses_.size() == 1 ? utils::Downcast<ConditionalBranches>(single_query->clauses_[0]) : nullptr;
  if (!branches) return QueryPart{CollectSingleQueryParts(symbol_table, storage, single_query), combinator};

  auto conditional = std::make_shared<ConditionalQueryParts>();
  conditional->discriminator = *branches->discriminator_;
  conditional->output_symbols = branches->output_symbols_;
  for (auto *predicate : branches->predicates_) {
    conditional->predicates.push_back(predicate);
    auto &filters = conditional->predicate_filters.emplace_back();
    if (!predicate) continue;
    filters = Filters::FromExpression(predicate, symbol_table, storage);
    for (auto &filter : filters) {
      SubqueryMatchingCollector collector(symbol_table, storage);
      filter.expression->Accept(collector);
      filter.subquery_matchings = collector.getSubqueryMatchings();
      filter.pattern_comprehension_matchings = collector.getPatternComprehensionMatchings();
    }
  }
  for (auto *body : branches->bodies_) {
    conditional->branches.push_back(CollectQueryParts(symbol_table, storage, body, is_subquery, commit_frequency));
  }
  return QueryPart{{}, combinator, std::move(conditional)};
}

/// A conditional branch has no directives of its own and inherits the enclosing `IN TRANSACTIONS` frequency.
QueryParts CollectQueryParts(SymbolTable &symbol_table, AstStorage &storage, CypherQuery *query, bool is_subquery,
                             Expression *commit_frequency) {
  std::vector<QueryPart> query_parts;

  auto *single_query = query->single_query_;
  MG_ASSERT(single_query, "Expected at least a single query");
  query_parts.push_back(CollectQueryPart(symbol_table, storage, single_query, nullptr, is_subquery, commit_frequency));

  bool distinct = false;
  for (auto *cypher_union : query->cypher_unions_) {
    if (cypher_union->distinct_) {
      distinct = true;
    }

    auto *single_query = cypher_union->single_query_;
    MG_ASSERT(single_query, "Expected UNION to have a query");
    query_parts.push_back(
        CollectQueryPart(symbol_table, storage, single_query, cypher_union, is_subquery, commit_frequency));
  }

  bool const writes = std::ranges::any_of(query_parts, [](const QueryPart &part) {
    return std::ranges::any_of(part.single_query_parts,
                               [](const SingleQueryPart &single_part) {
                                 return std::ranges::any_of(single_part.remaining_clauses,
                                                            [](const Clause *c) { return IsWritingClause(*c); }) ||
                                        std::ranges::any_of(single_part.subqueries,
                                                            [](const auto &subquery) { return subquery->writes; });
                               }) ||
           (part.conditional &&
            std::ranges::any_of(part.conditional->branches, [](const QueryParts &branch) { return branch.writes; }));
  });
  return QueryParts{.query_parts = query_parts,
                    .distinct = distinct,
                    .commit_frequency = commit_frequency,
                    .is_subquery = is_subquery,
                    .writes = writes};
}

}  // namespace

QueryParts CollectQueryParts(SymbolTable &symbol_table, AstStorage &storage, CypherQuery *query, bool is_subquery) {
  return CollectQueryParts(symbol_table, storage, query, is_subquery, query->pre_query_directives_.commit_frequency_);
}

// TODO: Think about converting all filtering expression into CNF to improve
// the granularity of filters which can be stand alone.
std::vector<Expression *> SplitExpression(Expression *expression, SplitExpressionMode mode) {
  std::vector<Expression *> expressions;
  std::stack<Expression *> pending_expressions;
  pending_expressions.push(expression);
  while (!pending_expressions.empty()) {
    auto *current_expression = pending_expressions.top();
    pending_expressions.pop();

    if (mode == SplitExpressionMode::AND) {
      if (auto *and_op = utils::Downcast<AndOperator>(current_expression)) {
        pending_expressions.push(and_op->expression1_);
        pending_expressions.push(and_op->expression2_);
        continue;
      }
    } else if (mode == SplitExpressionMode::OR) {
      if (auto *or_op = utils::Downcast<OrOperator>(current_expression)) {
        pending_expressions.push(or_op->expression1_);
        pending_expressions.push(or_op->expression2_);
        continue;
      }
    }

    expressions.push_back(current_expression);
  }

  return expressions;
}

Expression *SplitLabelsTests(Expression *expression, AstStorage &storage) {
  if (auto *conjunction = utils::Downcast<AndOperator>(expression)) {
    auto *lhs = SplitLabelsTests(conjunction->expression1_, storage);
    auto *rhs = SplitLabelsTests(conjunction->expression2_, storage);
    if (lhs == conjunction->expression1_ && rhs == conjunction->expression2_) return expression;
    return storage.Create<AndOperator>(lhs, rhs);
  }
  if (auto *outer = utils::Downcast<NotOperator>(expression)) {
    // Filter collection reads `NOT NOT x` as `x`, so the result holds what it collects. The negations go: an index
    // scan removes only a test among the AND operands.
    if (auto *inner = utils::Downcast<NotOperator>(outer->expression_)) {
      auto *operand = SplitLabelsTests(inner->expression_, storage);
      return operand == inner->expression_ ? expression : operand;
    }
    return expression;
  }
  auto *labels_test = utils::Downcast<LabelsTest>(expression);
  if (!labels_test) return expression;
  Expression *joined = nullptr;
  for (auto *piece : LabelsTest::Split(storage, *labels_test)) {
    joined = joined ? static_cast<Expression *>(storage.Create<AndOperator>(joined, piece)) : piece;
  }
  return joined ? joined : expression;
}

Expression *SubstituteExpression(Expression *expression, Expression *old, Expression *in) {
  if (expression == old) return in;

  std::stack<Expression *> pending_expressions;
  pending_expressions.push(expression);

  while (!pending_expressions.empty()) {
    auto *current_expression = pending_expressions.top();
    pending_expressions.pop();
    if (auto *and_op = utils::Downcast<AndOperator>(current_expression)) {
      if (and_op->expression1_ == old) {
        and_op->expression1_ = in;
        break;
      }
      if (and_op->expression2_ == old) {
        and_op->expression2_ = in;
        break;
      }
      pending_expressions.push(and_op->expression1_);
      pending_expressions.push(and_op->expression2_);
    }
  }
  return expression;
}

FilterInfo::FilterInfo(Type type, Expression *expression, std::unordered_set<Symbol> used_symbols,
                       std::optional<PropertyFilter> property_filter, std::optional<IdFilter> id_filter)
    : type(type),
      expression(expression),
      used_symbols(std::move(used_symbols)),
      property_filter(std::move(property_filter)),
      id_filter(std::move(id_filter)),
      subquery_matchings({}) {}

FilterInfo::FilterInfo(const FilterInfo &) = default;
FilterInfo &FilterInfo::operator=(const FilterInfo &) = default;
FilterInfo::FilterInfo(FilterInfo &&) noexcept = default;
FilterInfo &FilterInfo::operator=(FilterInfo &&) noexcept = default;
FilterInfo::~FilterInfo() = default;

}  // namespace memgraph::query::plan
