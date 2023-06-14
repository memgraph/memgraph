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

#include <algorithm>
#include <functional>
#include <stack>
#include <type_traits>
#include <unordered_map>
#include <variant>

#include "query/exceptions.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/ast/ast_visitor.hpp"
#include "query/plan/preprocess.hpp"
#include "utils/typeinfo.hpp"

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
  auto ignore_node = [&](auto *) {};
  auto collect_expansion = [&](auto *prev_node, auto *edge, auto *current_node) {
    UsedSymbolsCollector collector(symbol_table);
    if (edge->IsVariable()) {
      if (edge->lower_bound_) edge->lower_bound_->Accept(collector);
      if (edge->upper_bound_) edge->upper_bound_->Accept(collector);
      if (edge->filter_lambda_.expression) edge->filter_lambda_.expression->Accept(collector);
      // Remove symbols which are bound by lambda arguments.
      collector.symbols_.erase(symbol_table.at(*edge->filter_lambda_.inner_edge));
      collector.symbols_.erase(symbol_table.at(*edge->filter_lambda_.inner_node));
      if (edge->type_ == EdgeAtom::Type::WEIGHTED_SHORTEST_PATH || edge->type_ == EdgeAtom::Type::ALL_SHORTEST_PATHS) {
        collector.symbols_.erase(symbol_table.at(*edge->weight_lambda_.inner_edge));
        collector.symbols_.erase(symbol_table.at(*edge->weight_lambda_.inner_node));
      }
    }
    expansions.emplace_back(Expansion{prev_node, edge, edge->direction_, false, collector.symbols_, current_node});
  };
  for (const auto &pattern : patterns) {
    if (pattern->atoms_.size() == 1U) {
      auto *node = utils::Downcast<NodeAtom>(pattern->atoms_[0]);
      DMG_ASSERT(node, "First pattern atom is not a node");
      expansions.emplace_back(Expansion{node});
    } else {
      ForEachPattern(*pattern, ignore_node, collect_expansion);
    }
  }
  return expansions;
}

auto SplitExpressionOnAnd(Expression *expression) {
  // TODO: Think about converting all filtering expression into CNF to improve
  // the granularity of filters which can be stand alone.
  std::vector<Expression *> expressions;
  std::stack<Expression *> pending_expressions;
  pending_expressions.push(expression);
  while (!pending_expressions.empty()) {
    auto *current_expression = pending_expressions.top();
    pending_expressions.pop();
    if (auto *and_op = utils::Downcast<AndOperator>(current_expression)) {
      pending_expressions.push(and_op->expression1_);
      pending_expressions.push(and_op->expression2_);
    } else {
      expressions.push_back(current_expression);
    }
  }
  return expressions;
}

}  // namespace

PropertyFilter::PropertyFilter(const SymbolTable &symbol_table, const Symbol &symbol, PropertyIx property,
                               Expression *value, Type type)
    : symbol_(symbol), property_(property), type_(type), value_(value) {
  MG_ASSERT(type != Type::RANGE);
  UsedSymbolsCollector collector(symbol_table);
  value->Accept(collector);
  is_symbol_in_value_ = utils::Contains(collector.symbols_, symbol);
}

PropertyFilter::PropertyFilter(const SymbolTable &symbol_table, const Symbol &symbol, PropertyIx property,
                               const std::optional<PropertyFilter::Bound> &lower_bound,
                               const std::optional<PropertyFilter::Bound> &upper_bound)
    : symbol_(symbol), property_(property), type_(Type::RANGE), lower_bound_(lower_bound), upper_bound_(upper_bound) {
  UsedSymbolsCollector collector(symbol_table);
  if (lower_bound) {
    lower_bound->value()->Accept(collector);
  }
  if (upper_bound) {
    upper_bound->value()->Accept(collector);
  }
  is_symbol_in_value_ = utils::Contains(collector.symbols_, symbol);
}

PropertyFilter::PropertyFilter(const Symbol &symbol, PropertyIx property, Type type)
    : symbol_(symbol), property_(property), type_(type) {
  // As this constructor is used for property filters where
  // we don't have to evaluate the filter expression, we set
  // the is_symbol_in_value_ to false, although the filter
  // expression may actually contain the symbol whose property
  // we may be looking up.
}

IdFilter::IdFilter(const SymbolTable &symbol_table, const Symbol &symbol, Expression *value)
    : symbol_(symbol), value_(value) {
  MG_ASSERT(value);
  UsedSymbolsCollector collector(symbol_table);
  value->Accept(collector);
  is_symbol_in_value_ = utils::Contains(collector.symbols_, symbol);
}

void Filters::EraseFilter(const FilterInfo &filter) {
  // TODO: Ideally, we want to determine the equality of both expression trees,
  // instead of a simple pointer compare.
  all_filters_.erase(std::remove_if(all_filters_.begin(), all_filters_.end(),
                                    [&filter](const auto &f) { return f.expression == filter.expression; }),
                     all_filters_.end());
}

void Filters::EraseLabelFilter(const Symbol &symbol, LabelIx label, std::vector<Expression *> *removed_filters) {
  for (auto filter_it = all_filters_.begin(); filter_it != all_filters_.end();) {
    if (filter_it->type != FilterInfo::Type::Label) {
      ++filter_it;
      continue;
    }
    if (!utils::Contains(filter_it->used_symbols, symbol)) {
      ++filter_it;
      continue;
    }
    auto label_it = std::find(filter_it->labels.begin(), filter_it->labels.end(), label);
    if (label_it == filter_it->labels.end()) {
      ++filter_it;
      continue;
    }
    filter_it->labels.erase(label_it);
    DMG_ASSERT(!utils::Contains(filter_it->labels, label), "Didn't expect duplicated labels");
    if (filter_it->labels.empty()) {
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
          // First handle the inline property filter.
          auto *property_lookup = storage.Create<PropertyLookup>(atom->filter_lambda_.inner_edge, prop_pair.first);
          auto *prop_equal = storage.Create<EqualOperator>(property_lookup, prop_pair.second);
          // Currently, variable expand has no gains if we set PropertyFilter.
          all_filters_.emplace_back(FilterInfo{FilterInfo::Type::Generic, prop_equal, collector.symbols_});
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
              FilterInfo{FilterInfo::Type::Generic,
                         storage.Create<All>(identifier, atom->identifier_, storage.Create<Where>(prop_equal)),
                         collector.symbols_});
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
        filter_info.property_filter.emplace(symbol_table, symbol, prop_pair.first, prop_pair.second,
                                            PropertyFilter::Type::EQUAL);
        all_filters_.emplace_back(filter_info);
      }
      return;
    }
    throw SemanticException("Property map matching not supported in MATCH/MERGE clause!");
  };
  auto add_node_filter = [&](NodeAtom *node) {
    const auto &node_symbol = symbol_table.at(*node->identifier_);
    if (!node->labels_.empty()) {
      // Create a LabelsTest and store it.
      auto *labels_test = storage.Create<LabelsTest>(node->identifier_, node->labels_);
      auto label_filter = FilterInfo{FilterInfo::Type::Label, labels_test, std::unordered_set<Symbol>{node_symbol}};
      label_filter.labels = node->labels_;
      all_filters_.emplace_back(label_filter);
    }
    add_properties(node);
  };
  auto add_expand_filter = [&](NodeAtom *, EdgeAtom *edge, NodeAtom *node) {
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
void Filters::CollectWhereFilter(Where &where, const SymbolTable &symbol_table) {
  CollectFilterExpression(where.expression_, symbol_table);
}

// Adds the expression to `all_filters_` and collects additional
// information for potential property and label indexing.
void Filters::CollectFilterExpression(Expression *expr, const SymbolTable &symbol_table) {
  auto filters = SplitExpressionOnAnd(expr);
  for (const auto &filter : filters) {
    AnalyzeAndStoreFilter(filter, symbol_table);
  }
}

// Analyzes the filter expression by collecting information on filtering labels
// and properties to be used with indexing.
void Filters::AnalyzeAndStoreFilter(Expression *expr, const SymbolTable &symbol_table) {
  using Bound = PropertyFilter::Bound;
  UsedSymbolsCollector collector(symbol_table);
  expr->Accept(collector);
  auto make_filter = [&collector, &expr](FilterInfo::Type type) { return FilterInfo{type, expr, collector.symbols_}; };
  auto get_property_lookup = [](auto *maybe_lookup, auto *&prop_lookup, auto *&ident) -> bool {
    return (prop_lookup = utils::Downcast<PropertyLookup>(maybe_lookup)) &&
           (ident = utils::Downcast<Identifier>(prop_lookup->expression_));
  };
  // Checks if maybe_lookup is a property lookup, stores it as a
  // PropertyFilter and returns true. If it isn't, returns false.
  auto add_prop_equal = [&](auto *maybe_lookup, auto *val_expr) -> bool {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    if (get_property_lookup(maybe_lookup, prop_lookup, ident)) {
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter = PropertyFilter(symbol_table, symbol_table.at(*ident), prop_lookup->property_, val_expr,
                                              PropertyFilter::Type::EQUAL);
      all_filters_.emplace_back(filter);
      return true;
    }
    return false;
  };
  // Like add_prop_equal, but for adding regex match property filter.
  auto add_prop_regex_match = [&](auto *maybe_lookup, auto *val_expr) -> bool {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    if (get_property_lookup(maybe_lookup, prop_lookup, ident)) {
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter = PropertyFilter(symbol_table, symbol_table.at(*ident), prop_lookup->property_, val_expr,
                                              PropertyFilter::Type::REGEX_MATCH);
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
    if (get_property_lookup(expr1, prop_lookup, ident)) {
      // n.prop > value
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter.emplace(symbol_table, symbol_table.at(*ident), prop_lookup->property_,
                                     Bound(expr2, bound_type), std::nullopt);
      all_filters_.emplace_back(filter);
      is_prop_filter = true;
    }
    if (get_property_lookup(expr2, prop_lookup, ident)) {
      // value > n.prop
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter.emplace(symbol_table, symbol_table.at(*ident), prop_lookup->property_, std::nullopt,
                                     Bound(expr1, bound_type));
      all_filters_.emplace_back(filter);
      is_prop_filter = true;
    }
    return is_prop_filter;
  };
  // Check if maybe_id_fun is ID invocation on an identifier and add it as
  // IdFilter.
  auto add_id_equal = [&](auto *maybe_id_fun, auto *val_expr) -> bool {
    auto *id_fun = utils::Downcast<Function>(maybe_id_fun);
    if (!id_fun) return false;
    if (id_fun->function_name_ != kId) return false;
    if (id_fun->arguments_.size() != 1U) return false;
    auto *ident = utils::Downcast<Identifier>(id_fun->arguments_.front());
    if (!ident) return false;
    auto filter = make_filter(FilterInfo::Type::Id);
    filter.id_filter.emplace(symbol_table, symbol_table.at(*ident), val_expr);
    all_filters_.emplace_back(filter);
    return true;
  };
  // Checks if maybe_lookup is a property lookup, stores it as a
  // PropertyFilter and returns true. If it isn't, returns false.
  auto add_prop_in_list = [&](auto *maybe_lookup, auto *val_expr) -> bool {
    if (!utils::Downcast<ListLiteral>(val_expr)) return false;
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    if (get_property_lookup(maybe_lookup, prop_lookup, ident)) {
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter = PropertyFilter(symbol_table, symbol_table.at(*ident), prop_lookup->property_, val_expr,
                                              PropertyFilter::Type::IN);
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

    if (!get_property_lookup(maybe_is_null_check->expression_, prop_lookup, ident)) {
      return false;
    }

    auto filter = make_filter(FilterInfo::Type::Property);
    filter.property_filter =
        PropertyFilter(symbol_table.at(*ident), prop_lookup->property_, PropertyFilter::Type::IS_NOT_NULL);
    all_filters_.emplace_back(filter);
    return true;
  };
  // We are only interested to see the insides of And, because Or prevents
  // indexing since any labels and properties found there may be optional.
  DMG_ASSERT(!utils::IsSubtype(*expr, AndOperator::kType), "Expected AndOperators have been split.");
  if (auto *labels_test = utils::Downcast<LabelsTest>(expr)) {
    // Since LabelsTest may contain any expression, we can only use the
    // simplest test on an identifier.
    if (utils::Downcast<Identifier>(labels_test->expression_)) {
      auto filter = make_filter(FilterInfo::Type::Label);
      filter.labels = labels_test->labels_;
      all_filters_.emplace_back(filter);
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
    bool is_prop_filter = add_prop_equal(eq->expression1_, eq->expression2_);
    // And reversed.
    is_prop_filter |= add_prop_equal(eq->expression2_, eq->expression1_);
    // Try to get ID equality filter.
    bool is_id_filter = add_id_equal(eq->expression1_, eq->expression2_);
    is_id_filter |= add_id_equal(eq->expression2_, eq->expression1_);
    if (!is_prop_filter && !is_id_filter) {
      // No special filter was added, so just store a generic filter.
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *regex_match = utils::Downcast<RegexMatch>(expr)) {
    if (!add_prop_regex_match(regex_match->string_expr_, regex_match->regex_)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *gt = utils::Downcast<GreaterOperator>(expr)) {
    if (!add_prop_greater(gt->expression1_, gt->expression2_, Bound::Type::EXCLUSIVE)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *ge = utils::Downcast<GreaterEqualOperator>(expr)) {
    if (!add_prop_greater(ge->expression1_, ge->expression2_, Bound::Type::INCLUSIVE)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *lt = utils::Downcast<LessOperator>(expr)) {
    // Like greater, but in reverse.
    if (!add_prop_greater(lt->expression2_, lt->expression1_, Bound::Type::EXCLUSIVE)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *le = utils::Downcast<LessEqualOperator>(expr)) {
    // Like greater equal, but in reverse.
    if (!add_prop_greater(le->expression2_, le->expression1_, Bound::Type::INCLUSIVE)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *in = utils::Downcast<InListOperator>(expr)) {
    // IN isn't equivalent to Equal because IN isn't a symmetric operator. The
    // IN filter is captured here only if the property lookup occurs on the
    // left side of the operator. In that case, it's valid to do the IN list
    // optimization during the index lookup rewrite phase.
    if (!add_prop_in_list(in->expression1_, in->expression2_)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *is_not_null = utils::Downcast<NotOperator>(expr)) {
    if (!add_prop_is_not_null_check(is_not_null)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *exists = utils::Downcast<Exists>(expr)) {
    all_filters_.emplace_back(make_filter(FilterInfo::Type::Pattern));
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
  auto expansions = NormalizePatterns(symbol_table, patterns);
  std::unordered_set<Symbol> edge_symbols;
  for (const auto &expansion : expansions) {
    // Matching may already have some expansions, so offset our index.
    const size_t expansion_ix = matching.expansions.size();
    // Map node1 symbol to expansion
    const auto &node1_sym = symbol_table.at(*expansion.node1->identifier_);
    matching.node_symbol_to_expansions[node1_sym].insert(expansion_ix);
    // Add node1 to all symbols.
    matching.expansion_symbols.insert(node1_sym);
    if (expansion.edge) {
      const auto &edge_sym = symbol_table.at(*expansion.edge->identifier_);
      // Fill edge symbols for Cyphermorphism.
      edge_symbols.insert(edge_sym);
      // Map node2 symbol to expansion
      const auto &node2_sym = symbol_table.at(*expansion.node2->identifier_);
      matching.node_symbol_to_expansions[node2_sym].insert(expansion_ix);
      // Add edge and node2 to all symbols
      matching.expansion_symbols.insert(edge_sym);
      matching.expansion_symbols.insert(node2_sym);
    }
    matching.expansions.push_back(expansion);
  }
  if (!edge_symbols.empty()) {
    matching.edge_symbols.emplace_back(edge_symbols);
  }
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
    matching.filters.CollectWhereFilter(*where, symbol_table);
  }
}

void AddMatching(const Match &match, SymbolTable &symbol_table, AstStorage &storage, Matching &matching) {
  AddMatching(match.patterns_, match.where_, symbol_table, storage, matching);

  // If there are any pattern filters, we add those as well
  for (auto &filter : matching.filters) {
    PatternFilterVisitor visitor(symbol_table, storage);

    filter.expression->Accept(visitor);
    filter.matchings = visitor.getMatchings();
  }
}

void PatternFilterVisitor::Visit(Exists &op) {
  std::vector<Pattern *> patterns;
  patterns.push_back(op.pattern_);

  FilterMatching filter_matching;
  AddMatching(patterns, nullptr, symbol_table_, storage_, filter_matching);

  filter_matching.type = PatternFilterType::EXISTS;
  filter_matching.symbol = std::make_optional<Symbol>(symbol_table_.at(op));

  matchings_.push_back(std::move(filter_matching));
}

static void ParseForeach(query::Foreach &foreach, SingleQueryPart &query_part, AstStorage &storage,
                         SymbolTable &symbol_table) {
  for (auto *clause : foreach.clauses_) {
    if (auto *merge = utils::Downcast<query::Merge>(clause)) {
      query_part.merge_matching.emplace_back(Matching{});
      AddMatching({merge->pattern_}, nullptr, symbol_table, storage, query_part.merge_matching.back());
    } else if (auto *nested = utils::Downcast<query::Foreach>(clause)) {
      ParseForeach(*nested, query_part, storage, symbol_table);
    }
  }
}

// Converts a Query to multiple QueryParts. In the process new Ast nodes may be
// created, e.g. filter expressions.
std::vector<SingleQueryPart> CollectSingleQueryParts(SymbolTable &symbol_table, AstStorage &storage,
                                                     SingleQuery *single_query) {
  std::vector<SingleQueryPart> query_parts(1);
  auto *query_part = &query_parts.back();
  for (auto &clause : single_query->clauses_) {
    if (auto *match = utils::Downcast<Match>(clause)) {
      if (match->optional_) {
        query_part->optional_matching.emplace_back(Matching{});
        AddMatching(*match, symbol_table, storage, query_part->optional_matching.back());
      } else {
        DMG_ASSERT(query_part->optional_matching.empty(), "Match clause cannot follow optional match.");
        AddMatching(*match, symbol_table, storage, query_part->matching);
      }
    } else {
      query_part->remaining_clauses.push_back(clause);
      if (auto *merge = utils::Downcast<query::Merge>(clause)) {
        query_part->merge_matching.emplace_back(Matching{});
        AddMatching({merge->pattern_}, nullptr, symbol_table, storage, query_part->merge_matching.back());
      } else if (auto *call_subquery = utils::Downcast<query::CallSubquery>(clause)) {
        query_part->subqueries.emplace_back(
            std::make_shared<QueryParts>(CollectQueryParts(symbol_table, storage, call_subquery->cypher_query_)));
      } else if (auto *foreach = utils::Downcast<query::Foreach>(clause)) {
        ParseForeach(*foreach, *query_part, storage, symbol_table);
      } else if (utils::IsSubtype(*clause, With::kType) || utils::IsSubtype(*clause, query::Unwind::kType) ||
                 utils::IsSubtype(*clause, query::CallProcedure::kType) ||
                 utils::IsSubtype(*clause, query::LoadCsv::kType)) {
        // This query part is done, continue with a new one.
        query_parts.emplace_back(SingleQueryPart{});
        query_part = &query_parts.back();
      } else if (utils::IsSubtype(*clause, Return::kType)) {
        return query_parts;
      }
    }
  }
  return query_parts;
}

QueryParts CollectQueryParts(SymbolTable &symbol_table, AstStorage &storage, CypherQuery *query) {
  std::vector<QueryPart> query_parts;

  auto *single_query = query->single_query_;
  MG_ASSERT(single_query, "Expected at least a single query");
  query_parts.push_back(QueryPart{CollectSingleQueryParts(symbol_table, storage, single_query)});

  bool distinct = false;
  for (auto *cypher_union : query->cypher_unions_) {
    if (cypher_union->distinct_) {
      distinct = true;
    }

    auto *single_query = cypher_union->single_query_;
    MG_ASSERT(single_query, "Expected UNION to have a query");
    query_parts.push_back(QueryPart{CollectSingleQueryParts(symbol_table, storage, single_query), cypher_union});
  }
  return QueryParts{query_parts, distinct};
}

}  // namespace memgraph::query::plan
