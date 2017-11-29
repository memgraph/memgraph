#include "query/plan/preprocess.hpp"

#include <algorithm>
#include <functional>
#include <stack>

namespace query::plan {

namespace {

void ForEachPattern(
    Pattern &pattern, std::function<void(NodeAtom *)> base,
    std::function<void(NodeAtom *, EdgeAtom *, NodeAtom *)> collect) {
  DCHECK(!pattern.atoms_.empty()) << "Missing atoms in pattern";
  auto atoms_it = pattern.atoms_.begin();
  auto current_node = dynamic_cast<NodeAtom *>(*atoms_it++);
  DCHECK(current_node) << "First pattern atom is not a node";
  base(current_node);
  // Remaining atoms need to follow sequentially as (EdgeAtom, NodeAtom)*
  while (atoms_it != pattern.atoms_.end()) {
    auto edge = dynamic_cast<EdgeAtom *>(*atoms_it++);
    DCHECK(edge) << "Expected an edge atom in pattern.";
    DCHECK(atoms_it != pattern.atoms_.end())
        << "Edge atom should not end the pattern.";
    auto prev_node = current_node;
    current_node = dynamic_cast<NodeAtom *>(*atoms_it++);
    DCHECK(current_node) << "Expected a node atom in pattern.";
    collect(prev_node, edge, current_node);
  }
}

// Collects symbols from identifiers found in visited AST nodes.
class UsedSymbolsCollector : public HierarchicalTreeVisitor {
 public:
  explicit UsedSymbolsCollector(const SymbolTable &symbol_table)
      : symbol_table_(symbol_table) {}

  using HierarchicalTreeVisitor::PostVisit;
  using HierarchicalTreeVisitor::PreVisit;
  using HierarchicalTreeVisitor::Visit;

  bool PostVisit(All &all) override {
    // Remove the symbol which is bound by all, because we are only interested
    // in free (unbound) symbols.
    symbols_.erase(symbol_table_.at(*all.identifier_));
    return true;
  }

  bool Visit(Identifier &ident) override {
    symbols_.insert(symbol_table_.at(ident));
    return true;
  }

  bool Visit(PrimitiveLiteral &) override { return true; }
  bool Visit(ParameterLookup &) override { return true; }
  bool Visit(query::CreateIndex &) override { return true; }

  std::unordered_set<Symbol> symbols_;
  const SymbolTable &symbol_table_;
};

// Converts multiple Patterns to Expansions. Each Pattern can contain an
// arbitrarily long chain of nodes and edges. The conversion to an Expansion is
// done by splitting a pattern into triplets (node1, edge, node2). The triplets
// conserve the semantics of the pattern. For example, in a pattern:
// (m) -[e]- (n) -[f]- (o) the same can be achieved with:
// (m) -[e]- (n), (n) -[f]- (o).
// This representation makes it easier to permute from which node or edge we
// want to start expanding.
std::vector<Expansion> NormalizePatterns(
    const SymbolTable &symbol_table, const std::vector<Pattern *> &patterns) {
  std::vector<Expansion> expansions;
  auto ignore_node = [&](auto *) {};
  auto collect_expansion = [&](auto *prev_node, auto *edge,
                               auto *current_node) {
    UsedSymbolsCollector collector(symbol_table);
    // Remove symbols which are bound by variable expansions.
    if (edge->IsVariable()) {
      if (edge->lower_bound_) edge->lower_bound_->Accept(collector);
      if (edge->upper_bound_) edge->upper_bound_->Accept(collector);
      collector.symbols_.erase(symbol_table.at(*edge->inner_edge_));
      collector.symbols_.erase(symbol_table.at(*edge->inner_node_));
      if (edge->filter_expression_) edge->filter_expression_->Accept(collector);
    }
    expansions.emplace_back(Expansion{prev_node, edge, edge->direction_, false,
                                      collector.symbols_, current_node});
  };
  for (const auto &pattern : patterns) {
    if (pattern->atoms_.size() == 1U) {
      auto *node = dynamic_cast<NodeAtom *>(pattern->atoms_[0]);
      DCHECK(node) << "First pattern atom is not a node";
      expansions.emplace_back(Expansion{node});
    } else {
      ForEachPattern(*pattern, ignore_node, collect_expansion);
    }
  }
  return expansions;
}

// Fills the given Matching, by converting the Match patterns to normalized
// representation as Expansions. Filters used in the Match are also collected,
// as well as edge symbols which determine Cyphermorphism. Collecting filters
// will lift them out of a pattern and generate new expressions (just like they
// were in a Where clause).
void AddMatching(const std::vector<Pattern *> &patterns, Where *where,
                 SymbolTable &symbol_table, AstTreeStorage &storage,
                 Matching &matching) {
  auto expansions = NormalizePatterns(symbol_table, patterns);
  std::unordered_set<Symbol> edge_symbols;
  for (const auto &expansion : expansions) {
    // Matching may already have some expansions, so offset our index.
    const int expansion_ix = matching.expansions.size();
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
  for (auto *pattern : patterns) {
    matching.filters.CollectPatternFilters(*pattern, symbol_table, storage);
    if (pattern->identifier_->user_declared_) {
      std::vector<Symbol> path_elements;
      for (auto *pattern_atom : pattern->atoms_)
        path_elements.emplace_back(symbol_table.at(*pattern_atom->identifier_));
      matching.named_paths.emplace(symbol_table.at(*pattern->identifier_),
                                   std::move(path_elements));
    }
  }
  if (where) {
    matching.filters.CollectWhereFilter(*where, symbol_table);
  }
}
void AddMatching(const Match &match, SymbolTable &symbol_table,
                 AstTreeStorage &storage, Matching &matching) {
  return AddMatching(match.patterns_, match.where_, symbol_table, storage,
                     matching);
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
    if (auto *and_op = dynamic_cast<AndOperator *>(current_expression)) {
      pending_expressions.push(and_op->expression1_);
      pending_expressions.push(and_op->expression2_);
    } else {
      expressions.push_back(current_expression);
    }
  }
  return expressions;
}

}  // namespace

PropertyFilter::PropertyFilter(const SymbolTable &symbol_table,
                               const Symbol &symbol,
                               const GraphDbTypes::Property &property,
                               Expression *value)
    : symbol_(symbol), property_(property), value_(value) {
  UsedSymbolsCollector collector(symbol_table);
  value->Accept(collector);
  is_symbol_in_value_ = utils::Contains(collector.symbols_, symbol);
}

PropertyFilter::PropertyFilter(
    const SymbolTable &symbol_table, const Symbol &symbol,
    const GraphDbTypes::Property &property,
    const std::experimental::optional<PropertyFilter::Bound> &lower_bound,
    const std::experimental::optional<PropertyFilter::Bound> &upper_bound)
    : symbol_(symbol),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound) {
  UsedSymbolsCollector collector(symbol_table);
  if (lower_bound) {
    lower_bound->value()->Accept(collector);
  }
  if (upper_bound) {
    upper_bound->value()->Accept(collector);
  }
  is_symbol_in_value_ = utils::Contains(collector.symbols_, symbol);
}

void Filters::EraseFilter(const FilterInfo &filter) {
  // TODO: Ideally, we want to determine the equality of both expression trees,
  // instead of a simple pointer compare.
  all_filters_.erase(std::remove_if(all_filters_.begin(), all_filters_.end(),
                                    [&filter](const auto &f) {
                                      return f.expression == filter.expression;
                                    }),
                     all_filters_.end());
}

void Filters::EraseLabelFilter(const Symbol &symbol,
                               const GraphDbTypes::Label &label) {
  for (auto filter_it = all_filters_.begin();
       filter_it != all_filters_.end();) {
    if (filter_it->type != FilterInfo::Type::Label) {
      ++filter_it;
      continue;
    }
    if (!utils::Contains(filter_it->used_symbols, symbol)) {
      ++filter_it;
      continue;
    }
    auto label_it =
        std::find(filter_it->labels.begin(), filter_it->labels.end(), label);
    if (label_it == filter_it->labels.end()) {
      ++filter_it;
      continue;
    }
    filter_it->labels.erase(label_it);
    DCHECK(!utils::Contains(filter_it->labels, label))
        << "Didn't expect duplicated labels";
    if (filter_it->labels.empty()) {
      // If there are no labels to filter, then erase the whole FilterInfo.
      filter_it = all_filters_.erase(filter_it);
    } else {
      ++filter_it;
    }
  }
}

void Filters::CollectPatternFilters(Pattern &pattern, SymbolTable &symbol_table,
                                    AstTreeStorage &storage) {
  UsedSymbolsCollector collector(symbol_table);
  auto add_properties_variable = [&](EdgeAtom *atom) {
    const auto &symbol = symbol_table.at(*atom->identifier_);
    for (auto &prop_pair : atom->properties_) {
      // We need to store two property-lookup filters in all_filters. One is
      // used for inlining property filters into variable expansion, and
      // utilizes the inner_edge symbol. The other is used for post-expansion
      // filtering and does not use the inner_edge symbol, but the edge symbol
      // (a list of edges).
      {
        collector.symbols_.clear();
        prop_pair.second->Accept(collector);
        collector.symbols_.emplace(symbol_table.at(*atom->inner_node_));
        collector.symbols_.emplace(symbol_table.at(*atom->inner_edge_));
        // First handle the inline property filter.
        auto *property_lookup =
            storage.Create<PropertyLookup>(atom->inner_edge_, prop_pair.first);
        auto *prop_equal =
            storage.Create<EqualOperator>(property_lookup, prop_pair.second);
        // Currently, variable expand has no gains if we set PropertyFilter.
        all_filters_.emplace_back(FilterInfo{FilterInfo::Type::Generic,
                                             prop_equal, collector.symbols_});
      }
      {
        collector.symbols_.clear();
        prop_pair.second->Accept(collector);
        collector.symbols_.insert(symbol);  // PropertyLookup uses the symbol.
        // Now handle the post-expansion filter.
        // Create a new identifier and a symbol which will be filled in All.
        auto *identifier = atom->identifier_->Clone(storage);
        symbol_table[*identifier] =
            symbol_table.CreateSymbol(identifier->name_, false);
        // Create an equality expression and store it in all_filters_.
        auto *property_lookup =
            storage.Create<PropertyLookup>(identifier, prop_pair.first);
        auto *prop_equal =
            storage.Create<EqualOperator>(property_lookup, prop_pair.second);
        // Currently, variable expand has no gains if we set PropertyFilter.
        all_filters_.emplace_back(
            FilterInfo{FilterInfo::Type::Generic,
                       storage.Create<All>(identifier, atom->identifier_,
                                           storage.Create<Where>(prop_equal)),
                       collector.symbols_});
      }
    }
  };
  auto add_properties = [&](auto *atom) {
    const auto &symbol = symbol_table.at(*atom->identifier_);
    for (auto &prop_pair : atom->properties_) {
      // Create an equality expression and store it in all_filters_.
      auto *property_lookup =
          storage.Create<PropertyLookup>(atom->identifier_, prop_pair.first);
      auto *prop_equal =
          storage.Create<EqualOperator>(property_lookup, prop_pair.second);
      collector.symbols_.clear();
      prop_equal->Accept(collector);
      FilterInfo filter_info{FilterInfo::Type::Property, prop_equal,
                             collector.symbols_};
      // Store a PropertyFilter on the value of the property.
      filter_info.property_filter.emplace(
          symbol_table, symbol, prop_pair.first.second, prop_pair.second);
      all_filters_.emplace_back(filter_info);
    }
  };
  auto add_node_filter = [&](NodeAtom *node) {
    const auto &node_symbol = symbol_table.at(*node->identifier_);
    if (!node->labels_.empty()) {
      // Create a LabelsTest and store it.
      auto *labels_test =
          storage.Create<LabelsTest>(node->identifier_, node->labels_);
      auto label_filter = FilterInfo{FilterInfo::Type::Label, labels_test,
                                     std::unordered_set<Symbol>{node_symbol}};
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
void Filters::CollectWhereFilter(Where &where,
                                 const SymbolTable &symbol_table) {
  auto where_filters = SplitExpressionOnAnd(where.expression_);
  for (const auto &filter : where_filters) {
    AnalyzeAndStoreFilter(filter, symbol_table);
  }
}

// Analyzes the filter expression by collecting information on filtering labels
// and properties to be used with indexing.
void Filters::AnalyzeAndStoreFilter(Expression *expr,
                                    const SymbolTable &symbol_table) {
  using Bound = PropertyFilter::Bound;
  UsedSymbolsCollector collector(symbol_table);
  expr->Accept(collector);
  auto make_filter = [&collector, &expr](FilterInfo::Type type) {
    return FilterInfo{type, expr, collector.symbols_};
  };
  auto get_property_lookup = [](auto *maybe_lookup, auto *&prop_lookup,
                                auto *&ident) -> bool {
    return (prop_lookup = dynamic_cast<PropertyLookup *>(maybe_lookup)) &&
           (ident = dynamic_cast<Identifier *>(prop_lookup->expression_));
  };
  // Checks if maybe_lookup is a property lookup, stores it as a
  // PropertyFilter and returns true. If it isn't, returns false.
  auto add_prop_equal = [&](auto *maybe_lookup, auto *val_expr) -> bool {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    if (get_property_lookup(maybe_lookup, prop_lookup, ident)) {
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter =
          PropertyFilter(symbol_table, symbol_table.at(*ident),
                         prop_lookup->property_, val_expr);
      all_filters_.emplace_back(filter);
      return true;
    }
    return false;
  };
  // Checks if either the expr1 and expr2 are property lookups, adds them as
  // PropertyFilter and returns true. Otherwise, returns false.
  auto add_prop_greater = [&](auto *expr1, auto *expr2,
                              auto bound_type) -> bool {
    PropertyLookup *prop_lookup = nullptr;
    Identifier *ident = nullptr;
    bool is_prop_filter = false;
    if (get_property_lookup(expr1, prop_lookup, ident)) {
      // n.prop > value
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter.emplace(
          symbol_table, symbol_table.at(*ident), prop_lookup->property_,
          Bound(expr2, bound_type), std::experimental::nullopt);
      all_filters_.emplace_back(filter);
      is_prop_filter = true;
    }
    if (get_property_lookup(expr2, prop_lookup, ident)) {
      // value > n.prop
      auto filter = make_filter(FilterInfo::Type::Property);
      filter.property_filter.emplace(
          symbol_table, symbol_table.at(*ident), prop_lookup->property_,
          std::experimental::nullopt, Bound(expr1, bound_type));
      all_filters_.emplace_back(filter);
      is_prop_filter = true;
    }
    return is_prop_filter;
  };
  // We are only interested to see the insides of And, because Or prevents
  // indexing since any labels and properties found there may be optional.
  DCHECK(!dynamic_cast<AndOperator *>(expr))
      << "Expected AndOperators have been split.";
  if (auto *labels_test = dynamic_cast<LabelsTest *>(expr)) {
    // Since LabelsTest may contain any expression, we can only use the
    // simplest test on an identifier.
    if (dynamic_cast<Identifier *>(labels_test->expression_)) {
      auto filter = make_filter(FilterInfo::Type::Label);
      filter.labels = labels_test->labels_;
      all_filters_.emplace_back(filter);
    } else {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *eq = dynamic_cast<EqualOperator *>(expr)) {
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
    if (!is_prop_filter) {
      // No PropertyFilter was added, so just store a generic filter.
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *gt = dynamic_cast<GreaterOperator *>(expr)) {
    if (!add_prop_greater(gt->expression1_, gt->expression2_,
                          Bound::Type::EXCLUSIVE)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *ge = dynamic_cast<GreaterEqualOperator *>(expr)) {
    if (!add_prop_greater(ge->expression1_, ge->expression2_,
                          Bound::Type::INCLUSIVE)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *lt = dynamic_cast<LessOperator *>(expr)) {
    // Like greater, but in reverse.
    if (!add_prop_greater(lt->expression2_, lt->expression1_,
                          Bound::Type::EXCLUSIVE)) {
      all_filters_.emplace_back(make_filter(FilterInfo::Type::Generic));
    }
  } else if (auto *le = dynamic_cast<LessEqualOperator *>(expr)) {
    // Like greater equal, but in reverse.
    if (!add_prop_greater(le->expression2_, le->expression1_,
                          Bound::Type::INCLUSIVE)) {
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

// Converts a Query to multiple QueryParts. In the process new Ast nodes may be
// created, e.g. filter expressions.
std::vector<SingleQueryPart> CollectSingleQueryParts(
    SymbolTable &symbol_table, AstTreeStorage &storage,
    SingleQuery *single_query) {
  std::vector<SingleQueryPart> query_parts(1);
  auto *query_part = &query_parts.back();
  for (auto &clause : single_query->clauses_) {
    if (auto *match = dynamic_cast<Match *>(clause)) {
      if (match->optional_) {
        query_part->optional_matching.emplace_back(Matching{});
        AddMatching(*match, symbol_table, storage,
                    query_part->optional_matching.back());
      } else {
        DCHECK(query_part->optional_matching.empty())
            << "Match clause cannot follow optional match.";
        AddMatching(*match, symbol_table, storage, query_part->matching);
      }
    } else {
      query_part->remaining_clauses.push_back(clause);
      if (auto *merge = dynamic_cast<query::Merge *>(clause)) {
        query_part->merge_matching.emplace_back(Matching{});
        AddMatching({merge->pattern_}, nullptr, symbol_table, storage,
                    query_part->merge_matching.back());
      } else if (dynamic_cast<With *>(clause) ||
                 dynamic_cast<query::Unwind *>(clause)) {
        // This query part is done, continue with a new one.
        query_parts.emplace_back(SingleQueryPart{});
        query_part = &query_parts.back();
      } else if (dynamic_cast<Return *>(clause)) {
        return query_parts;
      }
    }
  }
  return query_parts;
}

QueryParts CollectQueryParts(SymbolTable &symbol_table,
                             AstTreeStorage &storage) {
  auto query = storage.query();
  std::vector<QueryPart> query_parts;
  bool distinct = false;

  if (auto *single_query = query->single_query_) {
    query_parts.push_back(QueryPart{
        CollectSingleQueryParts(symbol_table, storage, single_query)});
  }

  for (auto *cypher_union : query->cypher_unions_) {
    if (cypher_union->distinct_) {
      distinct = true;
    }

    if (auto *single_query = cypher_union->single_query_) {
      query_parts.push_back(QueryPart{
          CollectSingleQueryParts(symbol_table, storage, single_query),
          cypher_union});
    }
  }
  return QueryParts{query_parts, distinct};
};

}  // namespace query::plan
