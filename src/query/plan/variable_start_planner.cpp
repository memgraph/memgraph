// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/plan/variable_start_planner.hpp"

#include <limits>
#include <queue>
#include <utility>

#include "utils/flag_validation.hpp"
#include "utils/logging.hpp"
#include "utils/typeinfo.hpp"

// NOLINTNEXTLINE (cppcoreguidelines-avoid-non-const-global-variables)
DEFINE_VALIDATED_uint64(query_max_plans, 1000U, "Maximum number of generated plans for a query.",
                        FLAG_IN_RANGE(1, std::numeric_limits<std::uint64_t>::max()));

namespace memgraph::query::plan::impl {

namespace {

// Add applicable expansions for `node_symbol` to `next_expansions`. These
// expansions are removed from `atom_symbol_to_expansions`, while
// `seen_expansions` and `expanded_symbols` are populated with new data.
void AddNextExpansions(const Symbol &atom_symbol, const Matching &matching, const SymbolTable &symbol_table,
                       std::unordered_set<Symbol> &expanded_symbols,
                       std::unordered_map<Symbol, std::set<size_t>> &atom_symbol_to_expansions,
                       std::unordered_set<size_t> &seen_expansions, std::queue<Expansion> &next_expansions) {
  auto atom_to_expansions_it = atom_symbol_to_expansions.find(atom_symbol);
  if (atom_to_expansions_it == atom_symbol_to_expansions.end()) {
    return;
  }

  // Returns true if the expansion is a regular expand or if it is a variable
  // path expand, but with bound symbols used inside the range expression.
  auto can_expand = [&](auto &expansion) {
    for (const auto &range_symbol : expansion.symbols_in_range) {
      // If the symbols used in range need to be bound during this whole
      // expansion, we must check whether they have already been expanded and
      // therefore bound. If the symbols are not found in the whole expansion,
      // then the semantic analysis should guarantee that the symbols have been
      // bound long before we expand.
      if (matching.expansion_symbols.find(range_symbol) != matching.expansion_symbols.end() &&
          expanded_symbols.find(range_symbol) == expanded_symbols.end()) {
        return false;
      }
    }
    return true;
  };

  auto &atom_expansions = atom_to_expansions_it->second;
  auto atom_expansions_it = atom_expansions.begin();
  while (atom_expansions_it != atom_to_expansions_it->second.end()) {
    auto expansion_id = *atom_expansions_it;
    if (seen_expansions.find(expansion_id) != seen_expansions.end()) {
      // Skip and erase seen (already expanded) expansions.
      atom_expansions_it = atom_expansions.erase(atom_expansions_it);
      continue;
    }
    auto expansion = matching.expansions[expansion_id];
    if (!can_expand(expansion)) {
      // Skip but save expansions which need other symbols for later.
      ++atom_expansions_it;
      continue;
    }

    if (symbol_table.at(*expansion.node1->identifier_) != atom_symbol) {
      // We are not expanding from node1, so flip the expansion.
      const bool is_expanding_from_node2 = symbol_table.at(*expansion.node2->identifier_) == atom_symbol;
      const bool is_expanding_from_edge = symbol_table.at(*expansion.edge->identifier_) == atom_symbol;

      MG_ASSERT(expansion.node2 && (is_expanding_from_node2 || is_expanding_from_edge),
                "Expected node_symbol or edge_symbol to be bound");

      // if we are expanding from an edge, we will not do any change, but at some point need to throw
      // as expanding from edge in a path is invalid
      if (is_expanding_from_node2 && expansion.edge->type_ != EdgeAtom::Type::BREADTH_FIRST &&
          !expansion.edge->filter_lambda_.accumulated_path) {
        // BFS must *not* be flipped. Doing that changes the BFS results.
        // When filter lambda uses accumulated path, path must not be flipped.
        std::swap(expansion.node1, expansion.node2);
        expansion.is_flipped = true;
        if (expansion.direction != EdgeAtom::Direction::BOTH) {
          expansion.direction =
              expansion.direction == EdgeAtom::Direction::IN ? EdgeAtom::Direction::OUT : EdgeAtom::Direction::IN;
        }
      }
    }

    seen_expansions.insert(expansion_id);
    expanded_symbols.insert(symbol_table.at(*expansion.node1->identifier_));
    if (expansion.edge) {
      expanded_symbols.insert(symbol_table.at(*expansion.edge->identifier_));
      expanded_symbols.insert(symbol_table.at(*expansion.node2->identifier_));
    }
    next_expansions.emplace(std::move(expansion));
    atom_expansions_it = atom_expansions.erase(atom_expansions_it);
  }
  if (atom_expansions.empty()) {
    atom_symbol_to_expansions.erase(atom_to_expansions_it);
  }
}

// Generates expansions emanating from the start_node by forming a chain. When
// the chain can no longer be continued, a different starting node is picked
// among remaining expansions and the process continues. This is done until all
// matching.expansions are used.
std::vector<Expansion> ExpansionsFrom(const PatternAtom *start_atom, const Matching &matching,
                                      const SymbolTable &symbol_table) {
  // Make a copy of atom_symbol_to_expansions, because we will modify it as
  // expansions are chained.
  auto atom_symbol_to_expansions = matching.atom_symbol_to_expansions;
  std::unordered_set<size_t> seen_expansions;
  std::queue<Expansion> next_expansions;
  std::unordered_set<Symbol> expanded_symbols({symbol_table.at(*start_atom->identifier_)});

  auto add_next_expansions = [&](const auto *atom) {
    AddNextExpansions(symbol_table.at(*atom->identifier_), matching, symbol_table, expanded_symbols,
                      atom_symbol_to_expansions, seen_expansions, next_expansions);
  };

  add_next_expansions(start_atom);
  // Potential optimization: expansions and next_expansions could be merge into
  // a single vector and an index could be used to determine from which should
  // additional expansions be added.
  std::vector<Expansion> expansions;
  while (!next_expansions.empty()) {
    auto expansion = next_expansions.front();
    next_expansions.pop();
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast)
    if (expansions.empty() && utils::Downcast<EdgeAtom>(const_cast<PatternAtom *>(start_atom))) {
      expansion.expand_from_edge = true;
    }
    expansions.emplace_back(expansion);
    add_next_expansions(expansion.node1);
    if (expansion.node2) {
      add_next_expansions(expansion.node2);
    }
  }
  if (!atom_symbol_to_expansions.empty()) {
    // We could pick a new starting expansion, but to avoid runtime
    // complexity, simply append the remaining expansions. They should have the
    // correct order, since the original expansions were verified during
    // semantic analysis.
    for (size_t i = 0; i < matching.expansions.size(); ++i) {
      if (seen_expansions.find(i) != seen_expansions.end()) {
        continue;
      }
      expansions.emplace_back(matching.expansions[i]);
    }
  }
  return expansions;
}

// Collect all unique nodes from expansions. Uniqueness is determined by
// symbol uniqueness.
auto ExpansionAtoms(const std::vector<Expansion> &expansions, const SymbolTable &symbol_table) {
  std::unordered_set<PatternAtom *, PatternAtomSymbolHash, PatternAtomSymbolEqual> graph_atoms_set(
      expansions.size(), PatternAtomSymbolHash(symbol_table), PatternAtomSymbolEqual(symbol_table));
  std::vector<PatternAtom *> graph_atoms;

  auto try_insert_atom = [&](PatternAtom *atom) {
    if (atom && graph_atoms_set.insert(atom).second) {
      graph_atoms.push_back(atom);
    }
  };

  for (const auto &expansion : expansions) {
    try_insert_atom(expansion.node1);
    try_insert_atom(expansion.edge);
    try_insert_atom(expansion.node2);
  }

  return graph_atoms;
}

FilterMatching ToFilterMatching(Matching &matching) {
  FilterMatching filter_matching;
  filter_matching.expansions = matching.expansions;
  filter_matching.edge_symbols = matching.edge_symbols;
  filter_matching.filters = matching.filters;
  filter_matching.atom_symbol_to_expansions = matching.atom_symbol_to_expansions;
  filter_matching.named_paths = matching.named_paths;
  filter_matching.expansion_symbols = matching.expansion_symbols;

  return filter_matching;
}

}  // namespace

VaryMatchingStart::VaryMatchingStart(Matching matching, const SymbolTable &symbol_table)
    : matching_(matching),
      symbol_table_(symbol_table),
      graph_atoms_(ExpansionAtoms(matching.expansions, symbol_table)) {}

VaryMatchingStart::iterator::iterator(VaryMatchingStart *self, bool is_done)
    : self_(self),
      // Use the original matching as the first matching. We are only
      // interested in changing the expansions part, so the remaining fields
      // should stay the same. This also produces a matching for the case
      // when there are no nodes.
      current_matching_(self->matching_) {
  if (!self_->graph_atoms_.empty()) {
    // Overwrite the original matching expansions with the new ones by
    // generating it from the first start node.
    start_atoms_it_ = self_->graph_atoms_.begin();
    current_matching_.expansions = ExpansionsFrom(**start_atoms_it_, self_->matching_, self_->symbol_table_);
  }
  DMG_ASSERT(start_atoms_it_ || self_->graph_atoms_.empty(),
             "start_atoms_it_ should only be nullopt when self_->graph_atoms_ is empty");
  if (is_done) {
    start_atoms_it_ = self_->graph_atoms_.end();
  }
}

VaryMatchingStart::iterator &VaryMatchingStart::iterator::operator++() {
  if (!start_atoms_it_) {
    DMG_ASSERT(self_->graph_atoms_.empty(), "start_atoms_it_ should only be nullopt when self_->graph_atoms_ is empty");
    start_atoms_it_ = self_->graph_atoms_.end();
  }
  if (*start_atoms_it_ == self_->graph_atoms_.end()) {
    return *this;
  }
  ++*start_atoms_it_;
  // start_atoms_it_ can become equal to `end` and we shouldn't dereference
  // iterator in that case.
  if (*start_atoms_it_ == self_->graph_atoms_.end()) {
    return *this;
  }
  const auto &start_atom = **start_atoms_it_;
  current_matching_.expansions = ExpansionsFrom(start_atom, self_->matching_, self_->symbol_table_);
  return *this;
}

CartesianProduct<VaryMatchingStart> VaryMultiMatchingStarts(const std::vector<Matching> &matchings,
                                                            const SymbolTable &symbol_table) {
  std::vector<VaryMatchingStart> variants;
  variants.reserve(matchings.size());
  for (const auto &matching : matchings) {
    variants.emplace_back(matching, symbol_table);
  }
  return MakeCartesianProduct(std::move(variants));
}

CartesianProduct<VaryMatchingStart> VaryFilterMatchingStarts(const Matching &matching,
                                                             const SymbolTable &symbol_table) {
  auto filter_matchings_cnt = 0;
  for (const auto &filter : matching.filters) {
    filter_matchings_cnt += static_cast<int>(filter.matchings.size());
  }

  std::vector<VaryMatchingStart> variants;
  variants.reserve(filter_matchings_cnt);

  for (const auto &filter : matching.filters) {
    for (const auto &filter_matching : filter.matchings) {
      variants.emplace_back(filter_matching, symbol_table);
    }
  }

  return MakeCartesianProduct(std::move(variants));
}

VaryQueryPartMatching::VaryQueryPartMatching(SingleQueryPart query_part, const SymbolTable &symbol_table)
    : query_part_(std::move(query_part)),
      matchings_(VaryMatchingStart(query_part_.matching, symbol_table)),
      optional_matchings_(VaryMultiMatchingStarts(query_part_.optional_matching, symbol_table)),
      merge_matchings_(VaryMultiMatchingStarts(query_part_.merge_matching, symbol_table)),
      filter_matchings_(VaryFilterMatchingStarts(query_part_.matching, symbol_table)) {}

VaryQueryPartMatching::iterator::iterator(SingleQueryPart query_part, VaryMatchingStart::iterator matchings_begin,
                                          VaryMatchingStart::iterator matchings_end,
                                          CartesianProduct<VaryMatchingStart>::iterator optional_begin,
                                          CartesianProduct<VaryMatchingStart>::iterator optional_end,
                                          CartesianProduct<VaryMatchingStart>::iterator merge_begin,
                                          CartesianProduct<VaryMatchingStart>::iterator merge_end,
                                          CartesianProduct<VaryMatchingStart>::iterator filter_begin,
                                          CartesianProduct<VaryMatchingStart>::iterator filter_end)
    : current_query_part_(std::move(query_part)),
      matchings_it_(std::move(matchings_begin)),
      matchings_end_(std::move(matchings_end)),
      optional_it_(optional_begin),
      optional_begin_(optional_begin),
      optional_end_(std::move(optional_end)),
      merge_it_(merge_begin),
      merge_begin_(merge_begin),
      merge_end_(std::move(merge_end)),
      filter_it_(filter_begin),
      filter_begin_(filter_begin),
      filter_end_(std::move(filter_end)) {
  if (matchings_it_ != matchings_end_) {
    // Fill the query part with the first variation of matchings
    SetCurrentQueryPart();
  }
}

VaryQueryPartMatching::iterator &VaryQueryPartMatching::iterator::operator++() {
  // Produce parts for each possible combination. E.g. if we have:
  //    * matchings (m1) and (m2)
  //    * optional matchings (o1) and (o2)
  //    * merge matching (g1)
  //    * filter matching (f1) and (f2)
  // We want to produce parts for:
  //    * (m1), (o1), (g1), (f1)
  //    * (m1), (o1), (g1), (f2)
  //    * (m1), (o2), (g1), (f1)
  //    * (m1), (o2), (g1), (f2)
  //    * (m2), (o1), (g1), (f1)
  //    * (m2), (o1), (g1), (f2)
  //    * (m2), (o2), (g1), (f1)
  //    * (m2), (o2), (g1), (f2)

  // Create variations by changing the filter part first.
  if (filter_it_ != filter_end_) ++filter_it_;

  // Create variations by changing the merge part.
  if (filter_it_ == filter_end_) {
    filter_it_ = filter_begin_;
    if (merge_it_ != merge_end_) ++merge_it_;
  }

  // Create variations by changing the optional part.
  if (merge_it_ == merge_end_ && filter_it_ == filter_begin_) {
    merge_it_ = merge_begin_;
    if (optional_it_ != optional_end_) ++optional_it_;
  }

  if (optional_it_ == optional_end_ && merge_it_ == merge_begin_ && filter_it_ == filter_begin_) {
    optional_it_ = optional_begin_;
    if (matchings_it_ != matchings_end_) ++matchings_it_;
  }

  // We have reached the end, so return;
  if (matchings_it_ == matchings_end_) return *this;
  // Fill the query part with the new variation of matchings.
  SetCurrentQueryPart();
  return *this;
}

void VaryQueryPartMatching::iterator::SetCurrentQueryPart() {
  current_query_part_.matching = *matchings_it_;
  DMG_ASSERT(optional_it_ != optional_end_ || optional_begin_ == optional_end_,
             "Either there are no optional matchings or we can always "
             "generate a variation");
  if (optional_it_ != optional_end_) {
    current_query_part_.optional_matching = *optional_it_;
  }
  DMG_ASSERT(merge_it_ != merge_end_ || merge_begin_ == merge_end_,
             "Either there are no merge matchings or we can always generate "
             "a variation");
  if (merge_it_ != merge_end_) {
    current_query_part_.merge_matching = *merge_it_;
  }
  DMG_ASSERT(filter_it_ != filter_end_ || filter_begin_ == filter_end_,
             "Either there are no filter matchings or we can always generate"
             "a variation");

  auto all_filter_matchings = *filter_it_;
  auto all_filter_matchings_idx = 0;
  for (auto &filter : current_query_part_.matching.filters) {
    auto matchings_size = filter.matchings.size();

    std::vector<FilterMatching> new_matchings;
    new_matchings.reserve(matchings_size);

    for (auto i = 0; i < matchings_size; i++) {
      new_matchings.push_back(ToFilterMatching(all_filter_matchings[all_filter_matchings_idx]));
      new_matchings[i].symbol = filter.matchings[i].symbol;
      new_matchings[i].type = filter.matchings[i].type;
      new_matchings[i].subquery = filter.matchings[i].subquery;

      all_filter_matchings_idx++;
    }

    filter.matchings = std::move(new_matchings);
  }
}

bool VaryQueryPartMatching::iterator::operator==(const iterator &other) const {
  if (matchings_it_ == other.matchings_it_ && matchings_it_ == matchings_end_) {
    // matchings_it_ is the primary iterator. If both are at the end, then other
    // iterators can be at any position.
    return true;
  }
  return matchings_it_ == other.matchings_it_ && optional_it_ == other.optional_it_ && merge_it_ == other.merge_it_ &&
         filter_it_ == other.filter_it_;
}

}  // namespace memgraph::query::plan::impl
