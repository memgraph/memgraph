#include "query/plan/planner.hpp"

#include <limits>

#include "cppitertools/slice.hpp"
#include "gflags/gflags.h"

#include "utils/flag_validation.hpp"

DEFINE_VALIDATED_uint64(
    query_max_plans, 1000U, "Maximum number of generated plans for a query",
    FLAG_IN_RANGE(1, std::numeric_limits<std::uint64_t>::max()));

namespace query::plan {

namespace {

class NodeSymbolHash {
 public:
  NodeSymbolHash(const SymbolTable &symbol_table)
      : symbol_table_(symbol_table) {}

  size_t operator()(const NodeAtom *node_atom) const {
    return std::hash<Symbol>{}(symbol_table_.at(*node_atom->identifier_));
  }

 private:
  const SymbolTable &symbol_table_;
};

class NodeSymbolEqual {
 public:
  NodeSymbolEqual(const SymbolTable &symbol_table)
      : symbol_table_(symbol_table) {}

  size_t operator()(const NodeAtom *node_atom1,
                    const NodeAtom *node_atom2) const {
    return symbol_table_.at(*node_atom1->identifier_) ==
           symbol_table_.at(*node_atom2->identifier_);
  }

 private:
  const SymbolTable &symbol_table_;
};

// Finds the next Expansion which has one of its nodes among the already
// expanded symbols. The function may modify expansions, by flipping their nodes
// and direction. This is done, so that the return iterator always points to the
// expansion whose node1 is the already expanded one, while node2 may not be.
auto NextExpansion(const SymbolTable &symbol_table,
                   const std::unordered_set<Symbol> &expanded_symbols,
                   const std::unordered_set<Symbol> &all_expansion_symbols,
                   std::vector<Expansion> &expansions) {
  // Returns true if the expansion is a regular expand or if it is a variable
  // path expand, but with bound symbols used inside the range expression.
  auto can_expand = [&](auto &expansion) {
    for (const auto &range_symbol : expansion.symbols_in_range) {
      // If the symbols used in range need to be bound during this whole
      // expansion, we must check whether they have already been expanded and
      // therefore bound. If the symbols are not found in the whole expansion,
      // then the semantic analysis should guarantee that the symbols have been
      // bound long before we expand.
      if (all_expansion_symbols.find(range_symbol) !=
              all_expansion_symbols.end() &&
          expanded_symbols.find(range_symbol) == expanded_symbols.end()) {
        return false;
      }
    }
    return true;
  };
  auto expansion_it = expansions.begin();
  for (; expansion_it != expansions.end(); ++expansion_it) {
    if (!can_expand(*expansion_it)) {
      continue;
    }
    const auto &node1_symbol =
        symbol_table.at(*expansion_it->node1->identifier_);
    if (expanded_symbols.find(node1_symbol) != expanded_symbols.end()) {
      return expansion_it;
    }
    auto *node2 = expansion_it->node2;
    if (node2 &&
        expanded_symbols.find(symbol_table.at(*node2->identifier_)) !=
            expanded_symbols.end()) {
      // We need to flip the expansion, since we want to expand from node2.
      std::swap(expansion_it->node2, expansion_it->node1);
      if (expansion_it->direction != EdgeAtom::Direction::BOTH) {
        expansion_it->direction =
            expansion_it->direction == EdgeAtom::Direction::IN
                ? EdgeAtom::Direction::OUT
                : EdgeAtom::Direction::IN;
      }
      return expansion_it;
    }
  }
  return expansion_it;
}

// Generates expansions emanating from the start_node by forming a chain. When
// the chain can no longer be continued, a different starting node is picked
// among remaining expansions and the process continues. This is done until all
// original_expansions are used.
std::vector<Expansion> ExpansionsFrom(
    const NodeAtom *start_node, std::vector<Expansion> original_expansions,
    const SymbolTable &symbol_table) {
  std::vector<Expansion> expansions;
  std::unordered_set<Symbol> expanded_symbols(
      {symbol_table.at(*start_node->identifier_)});
  std::unordered_set<Symbol> all_expansion_symbols;
  for (const auto &expansion : original_expansions) {
    all_expansion_symbols.insert(
        symbol_table.at(*expansion.node1->identifier_));
    if (expansion.edge) {
      all_expansion_symbols.insert(
          symbol_table.at(*expansion.edge->identifier_));
      all_expansion_symbols.insert(
          symbol_table.at(*expansion.node2->identifier_));
    }
  }
  while (!original_expansions.empty()) {
    auto next_it = NextExpansion(symbol_table, expanded_symbols,
                                 all_expansion_symbols, original_expansions);
    if (next_it == original_expansions.end()) {
      // Pick a new starting expansion, since we cannot continue the chain.
      next_it = original_expansions.begin();
    }
    expanded_symbols.insert(symbol_table.at(*next_it->node1->identifier_));
    if (next_it->node2) {
      expanded_symbols.insert(symbol_table.at(*next_it->edge->identifier_));
      expanded_symbols.insert(symbol_table.at(*next_it->node2->identifier_));
    }
    expansions.emplace_back(*next_it);
    original_expansions.erase(next_it);
  }
  return expansions;
}

// Collect all unique nodes from expansions. Uniqueness is determined by
// symbol uniqueness.
auto ExpansionNodes(const std::vector<Expansion> &expansions,
                    const SymbolTable &symbol_table) {
  std::unordered_set<NodeAtom *, NodeSymbolHash, NodeSymbolEqual> nodes(
      expansions.size(), NodeSymbolHash(symbol_table),
      NodeSymbolEqual(symbol_table));
  for (const auto &expansion : expansions) {
    // TODO: Handle labels and properties from different node atoms.
    nodes.insert(expansion.node1);
    if (expansion.node2) {
      nodes.insert(expansion.node2);
    }
  }
  return nodes;
}

// Generates n matchings, where n is the number of nodes to match. Each Matching
// will have a different node as a starting node for expansion.
std::vector<Matching> VaryMatchingStart(const Matching &matching,
                                        const SymbolTable &symbol_table) {
  if (matching.expansions.empty()) {
    return std::vector<Matching>{matching};
  }
  const auto start_nodes = ExpansionNodes(matching.expansions, symbol_table);
  std::vector<Matching> permutations;
  for (const auto &start_node : start_nodes) {
    permutations.emplace_back(
        Matching{ExpansionsFrom(start_node, matching.expansions, symbol_table),
                 matching.edge_symbols, matching.filters});
  }
  return permutations;
}

// Produces a Cartesian product among vectors between begin and end iterator.
// For example:
//
//    std::vector<int> first_set{1,2,3};
//    std::vector<int> second_set{4,5};
//    std::vector<std::vector<int>> all_sets{first_set, second_set};
//    // prod should be {{1, 4}, {1, 5}, {2, 4}, {2, 5}, {3, 4}, {3, 5}}
//    auto product = CartesianProduct(all_sets.cbegin(), all_sets.cend());
//    for (const auto &set : product) {
//      ...
//    }
//
// The product is created lazily by iterating over the constructed
// CartesianProduct instance.
template <typename T>
class CartesianProduct {
 public:
  CartesianProduct(std::vector<std::vector<T>> sets)
      : original_sets_(std::move(sets)),
        begin_(original_sets_.cbegin()),
        end_(original_sets_.cend()) {}

  class iterator {
   public:
    typedef std::input_iterator_tag iterator_category;
    typedef std::vector<T> value_type;
    typedef long difference_type;
    typedef const std::vector<T> &reference;
    typedef const std::vector<T> *pointer;

    explicit iterator(CartesianProduct &self, bool is_done)
        : self_(self), is_done_(is_done) {
      if (is_done || self.begin_ == self.end_) {
        is_done_ = true;
        return;
      }
      auto begin = self.begin_;
      while (begin != self.end_) {
        auto set_it = begin->cbegin();
        if (set_it == begin->cend()) {
          // One of the sets is empty, so there is no product.
          is_done_ = true;
          return;
        }
        // Collect the first product, by taking the first element of each set.
        current_product_.emplace_back(*set_it);
        // Store starting iterators to all sets.
        sets_.emplace_back(begin, set_it);
        begin++;
      }
    }

    iterator &operator++() {
      if (is_done_) return *this;
      // Increment the leftmost set iterator.
      auto sets_it = sets_.begin();
      sets_it->second++;
      // If the leftmost is at the end, reset it and increment the next
      // leftmost.
      while (sets_it->second == sets_it->first->cend()) {
        sets_it->second = sets_it->first->cbegin();
        sets_it++;
        if (sets_it == sets_.end()) {
          // The leftmost set is the last set and it was exhausted, so we are
          // done.
          is_done_ = true;
          return *this;
        }
        sets_it->second++;
      }
      // We can now collect another product from the modified set iterators.
      debug_assert(
          current_product_.size() == sets_.size(),
          "Expected size of current_product_ to match the size of sets_");
      size_t i = 0;
      // Change only the prefix of the product, remaining elements (after
      // sets_it) should be the same.
      auto last_unmodified = sets_it + 1;
      for (auto kv_it = sets_.begin(); kv_it != last_unmodified; ++kv_it) {
        current_product_[i++] = *kv_it->second;
      }
      return *this;
    }

    bool operator==(const iterator &other) const {
      if (self_.begin_ != other.self_.begin_ || self_.end_ != other.self_.end_)
        return false;
      return (is_done_ && other.is_done_) || (sets_ == other.sets_);
    }

    bool operator!=(const iterator &other) const { return !(*this == other); }

    // Iterator interface says that dereferencing a past-the-end iterator is
    // undefined, so don't bother checking if we are done.
    reference operator*() const { return current_product_; }
    pointer operator->() const { return &current_product_; }

   private:
    CartesianProduct &self_;
    // Vector of (original_sets_iterator, set_iterator) pairs. The
    // original_sets_iterator points to the set among all the sets, while the
    // set_iterator points to an element inside the pointed to set.
    std::vector<
        std::pair<decltype(self_.begin_), decltype(self_.begin_->cbegin())>>
        sets_;
    // Currently built product from pointed to elements in all sets.
    std::vector<T> current_product_;
    // Set to true when we have generated all products.
    bool is_done_ = false;
  };

  auto begin() { return iterator(*this, false); }
  auto end() { return iterator(*this, true); }

 private:
  friend class iterator;
  // The original sets whose Cartesian product we are calculating.
  std::vector<std::vector<T>> original_sets_;
  // Iterators to the beginning and end of original_sets_.
  typename std::vector<std::vector<T>>::const_iterator begin_;
  typename std::vector<std::vector<T>>::const_iterator end_;
};

// Similar to VaryMatchingStart, but varies the starting nodes for all given
// matchings. After all matchings produce multiple alternative starts, the
// Cartesian product of all of them is returned.
auto VaryMultiMatchingStarts(const std::vector<Matching> &matchings,
                             const SymbolTable &symbol_table) {
  std::vector<std::vector<Matching>> variants;
  for (const auto &matching : matchings) {
    variants.emplace_back(VaryMatchingStart(matching, symbol_table));
  }
  return iter::slice(CartesianProduct<Matching>(std::move(variants)), 0UL,
                     FLAGS_query_max_plans);
}

// Produces alternative query parts out of a single part by varying how each
// graph matching is done.
std::vector<QueryPart> VaryQueryPartMatching(const QueryPart &query_part,
                                             const SymbolTable &symbol_table) {
  std::vector<QueryPart> variants;
  // Get multiple regular matchings, each starting from different node.
  auto matchings = VaryMatchingStart(query_part.matching, symbol_table);
  // Get multiple optional matchings, where each combination has different
  // starting nodes.
  auto optional_matchings =
      VaryMultiMatchingStarts(query_part.optional_matching, symbol_table);
  // Like optional matching, but for merge matchings.
  auto merge_matchings =
      VaryMultiMatchingStarts(query_part.merge_matching, symbol_table);
  // After we have all valid combinations of each matching, we need to produce
  // combinations of them. This is similar to Cartesian product, but some
  // matchings can be empty (optional and merge) and `matchings` is of different
  // type (vector) than `optional_matchings` and `merge_matchings` (which are
  // vectors of vectors).
  for (const auto &matching : matchings) {
    // matchings will always have at least a single element, so we can use a for
    // loop. On the other hand, optional and merge matchings can be empty so we
    // need an iterator and do...while loop.
    auto optional_it = optional_matchings.begin();
    auto optional_end = optional_matchings.end();
    do {
      auto merge_it = merge_matchings.begin();
      auto merge_end = merge_matchings.end();
      do {
        // Produce parts for each possible combination. E.g. if we have:
        //    * matchings (m1) and (m2)
        //    * optional matchings (o1) and (o2)
        //    * merge matching (g1)
        // We want to produce parts for:
        //    * (m1), (o1), (g1)
        //    * (m1), (o2), (g1)
        //    * (m2), (o1), (g1)
        //    * (m2), (o2), (g1)
        variants.emplace_back(QueryPart{matching});
        variants.back().remaining_clauses = query_part.remaining_clauses;
        if (optional_it != optional_matchings.end()) {
          // In case we started with empty optional matchings.
          variants.back().optional_matching = *optional_it;
        }
        if (merge_it != merge_matchings.end()) {
          // In case we started with empty merge matchings.
          variants.back().merge_matching = *merge_it;
        }
        // Since we can start with the iterator at the end, we have to first
        // compare it and then increment it. After we increment, we need to
        // check again to avoid generating with empty matching.
      } while (merge_it != merge_end && ++merge_it != merge_end);
    } while (optional_it != optional_end && ++optional_it != optional_end);
  }
  return variants;
}

// Generates different, equivalent query parts by taking different graph
// matching routes for each query part.
auto VaryQueryMatching(const std::vector<QueryPart> &query_parts,
                       const SymbolTable &symbol_table) {
  std::vector<std::vector<QueryPart>> alternative_query_parts;
  for (const auto &query_part : query_parts) {
    alternative_query_parts.emplace_back(
        VaryQueryPartMatching(query_part, symbol_table));
  }
  return iter::slice(
      CartesianProduct<QueryPart>(std::move(alternative_query_parts)), 0UL,
      FLAGS_query_max_plans);
}

}  // namespace

std::vector<std::unique_ptr<LogicalOperator>> VariableStartPlanner::Plan(
    std::vector<QueryPart> &query_parts) {
  std::vector<std::unique_ptr<LogicalOperator>> plans;
  auto alternatives = VaryQueryMatching(query_parts, context_.symbol_table);
  RuleBasedPlanner rule_planner(context_);
  for (auto alternative_query_parts : alternatives) {
    context_.bound_symbols.clear();
    plans.emplace_back(rule_planner.Plan(alternative_query_parts));
  }
  return plans;
}

}  // namespace query::plan
