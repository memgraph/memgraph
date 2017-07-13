#include "query/plan/planner.hpp"

#include <limits>

#include <gflags/gflags.h>
#include <glog/logging.h>

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
// expanded nodes. The function may modify expansions, by flipping their nodes
// and direction. This is done, so that the return iterator always points to the
// expansion whose node1 is the already expanded one, while node2 may not be.
auto NextExpansion(const std::unordered_set<const NodeAtom *, NodeSymbolHash,
                                            NodeSymbolEqual> &expanded_nodes,
                   std::vector<Expansion> &expansions) {
  auto expansion_it = expansions.begin();
  for (; expansion_it != expansions.end(); ++expansion_it) {
    if (expanded_nodes.find(expansion_it->node1) != expanded_nodes.end()) {
      return expansion_it;
    }
    auto *node2 = expansion_it->node2;
    if (node2 && expanded_nodes.find(node2) != expanded_nodes.end()) {
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
  std::unordered_set<const NodeAtom *, NodeSymbolHash, NodeSymbolEqual>
      expanded_nodes({start_node}, original_expansions.size(),
                     NodeSymbolHash(symbol_table),
                     NodeSymbolEqual(symbol_table));
  while (!original_expansions.empty()) {
    auto next_it = NextExpansion(expanded_nodes, original_expansions);
    if (next_it == original_expansions.end()) {
      // Pick a new starting expansion, since we cannot continue the chain.
      next_it = original_expansions.begin();
    }
    expanded_nodes.insert(next_it->node1);
    if (next_it->node2) {
      expanded_nodes.insert(next_it->node2);
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
//    auto prod = CartesianProduct(all_sets.cbegin(), all_sets.cend())
template <typename T>
std::vector<std::vector<T>> CartesianProduct(
    typename std::vector<std::vector<T>>::const_iterator begin,
    typename std::vector<std::vector<T>>::const_iterator end) {
  std::vector<std::vector<T>> products;
  if (begin == end) {
    return products;
  }
  auto later_products = CartesianProduct<T>(begin + 1, end);
  for (const auto &elem : *begin) {
    if (later_products.empty()) {
      products.emplace_back(std::vector<T>{elem});
    } else {
      for (const auto &rest : later_products) {
        std::vector<T> product{elem};
        product.insert(product.end(), rest.begin(), rest.end());
        products.emplace_back(std::move(product));
      }
    }
  }
  return products;
}

template <typename T>
std::uint64_t CartesianProductSize(const std::vector<std::vector<T>> &sets) {
  std::uint64_t n = 1U;
  for (const auto &set : sets) {
    if (set.empty()) {
      return 0U;
    }
    std::uint64_t new_n = n * set.size();
    if (new_n < n || new_n < set.size()) {
      DLOG(WARNING) << "Unsigned wrap-around when calculating expected size of "
                       "Cartesian product.";
      return std::numeric_limits<std::uint64_t>::max();
    }
    n = new_n;
  }
  return n;
}

// Shortens variants if their Cartesian product exceeds the query_max_plans
// flag.
template <typename T>
void LimitPlans(std::vector<std::vector<T>> &variants) {
  size_t to_shorten = 0U;
  while (CartesianProductSize(variants) > FLAGS_query_max_plans) {
    if (variants[to_shorten].size() > 1U) {
      variants[to_shorten].pop_back();
    }
    to_shorten = (to_shorten + 1U) % variants.size();
  }
}

// Similar to VaryMatchingStart, but varies the starting nodes for all given
// matchings. After all matchings produce multiple alternative starts, the
// Cartesian product of all of them is returned.
std::vector<std::vector<Matching>> VaryMultiMatchingStarts(
    const std::vector<Matching> &matchings, const SymbolTable &symbol_table) {
  std::vector<std::vector<Matching>> variants;
  for (const auto &matching : matchings) {
    variants.emplace_back(VaryMatchingStart(matching, symbol_table));
  }
  LimitPlans(variants);
  return CartesianProduct<Matching>(variants.cbegin(), variants.cend());
}

// Produces alternative query parts out of a single part by varying how each
// graph matching is done.
std::vector<QueryPart> VaryQuertPartMatching(const QueryPart &query_part,
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
std::vector<std::vector<QueryPart>> VaryQueryMatching(
    const std::vector<QueryPart> &query_parts,
    const SymbolTable &symbol_table) {
  std::vector<std::vector<QueryPart>> alternative_query_parts;
  for (const auto &query_part : query_parts) {
    alternative_query_parts.emplace_back(
        VaryQuertPartMatching(query_part, symbol_table));
  }
  LimitPlans(alternative_query_parts);
  return CartesianProduct<QueryPart>(alternative_query_parts.cbegin(),
                                     alternative_query_parts.cend());
}

}  // namespace

std::vector<std::unique_ptr<LogicalOperator>> VariableStartPlanner::Plan(
    std::vector<QueryPart> &query_parts) {
  std::vector<std::unique_ptr<LogicalOperator>> plans;
  auto alternatives = VaryQueryMatching(query_parts, context_.symbol_table);
  RuleBasedPlanner rule_planner(context_);
  for (auto &alternative_query_parts : alternatives) {
    context_.bound_symbols.clear();
    plans.emplace_back(rule_planner.Plan(alternative_query_parts));
  }
  return plans;
}

}  // namespace query::plan
