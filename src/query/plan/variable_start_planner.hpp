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

/// @file
#pragma once

#include "cppitertools/imap.hpp"
#include "cppitertools/slice.hpp"
#include "gflags/gflags.h"

#include "query/plan/rule_based_planner.hpp"

DECLARE_uint64(query_max_plans);

namespace memgraph::query::plan {

/// Produces a Cartesian product among vectors between begin and end iterator.
/// For example:
///
///    std::vector<int> first_set{1,2,3};
///    std::vector<int> second_set{4,5};
///    std::vector<std::vector<int>> all_sets{first_set, second_set};
///    // prod should be {{1, 4}, {1, 5}, {2, 4}, {2, 5}, {3, 4}, {3, 5}}
///    auto product = MakeCartesianProduct(all_sets);
///    for (const auto &set : product) {
///      ...
///    }
///
/// The product is created lazily by iterating over the constructed
/// CartesianProduct instance.
template <typename TSet>
class CartesianProduct {
 private:
  // The original sets whose Cartesian product we are calculating.
  std::vector<TSet> original_sets_;
  // Iterators to the beginning and end of original_sets_.
  decltype(original_sets_.begin()) begin_;
  decltype(original_sets_.end()) end_;

  // Type of the set element.
  using TElement = typename decltype(begin_->begin())::value_type;

 public:
  CartesianProduct(std::vector<TSet> sets)
      : original_sets_(std::move(sets)), begin_(original_sets_.begin()), end_(original_sets_.end()) {}

  class iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = std::vector<TElement>;
    using difference_type = long;
    using reference = const std::vector<TElement> &;
    using pointer = const std::vector<TElement> *;

    explicit iterator(CartesianProduct *self, bool is_done) : self_(self), is_done_(is_done) {
      if (is_done || self->begin_ == self->end_) {
        is_done_ = true;
        return;
      }
      auto begin = self->begin_;
      while (begin != self->end_) {
        auto set_it = begin->begin();
        if (set_it == begin->end()) {
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
      ++sets_it->second;
      // If the leftmost is at the end, reset it and increment the next
      // leftmost.
      while (sets_it->second == sets_it->first->end()) {
        sets_it->second = sets_it->first->begin();
        sets_it++;
        if (sets_it == sets_.end()) {
          // The leftmost set is the last set and it was exhausted, so we are
          // done.
          is_done_ = true;
          return *this;
        }
        ++sets_it->second;
      }
      // We can now collect another product from the modified set iterators.
      DMG_ASSERT(current_product_.size() == sets_.size(),
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
      if (self_->begin_ != other.self_->begin_ || self_->end_ != other.self_->end_) return false;
      return (is_done_ && other.is_done_) || (sets_ == other.sets_);
    }

    bool operator!=(const iterator &other) const { return !(*this == other); }

    // Iterator interface says that dereferencing a past-the-end iterator is
    // undefined, so don't bother checking if we are done.
    reference operator*() const { return current_product_; }
    pointer operator->() const { return &current_product_; }

   private:
    // Pointer instead of reference to auto generate copy constructor and
    // assignment.
    CartesianProduct *self_;
    // Vector of (original_sets_iterator, set_iterator) pairs. The
    // original_sets_iterator points to the set among all the sets, while the
    // set_iterator points to an element inside the pointed to set.
    std::vector<std::pair<decltype(self_->begin_), decltype(self_->begin_->begin())>> sets_;
    // Currently built product from pointed to elements in all sets.
    std::vector<TElement> current_product_;
    // Set to true when we have generated all products.
    bool is_done_ = false;
  };

  auto begin() { return iterator(this, false); }
  auto end() { return iterator(this, true); }

 private:
  friend class iterator;
};

/// Convenience function for creating CartesianProduct by deducing template
/// arguments from function arguments.
template <typename TSet>
auto MakeCartesianProduct(std::vector<TSet> sets) {
  return CartesianProduct<TSet>(std::move(sets));
}

namespace impl {

class NodeSymbolHash {
 public:
  explicit NodeSymbolHash(const SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  size_t operator()(const NodeAtom *node_atom) const {
    return std::hash<Symbol>{}(symbol_table_.at(*node_atom->identifier_));
  }

 private:
  const SymbolTable &symbol_table_;
};

class NodeSymbolEqual {
 public:
  explicit NodeSymbolEqual(const SymbolTable &symbol_table) : symbol_table_(symbol_table) {}

  bool operator()(const NodeAtom *node_atom1, const NodeAtom *node_atom2) const {
    return symbol_table_.at(*node_atom1->identifier_) == symbol_table_.at(*node_atom2->identifier_);
  }

 private:
  const SymbolTable &symbol_table_;
};

// Generates n matchings, where n is the number of nodes to match. Each Matching
// will have a different node as a starting node for expansion.
class VaryMatchingStart {
 public:
  VaryMatchingStart(Matching, const SymbolTable &);

  class iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = Matching;
    using difference_type = long;
    using reference = const Matching &;
    using pointer = const Matching *;

    iterator(VaryMatchingStart *, bool);

    iterator &operator++();
    reference operator*() const { return current_matching_; }
    pointer operator->() const { return &current_matching_; }
    bool operator==(const iterator &other) const {
      return self_ == other.self_ && start_nodes_it_ == other.start_nodes_it_;
    }
    bool operator!=(const iterator &other) const { return !(*this == other); }

   private:
    // Pointer instead of reference to auto generate copy constructor and
    // assignment.
    VaryMatchingStart *self_;
    Matching current_matching_;
    // Iterator over start nodes. Optional is used for differentiating the case
    // when there are no start nodes vs. VaryMatchingStart::iterator itself
    // being at the end. When there are no nodes, this iterator needs to produce
    // a single result, which is the original matching passed in. Setting
    // start_nodes_it_ to end signifies the end of our iteration.
    std::optional<std::unordered_set<NodeAtom *, NodeSymbolHash, NodeSymbolEqual>::iterator> start_nodes_it_;
  };

  auto begin() { return iterator(this, false); }
  auto end() { return iterator(this, true); }

 private:
  friend class iterator;
  Matching matching_;
  const SymbolTable &symbol_table_;
  std::unordered_set<NodeAtom *, NodeSymbolHash, NodeSymbolEqual> nodes_;
};

// Similar to VaryMatchingStart, but varies the starting nodes for all given
// matchings. After all matchings produce multiple alternative starts, the
// Cartesian product of all of them is returned.
CartesianProduct<VaryMatchingStart> VaryMultiMatchingStarts(const std::vector<Matching> &, const SymbolTable &);

CartesianProduct<VaryMatchingStart> VaryFilterMatchingStarts(const Matching &matching, const SymbolTable &symbol_table);

// Produces alternative query parts out of a single part by varying how each
// graph matching is done.
class VaryQueryPartMatching {
 public:
  VaryQueryPartMatching(SingleQueryPart, const SymbolTable &);

  class iterator {
   public:
    using iterator_category = std::input_iterator_tag;
    using value_type = SingleQueryPart;
    using difference_type = long;
    using reference = const SingleQueryPart &;
    using pointer = const SingleQueryPart *;

    iterator(const SingleQueryPart &, VaryMatchingStart::iterator, VaryMatchingStart::iterator,
             CartesianProduct<VaryMatchingStart>::iterator, CartesianProduct<VaryMatchingStart>::iterator,
             CartesianProduct<VaryMatchingStart>::iterator, CartesianProduct<VaryMatchingStart>::iterator,
             CartesianProduct<VaryMatchingStart>::iterator, CartesianProduct<VaryMatchingStart>::iterator);

    iterator &operator++();
    reference operator*() const { return current_query_part_; }
    pointer operator->() const { return &current_query_part_; }
    bool operator==(const iterator &) const;
    bool operator!=(const iterator &other) const { return !(*this == other); }

   private:
    void SetCurrentQueryPart();

    SingleQueryPart current_query_part_;
    VaryMatchingStart::iterator matchings_it_;
    VaryMatchingStart::iterator matchings_end_;
    CartesianProduct<VaryMatchingStart>::iterator optional_it_;
    CartesianProduct<VaryMatchingStart>::iterator optional_begin_;
    CartesianProduct<VaryMatchingStart>::iterator optional_end_;
    CartesianProduct<VaryMatchingStart>::iterator merge_it_;
    CartesianProduct<VaryMatchingStart>::iterator merge_begin_;
    CartesianProduct<VaryMatchingStart>::iterator merge_end_;
    CartesianProduct<VaryMatchingStart>::iterator filter_it_;
    CartesianProduct<VaryMatchingStart>::iterator filter_begin_;
    CartesianProduct<VaryMatchingStart>::iterator filter_end_;
  };

  auto begin() {
    return iterator(query_part_, matchings_.begin(), matchings_.end(), optional_matchings_.begin(),
                    optional_matchings_.end(), merge_matchings_.begin(), merge_matchings_.end(),
                    filter_matchings_.begin(), filter_matchings_.end());
  }
  auto end() {
    return iterator(query_part_, matchings_.end(), matchings_.end(), optional_matchings_.end(),
                    optional_matchings_.end(), merge_matchings_.end(), merge_matchings_.end(), filter_matchings_.end(),
                    filter_matchings_.end());
  }

 private:
  SingleQueryPart query_part_;
  // Multiple regular matchings, each starting from different node.
  VaryMatchingStart matchings_;
  // Multiple optional matchings, where each combination has different starting
  // nodes.
  CartesianProduct<VaryMatchingStart> optional_matchings_;
  // Like optional matching, but for merge matchings.
  CartesianProduct<VaryMatchingStart> merge_matchings_;
  CartesianProduct<VaryMatchingStart> filter_matchings_;
};

}  // namespace impl

/// @brief Planner which generates multiple plans by changing the order of graph
/// traversal.
///
/// This planner picks different starting nodes from which to start graph
/// traversal. Generating a single plan is backed by @c RuleBasedPlanner.
///
/// @sa MakeLogicalPlan
template <class TPlanningContext>
class VariableStartPlanner {
 private:
  TPlanningContext *context_;

  // Generates different, equivalent query parts by taking different graph
  // matching routes for each query part.
  auto VaryQueryMatching(const QueryParts &query_parts, const SymbolTable &symbol_table) {
    std::vector<impl::VaryQueryPartMatching> varying_query_matchings;

    auto single_query_parts = ExtractSingleQueryParts(std::make_unique<QueryParts>(query_parts));

    for (const auto &single_query_part : single_query_parts) {
      varying_query_matchings.emplace_back(single_query_part, symbol_table);
    }

    return iter::slice(MakeCartesianProduct(std::move(varying_query_matchings)), 0UL, FLAGS_query_max_plans);
  }

  std::vector<SingleQueryPart> ExtractSingleQueryParts(const std::shared_ptr<QueryParts> query_parts) {
    std::vector<SingleQueryPart> results;

    for (const auto &query_part : query_parts->query_parts) {
      for (const auto &single_query_part : query_part.single_query_parts) {
        results.push_back(single_query_part);

        for (const auto &subquery : single_query_part.subqueries) {
          const auto subquery_results = ExtractSingleQueryParts(subquery);
          results.insert(results.end(), std::make_move_iterator(subquery_results.begin()),
                         std::make_move_iterator(subquery_results.end()));
        }
      }
    }

    return results;
  }

  QueryParts ReconstructQueryParts(const QueryParts &old_query_parts,
                                   const std::vector<SingleQueryPart> &single_query_parts_variation, uint64_t &index) {
    auto reconstructed_query_parts = old_query_parts;

    for (auto i = 0; i < old_query_parts.query_parts.size(); i++) {
      const auto &old_query_part = old_query_parts.query_parts[i];
      for (auto j = 0; j < old_query_part.single_query_parts.size(); j++) {
        const auto &old_single_query_part = old_query_part.single_query_parts[j];
        reconstructed_query_parts.query_parts[i].single_query_parts[j] = single_query_parts_variation[index++];

        for (auto k = 0; k < old_single_query_part.subqueries.size(); k++) {
          const auto &subquery = old_single_query_part.subqueries[k];
          reconstructed_query_parts.query_parts[i].single_query_parts[j].subqueries[k] =
              std::make_shared<QueryParts>(ReconstructQueryParts(*subquery, single_query_parts_variation, index));
        }
      }
    }

    return reconstructed_query_parts;
  }

 public:
  explicit VariableStartPlanner(TPlanningContext *context) : context_(context) {}

  /// @brief Generate multiple plans by varying the order of graph traversal.
  auto Plan(const QueryParts &query_parts) {
    return iter::imap(
        [context = context_, old_query_parts = query_parts, this](const auto &alternative_query_parts) {
          uint64_t index = 0;
          auto reconstructed_query_parts = ReconstructQueryParts(old_query_parts, alternative_query_parts, index);

          RuleBasedPlanner<TPlanningContext> rule_planner(context);
          context->bound_symbols.clear();
          return rule_planner.Plan(reconstructed_query_parts);
        },
        VaryQueryMatching(query_parts, *context_->symbol_table));
  }

  /// @brief The result of plan generation is an iterable of roots to multiple
  /// generated operator trees.
  using PlanResult = std::result_of_t<decltype (&VariableStartPlanner<TPlanningContext>::Plan)(
      VariableStartPlanner<TPlanningContext>, QueryParts &)>;
};

}  // namespace memgraph::query::plan
