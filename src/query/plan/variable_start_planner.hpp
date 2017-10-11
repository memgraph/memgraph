/// @file
#pragma once

#include "cppitertools/slice.hpp"
#include "gflags/gflags.h"

#include "query/plan/rule_based_planner.hpp"

DECLARE_uint64(query_max_plans);

namespace query::plan {

/// Produces a Cartesian product among vectors between begin and end iterator.
/// For example:
///
///    std::vector<int> first_set{1,2,3};
///    std::vector<int> second_set{4,5};
///    std::vector<std::vector<int>> all_sets{first_set, second_set};
///    // prod should be {{1, 4}, {1, 5}, {2, 4}, {2, 5}, {3, 4}, {3, 5}}
///    auto product = CartesianProduct(all_sets.cbegin(), all_sets.cend());
///    for (const auto &set : product) {
///      ...
///    }
///
/// The product is created lazily by iterating over the constructed
/// CartesianProduct instance.
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
      DCHECK(current_product_.size() == sets_.size())
          << "Expected size of current_product_ to match the size of sets_";
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

/// Convenience function for creating CartesianProduct by deducing template
/// arguments from function arguments.
template <typename T>
auto MakeCartesianProduct(std::vector<std::vector<T>> sets) {
  return CartesianProduct<T>(sets);
}

namespace impl {

std::vector<QueryPart> VaryQueryPartMatching(const QueryPart &query_part,
                                             const SymbolTable &symbol_table);

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
 public:
  explicit VariableStartPlanner(TPlanningContext &context)
      : context_(context) {}

  /// @brief The result of plan generation is a vector of roots to multiple
  /// generated operator trees.
  using PlanResult = std::vector<std::unique_ptr<LogicalOperator>>;
  /// @brief Generate multiple plans by varying the order of graph traversal.
  PlanResult Plan(std::vector<QueryPart> &query_parts) {
    std::vector<std::unique_ptr<LogicalOperator>> plans;
    auto alternatives = VaryQueryMatching(query_parts, context_.symbol_table);
    RuleBasedPlanner<TPlanningContext> rule_planner(context_);
    for (auto alternative_query_parts : alternatives) {
      context_.bound_symbols.clear();
      plans.emplace_back(rule_planner.Plan(alternative_query_parts));
    }
    return plans;
  }

 private:
  TPlanningContext &context_;

  // Generates different, equivalent query parts by taking different graph
  // matching routes for each query part.
  auto VaryQueryMatching(const std::vector<QueryPart> &query_parts,
                         const SymbolTable &symbol_table) {
    std::vector<std::vector<QueryPart>> alternative_query_parts;
    for (const auto &query_part : query_parts) {
      alternative_query_parts.emplace_back(
          impl::VaryQueryPartMatching(query_part, symbol_table));
    }
    return iter::slice(MakeCartesianProduct(std::move(alternative_query_parts)),
                       0UL, FLAGS_query_max_plans);
  }
};

}  // namespace query::plan
