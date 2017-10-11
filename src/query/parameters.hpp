//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 08.03.17.
//

#ifndef MEMGRAPH_PARAMETERS_HPP
#define MEMGRAPH_PARAMETERS_HPP

#include <algorithm>
#include <utility>
#include <vector>

#include "query/typed_value.hpp"

/**
 * Encapsulates user provided parameters (and stripped literals)
 * and provides ways of obtaining them by position.
 */
struct Parameters {
 public:
  /**
   * Adds a value to the stripped arguments under a token position.
   *
   * @param position Token position in query of value.
   * @param value
   */
  void Add(int position, const query::TypedValue &value) {
    storage_.emplace_back(position, value);
  }

  /**
   *  Returns the value found for the given token position.
   *
   *  @param position Token position in query of value.
   *  @return Value for the given token position.
   */
  const query::TypedValue &AtTokenPosition(int position) const {
    auto found = std::find_if(storage_.begin(), storage_.end(),
                              [&](const std::pair<int, query::TypedValue> a) {
                                return a.first == position;
                              });
    CHECK(found != storage_.end())
        << "Token position must be present in container";
    return found->second;
  }

  /**
   * Returns the position-th stripped value. Asserts that this
   * container has at least (position + 1) elements.
   *
   * @param position Which stripped param is sought.
   * @return Token position and value for sought param.
   */
  const std::pair<int, query::TypedValue> &At(int position) const {
    CHECK(position < static_cast<int>(storage_.size())) << "Invalid position";
    return storage_[position];
  }

  /** Returns the number of arguments in this container */
  int size() const { return storage_.size(); }

 private:
  std::vector<std::pair<int, query::TypedValue>> storage_;
};

#endif  // MEMGRAPH_PARAMETERS_HPP
