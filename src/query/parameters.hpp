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

#pragma once

#include <algorithm>
#include <utility>
#include <vector>

#include "storage/v2/property_value.hpp"
#include "utils/logging.hpp"

/**
 * Encapsulates user provided parameters (and stripped literals)
 * and provides ways of obtaining them by position.
 */
namespace memgraph::query {

struct Parameters {
 public:
  /**
   * Adds a value to the stripped arguments under a token position.
   *
   * @param position Token position in query of value.
   * @param value
   */
  void Add(int position, const storage::ExternalPropertyValue &value) { storage_.emplace_back(position, value); }

  /**
   *  Returns the value found for the given token position.
   *
   *  @param position Token position in query of value.
   *  @return Value for the given token position.
   */
  const storage::ExternalPropertyValue &AtTokenPosition(int position) const {
    auto found = std::find_if(storage_.begin(), storage_.end(), [&](const auto &a) { return a.first == position; });
    MG_ASSERT(found != storage_.end(), "Token position must be present in container");
    return found->second;
  }

  /**
   * Returns the position-th stripped value. Asserts that this
   * container has at least (position + 1) elements.
   *
   * @param position Which stripped param is sought.
   * @return Token position and value for sought param.
   */
  const std::pair<int, storage::ExternalPropertyValue> &At(int position) const {
    MG_ASSERT(position < static_cast<int>(storage_.size()), "Invalid position");
    return storage_[position];
  }

  /** Returns the number of arguments in this container */
  auto size() const { return storage_.size(); }

  auto begin() const { return storage_.begin(); }
  auto end() const { return storage_.end(); }

 private:
  std::vector<std::pair<int, storage::ExternalPropertyValue>> storage_;
};

}  // namespace memgraph::query
