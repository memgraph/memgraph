// Copyright 2022 Memgraph Ltd.
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
#include <unordered_map>
#include <utility>

#include "storage/v3/property_value.hpp"
#include "utils/logging.hpp"

/**
 * Encapsulates user provided parameters (and stripped literals)
 * and provides ways of obtaining them by position.
 */
namespace memgraph::query::v2 {

struct Parameters {
 public:
  /**
   * Adds a value to the stripped arguments under a token position.
   *
   * @param position Token position in query of value.
   * @param value
   */
  void Add(int position, const storage::v3::PropertyValue &value) { storage_.emplace(position, value); }

  /**
   *  Returns the value found for the given token position.
   *
   *  @param position Token position in query of value.
   *  @return Value for the given token position.
   */
  const storage::v3::PropertyValue &AtTokenPosition(int position) const {
    auto found = storage_.find(position);
    MG_ASSERT(found != storage_.end(), "Token position must be present in container");
    return found->second;
  }

  /** Returns the number of arguments in this container */
  auto size() const { return storage_.size(); }

  auto begin() const { return storage_.begin(); }
  auto end() const { return storage_.end(); }

 private:
  std::unordered_map<int, storage::v3::PropertyValue> storage_;
};

}  // namespace memgraph::query::v2
