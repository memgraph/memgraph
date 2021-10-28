// Copyright 2021 Memgraph Ltd.
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

#include <functional>

namespace utils {

/**
 * Calls a function in it's destructor (on scope exit).
 *
 * Example usage:
 *
 * void long_function() {
 *     resource.enable();
 *     // long block of code, might throw an exception
 *     resource.disable(); // we want this to happen for sure, and function end
 * }
 *
 * Can be nicer and safer:
 *
 * void long_function() {
 *     resource.enable();
 *     OnScopeExit on_exit([&resource] { resource.disable(); });
 *     // long block of code, might trow an exception
 * }
 */
class OnScopeExit {
 public:
  explicit OnScopeExit(const std::function<void()> &function) : function_(function) {}
  ~OnScopeExit() { function_(); }

  void Disable() {
    function_ = [] {};
  }

 private:
  std::function<void()> function_;
};

}  // namespace utils
