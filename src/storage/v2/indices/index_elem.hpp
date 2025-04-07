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

#include <atomic>
#include <cstdint>
#include <stop_token>

#include "utils/skip_list.hpp"

namespace memgraph::storage {

template <typename T>
struct IndexElem {
  enum class State : uint8_t { INCOMPLETE, READY, DELETED };
  std::atomic<State> state{State::INCOMPLETE};
  std::stop_source stop_token;  // TODO Use this to signal index population to stop, once DROP can happen in parallel
  utils::SkipList<T> index;
};

}  // namespace memgraph::storage
