// Copyright 2024 Memgraph Ltd.
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

#include <map>

#include "utils/memory.hpp"

namespace memgraph::utils::pmr {

// Use transparent std::less<void> which forwards to `operator<`, so that, for
// example, it's possible to use `find` with C-style (null terminated) strings
// without actually constructing (and allocating) a key.
// Only find() uses transparent comparison; C++26 will expand the API
template <class Key, class T, class Compare = std::less<void>>
using map = std::map<Key, T, Compare, utils::Allocator<std::pair<const Key, T>>>;

}  // namespace memgraph::utils::pmr
