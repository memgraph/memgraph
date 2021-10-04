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

#include <unordered_map>

#include "utils/memory.hpp"

namespace utils::pmr {

// Use transparent std::equal_to<void> which forwards to `operator==`, so that,
// for example, it's possible to use `find` with C-style (null terminated)
// strings without actually constructing (and allocating) a key.
template <class Key, class T, class Hash = std::hash<Key>, class Pred = std::equal_to<void>>
using unordered_map = std::unordered_map<Key, T, Hash, Pred, utils::Allocator<std::pair<const Key, T>>>;

}  // namespace utils::pmr
