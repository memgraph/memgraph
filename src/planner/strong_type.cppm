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

module;

#include "strong_type/strong_type.hpp"

export module rollbear.strong_type;

// Re-export the entire strong namespace
export namespace strong {
using strong::type;
using strong::type_is;

// Export all the modifiers/skills
using strong::affine_point;
using strong::arithmetic;
using strong::bicrementable;
using strong::bitarithmetic;
using strong::boolean;
using strong::convertible_to;
using strong::decrementable;
using strong::difference;
using strong::equality;
using strong::formattable;
using strong::hashable;
using strong::implicitly_convertible_to;
using strong::incrementable;
using strong::indexed;
using strong::istreamable;
using strong::iterator;
using strong::ordered;
using strong::ostreamable;
using strong::pointer;
using strong::range;
using strong::regular;
using strong::semiregular;
}  // namespace strong
