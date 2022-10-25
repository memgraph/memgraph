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

#include <functional>
#include <typeinfo>

namespace memgraph::utils {

using TypeInfoRef = std::reference_wrapper<const std::type_info>;

struct TypeInfoHasher {
  std::size_t operator()(TypeInfoRef code) const { return code.get().hash_code(); }
};

struct TypeInfoEqualTo {
  bool operator()(TypeInfoRef lhs, TypeInfoRef rhs) const { return lhs.get() == rhs.get(); }
};

}  // namespace memgraph::utils
