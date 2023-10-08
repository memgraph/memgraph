// Copyright 2023 Memgraph Ltd.
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

#include <fmt/format.h>

namespace memgraph::utils {

// TODO(gitbuda): fmt.get() isn't there in toolchain-v4 -> probably make ifdef.
template <typename... Args>
std::string MessageWithLink(fmt::format_string<Args...> fmt, Args &&...args) {
  return fmt::format(fmt::runtime(fmt::format(fmt::runtime("{} For more details, visit {{}}."), fmt.get())),
                     std::forward<Args>(args)...);
}

}  // namespace memgraph::utils
