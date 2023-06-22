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

#include <string>

#include "query/frontend/ast/ast.hpp"
#include "spdlog/spdlog.h"

namespace memgraph::utils {

// Get ID by which FrameChangeCollector struct can cache in_list.expression2_
inline std::optional<std::string> GetFrameChangeId(memgraph::query::InListOperator &in_list) {
  if (in_list.expression2_->GetTypeInfo() == memgraph::query::ListLiteral::kType) {
    std::stringstream ss;
    ss << static_cast<const void *>(in_list.expression2_);
    return ss.str();
  }
  if (in_list.expression2_->GetTypeInfo() == memgraph::query::Identifier::kType) {
    auto *identifier = utils::Downcast<memgraph::query::Identifier>(in_list.expression2_);
    return identifier->name_;
  }
  return {};
};

}  // namespace memgraph::utils
