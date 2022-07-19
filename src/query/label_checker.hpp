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

#include "auth/models.hpp"
#include "query/frontend/ast/ast.hpp"
#include "storage/v2/id_types.hpp"

namespace memgraph::query {
class LabelChecker {
 public:
  virtual bool IsUserAuthorized(const std::vector<memgraph::storage::LabelId> &label,
                                memgraph::query::DbAccessor *dba) const = 0;
};
}  // namespace memgraph::query
