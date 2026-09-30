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

#include <iosfwd>

#include "query/db_accessor.hpp"
#include "query/frontend/ast/query/expression.hpp"
#include "query/frontend/ast/query/named_expression.hpp"

namespace memgraph::query {

void PrintExpression(Expression *expr, std::ostream *out, const DbAccessor &dba);
void PrintExpression(NamedExpression *expr, std::ostream *out, const DbAccessor &dba);

}  // namespace memgraph::query
