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

#ifndef MG_AST_INCLUDE_PATH
#ifdef MG_CLANG_TIDY_CHECK
#define MG_AST_INCLUDE_PATH "query/v2/frontend/ast/ast.hpp"
#else
#error Missing AST include path
#endif
#endif

#ifndef MG_INJECTED_NAMESPACE_NAME
#ifdef MG_CLANG_TIDY_CHECK
#define MG_INJECTED_NAMESPACE_NAME memgraph::query::v2
#else
#error Missing AST namespace
#endif
#endif

#include MG_AST_INCLUDE_PATH

namespace memgraph::expr {
using namespace MG_INJECTED_NAMESPACE_NAME;
}  // namespace memgraph::expr
