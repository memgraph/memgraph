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

#ifdef MG_AST_INCLUDE_PATH
#error You are probably trying to include some files of mg-expr from both the storage and query engines! You will have a rought time kid!
#endif

#define MG_AST_INCLUDE_PATH "storage/v3/bindings/ast/ast.hpp"  // NOLINT(cppcoreguidelines-macro-usage)
#define MG_INJECTED_NAMESPACE_NAME memgraph::storage::v3       // NOLINT(cppcoreguidelines-macro-usage)
