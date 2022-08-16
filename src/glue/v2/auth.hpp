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

#include "auth/models.hpp"
#include "query/v2/frontend/ast/ast.hpp"

namespace memgraph::glue::v2 {

/**
 * This function converts query::AuthQuery::Privilege to its corresponding
 * auth::Permission.
 */
auth::Permission PrivilegeToPermission(query::v2::AuthQuery::Privilege privilege);

}  // namespace memgraph::glue::v2
