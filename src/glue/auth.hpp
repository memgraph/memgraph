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

#include "auth/models.hpp"
#include "query/frontend/ast/query/auth_query.hpp"

namespace memgraph::glue {

/**
 * This function converts query::AuthQuery::Privilege to its corresponding
 * auth::Permission.
 */
auth::Permission PrivilegeToPermission(query::AuthQuery::Privilege privilege);

#ifdef MG_ENTERPRISE
/**
 * Converts query::AuthQuery::FineGrainedPrivilege to its corresponding
 * auth::EntityPermission.
 */
auth::FineGrainedPermission FineGrainedPrivilegeToFineGrainedPermission(
    query::AuthQuery::FineGrainedPrivilege fine_grained_privilege);
#endif
}  // namespace memgraph::glue
