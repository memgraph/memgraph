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

#include "dbms/database_access.hpp"
#include "dbms/database.hpp"

namespace memgraph::dbms {
DatabaseProtector::DatabaseProtector(DatabaseAccess access) : access_(std::move(access)) {}

auto DatabaseProtector::clone() const -> storage::DatabaseProtectorPtr {
  return std::unique_ptr<storage::DatabaseProtector>{new DatabaseProtector{access_}};
}
}  // namespace memgraph::dbms
