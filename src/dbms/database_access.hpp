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

#include "storage/v2/database_access.hpp"
#include "utils/gatekeeper.hpp"

namespace memgraph::dbms {
class Database;
}

extern template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;

namespace memgraph::dbms {
using DatabaseAccess = memgraph::utils::Gatekeeper<memgraph::dbms::Database>::Accessor;

struct DatabaseProtector : storage::DatabaseProtector {
  explicit DatabaseProtector(DatabaseAccess access);
  auto clone() const -> storage::DatabaseProtectorPtr override;

 private:
  DatabaseAccess access_;
};

}  // namespace memgraph::dbms
