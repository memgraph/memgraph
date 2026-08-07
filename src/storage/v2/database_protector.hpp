// Copyright 2026 Memgraph Ltd.
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

#include <memory>

namespace memgraph::storage {

/**
 * @brief We need to protect the database using a DatabaseAccess, and we need to keep the replication/storage/dbms
 * untied. To achieve that we are using std::any, but beware to pass in the correct type using DatabaseAccess =
 * memgraph::utils::Gatekeeper<memgraph::dbms::Database>::Accessor;
 */
struct DatabaseProtector;

using DatabaseProtectorPtr = std::unique_ptr<DatabaseProtector>;

struct DatabaseProtector {
  virtual auto clone() const -> DatabaseProtectorPtr = 0;

  /// A protector holds its tenant alive. Background work that intends to re-arm itself (enqueue
  /// more work, clone() this protector again) must ask here first and stop instead -- otherwise it
  /// keeps a tenant that has already been accepted for deletion alive indefinitely. `false` is the
  /// honest default for a protector that does not represent a droppable tenant at all. A `true`
  /// answer is only a cooperative request to stop: it never revokes the protector, which stays
  /// valid and stays held by the caller until the caller itself lets it go.
  virtual auto is_tenant_marked_for_deletion() const -> bool { return false; }

  virtual ~DatabaseProtector() = default;
};

}  // namespace memgraph::storage
