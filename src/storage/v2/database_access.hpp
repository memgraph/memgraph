// Copyright 2024 Memgraph Ltd.
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

#include <any>

namespace memgraph::storage {

/**
 * @brief We need to protect the database using a DatabaseAccess, and we need to keep the replication/storage/dbms
 * untied. To achieve that we are using std::any, but beware to pass in the correct type using DatabaseAccess =
 * memgraph::utils::Gatekeeper<memgraph::dbms::Database>::Accessor;
 */
using DatabaseAccessProtector = std::any;

}  // namespace memgraph::storage
