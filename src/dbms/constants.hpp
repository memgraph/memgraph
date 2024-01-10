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

namespace memgraph::dbms {

constexpr static const char *kDefaultDB = "memgraph";  //!< Name of the default database

#ifdef MG_EXPERIMENTAL_REPLICATION_MULTITENANCY
constexpr bool allow_mt_repl = true;
#else
constexpr bool allow_mt_repl = false;
#endif

}  // namespace memgraph::dbms
