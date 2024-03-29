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

#include "interactive_planning.hpp"

#include <gflags/gflags.h>

#include "storage/v2/inmemory/storage.hpp"
using memgraph::replication_coordination_glue::ReplicationRole;
DECLARE_int32(min_log_level);

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  spdlog::set_level(spdlog::level::err);
  std::unique_ptr<memgraph::storage::Storage> db(new memgraph::storage::InMemoryStorage());
  auto storage_dba = db->Access(ReplicationRole::MAIN);
  memgraph::query::DbAccessor dba(storage_dba.get());
  RunInteractivePlanning(&dba);
  return 0;
}
