// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
#include "dbms/database_handler.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;

namespace memgraph::dbms {

Database::Database(const storage::Config &config)
    : trigger_store_(config.durability.storage_directory / "triggers"),
      streams_{config.durability.storage_directory / "streams"},
      config_{config} {
  if (config.force_on_disk || utils::DirExists(config.disk.main_storage_directory)) {
    storage_ = std::make_unique<storage::DiskStorage>(config);
  } else {
    storage_ = std::make_unique<storage::InMemoryStorage>(config);
  }
}

}  // namespace memgraph::dbms
