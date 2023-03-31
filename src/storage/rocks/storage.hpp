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

#pragma once

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include "storage/rocks/loopback.hpp"
#include "storage/rocks/serialization.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex_accessor.hpp"

namespace memgraph::storage::rocks {

class RocksDBStorage {
 public:
  explicit RocksDBStorage() {
    options_.create_if_missing = true;
    options_.OptimizeLevelStyleCompaction();
    rocksdb::Status status = rocksdb::DB::Open(options_, "~/rocksdb/", &db_);
    MG_ASSERT(status.ok());
  }

  /*
  Reads all vertices stored in the database.
  */
  storage::Vertex Vertices() { return decoder_.ReadVertex(); }

  /*
  Serialize and store in-memory vertex to the disk.
  */
  void StoreVertex(const query::VertexAccessor &vertex) { encoder_.WriteVertex(vertex); }

 private:
  // rocksdb internals
  rocksdb::Options options_;
  rocksdb::DB *db_;
  slk::Loopback loopback_;
  slk::Encoder encoder_{loopback_.GetBuilder()};
  slk::Decoder decoder_{loopback_.GetReader()};
};

}  // namespace memgraph::storage::rocks
