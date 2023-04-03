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
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/status.h>
#include <string>
#include <string_view>
#include "query/db_accessor.hpp"
#include "spdlog/spdlog.h"
#include "storage/rocks/loopback.hpp"
#include "storage/rocks/serialization.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage::rocks {

class RocksDBStorage {
 public:
  explicit RocksDBStorage() {
    // options_.create_if_missing = true;
    options_.OptimizeLevelStyleCompaction();
    std::filesystem::path rocksdb_path = "rocks_experiment";
    if (!memgraph::utils::EnsureDir(rocksdb_path)) {
      SPDLOG_ERROR("Unable to create storage folder on disk.");
      // TODO: throw some error
    }
    rocksdb::Status status = rocksdb::DB::Open(options_, rocksdb_path, &db_);
    MG_ASSERT(status.ok());
    InsertStartingVertices();
  }

  ~RocksDBStorage() {
    rocksdb::Status status = db_->Close();
    MG_ASSERT(status.ok());
    delete db_;
  }

  // receives just a mock of vertex data needed for serializing vertex (used as a key in the RocksDB)
  std::string SerializeVertex(const std::vector<std::string> labels, const storage::Gid &gid) {
    std::string result;
    std::string ser_Alabels =
        std::accumulate(labels.begin(), labels.end(), result,
                        [](const std::string &join, const std::string &label) { return join + ":" + label; });
    return result;
  }

  void InsertStartingVertices() {
    std::vector<std::vector<std::string>> labels{{"Person", "Player"}, {"Person", "Referee"}, {"Ball"}};
    for (int64_t i = 0; i < 10; ++i) {
      std::string key = SerializeVertex(labels[i % 3], storage::Gid::FromUint(i + 1000));
      rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, "properties_" + std::to_string(i));
      MG_ASSERT(status.ok());
    }
  }

  /*
  Read all vertices stored in the database.
  */
  std::vector<storage::Vertex> Vertices() {
    std::vector<storage::Vertex> vertices;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      uint64_t key = std::stoull(it->key().ToString());
      std::string value = it->value().ToString();
      spdlog::debug("Key: {} Value: {}", key, value);
      vertices.emplace_back(Gid::FromUint(key), nullptr);
    }
    return vertices;
  }

  /*
  Read all vertices that have a specified label.
  */
  std::vector<storage::Vertex> Vertices(const std::string_view label) {
    std::vector<storage::Vertex> vertices;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      uint64_t key = std::stoull(it->key().ToString());
      std::string value = it->value().ToString();
      spdlog::debug("Key: {} Value: {}", key, value);
      vertices.emplace_back(Gid::FromUint(key), nullptr);
    }
    return vertices;
  }

  /*
  Serialize and store in-memory vertex to the disk.
  */
  bool StoreVertex(query::VertexAccessor *vertex) {
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), std::to_string(vertex->Gid().AsUint()), "properties");
    return status.ok();
  }

 private:
  // rocksdb internals
  rocksdb::Options options_;
  rocksdb::DB *db_;
  // slk::Loopback loopback_;
  // slk::Encoder encoder_{loopback_.GetBuilder()};
  // slk::Decoder decoder_{loopback_.GetReader()};
};

}  // namespace memgraph::storage::rocks
