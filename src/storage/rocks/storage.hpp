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
#include <numeric>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include "query/common.hpp"
#include "query/db_accessor.hpp"
#include "spdlog/spdlog.h"
#include "storage/rocks/loopback.hpp"
#include "storage/rocks/serialization.hpp"
#include "storage/v2/delta.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::storage::rocks {

class RocksDBStorage {
 public:
  explicit RocksDBStorage() {
    options_.create_if_missing = true;
    options_.OptimizeLevelStyleCompaction();
    std::filesystem::path rocksdb_path = "./rocks_experiment_test";
    if (!utils::EnsureDir(rocksdb_path)) {
      SPDLOG_ERROR("Unable to create storage folder on disk.");
      // TODO: throw some error
    }
    rocksdb::Status status = rocksdb::DB::Open(options_, rocksdb_path, &db_);
    if (!status.ok()) {
      spdlog::error(status.ToString());
    }
  }

  ~RocksDBStorage() {
    rocksdb::Status status = db_->Close();
    MG_ASSERT(status.ok());
    delete db_;
  }

  // STORING PART
  // -----------------------------------------------------------

  // Serialize and store in-memory vertex to the disk.
  // TODO: write the exact format
  // Properties are serialized as the value
  bool StoreVertex(const query::VertexAccessor &vertex_acc) {
    auto status =
        db_->Put(rocksdb::WriteOptions(), SerializeVertex(vertex_acc), SerializeProperties(vertex_acc.PropertyStore()));
    // TODO: we need a better status check
    return status.ok();
  }

  // TODO: remove config being sent as the parameter. Later will be added as the part of the storage accessor as in the
  // memory version. for now assume that we always operate with edges having properties
  bool StoreEdge(const query::EdgeAccessor &edge_acc) {
    // This can be improved with IIFE initialization lambda concept
    std::string src_dest_key;
    std::string dest_src_key;
    std::tie(src_dest_key, dest_src_key) = SerializeEdge(edge_acc);
    const std::string value = SerializeProperties(edge_acc.PropertyStore());
    // const std::string value;
    auto src_dest_status = db_->Put(rocksdb::WriteOptions(), src_dest_key, value);
    auto dest_src_status = db_->Put(rocksdb::WriteOptions(), dest_src_key, value);
    // TODO: improve a status check
    return src_dest_status.ok() && dest_src_status.ok();
  }

  // UPDATE PART
  // -----------------------------------------------------------

  // Clear all entries from the database.
  void Clear() {
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      db_->Delete(rocksdb::WriteOptions(), it->key().ToString());
    }
  }

  // READ PART
  // -----------------------------------------------------------

  // TODO: change it to std::optional if the value doesn't exist
  // TODO: if the need comes for using also a GID object, use std::variant
  query::VertexAccessor Vertex(const std::string_view gid, query::DbAccessor &dba) {
    std::string value;
    auto status = db_->Get(rocksdb::ReadOptions(), gid, &value);
    MG_ASSERT(status.ok());
    return DeserializeVertex(gid, value, dba);
  }

  // Read all vertices stored in the database.
  // TODO: remove the semantics of unique pointers
  std::vector<query::VertexAccessor> Vertices(query::DbAccessor &dba) {
    std::vector<query::VertexAccessor> vertices;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      std::string key = it->key().ToString();
      std::string value = it->value().ToString();
      spdlog::debug("Key: {} Value: {}", key, value);
      vertices.push_back(DeserializeVertex(key, value, dba));
    }
    return vertices;
  }

  // This method is used only for testing. Receives explicit key from which the edge accessor should be loaded
  query::EdgeAccessor LoadEdge(const std::string_view key, query::DbAccessor &dba) {
    std::string value;
    auto status = db_->Get(rocksdb::ReadOptions(), key, &value);
    MG_ASSERT(status.ok());
    return DeserializeEdge(key, value, dba);
  }

 protected:
  std::string SerializeProperties(const auto &&properties) {
    if (properties.HasError()) {
      return "";
    }
    return *properties;
  }

  std::string SerializeLabels(const auto &&labels) {
    if (labels.HasError() || (*labels).empty()) {
      return "";
    }
    std::string result = std::to_string((*labels)[0].AsUint());
    std::string ser_labels = std::accumulate(
        std::next((*labels).begin()), (*labels).end(), result,
        [](const std::string &join, const auto &label_id) { return join + "," + std::to_string(label_id.AsUint()); });
    return ser_labels;
  }

  // TODO: write a documentation for the method
  // TODO: add deserialization equivalent method
  inline std::string SerializeIdType(const auto &id) { return std::to_string(id.AsUint()); }

  std::string SerializeVertex(const query::VertexAccessor &vertex_acc) {
    // don't put before serialize labels delimiter
    std::string result = SerializeLabels(vertex_acc.Labels(storage::View::OLD)) + "|";
    result += SerializeIdType(vertex_acc.Gid()) + "|";
    return result;
  }

  query::VertexAccessor DeserializeVertex(const std::string_view key, const std::string_view value,
                                          query::DbAccessor &dba) {
    // Create vertex
    auto impl = query::VertexAccessor(dba.InsertVertex());
    spdlog::info("Key to deserialize: {}", key);
    const auto vertex_parts = utils::Split(key, "|");
    // Deserialize labels
    if (!vertex_parts[0].empty()) {
      const auto labels = utils::Split(vertex_parts[0], ",");
      for (const auto &label : labels) {
        const storage::LabelId label_id = storage::LabelId::FromUint(std::stoull(label));
        auto maybe_error = impl.AddLabel(label_id);
        if (maybe_error.HasError()) {
          switch (maybe_error.GetError()) {
            case storage::Error::SERIALIZATION_ERROR:
              throw utils::BasicException("Serialization");
            case storage::Error::DELETED_OBJECT:
              throw utils::BasicException("Trying to set a label on a deleted node.");
            case storage::Error::VERTEX_HAS_EDGES:
            case storage::Error::PROPERTIES_DISABLED:
            case storage::Error::NONEXISTENT_OBJECT:
              throw utils::BasicException("Unexpected error when setting a label.");
          }
        }
      }
    }
    // deserialize gid
    impl.SetGid(storage::Gid::FromUint(std::stoull(vertex_parts[1])));
    // deserialize properties
    impl.SetPropertyStore(value);
    return impl;
  }

  // Serializes edge accessor to obtain a key for the key-value store
  // returns two string because there will be two keys since edge is stored in both directions
  std::pair<std::string, std::string> SerializeEdge(const query::EdgeAccessor &edge_acc) {
    // Serialized objects
    auto from_gid = SerializeIdType(edge_acc.From().Gid());
    auto to_gid = SerializeIdType(edge_acc.To().Gid());
    auto edge_type = SerializeIdType(edge_acc.EdgeType());
    auto edge_gid = SerializeIdType(edge_acc.Gid());
    // source->destination key
    std::string src_dest_key = from_gid + "|";
    src_dest_key += to_gid + "|";
    src_dest_key += "0|";
    src_dest_key += edge_type + "|";
    src_dest_key += edge_gid + "|";
    // destination->source key
    std::string dest_src_key = to_gid + "|";
    dest_src_key += from_gid + "|";
    dest_src_key += "1|";
    dest_src_key += edge_type + "|";
    dest_src_key += edge_gid + "|";
    return {src_dest_key, dest_src_key};
  }

  // deserialize edge from the given key-value
  query::EdgeAccessor DeserializeEdge(const std::string_view key, const std::string_view value,
                                      query::DbAccessor &dba) {
    // first you need to deserialize vertices
    const auto edge_parts = utils::Split(key, "|");
    std::string from_gid;
    std::string to_gid;
    // TODO: Use IIFE to load
    if (edge_parts[2] == "0") {  // out edge
      from_gid = edge_parts[0];
      to_gid = edge_parts[1];
    } else {  // in edge
      from_gid = edge_parts[1];
      to_gid = edge_parts[0];
    }
    // load vertex accessors
    auto from_acc = Vertex(from_gid, dba);
    auto to_acc = Vertex(to_gid, dba);
    // TODO: remove to deserialization edge type id method
    const auto edge_type_id = storage::EdgeTypeId::FromUint(std::stoull(edge_parts[3]));
    // TODO: remove to deserialization edge type id method
    const auto edge_gid = storage::Gid::FromUint(std::stoull(edge_parts[4]));
    const auto maybe_edge = dba.InsertEdge(&from_acc, &to_acc, edge_type_id);
    MG_ASSERT(maybe_edge.HasValue());
    auto edge_impl = query::EdgeAccessor(*maybe_edge);
    // in the new storage API, setting gid must be done atomically
    edge_impl.SetGid(edge_gid);
    return edge_impl;
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
