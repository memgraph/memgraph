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
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/result.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "storage/v2/view.hpp"
#include "utils/algorithm.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::storage::rocks {

inline void CheckRocksDBStatus(rocksdb::Status status) { MG_ASSERT(status.ok(), "rocksdb: {}", status.ToString()); }

class RocksDBStorage {
 public:
  explicit RocksDBStorage() {
    options_.create_if_missing = true;
    // options_.OptimizeLevelStyleCompaction();
    std::filesystem::path rocksdb_path = "./rocks_experiment_unit_test";
    MG_ASSERT(utils::EnsureDir(rocksdb_path), "Unable to create storage folder on the disk.");
    CheckRocksDBStatus(rocksdb::DB::Open(options_, rocksdb_path, &db_));
    CheckRocksDBStatus(db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "vertex", &vertex_chandle));
    CheckRocksDBStatus(db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "edge", &edge_chandle));
  }

  // TODO: explicitly delete other constructors

  ~RocksDBStorage() {
    CheckRocksDBStatus(db_->DropColumnFamily(vertex_chandle));
    CheckRocksDBStatus(db_->DropColumnFamily(edge_chandle));
    CheckRocksDBStatus(db_->Close());
    delete db_;
  }

  // STORING PART
  // -----------------------------------------------------------

  // Serialize and store in-memory vertex to the disk.
  // TODO: write the exact format
  // Properties are serialized as the value
  void StoreVertex(const query::VertexAccessor &vertex_acc) {
    CheckRocksDBStatus(db_->Put(rocksdb::WriteOptions(), vertex_chandle, SerializeVertex(vertex_acc),
                                SerializeProperties(vertex_acc.PropertyStore())));
  }

  // TODO: remove config being sent as the parameter. Later will be added as the part of the storage accessor as in the
  // memory version. for now assume that we always operate with edges having properties
  void StoreEdge(const query::EdgeAccessor &edge_acc) {
    auto [src_dest_key, dest_src_key] = SerializeEdge(edge_acc);
    const std::string value = SerializeProperties(edge_acc.PropertyStore());
    CheckRocksDBStatus(db_->Put(rocksdb::WriteOptions(), edge_chandle, src_dest_key, value));
    CheckRocksDBStatus(db_->Put(rocksdb::WriteOptions(), edge_chandle, dest_src_key, value));
  }

  // UPDATE PART
  // -----------------------------------------------------------

  // Clear all entries from the database.
  // TODO: check if this deletes all entries, or you also need to specify handle here
  // TODO: This will not be needed in the production code and can possibly removed in testing
  void Clear() {
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      db_->Delete(rocksdb::WriteOptions(), it->key().ToString());
    }
    delete it;
  }

  // READ PART
  // -----------------------------------------------------------

  // TODO: change it to std::optional if the value doesn't exist
  // TODO: if the need comes for using also a GID object, use std::variant
  // This should be part of edge accessor since it should be impossible to search vertex by a gid
  // This should again be changed when we have mulitple same vertices
  std::optional<query::VertexAccessor> Vertex(const std::string_view gid, query::DbAccessor &dba) {
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions(), vertex_chandle);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      const std::string_view key = it->key().ToStringView();
      if (key.starts_with(gid)) {
        return DeserializeVertex(key, it->value().ToStringView(), dba);
      }
    }
    delete it;
    return std::nullopt;
  }

  // TODO: currently search in the same database instance vertices and edges but change later to column families
  // Out edges of one vertex_acc with gid src_gid have following format in the RocksDB:
  // src_gid | other_vertex_gid | 0 | ...
  // other_vertex_gid | src_gid | 1 | ...
  // we use the firt way since this should be possible to optimize using Bloom filters and prefix search
  std::vector<query::EdgeAccessor> OutEdges(const query::VertexAccessor &vertex_acc, query::DbAccessor &dba) {
    std::vector<query::EdgeAccessor> out_edges;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions(), edge_chandle);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      const std::string_view key = it->key().ToStringView();
      const auto vertex_parts = utils::Split(key, "|");
      if (vertex_parts[0] == SerializeIdType(vertex_acc.Gid()) && vertex_parts[2] == "0") {
        out_edges.push_back(DeserializeEdge(key, it->value().ToStringView(), dba));
      }
    }
    delete it;
    return out_edges;
  }

  // InEdges of one vertex_acc with GID "dest_gid" have following format in the RocksDB:
  // other_vertex_gid | dest_gid | 0 | ...
  // dest_gid | other_verte_gid | 1 | ...
  // we use the second way since this should be possible to optimize using Bloom filters and prefix search.
  std::vector<query::EdgeAccessor> InEdges(const query::VertexAccessor &vertex_acc, query::DbAccessor &dba) {
    std::vector<query::EdgeAccessor> in_edges;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions(), edge_chandle);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      const std::string_view key = it->key().ToStringView();
      const auto vertex_parts = utils::Split(key, "|");
      if (vertex_parts[0] == SerializeIdType(vertex_acc.Gid()) && vertex_parts[2] == "1") {
        in_edges.push_back(DeserializeEdge(key, it->value().ToStringView(), dba));
      }
    }
    delete it;
    return in_edges;
  }

  // Read all vertices stored in the database by a label
  // TODO: rewrite the code with some lambda operations
  // certainly can be a bit optimized so that new object isn't created if can be discarded
  std::vector<query::VertexAccessor> Vertices(query::DbAccessor &dba, const storage::LabelId &label_id) {
    std::vector<query::VertexAccessor> vertices;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions(), vertex_chandle);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      auto vertex = DeserializeVertex(it->key().ToStringView(), it->value().ToStringView(), dba);
      if (const auto res = vertex.HasLabel(storage::View::OLD, label_id); !res.HasError() && *res) {
        vertices.push_back(vertex);
      }
    }
    delete it;
    return vertices;
  }

  // TODO: rewrite the code with means of lambda operation
  // TODO: we need to this, otherwise we will have to change a lot of things as we are dealing on the low level
  // certainly can be a bit optimized so that new object isn't created if can be discarded
  std::vector<query::VertexAccessor> Vertices(query::DbAccessor &dba, const storage::PropertyId &property_id,
                                              const storage::PropertyValue &prop_value) {
    std::vector<query::VertexAccessor> vertices;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions(), vertex_chandle);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      auto vertex = DeserializeVertex(it->key().ToStringView(), it->value().ToStringView(), dba);
      if (const auto res = vertex.GetProperty(storage::View::OLD, property_id); !res.HasError() && *res == prop_value) {
        vertices.push_back(vertex);
      }
    }
    delete it;
    return vertices;
  }

  // Read all vertices stored in the database.
  std::vector<query::VertexAccessor> Vertices(query::DbAccessor &dba) {
    std::vector<query::VertexAccessor> vertices;
    rocksdb::Iterator *it = db_->NewIterator(rocksdb::ReadOptions(), vertex_chandle);
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      vertices.push_back(DeserializeVertex(it->key().ToStringView(), it->value().ToStringView(), dba));
    }
    delete it;
    return vertices;
  }

 protected:
  inline std::string SerializeProperties(const auto &&properties) { return properties; }

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
    std::string result = SerializeLabels(vertex_acc.Labels(storage::View::OLD)) + "|";
    result += SerializeIdType(vertex_acc.Gid());
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
    src_dest_key += edge_gid;
    // destination->source key
    std::string dest_src_key = to_gid + "|";
    dest_src_key += from_gid + "|";
    dest_src_key += "1|";
    dest_src_key += edge_type + "|";
    dest_src_key += edge_gid;
    return {src_dest_key, dest_src_key};
  }

  // deserialize edge from the given key-value
  query::EdgeAccessor DeserializeEdge(const std::string_view key, const std::string_view value,
                                      query::DbAccessor &dba) {
    const auto edge_parts = utils::Split(key, "|");
    auto [from_gid, to_gid] = std::invoke(
        [&](const auto &edge_parts) {
          if (edge_parts[2] == "0") {  // out edge
            return std::make_pair(edge_parts[0], edge_parts[1]);
          }
          // in edge
          return std::make_pair(edge_parts[1], edge_parts[0]);
        },
        edge_parts);
    // load vertex accessors
    auto from_acc = Vertex(from_gid, dba);
    auto to_acc = Vertex(to_gid, dba);
    if (!from_acc.has_value() || !to_acc.has_value()) {
      throw utils::BasicException("Non-existing vertices during edge deserialization");
    }
    // TODO: remove to deserialization edge type id method
    const auto edge_type_id = storage::EdgeTypeId::FromUint(std::stoull(edge_parts[3]));
    // TODO: remove to deserialization edge type id method
    const auto edge_gid = storage::Gid::FromUint(std::stoull(edge_parts[4]));
    const auto maybe_edge = dba.InsertEdge(&*from_acc, &*to_acc, edge_type_id);
    MG_ASSERT(maybe_edge.HasValue());
    auto edge_impl = query::EdgeAccessor(*maybe_edge);
    // in the new storage API, setting gid must be done atomically
    edge_impl.SetGid(edge_gid);
    edge_impl.SetPropertyStore(value);
    return edge_impl;
  }

 private:
  rocksdb::Options options_;
  rocksdb::DB *db_;
  rocksdb::ColumnFamilyHandle *vertex_chandle = nullptr;
  rocksdb::ColumnFamilyHandle *edge_chandle = nullptr;
};

}  // namespace memgraph::storage::rocks
