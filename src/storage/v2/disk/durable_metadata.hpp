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

#include <cstdint>
#include <set>
#include <vector>

#include "kvstore/kvstore.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

class DurableMetadata {
 public:
  explicit DurableMetadata(const Config &config);

  DurableMetadata(const DurableMetadata &) = delete;
  DurableMetadata &operator=(const DurableMetadata &) = delete;
  DurableMetadata &operator=(DurableMetadata &&) = delete;

  DurableMetadata(DurableMetadata &&other) noexcept;

  ~DurableMetadata() = default;

  std::optional<uint64_t> LoadTimestampIfExists() const;
  std::optional<uint64_t> LoadVertexCountIfExists() const;
  std::optional<uint64_t> LoadEdgeCountIfExists() const;
  std::optional<std::vector<std::string>> LoadLabelIndexInfoIfExists() const;
  std::optional<std::vector<std::string>> LoadLabelPropertyIndexInfoIfExists() const;
  std::optional<std::vector<std::string>> LoadExistenceConstraintInfoIfExists() const;
  std::optional<std::vector<std::string>> LoadUniqueConstraintInfoIfExists() const;

  void SaveBeforeClosingDB(uint64_t timestamp, uint64_t vertex_count, uint64_t edge_count);

  bool PersistLabelIndexCreation(LabelId label);

  bool PersistLabelIndexDeletion(LabelId label);

  bool PersistLabelPropertyIndexAndExistenceConstraintCreation(LabelId label, PropertyId property,
                                                               const std::string &key);

  bool PersistLabelPropertyIndexAndExistenceConstraintDeletion(LabelId label, PropertyId property,
                                                               const std::string &key);

  bool PersistUniqueConstraintCreation(LabelId label, const std::set<PropertyId> &properties);

  bool PersistUniqueConstraintDeletion(LabelId label, const std::set<PropertyId> &properties);

 private:
  std::optional<uint64_t> LoadPropertyIfExists(const std::string &property) const;
  std::optional<std::vector<std::string>> LoadInfoFromAuxiliaryStorages(const std::string &property) const;

  kvstore::KVStore durability_kvstore_;
  Config config_;
};

}  // namespace memgraph::storage
