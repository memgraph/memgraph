// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v3/shard.hpp"

#include <algorithm>
#include <cstdint>
#include <iterator>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>

#include <bits/ranges_algo.h>
#include <gflags/gflags.h>
#include <spdlog/spdlog.h>

#include "io/network/endpoint.hpp"
#include "io/time.hpp"
#include "storage/v3/constraints.hpp"
#include "storage/v3/durability/durability.hpp"
#include "storage/v3/durability/metadata.hpp"
#include "storage/v3/durability/paths.hpp"
#include "storage/v3/durability/snapshot.hpp"
#include "storage/v3/durability/wal.hpp"
#include "storage/v3/edge_accessor.hpp"
#include "storage/v3/id_types.hpp"
#include "storage/v3/indices.hpp"
#include "storage/v3/key_store.hpp"
#include "storage/v3/mvcc.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schema_validator.hpp"
#include "storage/v3/transaction.hpp"
#include "storage/v3/vertex.hpp"
#include "storage/v3/vertex_accessor.hpp"
#include "storage/v3/vertices_skip_list.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/memory_tracker.hpp"
#include "utils/message.hpp"
#include "utils/result.hpp"
#include "utils/rw_lock.hpp"
#include "utils/skip_list.hpp"
#include "utils/spin_lock.hpp"
#include "utils/stat.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage::v3 {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

namespace {

uint64_t GetCleanupBeforeTimestamp(const std::map<uint64_t, std::unique_ptr<Transaction>> &transactions,
                                   const io::Time clean_up_before) {
  MG_ASSERT(!transactions.empty(), "There are no transactions!");
  const auto it = std::lower_bound(
      transactions.begin(), transactions.end(), clean_up_before,
      [](const std::pair<const uint64_t, std::unique_ptr<Transaction>> &trans, const io::Time clean_up_before) {
        return trans.second->start_timestamp.coordinator_wall_clock < clean_up_before;
      });
  if (it == transactions.end()) {
    // all of the transaction are old enough to be cleaned up, return a timestamp that is higher than all of them
    return transactions.rbegin()->first + 1;
  }
  return it->first;
}

}  // namespace

auto AdvanceToVisibleVertex(VerticesSkipList::Iterator it, VerticesSkipList::Iterator end,
                            std::optional<VertexAccessor> *vertex, Transaction *tx, View view, Indices *indices,
                            Constraints *constraints, Config::Items config, const VertexValidator &vertex_validator) {
  while (it != end) {
    *vertex = VertexAccessor::Create(&it->vertex, tx, indices, constraints, config, vertex_validator, view);
    if (!*vertex) {
      ++it;
      continue;
    }
    break;
  }
  return it;
}

AllVerticesIterable::Iterator::Iterator(AllVerticesIterable *self, VerticesSkipList::Iterator it)
    : self_(self),
      it_(AdvanceToVisibleVertex(it, self->vertices_accessor_.end(), &self->vertex_, self->transaction_, self->view_,
                                 self->indices_, self_->constraints_, self->config_, *self_->vertex_validator_)) {}

VertexAccessor AllVerticesIterable::Iterator::operator*() const { return *self_->vertex_; }

AllVerticesIterable::Iterator &AllVerticesIterable::Iterator::operator++() {
  ++it_;
  it_ = AdvanceToVisibleVertex(it_, self_->vertices_accessor_.end(), &self_->vertex_, self_->transaction_, self_->view_,
                               self_->indices_, self_->constraints_, self_->config_, *self_->vertex_validator_);
  return *this;
}

VerticesIterable::VerticesIterable(AllVerticesIterable vertices) : type_(Type::ALL) {
  new (&all_vertices_) AllVerticesIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(LabelIndex::Iterable vertices) : type_(Type::BY_LABEL) {
  new (&vertices_by_label_) LabelIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(LabelPropertyIndex::Iterable vertices) : type_(Type::BY_LABEL_PROPERTY) {
  new (&vertices_by_label_property_) LabelPropertyIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(VerticesIterable &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_vertices_) AllVerticesIterable(std::move(other.all_vertices_));
      break;
    case Type::BY_LABEL:
      new (&vertices_by_label_) LabelIndex::Iterable(std::move(other.vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&vertices_by_label_property_) LabelPropertyIndex::Iterable(std::move(other.vertices_by_label_property_));
      break;
  }
}

VerticesIterable &VerticesIterable::operator=(VerticesIterable &&other) noexcept {
  switch (type_) {
    case Type::ALL:
      all_vertices_.AllVerticesIterable::~AllVerticesIterable();
      break;
    case Type::BY_LABEL:
      vertices_by_label_.LabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY:
      vertices_by_label_property_.LabelPropertyIndex::Iterable::~Iterable();
      break;
  }
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_vertices_) AllVerticesIterable(std::move(other.all_vertices_));
      break;
    case Type::BY_LABEL:
      new (&vertices_by_label_) LabelIndex::Iterable(std::move(other.vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&vertices_by_label_property_) LabelPropertyIndex::Iterable(std::move(other.vertices_by_label_property_));
      break;
  }
  return *this;
}

VerticesIterable::~VerticesIterable() {
  switch (type_) {
    case Type::ALL:
      all_vertices_.AllVerticesIterable::~AllVerticesIterable();
      break;
    case Type::BY_LABEL:
      vertices_by_label_.LabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY:
      vertices_by_label_property_.LabelPropertyIndex::Iterable::~Iterable();
      break;
  }
}

VerticesIterable::Iterator VerticesIterable::begin() {
  switch (type_) {
    case Type::ALL:
      return Iterator(all_vertices_.begin());
    case Type::BY_LABEL:
      return Iterator(vertices_by_label_.begin());
    case Type::BY_LABEL_PROPERTY:
      return Iterator(vertices_by_label_property_.begin());
  }
}

VerticesIterable::Iterator VerticesIterable::end() {
  switch (type_) {
    case Type::ALL:
      return Iterator(all_vertices_.end());
    case Type::BY_LABEL:
      return Iterator(vertices_by_label_.end());
    case Type::BY_LABEL_PROPERTY:
      return Iterator(vertices_by_label_property_.end());
  }
}

VerticesIterable::Iterator::Iterator(AllVerticesIterable::Iterator it) : type_(Type::ALL) {
  new (&all_it_) AllVerticesIterable::Iterator(it);
}

VerticesIterable::Iterator::Iterator(LabelIndex::Iterable::Iterator it) : type_(Type::BY_LABEL) {
  new (&by_label_it_) LabelIndex::Iterable::Iterator(it);
}

VerticesIterable::Iterator::Iterator(LabelPropertyIndex::Iterable::Iterator it) : type_(Type::BY_LABEL_PROPERTY) {
  new (&by_label_property_it_) LabelPropertyIndex::Iterable::Iterator(it);
}

VerticesIterable::Iterator::Iterator(const VerticesIterable::Iterator &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL:
      new (&by_label_it_) LabelIndex::Iterable::Iterator(other.by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&by_label_property_it_) LabelPropertyIndex::Iterable::Iterator(other.by_label_property_it_);
      break;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator=(const VerticesIterable::Iterator &other) {
  if (this == &other) {
    return *this;
  }
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL:
      new (&by_label_it_) LabelIndex::Iterable::Iterator(other.by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&by_label_property_it_) LabelPropertyIndex::Iterable::Iterator(other.by_label_property_it_);
      break;
  }
  return *this;
}

VerticesIterable::Iterator::Iterator(VerticesIterable::Iterator &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL:
      new (&by_label_it_) LabelIndex::Iterable::Iterator(other.by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&by_label_property_it_) LabelPropertyIndex::Iterable::Iterator(other.by_label_property_it_);
      break;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator=(VerticesIterable::Iterator &&other) noexcept {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL:
      new (&by_label_it_) LabelIndex::Iterable::Iterator(other.by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY:
      new (&by_label_property_it_) LabelPropertyIndex::Iterable::Iterator(other.by_label_property_it_);
      break;
  }
  return *this;
}

VerticesIterable::Iterator::~Iterator() { Destroy(); }

void VerticesIterable::Iterator::Destroy() noexcept {
  switch (type_) {
    case Type::ALL:
      all_it_.AllVerticesIterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL:
      by_label_it_.LabelIndex::Iterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_PROPERTY:
      by_label_property_it_.LabelPropertyIndex::Iterable::Iterator::~Iterator();
      break;
  }
}

VertexAccessor VerticesIterable::Iterator::operator*() const {
  switch (type_) {
    case Type::ALL:
      return *all_it_;
    case Type::BY_LABEL:
      return *by_label_it_;
    case Type::BY_LABEL_PROPERTY:
      return *by_label_property_it_;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator++() {
  switch (type_) {
    case Type::ALL:
      ++all_it_;
      break;
    case Type::BY_LABEL:
      ++by_label_it_;
      break;
    case Type::BY_LABEL_PROPERTY:
      ++by_label_property_it_;
      break;
  }
  return *this;
}

bool VerticesIterable::Iterator::operator==(const Iterator &other) const {
  switch (type_) {
    case Type::ALL:
      return all_it_ == other.all_it_;
    case Type::BY_LABEL:
      return by_label_it_ == other.by_label_it_;
    case Type::BY_LABEL_PROPERTY:
      return by_label_property_it_ == other.by_label_property_it_;
  }
}

Shard::Shard(const LabelId primary_label, const PrimaryKey min_primary_key,
             const std::optional<PrimaryKey> max_primary_key, Config config)
    : primary_label_{primary_label},
      min_primary_key_{min_primary_key},
      max_primary_key_{max_primary_key},
      schema_validator_{schemas_},
      vertex_validator_{schema_validator_, primary_label},
      indices_{&constraints_, config.items, vertex_validator_},
      isolation_level_{config.transaction.isolation_level},
      config_{config},
      snapshot_directory_{config_.durability.storage_directory / durability::kSnapshotDirectory},
      wal_directory_{config_.durability.storage_directory / durability::kWalDirectory},
      lock_file_path_{config_.durability.storage_directory / durability::kLockFile},
      uuid_{utils::GenerateUUID()},
      epoch_id_{utils::GenerateUUID()},
      global_locker_{file_retainer_.AddLocker()} {
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
      config_.durability.snapshot_on_exit || config_.durability.recover_on_startup) {
    // Create the directory initially to crash the database in case of
    // permission errors. This is done early to crash the database on startup
    // instead of crashing the database for the first time during runtime (which
    // could be an unpleasant surprise).
    utils::EnsureDirOrDie(snapshot_directory_);
    // Same reasoning as above.
    utils::EnsureDirOrDie(wal_directory_);

    // Verify that the user that started the process is the same user that is
    // the owner of the storage directory.
    durability::VerifyStorageDirectoryOwnerAndProcessUserOrDie(config_.durability.storage_directory);

    // Create the lock file and open a handle to it. This will crash the
    // database if it can't open the file for writing or if any other process is
    // holding the file opened.
    lock_file_handle_.Open(lock_file_path_, utils::OutputFile::Mode::OVERWRITE_EXISTING);
    MG_ASSERT(lock_file_handle_.AcquireLock(),
              "Couldn't acquire lock on the storage directory {}!\n"
              "Another Memgraph process is currently running with the same "
              "storage directory, please stop it first before starting this "
              "process!",
              config_.durability.storage_directory);
  }
  if (config_.durability.recover_on_startup) {
    // auto info = std::optional<durability::RecoveryInfo>{};

    // durability::RecoverData(snapshot_directory_, wal_directory_, &uuid_, &epoch_id_, &epoch_history_, &vertices_,
    //                         &edges_, &edge_count_, &name_id_mapper_, &indices_, &constraints_, config_.items,
    //                         &wal_seq_num_);
    // if (info) {
    //   timestamp_ = std::max(timestamp_, info->next_timestamp);
    //   if (info->last_commit_timestamp) {
    //     last_commit_timestamp_ = *info->last_commit_timestamp;
    //   }
    // }
  } else if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED ||
             config_.durability.snapshot_on_exit) {
    bool files_moved = false;
    auto backup_root = config_.durability.storage_directory / durability::kBackupDirectory;
    for (const auto &[path, dirname, what] :
         {std::make_tuple(snapshot_directory_, durability::kSnapshotDirectory, "snapshot"),
          std::make_tuple(wal_directory_, durability::kWalDirectory, "WAL")}) {
      if (!utils::DirExists(path)) continue;
      auto backup_curr = backup_root / dirname;
      std::error_code error_code;
      for (const auto &item : std::filesystem::directory_iterator(path, error_code)) {
        utils::EnsureDirOrDie(backup_root);
        utils::EnsureDirOrDie(backup_curr);
        std::error_code item_error_code;
        std::filesystem::rename(item.path(), backup_curr / item.path().filename(), item_error_code);
        MG_ASSERT(!item_error_code, "Couldn't move {} file {} because of: {}", what, item.path(),
                  item_error_code.message());
        files_moved = true;
      }
      MG_ASSERT(!error_code, "Couldn't backup {} files because of: {}", what, error_code.message());
    }
    if (files_moved) {
      spdlog::warn(
          "Since Memgraph was not supposed to recover on startup and "
          "durability is enabled, your current durability files will likely "
          "be overridden. To prevent important data loss, Memgraph has stored "
          "those files into a .backup directory inside the storage directory.");
    }
  }
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
    // TODO(antaljanosbenjamin): handle snapshots
    // snapshot_runner_.Run("Snapshot", config_.durability.snapshot_interval, [this] {
    //   if (auto maybe_error = this->CreateSnapshot(); maybe_error.HasError()) {
    //     switch (maybe_error.GetError()) {
    //       case CreateSnapshotError::DisabledForReplica:
    //         spdlog::warn(
    //             utils::MessageWithLink("Snapshots are disabled for replicas.", "https://memgr.ph/replication"));
    //         break;
    //     }
    //   }
    // });
  }
}

Shard::~Shard() {
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_ = std::nullopt;
  }
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
    // TODO(antaljanosbenjamin): stop snapshot creation
  }
  if (config_.durability.snapshot_on_exit) {
    MG_ASSERT(CreateSnapshot(), "Cannot create snapshot!");
  }
}

Shard::Accessor::Accessor(Shard &shard, Transaction &transaction)
    : shard_(&shard), transaction_(&transaction), config_(shard_->config_.items) {}

ResultSchema<VertexAccessor> Shard::Accessor::CreateVertexAndValidate(
    LabelId primary_label, const std::vector<LabelId> &labels,
    const std::vector<std::pair<PropertyId, PropertyValue>> &properties) {
  if (primary_label != shard_->primary_label_) {
    throw utils::BasicException("Cannot add vertex to shard which does not hold the given primary label!");
  }
  auto maybe_schema_violation = GetSchemaValidator().ValidateVertexCreate(primary_label, labels, properties);
  if (maybe_schema_violation) {
    return {std::move(*maybe_schema_violation)};
  }
  OOMExceptionEnabler oom_exception;
  // Extract key properties
  std::vector<PropertyValue> primary_properties;
  for ([[maybe_unused]] const auto &[property_id, property_type] : shard_->GetSchema(primary_label)->second) {
    // We know there definitely is key in properties since we have validated
    primary_properties.push_back(
        std::ranges::find_if(properties, [property_id = property_id](const auto &property_pair) {
          return property_pair.first == property_id;
        })->second);
  }
  auto acc = shard_->vertices_.access();
  auto *delta = CreateDeleteObjectDelta(transaction_);
  auto [it, inserted] = acc.insert({Vertex{delta, primary_properties}});

  VertexAccessor vertex_acc{&it->vertex,           transaction_, &shard_->indices_,
                            &shard_->constraints_, config_,      shard_->vertex_validator_};
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");

  // TODO(jbajic) Improve, maybe delay index update
  for (const auto &[property_id, property_value] : properties) {
    if (!shard_->schemas_.IsPropertyKey(primary_label, property_id)) {
      if (const auto err = vertex_acc.SetProperty(property_id, property_value); err.HasError()) {
        return {err.GetError()};
      }
    }
  }
  // Set secondary labels
  for (auto label : labels) {
    if (const auto err = vertex_acc.AddLabel(label); err.HasError()) {
      return {err.GetError()};
    }
  }
  delta->prev.Set(&it->vertex);
  return vertex_acc;
}

std::optional<VertexAccessor> Shard::Accessor::FindVertex(std::vector<PropertyValue> primary_key, View view) {
  auto acc = shard_->vertices_.access();
  // Later on use label space
  auto it = acc.find(primary_key);
  if (it == acc.end()) {
    return std::nullopt;
  }
  return VertexAccessor::Create(&it->vertex, transaction_, &shard_->indices_, &shard_->constraints_, config_,
                                shard_->vertex_validator_, view);
}

Result<std::optional<VertexAccessor>> Shard::Accessor::DeleteVertex(VertexAccessor *vertex) {
  MG_ASSERT(vertex->transaction_ == transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = vertex->vertex_;

  if (!PrepareForWrite(transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

  if (vertex_ptr->deleted) {
    return std::optional<VertexAccessor>{};
  }

  if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty()) return Error::VERTEX_HAS_EDGES;

  CreateAndLinkDelta(transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return std::make_optional<VertexAccessor>(vertex_ptr, transaction_, &shard_->indices_, &shard_->constraints_, config_,
                                            shard_->vertex_validator_, true);
}

Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> Shard::Accessor::DetachDeleteVertex(
    VertexAccessor *vertex) {
  using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;

  MG_ASSERT(vertex->transaction_ == transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = vertex->vertex_;

  std::vector<Vertex::EdgeLink> in_edges;
  std::vector<Vertex::EdgeLink> out_edges;

  {
    if (!PrepareForWrite(transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

    if (vertex_ptr->deleted) return std::optional<ReturnType>{};

    in_edges = vertex_ptr->in_edges;
    out_edges = vertex_ptr->out_edges;
  }

  std::vector<EdgeAccessor> deleted_edges;
  const VertexId vertex_id{shard_->primary_label_, vertex_ptr->keys.Keys()};
  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, from_vertex, vertex_id, transaction_, &shard_->indices_, &shard_->constraints_,
                   config_);
    auto ret = DeleteEdge(e.FromVertex(), e.ToVertex(), e.Gid());
    if (ret.HasError()) {
      MG_ASSERT(ret.GetError() == Error::SERIALIZATION_ERROR, "Invalid database state!");
      return ret.GetError();
    }

    if (ret.GetValue()) {
      deleted_edges.push_back(*ret.GetValue());
    }
  }
  for (const auto &item : out_edges) {
    auto [edge_type, to_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, vertex_id, to_vertex, transaction_, &shard_->indices_, &shard_->constraints_,
                   config_);
    auto ret = DeleteEdge(e.FromVertex(), e.ToVertex(), e.Gid());
    if (ret.HasError()) {
      MG_ASSERT(ret.GetError() == Error::SERIALIZATION_ERROR, "Invalid database state!");
      return ret.GetError();
    }

    if (ret.GetValue()) {
      deleted_edges.push_back(*ret.GetValue());
    }
  }

  // We need to check again for serialization errors because we unlocked the
  // vertex. Some other transaction could have modified the vertex in the
  // meantime if we didn't have any edges to delete.

  if (!PrepareForWrite(transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

  MG_ASSERT(!vertex_ptr->deleted, "Invalid database state!");

  CreateAndLinkDelta(transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return std::make_optional<ReturnType>(VertexAccessor{vertex_ptr, transaction_, &shard_->indices_,
                                                       &shard_->constraints_, config_, shard_->vertex_validator_, true},
                                        std::move(deleted_edges));
}

Result<EdgeAccessor> Shard::Accessor::CreateEdge(VertexId from_vertex_id, VertexId to_vertex_id,
                                                 const EdgeTypeId edge_type, const Gid gid) {
  OOMExceptionEnabler oom_exception;
  Vertex *from_vertex{nullptr};
  Vertex *to_vertex{nullptr};

  auto acc = shard_->vertices_.access();

  const auto from_is_local = shard_->IsVertexBelongToShard(from_vertex_id);
  const auto to_is_local = shard_->IsVertexBelongToShard(to_vertex_id);
  MG_ASSERT(from_is_local || to_is_local, "Trying to create an edge without having a local vertex");

  if (from_is_local) {
    auto it = acc.find(from_vertex_id.primary_key);
    MG_ASSERT(it != acc.end(), "Cannot find local vertex");
    from_vertex = &it->vertex;
  }

  if (to_is_local) {
    auto it = acc.find(to_vertex_id.primary_key);
    MG_ASSERT(it != acc.end(), "Cannot find local vertex");
    to_vertex = &it->vertex;
  }

  if (from_is_local) {
    if (!PrepareForWrite(transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
    if (from_vertex->deleted) return Error::DELETED_OBJECT;
  }
  if (to_is_local && to_vertex != from_vertex) {
    if (!PrepareForWrite(transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = shard_->edges_.access();
    auto *delta = CreateDeleteObjectDelta(transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    delta->prev.Set(&*it);
  }

  if (from_is_local) {
    CreateAndLinkDelta(transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex_id, edge);
    from_vertex->out_edges.emplace_back(edge_type, to_vertex_id, edge);
  }
  if (to_is_local) {
    CreateAndLinkDelta(transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex_id, edge);
    to_vertex->in_edges.emplace_back(edge_type, from_vertex_id, edge);
  }
  // Increment edge count.
  ++shard_->edge_count_;

  return EdgeAccessor(edge, edge_type, std::move(from_vertex_id), std::move(to_vertex_id), transaction_,
                      &shard_->indices_, &shard_->constraints_, config_);
}

Result<std::optional<EdgeAccessor>> Shard::Accessor::DeleteEdge(VertexId from_vertex_id, VertexId to_vertex_id,
                                                                const Gid edge_id) {
  Vertex *from_vertex{nullptr};
  Vertex *to_vertex{nullptr};

  auto acc = shard_->vertices_.access();

  const auto from_is_local = shard_->IsVertexBelongToShard(from_vertex_id);
  const auto to_is_local = shard_->IsVertexBelongToShard(to_vertex_id);

  if (from_is_local) {
    auto it = acc.find(from_vertex_id.primary_key);
    MG_ASSERT(it != acc.end(), "Cannot find local vertex");
    from_vertex = &it->vertex;
  }

  if (to_is_local) {
    auto it = acc.find(to_vertex_id.primary_key);
    MG_ASSERT(it != acc.end(), "Cannot find local vertex");
    to_vertex = &it->vertex;
  }

  MG_ASSERT(from_is_local || to_is_local, "Trying to delete an edge without having a local vertex");

  if (from_is_local) {
    if (!PrepareForWrite(transaction_, from_vertex)) {
      return Error::SERIALIZATION_ERROR;
    }
    MG_ASSERT(!from_vertex->deleted, "Invalid database state!");
  }
  if (to_is_local && to_vertex != from_vertex) {
    if (!PrepareForWrite(transaction_, to_vertex)) {
      return Error::SERIALIZATION_ERROR;
    }
    MG_ASSERT(!to_vertex->deleted, "Invalid database state!");
  }

  const auto edge_ref = std::invoke([edge_id, this]() -> EdgeRef {
    if (!config_.properties_on_edges) {
      return EdgeRef(edge_id);
    }
    auto edge_acc = shard_->edges_.access();
    auto res = edge_acc.find(edge_id);
    MG_ASSERT(res != edge_acc.end(), "Cannot find edge");
    return EdgeRef(&*res);
  });

  std::optional<EdgeTypeId> edge_type{};
  auto delete_edge_from_storage = [&edge_type, &edge_ref, this](std::vector<Vertex::EdgeLink> &edges) mutable {
    auto it = std::find_if(edges.begin(), edges.end(),
                           [&edge_ref](const Vertex::EdgeLink &link) { return std::get<2>(link) == edge_ref; });
    if (config_.properties_on_edges) {
      MG_ASSERT(it != edges.end(), "Invalid database state!");
    } else if (it == edges.end()) {
      return false;
    }
    edge_type = std::get<0>(*it);
    std::swap(*it, *edges.rbegin());
    edges.pop_back();
    return true;
  };
  // NOLINTNEXTLINE(clang-analyzer-core.NonNullParamChecker)
  auto success_on_to = to_is_local ? delete_edge_from_storage(to_vertex->in_edges) : false;
  auto success_on_from = from_is_local ? delete_edge_from_storage(from_vertex->out_edges) : false;

  if (config_.properties_on_edges) {
    // Because of the check above, we are sure that the vertex exists.
    // One vertex is always local to the shard, so at least one of the operation should always succeed
    MG_ASSERT((to_is_local == success_on_to) && (from_is_local == success_on_from), "Invalid database state!");
  } else {
    // We might get here with self-edges, because without the edge object we cannot detect already deleted edges, thus
    // it is possible that both of the operation fails
    if (!success_on_to && !success_on_from) {
      // The edge is already deleted.
      return std::optional<EdgeAccessor>{};
    }

    MG_ASSERT((!to_is_local || !from_is_local) || (success_on_to == success_on_from), "Invalid database state!");
  }

  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;
    CreateAndLinkDelta(transaction_, edge_ptr, Delta::RecreateObjectTag());
    edge_ptr->deleted = true;
  }

  MG_ASSERT(edge_type.has_value(), "Edge type is not determined");

  if (from_is_local) {
    CreateAndLinkDelta(transaction_, from_vertex, Delta::AddOutEdgeTag(), *edge_type, to_vertex_id, edge_ref);
  }
  if (to_is_local) {
    CreateAndLinkDelta(transaction_, to_vertex, Delta::AddInEdgeTag(), *edge_type, from_vertex_id, edge_ref);
  }

  // Decrement edge count.
  --shard_->edge_count_;

  return std::make_optional<EdgeAccessor>(edge_ref, *edge_type, std::move(from_vertex_id), std::move(to_vertex_id),
                                          transaction_, &shard_->indices_, &shard_->constraints_, config_, true);
}

const std::string &Shard::Accessor::LabelToName(LabelId label) const { return shard_->LabelToName(label); }

const std::string &Shard::Accessor::PropertyToName(PropertyId property) const {
  return shard_->PropertyToName(property);
}

const std::string &Shard::Accessor::EdgeTypeToName(EdgeTypeId edge_type) const {
  return shard_->EdgeTypeToName(edge_type);
}

void Shard::Accessor::AdvanceCommand() { ++transaction_->command_id; }

utils::BasicResult<ConstraintViolation, void> Shard::Accessor::Commit(coordinator::Hlc commit_timestamp) {
  MG_ASSERT(!transaction_->is_aborted, "The transaction is already aborted!");
  MG_ASSERT(!transaction_->must_abort, "The transaction can't be committed!");
  MG_ASSERT(transaction_->start_timestamp.logical_id < commit_timestamp.logical_id,
            "Commit timestamp must be older than start timestamp!");
  MG_ASSERT(transaction_->commit_info != nullptr, "Invalid database state!");
  MG_ASSERT(!transaction_->commit_info->is_locally_committed, "The transaction is already committed!");
  transaction_->commit_info->timestamp = commit_timestamp;
  transaction_->commit_info->is_locally_committed = true;

  return {};
}

void Shard::Accessor::Abort() {
  MG_ASSERT(!transaction_->is_aborted, "The transaction is already aborted!");
  MG_ASSERT(!transaction_->commit_info->is_locally_committed, "The transaction is already committed!");

  for (const auto &delta : transaction_->deltas) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto *vertex = prev.vertex;
        Delta *current = vertex->delta;
        while (current != nullptr && current->commit_info->timestamp == transaction_->start_timestamp) {
          switch (current->action) {
            case Delta::Action::REMOVE_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(), current->label);
              MG_ASSERT(it != vertex->labels.end(), "Invalid database state!");
              std::swap(*it, *vertex->labels.rbegin());
              vertex->labels.pop_back();
              break;
            }
            case Delta::Action::ADD_LABEL: {
              auto it = std::find(vertex->labels.begin(), vertex->labels.end(), current->label);
              MG_ASSERT(it == vertex->labels.end(), "Invalid database state!");
              vertex->labels.push_back(current->label);
              break;
            }
            case Delta::Action::SET_PROPERTY: {
              vertex->properties.SetProperty(current->property.key, current->property.value);
              break;
            }
            case Delta::Action::ADD_IN_EDGE: {
              Vertex::EdgeLink link{current->vertex_edge.edge_type, current->vertex_edge.vertex_id,
                                    current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
              MG_ASSERT(it == vertex->in_edges.end(), "Invalid database state!");
              vertex->in_edges.push_back(link);
              break;
            }
            case Delta::Action::ADD_OUT_EDGE: {
              Vertex::EdgeLink link{current->vertex_edge.edge_type, current->vertex_edge.vertex_id,
                                    current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link);
              MG_ASSERT(it == vertex->out_edges.end(), "Invalid database state!");
              vertex->out_edges.push_back(link);
              // Increment edge count. We only increment the count here because
              // the information in `ADD_IN_EDGE` and `Edge/RECREATE_OBJECT` is
              // redundant. Also, `Edge/RECREATE_OBJECT` isn't available when
              // edge properties are disabled.
              ++shard_->edge_count_;
              break;
            }
            case Delta::Action::REMOVE_IN_EDGE: {
              Vertex::EdgeLink link{current->vertex_edge.edge_type, current->vertex_edge.vertex_id,
                                    current->vertex_edge.edge};
              auto it = std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
              MG_ASSERT(it != vertex->in_edges.end(), "Invalid database state!");
              std::swap(*it, *vertex->in_edges.rbegin());
              vertex->in_edges.pop_back();
              break;
            }
            case Delta::Action::REMOVE_OUT_EDGE: {
              Vertex::EdgeLink link{current->vertex_edge.edge_type, current->vertex_edge.vertex_id,
                                    current->vertex_edge.edge};
              auto it = std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link);
              MG_ASSERT(it != vertex->out_edges.end(), "Invalid database state!");
              std::swap(*it, *vertex->out_edges.rbegin());
              vertex->out_edges.pop_back();
              // Decrement edge count. We only decrement the count here because
              // the information in `REMOVE_IN_EDGE` and `Edge/DELETE_OBJECT` is
              // redundant. Also, `Edge/DELETE_OBJECT` isn't available when edge
              // properties are disabled.
              --shard_->edge_count_;
              break;
            }
            case Delta::Action::DELETE_OBJECT: {
              vertex->deleted = true;
              shard_->deleted_vertices_.push_back(vertex->keys.Keys());
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              vertex->deleted = false;
              break;
            }
          }
          current = current->next;
        }
        vertex->delta = current;
        if (current != nullptr) {
          current->prev.Set(vertex);
        }

        break;
      }
      case PreviousPtr::Type::EDGE: {
        auto *edge = prev.edge;
        Delta *current = edge->delta;
        while (current != nullptr && current->commit_info->timestamp == transaction_->start_timestamp) {
          switch (current->action) {
            case Delta::Action::SET_PROPERTY: {
              edge->properties.SetProperty(current->property.key, current->property.value);
              break;
            }
            case Delta::Action::DELETE_OBJECT: {
              edge->deleted = true;
              shard_->deleted_edges_.push_back(edge->gid);
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              edge->deleted = false;
              break;
            }
            case Delta::Action::REMOVE_LABEL:
            case Delta::Action::ADD_LABEL:
            case Delta::Action::ADD_IN_EDGE:
            case Delta::Action::ADD_OUT_EDGE:
            case Delta::Action::REMOVE_IN_EDGE:
            case Delta::Action::REMOVE_OUT_EDGE: {
              LOG_FATAL("Invalid database state!");
              break;
            }
          }
          current = current->next;
        }
        edge->delta = current;
        if (current != nullptr) {
          current->prev.Set(edge);
        }

        break;
      }
      case PreviousPtr::Type::DELTA:
      // pointer probably couldn't be set because allocation failed
      case PreviousPtr::Type::NULLPTR:
        break;
    }
  }

  transaction_->deltas.clear();

  transaction_->is_aborted = true;
  shard_->has_any_transaction_aborted_since_last_gc = true;
}

void Shard::Accessor::FinalizeTransaction() {
  MG_ASSERT(transaction_->is_aborted || transaction_->commit_info->is_locally_committed,
            "Cannot finalize active transaction!");
  if (transaction_->commit_info->is_locally_committed) {
  }
}

const std::string &Shard::LabelToName(LabelId label) const { return name_id_mapper_.IdToName(label.AsUint()); }

const std::string &Shard::PropertyToName(PropertyId property) const {
  return name_id_mapper_.IdToName(property.AsUint());
}

const std::string &Shard::EdgeTypeToName(EdgeTypeId edge_type) const {
  return name_id_mapper_.IdToName(edge_type.AsUint());
}

bool Shard::CreateIndex(LabelId label, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  // TODO(jbajic) response should be different when label == primary_label
  if (label == primary_label_ || !indices_.label_index.CreateIndex(label, vertices_.access())) {
    return false;
  }
  // TODO(antaljanosbenjamin): do we need to mark the transaction committed?
  // const auto commit_timestamp = CommitTimestamp();
  // AppendToWal(durability::StorageGlobalOperation::LABEL_INDEX_CREATE, label, {}, commit_timestamp);
  // last_commit_timestamp_ = commit_timestamp;
  return true;
}

bool Shard::CreateIndex(LabelId label, PropertyId property,
                        const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  // TODO(jbajic) response should be different when index conflicts with schema
  if (label == primary_label_ && schemas_.GetSchema(primary_label_)->second.size() == 1 &&
      schemas_.GetSchema(primary_label_)->second[0].property_id == property) {
    // Index already exists on primary key
    return false;
  }
  if (!indices_.label_property_index.CreateIndex(label, property, vertices_.access())) {
    return false;
  }
  // const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  // AppendToWal(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE, label, {property}, commit_timestamp);
  // commit_log_->MarkFinished(commit_timestamp);
  // last_commit_timestamp_ = commit_timestamp;
  return true;
}

bool Shard::DropIndex(LabelId label, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  if (!indices_.label_index.DropIndex(label)) return false;
  // const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  // AppendToWal(durability::StorageGlobalOperation::LABEL_INDEX_DROP, label, {}, commit_timestamp);
  // commit_log_->MarkFinished(commit_timestamp);
  // last_commit_timestamp_ = commit_timestamp;
  return true;
}

bool Shard::DropIndex(LabelId label, PropertyId property, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  if (!indices_.label_property_index.DropIndex(label, property)) return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  // const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  // AppendToWal(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP, label, {property}, commit_timestamp);
  // commit_log_->MarkFinished(commit_timestamp);
  // last_commit_timestamp_ = commit_timestamp;
  return true;
}

IndicesInfo Shard::ListAllIndices() const {
  return {indices_.label_index.ListIndices(), indices_.label_property_index.ListIndices()};
}

utils::BasicResult<ConstraintViolation, bool> Shard::CreateExistenceConstraint(
    LabelId /*label*/, PropertyId /*property*/, const std::optional<uint64_t> /*desired_commit_timestamp*/) {
  // TODO Fix constraints
  // auto ret = ::memgraph::storage::v3::CreateExistenceConstraint(&constraints_, label, property, vertices_.access());
  // if (ret.HasError() || !ret.GetValue()) return ret;
  return false;
  // const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  // AppendToWal(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE, label, {property}, commit_timestamp);
  // commit_log_->MarkFinished(commit_timestamp);
  // last_commit_timestamp_ = commit_timestamp;
}

bool Shard::DropExistenceConstraint(LabelId label, PropertyId property,
                                    const std::optional<uint64_t> desired_commit_timestamp) {
  if (!memgraph::storage::v3::DropExistenceConstraint(&constraints_, label, property)) return false;
  // const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  // AppendToWal(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP, label, {property}, commit_timestamp);
  // commit_log_->MarkFinished(commit_timestamp);
  // last_commit_timestamp_ = commit_timestamp;
  return true;
}

utils::BasicResult<ConstraintViolation, UniqueConstraints::CreationStatus> Shard::CreateUniqueConstraint(
    LabelId label, const std::set<PropertyId> &properties, const std::optional<uint64_t> desired_commit_timestamp) {
  // TODO Fix constraints
  // auto ret = constraints_.unique_constraints.CreateConstraint(label, properties, vertices_.access());
  // if (ret.HasError() || ret.GetValue() != UniqueConstraints::CreationStatus::SUCCESS) {
  //   return ret;
  // }
  return UniqueConstraints::CreationStatus::ALREADY_EXISTS;
  // const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  // AppendToWal(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE, label, properties, commit_timestamp);
  // commit_log_->MarkFinished(commit_timestamp);
  // last_commit_timestamp_ = commit_timestamp;
  // return UniqueConstraints::CreationStatus::SUCCESS;
}

UniqueConstraints::DeletionStatus Shard::DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                                              const std::optional<uint64_t> desired_commit_timestamp) {
  auto ret = constraints_.unique_constraints.DropConstraint(label, properties);
  if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    return ret;
  }
  // const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  // AppendToWal(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP, label, properties, commit_timestamp);
  // commit_log_->MarkFinished(commit_timestamp);
  // last_commit_timestamp_ = commit_timestamp;
  return UniqueConstraints::DeletionStatus::SUCCESS;
}

const SchemaValidator &Shard::Accessor::GetSchemaValidator() const { return shard_->schema_validator_; }

ConstraintsInfo Shard::ListAllConstraints() const {
  return {ListExistenceConstraints(constraints_), constraints_.unique_constraints.ListConstraints()};
}

SchemasInfo Shard::ListAllSchemas() const { return {schemas_.ListSchemas()}; }

const Schemas::Schema *Shard::GetSchema(const LabelId primary_label) const { return schemas_.GetSchema(primary_label); }

bool Shard::CreateSchema(const LabelId primary_label, const std::vector<SchemaProperty> &schemas_types) {
  return schemas_.CreateSchema(primary_label, schemas_types);
}

bool Shard::DropSchema(const LabelId primary_label) { return schemas_.DropSchema(primary_label); }

StorageInfo Shard::GetInfo() const {
  auto vertex_count = vertices_.size();
  double average_degree = 0.0;
  if (vertex_count) {
    average_degree = 2.0 * static_cast<double>(edge_count_) / static_cast<double>(vertex_count);
  }
  return {vertex_count, edge_count_, average_degree, utils::GetMemoryUsage(),
          utils::GetDirDiskUsage(config_.durability.storage_directory)};
}

VerticesIterable Shard::Accessor::Vertices(LabelId label, View view) {
  return VerticesIterable(shard_->indices_.label_index.Vertices(label, view, transaction_));
}

VerticesIterable Shard::Accessor::Vertices(LabelId label, PropertyId property, View view) {
  return VerticesIterable(
      shard_->indices_.label_property_index.Vertices(label, property, std::nullopt, std::nullopt, view, transaction_));
}

VerticesIterable Shard::Accessor::Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) {
  return VerticesIterable(shard_->indices_.label_property_index.Vertices(
      label, property, utils::MakeBoundInclusive(value), utils::MakeBoundInclusive(value), view, transaction_));
}

VerticesIterable Shard::Accessor::Vertices(LabelId label, PropertyId property,
                                           const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                           const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) {
  return VerticesIterable(
      shard_->indices_.label_property_index.Vertices(label, property, lower_bound, upper_bound, view, transaction_));
}

void Shard::CollectGarbage(const io::Time current_time) {
  if (start_logical_id_to_transaction_.empty()) {
    // we are not aware of any transaction, thus no aborted or committed transactions, there is nothing to clean up
    return;
  }

  const auto clean_up_before_wall_clock = current_time - config_.gc.reclamation_interval;
  // TODO(antaljanosbenjamin) Fix before merge, get the correct logical is from start_logical_id_to_transaction
  const auto clean_up_before_timestamp =
      GetCleanupBeforeTimestamp(start_logical_id_to_transaction_, clean_up_before_wall_clock);

  // TODO(antaljanosbenjamin): Fix before merge, run index clean_up when a transaction aborted
  auto cleaned_up_committed_transaction = false;

  for (auto it = start_logical_id_to_transaction_.begin();
       it != start_logical_id_to_transaction_.end() &&
       it->second->start_timestamp.logical_id < clean_up_before_timestamp;) {
    auto &transaction = *it->second;

    if (transaction.commit_info->is_locally_committed) {
      cleaned_up_committed_transaction = true;
      auto commit_timestamp = transaction.commit_info->timestamp;

      for (Delta &delta : transaction.deltas) {
        while (true) {
          auto prev = delta.prev.Get();
          switch (prev.type) {
            case PreviousPtr::Type::VERTEX: {
              Vertex *vertex = prev.vertex;
              vertex->delta = nullptr;
              if (vertex->deleted) {
                deleted_vertices_.push_back(vertex->keys.Keys());
              }
              break;
            }
            case PreviousPtr::Type::EDGE: {
              Edge *edge = prev.edge;

              edge->delta = nullptr;
              if (edge->deleted) {
                deleted_edges_.push_back(edge->gid);
              }
              break;
            }
            case PreviousPtr::Type::DELTA: {
              if (prev.delta->commit_info->timestamp == commit_timestamp) {
                // The delta that is newer than this one is also a delta from this
                // transaction. We skip the current delta and will unlink it as a
                // part of the suffix later.
                break;
              }
              Delta *prev_delta = prev.delta;
              prev_delta->next = nullptr;
              break;
            }
            case PreviousPtr::Type::NULLPTR: {
              LOG_FATAL("Invalid pointer!");
            }
          }
          break;
        }
      }
    } else if (!transaction.is_aborted) {
      Accessor(*this, transaction).Abort();
    }
    it = start_logical_id_to_transaction_.erase(it);
  }

  // After unlinking deltas from vertices, we refresh the indices. That way
  // we're sure that none of the vertices from `current_deleted_vertices`
  // appears in an index, and we can safely remove the from the main storage
  // after the last currently active transaction is finished.
  if (cleaned_up_committed_transaction || has_any_transaction_aborted_since_last_gc) {
    // This operation is very expensive as it traverses through all of the items
    // in every index every time.
    RemoveObsoleteEntries(&indices_, clean_up_before_timestamp);
  }

  auto vertex_acc = vertices_.access();
  for (const auto &vertex : deleted_vertices_) {
    MG_ASSERT(vertex_acc.remove(vertex), "Invalid database state!");
  }
  deleted_vertices_.clear();
  {
    auto edge_acc = edges_.access();
    for (auto edge : deleted_edges_) {
      MG_ASSERT(edge_acc.remove(edge), "Invalid database state!");
    }
  }
  deleted_edges_.clear();
}

bool Shard::InitializeWalFile() {
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL)
    return false;
  if (!wal_file_) {
    wal_file_.emplace(wal_directory_, uuid_, epoch_id_, config_.items, &name_id_mapper_, wal_seq_num_++,
                      &file_retainer_);
  }
  return true;
}

void Shard::FinalizeWalFile() {
  ++wal_unsynced_transactions_;
  if (wal_unsynced_transactions_ >= config_.durability.wal_file_flush_every_n_tx) {
    wal_file_->Sync();
    wal_unsynced_transactions_ = 0;
  }
  if (wal_file_->GetSize() / 1024 >= config_.durability.wal_file_size_kibibytes) {
    wal_file_->FinalizeWal();
    wal_file_ = std::nullopt;
    wal_unsynced_transactions_ = 0;
  } else {
    // Try writing the internal buffer if possible, if not
    // the data should be written as soon as it's possible
    // (triggered by the new transaction commit, or some
    // reading thread EnabledFlushing)
    wal_file_->TryFlushing();
  }
}

void Shard::AppendToWal(const Transaction &transaction, uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  // Traverse deltas and append them to the WAL file.
  // A single transaction will always be contained in a single WAL file.
  auto current_commit_timestamp = transaction.commit_info->timestamp;

  // Helper lambda that traverses the delta chain on order to find the first
  // delta that should be processed and then appends all discovered deltas.
  auto find_and_apply_deltas = [&](const auto *delta, const auto &parent, auto filter) {
    while (true) {
      auto *older = delta->next;
      if (older == nullptr || older->commit_info->timestamp != current_commit_timestamp) {
        break;
      }
      delta = older;
    }
    while (true) {
      if (filter(delta->action)) {
        wal_file_->AppendDelta(*delta, parent, final_commit_timestamp);
      }
      auto prev = delta->prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::DELTA) break;
      delta = prev.delta;
    }
  };

  // The deltas are ordered correctly in the `transaction.deltas` buffer, but we
  // don't traverse them in that order. That is because for each delta we need
  // information about the vertex or edge they belong to and that information
  // isn't stored in the deltas themselves. In order to find out information
  // about the corresponding vertex or edge it is necessary to traverse the
  // delta chain for each delta until a vertex or edge is encountered. This
  // operation is very expensive as the chain grows.
  // Instead, we traverse the edges until we find a vertex or edge and traverse
  // their delta chains. This approach has a drawback because we lose the
  // correct order of the operations. Because of that, we need to traverse the
  // deltas several times and we have to manually ensure that the stored deltas
  // will be ordered correctly.

  // 1. Process all Vertex deltas and store all operations that create vertices
  // and modify vertex data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
          return true;

        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 2. Process all Vertex deltas and store all operations that create edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::REMOVE_OUT_EDGE:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
          return false;
      }
    });
  }
  // 3. Process all Edge deltas and store all operations that modify edge data.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::EDGE) continue;
    find_and_apply_deltas(&delta, *prev.edge, [](auto action) {
      switch (action) {
        case Delta::Action::SET_PROPERTY:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 4. Process all Vertex deltas and store all operations that delete edges.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::ADD_OUT_EDGE:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::RECREATE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }
  // 5. Process all Vertex deltas and store all operations that delete vertices.
  for (const auto &delta : transaction.deltas) {
    auto prev = delta.prev.Get();
    MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
    if (prev.type != PreviousPtr::Type::VERTEX) continue;
    find_and_apply_deltas(&delta, *prev.vertex, [](auto action) {
      switch (action) {
        case Delta::Action::RECREATE_OBJECT:
          return true;

        case Delta::Action::DELETE_OBJECT:
        case Delta::Action::SET_PROPERTY:
        case Delta::Action::ADD_LABEL:
        case Delta::Action::REMOVE_LABEL:
        case Delta::Action::ADD_IN_EDGE:
        case Delta::Action::ADD_OUT_EDGE:
        case Delta::Action::REMOVE_IN_EDGE:
        case Delta::Action::REMOVE_OUT_EDGE:
          return false;
      }
    });
  }

  // Add a delta that indicates that the transaction is fully written to the WAL
  // file.
  wal_file_->AppendTransactionEnd(final_commit_timestamp);

  FinalizeWalFile();
}

void Shard::AppendToWal(durability::StorageGlobalOperation operation, LabelId label,
                        const std::set<PropertyId> &properties, uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  wal_file_->AppendOperation(operation, label, properties, final_commit_timestamp);
  FinalizeWalFile();
}

bool Shard::CreateSnapshot() {
  // Create the transaction used to create the snapshot.
  // auto transaction = CreateTransaction(IsolationLevel::SNAPSHOT_ISOLATION);

  // Create snapshot.
  // durability::CreateSnapshot(&transaction, snapshot_directory_, wal_directory_,
  //                            config_.durability.snapshot_retention_count, &vertices_, &edges_,
  //                            &name_id_mapper_, &indices_, &constraints_, config_.items, schema_validator_,
  //                            uuid_, epoch_id_, epoch_history_, &file_retainer_);
  return true;
}

Transaction &Shard::GetTransaction(const coordinator::Hlc start_timestamp, IsolationLevel isolation_level) {
  // TODO(antaljanosbenjamin)
  if (const auto it = start_logical_id_to_transaction_.find(start_timestamp.logical_id);
      it != start_logical_id_to_transaction_.end()) {
    MG_ASSERT(it->second->start_timestamp.coordinator_wall_clock == start_timestamp.coordinator_wall_clock,
              "Logical id and wall clock don't match in already seen hybrid logical clock!");
    MG_ASSERT(it->second->isolation_level == isolation_level,
              "Isolation level doesn't match in already existing transaction!");
    return *it->second;
  }
  const auto insert_result = start_logical_id_to_transaction_.emplace(
      start_timestamp.logical_id, std::make_unique<Transaction>(start_timestamp, isolation_level));
  MG_ASSERT(insert_result.second, "Transaction creation failed!");
  return *insert_result.first->second;
}

bool Shard::LockPath() {
  auto locker_accessor = global_locker_.Access();
  return locker_accessor.AddPath(config_.durability.storage_directory);
}

bool Shard::UnlockPath() {
  {
    auto locker_accessor = global_locker_.Access();
    if (!locker_accessor.RemovePath(config_.durability.storage_directory)) {
      return false;
    }
  }

  // We use locker accessor in seperate scope so we don't produce deadlock
  // after we call clean queue.
  file_retainer_.CleanQueue();
  return true;
}

bool Shard::IsVertexBelongToShard(const VertexId &vertex_id) const {
  return vertex_id.primary_label == primary_label_ && vertex_id.primary_key >= min_primary_key_ &&
         (!max_primary_key_.has_value() || vertex_id.primary_key < *max_primary_key_);
}

void Shard::SetIsolationLevel(IsolationLevel isolation_level) { isolation_level_ = isolation_level; }

}  // namespace memgraph::storage::v3
