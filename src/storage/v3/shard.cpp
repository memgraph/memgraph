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
#include <atomic>
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
#include "storage/v3/replication/config.hpp"
#include "storage/v3/replication/replication_client.hpp"
#include "storage/v3/replication/replication_server.hpp"
#include "storage/v3/replication/rpc.hpp"
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
inline constexpr uint16_t kEpochHistoryRetention = 1000;

void InsertVertexPKIntoList(auto &container, const PrimaryKey &primary_key) { container.push_back(primary_key); }
}  // namespace

auto AdvanceToVisibleVertex(VerticesSkipList::Iterator it, VerticesSkipList::Iterator end,
                            std::optional<VertexAccessor> *vertex, Transaction *tx, View view, Indices *indices,
                            Constraints *constraints, Config::Items config, const SchemaValidator &schema_validator) {
  while (it != end) {
    *vertex = VertexAccessor::Create(&it->vertex, tx, indices, constraints, config, schema_validator, view);
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
                                 self->indices_, self_->constraints_, self->config_, *self_->schema_validator_)) {}

VertexAccessor AllVerticesIterable::Iterator::operator*() const { return *self_->vertex_; }

AllVerticesIterable::Iterator &AllVerticesIterable::Iterator::operator++() {
  ++it_;
  it_ = AdvanceToVisibleVertex(it_, self_->vertices_accessor_.end(), &self_->vertex_, self_->transaction_, self_->view_,
                               self_->indices_, self_->constraints_, self_->config_, *self_->schema_validator_);
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
      indices_{&constraints_, config.items, schema_validator_},
      isolation_level_{config.transaction.isolation_level},
      config_{config},
      snapshot_directory_{config_.durability.storage_directory / durability::kSnapshotDirectory},
      wal_directory_{config_.durability.storage_directory / durability::kWalDirectory},
      lock_file_path_{config_.durability.storage_directory / durability::kLockFile},
      uuid_{utils::GenerateUUID()},
      epoch_id_{utils::GenerateUUID()},
      global_locker_{file_retainer_.AddLocker()} {
  if (config_.durability.snapshot_wal_mode == Config::Durability::SnapshotWalMode::DISABLED &&
      replication_role_ == ReplicationRole::MAIN) {
    spdlog::warn(
        "The instance has the MAIN replication role, but durability logs and snapshots are disabled. Please consider "
        "enabling durability by using --storage-snapshot-interval-sec and --storage-wal-enabled flags because "
        "without write-ahead logs this instance is not replicating any data.");
  }
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
    auto info = std::optional<durability::RecoveryInfo>{};

    // durability::RecoverData(snapshot_directory_, wal_directory_, &uuid_, &epoch_id_, &epoch_history_, &vertices_,
    //                         &edges_, &edge_count_, &name_id_mapper_, &indices_, &constraints_, config_.items,
    //                         &wal_seq_num_);
    if (info) {
      edge_id_ = info->next_edge_id;
      timestamp_ = std::max(timestamp_, info->next_timestamp);
      if (info->last_commit_timestamp) {
        last_commit_timestamp_ = *info->last_commit_timestamp;
      }
    }
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

  if (timestamp_ == kTimestampInitialId) {
    commit_log_.emplace();
  } else {
    commit_log_.emplace(timestamp_);
  }
}

Shard::~Shard() {
  {
    // Clear replication data
    replication_server_.reset();
    replication_clients_.WithLock([&](auto &clients) { clients.clear(); });
  }
  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_ = std::nullopt;
  }
  if (config_.durability.snapshot_wal_mode != Config::Durability::SnapshotWalMode::DISABLED) {
    // TODO(antaljanosbenjamin): stop snapshot creation
  }
  if (config_.durability.snapshot_on_exit) {
    if (auto maybe_error = this->CreateSnapshot(); maybe_error.HasError()) {
      switch (maybe_error.GetError()) {
        case CreateSnapshotError::DisabledForReplica:
          spdlog::warn(utils::MessageWithLink("Snapshots are disabled for replicas.", "https://memgr.ph/replication"));
          break;
      }
    }
  }
}

Shard::Accessor::Accessor(Shard *shard, IsolationLevel isolation_level)
    : shard_(shard),
      transaction_(shard->CreateTransaction(isolation_level)),
      is_transaction_active_(true),
      config_(shard->config_.items) {}

Shard::Accessor::Accessor(Accessor &&other) noexcept
    : shard_(other.shard_),
      transaction_(std::move(other.transaction_)),
      commit_timestamp_(other.commit_timestamp_),
      is_transaction_active_(other.is_transaction_active_),
      config_(other.config_) {
  // Don't allow the other accessor to abort our transaction in destructor.
  other.is_transaction_active_ = false;
  other.commit_timestamp_.reset();
}

Shard::Accessor::~Accessor() {
  if (is_transaction_active_) {
    Abort();
  }

  FinalizeTransaction();
}

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

  // Get secondary properties
  std::vector<std::pair<PropertyId, PropertyValue>> secondary_properties;
  for (const auto &[property_id, property_value] : properties) {
    if (!shard_->schemas_.IsPropertyKey(primary_label, property_id)) {
      secondary_properties.emplace_back(property_id, property_value);
    }
  }

  auto acc = shard_->vertices_.access();
  auto *delta = CreateDeleteObjectDelta(&transaction_);
  auto [it, inserted] = acc.insert({Vertex{delta, primary_label, primary_properties, labels, secondary_properties}});
  MG_ASSERT(inserted, "The vertex must be inserted here!");
  MG_ASSERT(it != acc.end(), "Invalid Vertex accessor!");
  delta->prev.Set(&it->vertex);
  return VertexAccessor{&it->vertex,           &transaction_, &shard_->indices_,
                        &shard_->constraints_, config_,       shard_->schema_validator_};
}

std::optional<VertexAccessor> Shard::Accessor::FindVertex(std::vector<PropertyValue> primary_key, View view) {
  auto acc = shard_->vertices_.access();
  // Later on use label space
  auto it = acc.find(primary_key);
  if (it == acc.end()) {
    return std::nullopt;
  }
  return VertexAccessor::Create(&it->vertex, &transaction_, &shard_->indices_, &shard_->constraints_, config_,
                                shard_->schema_validator_, view);
}

Result<std::optional<VertexAccessor>> Shard::Accessor::DeleteVertex(VertexAccessor *vertex) {
  MG_ASSERT(vertex->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = vertex->vertex_;

  if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

  if (vertex_ptr->deleted) {
    return std::optional<VertexAccessor>{};
  }

  if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty()) return Error::VERTEX_HAS_EDGES;

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return std::make_optional<VertexAccessor>(vertex_ptr, &transaction_, &shard_->indices_, &shard_->constraints_,
                                            config_, shard_->schema_validator_, true);
}

Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> Shard::Accessor::DetachDeleteVertex(
    VertexAccessor *vertex) {
  using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;

  MG_ASSERT(vertex->transaction_ == &transaction_,
            "VertexAccessor must be from the same transaction as the storage "
            "accessor when deleting a vertex!");
  auto *vertex_ptr = vertex->vertex_;

  std::vector<Vertex::EdgeLink> in_edges;
  std::vector<Vertex::EdgeLink> out_edges;

  {
    if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

    if (vertex_ptr->deleted) return std::optional<ReturnType>{};

    in_edges = vertex_ptr->in_edges;
    out_edges = vertex_ptr->out_edges;
  }

  std::vector<EdgeAccessor> deleted_edges;
  const auto vertex_id = Id(*vertex_ptr);
  for (const auto &item : in_edges) {
    auto [edge_type, from_vertex, edge] = item;
    EdgeAccessor e(edge, edge_type, from_vertex, vertex_id, &transaction_, &shard_->indices_, &shard_->constraints_,
                   config_, shard_->schema_validator_);
    auto ret = DeleteEdge(&e);
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
    EdgeAccessor e(edge, edge_type, vertex_id, to_vertex, &transaction_, &shard_->indices_, &shard_->constraints_,
                   config_, shard_->schema_validator_);
    auto ret = DeleteEdge(&e);
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

  if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

  MG_ASSERT(!vertex_ptr->deleted, "Invalid database state!");

  CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
  vertex_ptr->deleted = true;

  return std::make_optional<ReturnType>(VertexAccessor{vertex_ptr, &transaction_, &shard_->indices_,
                                                       &shard_->constraints_, config_, shard_->schema_validator_, true},
                                        std::move(deleted_edges));
}

Result<EdgeAccessor> Shard::Accessor::CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type) {
  OOMExceptionEnabler oom_exception;
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

  if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  if (from_vertex->deleted) return Error::DELETED_OBJECT;

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  auto gid = Gid::FromUint(shard_->edge_id_++);
  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = shard_->edges_.access();
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    delta->prev.Set(&*it);
  }
  auto from_vertex_id = Id(*from_vertex);
  auto to_vertex_id = Id(*to_vertex);

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex_id, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex_id, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex_id, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex_id, edge);

  // Increment edge count.
  ++shard_->edge_count_;

  return EdgeAccessor(edge, edge_type, std::move(from_vertex_id), std::move(to_vertex_id), &transaction_,
                      &shard_->indices_, &shard_->constraints_, config_, shard_->schema_validator_);
}

Result<EdgeAccessor> Shard::Accessor::CreateEdge(VertexAccessor *from, VertexAccessor *to, EdgeTypeId edge_type,
                                                 Gid gid) {
  OOMExceptionEnabler oom_exception;
  MG_ASSERT(from->transaction_ == to->transaction_,
            "VertexAccessors must be from the same transaction when creating "
            "an edge!");
  MG_ASSERT(from->transaction_ == &transaction_,
            "VertexAccessors must be from the same transaction in when "
            "creating an edge!");

  auto *from_vertex = from->vertex_;
  auto *to_vertex = to->vertex_;

  if (!PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  if (from_vertex->deleted) return Error::DELETED_OBJECT;

  if (to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    if (to_vertex->deleted) return Error::DELETED_OBJECT;
  }

  // NOTE: When we update the next `edge_id_` here we perform a RMW
  // (read-modify-write) operation that ISN'T atomic! But, that isn't an issue
  // because this function is only called from the replication delta applier
  // that runs single-threadedly and while this instance is set-up to apply
  // threads (it is the replica), it is guaranteed that no other writes are
  // possible.
  shard_->edge_id_ = std::max(shard_->edge_id_, gid.AsUint() + 1);

  EdgeRef edge(gid);
  if (config_.properties_on_edges) {
    auto acc = shard_->edges_.access();
    auto *delta = CreateDeleteObjectDelta(&transaction_);
    auto [it, inserted] = acc.insert(Edge(gid, delta));
    MG_ASSERT(inserted, "The edge must be inserted here!");
    MG_ASSERT(it != acc.end(), "Invalid Edge accessor!");
    edge = EdgeRef(&*it);
    delta->prev.Set(&*it);
  }
  auto from_vertex_id = Id(*from_vertex);
  auto to_vertex_id = Id(*to_vertex);

  CreateAndLinkDelta(&transaction_, from_vertex, Delta::RemoveOutEdgeTag(), edge_type, to_vertex_id, edge);
  from_vertex->out_edges.emplace_back(edge_type, to_vertex_id, edge);

  CreateAndLinkDelta(&transaction_, to_vertex, Delta::RemoveInEdgeTag(), edge_type, from_vertex_id, edge);
  to_vertex->in_edges.emplace_back(edge_type, from_vertex_id, edge);

  // Increment edge count.
  ++shard_->edge_count_;

  return EdgeAccessor(edge, edge_type, std::move(from_vertex_id), std::move(to_vertex_id), &transaction_,
                      &shard_->indices_, &shard_->constraints_, config_, shard_->schema_validator_);
}

Result<std::optional<EdgeAccessor>> Shard::Accessor::DeleteEdge(EdgeAccessor *edge) {
  MG_ASSERT(edge->transaction_ == &transaction_,
            "EdgeAccessor must be from the same transaction as the storage "
            "accessor when deleting an edge!");
  auto edge_ref = edge->edge_;
  auto edge_type = edge->edge_type_;

  if (config_.properties_on_edges) {
    auto *edge_ptr = edge_ref.ptr;

    if (!PrepareForWrite(&transaction_, edge_ptr)) return Error::SERIALIZATION_ERROR;

    if (edge_ptr->deleted) return std::optional<EdgeAccessor>{};
  }
  // TODO(antaljanosbenjamin): FIX ME before merge, handling edge deletion properly with "remove" vertices
  const auto &from_vertex_id = edge->from_vertex_;
  const auto &to_vertex_id = edge->to_vertex_;

  Vertex *from_vertex{nullptr};
  Vertex *to_vertex{nullptr};

  auto acc = shard_->vertices_.access();

  if (shard_->IsVertexBelongToShard(from_vertex_id)) {
    auto it = acc.find(from_vertex_id.primary_key);
    if (it != acc.end()) {
      from_vertex = &it->vertex;
    }
  }

  if (shard_->IsVertexBelongToShard(to_vertex_id)) {
    auto it = acc.find(to_vertex_id.primary_key);
    if (it != acc.end()) {
      to_vertex = &it->vertex;
    }
  }

  const auto from_is_local = nullptr != from_vertex;
  const auto to_is_local = nullptr != to_vertex;
  MG_ASSERT(from_is_local || to_is_local, "Trying to delete an edges without having a local vertex");

  if (from_is_local && !PrepareForWrite(&transaction_, from_vertex)) return Error::SERIALIZATION_ERROR;
  MG_ASSERT(!from_vertex->deleted, "Invalid database state!");

  if (to_is_local && to_vertex != from_vertex) {
    if (!PrepareForWrite(&transaction_, to_vertex)) return Error::SERIALIZATION_ERROR;
    MG_ASSERT(!to_vertex->deleted, "Invalid database state!");
  }

  auto delete_edge_from_storage = [&edge_type, &edge_ref, this](const VertexId &vertex_id,
                                                                std::vector<Vertex::EdgeLink> &edges) {
    std::tuple<EdgeTypeId, VertexId, EdgeRef> link(edge_type, vertex_id, edge_ref);
    auto it = std::find(edges.begin(), edges.end(), link);
    if (config_.properties_on_edges) {
      MG_ASSERT(it != edges.end(), "Invalid database state!");
    } else if (it == edges.end()) {
      return false;
    }
    std::swap(*it, *edges.rbegin());
    edges.pop_back();
    return true;
  };

  auto success_on_to = to_is_local ? delete_edge_from_storage(to_vertex_id, to_vertex->out_edges) : false;
  auto success_on_from = from_is_local ? delete_edge_from_storage(from_vertex_id, from_vertex->in_edges) : false;

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
    CreateAndLinkDelta(&transaction_, edge_ptr, Delta::RecreateObjectTag());
    edge_ptr->deleted = true;
  }
  if (from_is_local) {
    CreateAndLinkDelta(&transaction_, from_vertex, Delta::AddOutEdgeTag(), edge_type, to_vertex_id, edge_ref);
  }
  if (to_is_local) {
    CreateAndLinkDelta(&transaction_, to_vertex, Delta::AddInEdgeTag(), edge_type, from_vertex_id, edge_ref);
  }

  // Decrement edge count.
  --shard_->edge_count_;

  return std::make_optional<EdgeAccessor>(edge_ref, edge_type, from_vertex_id, to_vertex_id, &transaction_,
                                          &shard_->indices_, &shard_->constraints_, config_, shard_->schema_validator_,
                                          true);
}

const std::string &Shard::Accessor::LabelToName(LabelId label) const { return shard_->LabelToName(label); }

const std::string &Shard::Accessor::PropertyToName(PropertyId property) const {
  return shard_->PropertyToName(property);
}

const std::string &Shard::Accessor::EdgeTypeToName(EdgeTypeId edge_type) const {
  return shard_->EdgeTypeToName(edge_type);
}

void Shard::Accessor::AdvanceCommand() { ++transaction_.command_id; }

utils::BasicResult<ConstraintViolation, void> Shard::Accessor::Commit(
    const std::optional<uint64_t> desired_commit_timestamp) {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");
  MG_ASSERT(!transaction_.must_abort, "The transaction can't be committed!");

  if (transaction_.deltas.empty()) {
    // We don't have to update the commit timestamp here because no one reads
    // it.
    shard_->commit_log_->MarkFinished(transaction_.start_timestamp);
  } else {
    // Validate that existence constraints are satisfied for all modified
    // vertices.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) {
        continue;
      }
      // No need to take any locks here because we modified this vertex and no
      // one else can touch it until we commit.
      auto validation_result = ValidateExistenceConstraints(*prev.vertex, shard_->constraints_);
      if (validation_result) {
        Abort();
        return {*validation_result};
      }
    }

    // Result of validating the vertex against unique constraints. It has to be
    // declared outside of the critical section scope because its value is
    // tested for Abort call which has to be done out of the scope.
    std::optional<ConstraintViolation> unique_constraint_violation;

    // Save these so we can mark them used in the commit log.
    uint64_t start_timestamp = transaction_.start_timestamp;

    commit_timestamp_.emplace(shard_->CommitTimestamp(desired_commit_timestamp));

    // Before committing and validating vertices against unique constraints,
    // we have to update unique constraints with the vertices that are going
    // to be validated/committed.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) {
        continue;
      }
      shard_->constraints_.unique_constraints.UpdateBeforeCommit(prev.vertex, transaction_);
    }

    // Validate that unique constraints are satisfied for all modified
    // vertices.
    for (const auto &delta : transaction_.deltas) {
      auto prev = delta.prev.Get();
      MG_ASSERT(prev.type != PreviousPtr::Type::NULLPTR, "Invalid pointer!");
      if (prev.type != PreviousPtr::Type::VERTEX) {
        continue;
      }

      // No need to take any locks here because we modified this vertex and no
      // one else can touch it until we commit.
      unique_constraint_violation =
          shard_->constraints_.unique_constraints.Validate(*prev.vertex, transaction_, *commit_timestamp_);
      if (unique_constraint_violation) {
        break;
      }
    }

    if (!unique_constraint_violation) {
      // Write transaction to WAL while holding the engine lock to make sure
      // that committed transactions are sorted by the commit timestamp in the
      // WAL files. We supply the new commit timestamp to the function so that
      // it knows what will be the final commit timestamp. The WAL must be
      // written before actually committing the transaction (before setting
      // the commit timestamp) so that no other transaction can see the
      // modifications before they are written to disk.
      // Replica can log only the write transaction received from Main
      // so the Wal files are consistent
      if (shard_->replication_role_ == ReplicationRole::MAIN || desired_commit_timestamp.has_value()) {
        shard_->AppendToWal(transaction_, *commit_timestamp_);
      }

      // TODO(antaljanosbenjamin): Figure out:
      //   1. How the committed transactions are sorted in `committed_transactions_`
      //   2. Why it was necessary to lock `committed_transactions_` when it was not accessed at all
      // TODO: Update all deltas to have a local copy of the commit timestamp
      MG_ASSERT(transaction_.commit_timestamp != nullptr, "Invalid database state!");
      transaction_.commit_timestamp->store(*commit_timestamp_, std::memory_order_release);
      // Replica can only update the last commit timestamp with
      // the commits received from main.
      if (shard_->replication_role_ == ReplicationRole::MAIN || desired_commit_timestamp.has_value()) {
        // Update the last commit timestamp
        shard_->last_commit_timestamp_ = *commit_timestamp_;
      }

      shard_->commit_log_->MarkFinished(start_timestamp);
    }

    if (unique_constraint_violation) {
      Abort();
      return {*unique_constraint_violation};
    }
  }
  is_transaction_active_ = false;

  return {};
}

void Shard::Accessor::Abort() {
  MG_ASSERT(is_transaction_active_, "The transaction is already terminated!");

  for (const auto &delta : transaction_.deltas) {
    auto prev = delta.prev.Get();
    switch (prev.type) {
      case PreviousPtr::Type::VERTEX: {
        auto *vertex = prev.vertex;
        Delta *current = vertex->delta;
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) == transaction_.transaction_id) {
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
              InsertVertexPKIntoList(shard_->deleted_vertices_, vertex->keys.Keys());
              break;
            }
            case Delta::Action::RECREATE_OBJECT: {
              vertex->deleted = false;
              break;
            }
          }
          current = current->next.load(std::memory_order_acquire);
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
        while (current != nullptr &&
               current->timestamp->load(std::memory_order_acquire) == transaction_.transaction_id) {
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
          current = current->next.load(std::memory_order_acquire);
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

  {
    uint64_t mark_timestamp = shard_->timestamp_;

    // Release engine lock because we don't have to hold it anymore and
    // emplace back could take a long time.
    shard_->garbage_undo_buffers_.emplace_back(mark_timestamp, std::move(transaction_.deltas));
  }

  shard_->commit_log_->MarkFinished(transaction_.start_timestamp);
  is_transaction_active_ = false;
}

void Shard::Accessor::FinalizeTransaction() {
  if (commit_timestamp_) {
    shard_->commit_log_->MarkFinished(*commit_timestamp_);
    shard_->committed_transactions_.emplace_back(std::move(transaction_));
    commit_timestamp_.reset();
  }
}

const std::string &Shard::LabelToName(LabelId label) const { return name_id_mapper_.IdToName(label.AsUint()); }

const std::string &Shard::PropertyToName(PropertyId property) const {
  return name_id_mapper_.IdToName(property.AsUint());
}

const std::string &Shard::EdgeTypeToName(EdgeTypeId edge_type) const {
  return name_id_mapper_.IdToName(edge_type.AsUint());
}

bool Shard::CreateIndex(LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  // TODO Fix Index
  return false;
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::LABEL_INDEX_CREATE, label, {}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;
  return true;
}

bool Shard::CreateIndex(LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  // TODO Fix Index
  // if (!indices_.label_property_index.CreateIndex(label, property, labelspace.access())) return false;
  return false;
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_CREATE, label, {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;
  return true;
}

bool Shard::DropIndex(LabelId label, const std::optional<uint64_t> desired_commit_timestamp) {
  if (!indices_.label_index.DropIndex(label)) return false;
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::LABEL_INDEX_DROP, label, {}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;
  return true;
}

bool Shard::DropIndex(LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  if (!indices_.label_property_index.DropIndex(label, property)) return false;
  // For a description why using `timestamp_` is correct, see
  // `CreateIndex(LabelId label)`.
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::LABEL_PROPERTY_INDEX_DROP, label, {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;
  return true;
}

IndicesInfo Shard::ListAllIndices() const {
  return {indices_.label_index.ListIndices(), indices_.label_property_index.ListIndices()};
}

utils::BasicResult<ConstraintViolation, bool> Shard::CreateExistenceConstraint(
    LabelId label, PropertyId property, const std::optional<uint64_t> desired_commit_timestamp) {
  // TODO Fix constraints
  // auto ret = ::memgraph::storage::v3::CreateExistenceConstraint(&constraints_, label, property, vertices_.access());
  // if (ret.HasError() || !ret.GetValue()) return ret;
  return false;
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_CREATE, label, {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;
  return true;
}

bool Shard::DropExistenceConstraint(LabelId label, PropertyId property,
                                    const std::optional<uint64_t> desired_commit_timestamp) {
  if (!memgraph::storage::v3::DropExistenceConstraint(&constraints_, label, property)) return false;
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::EXISTENCE_CONSTRAINT_DROP, label, {property}, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;
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
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_CREATE, label, properties, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;
  return UniqueConstraints::CreationStatus::SUCCESS;
}

UniqueConstraints::DeletionStatus Shard::DropUniqueConstraint(LabelId label, const std::set<PropertyId> &properties,
                                                              const std::optional<uint64_t> desired_commit_timestamp) {
  auto ret = constraints_.unique_constraints.DropConstraint(label, properties);
  if (ret != UniqueConstraints::DeletionStatus::SUCCESS) {
    return ret;
  }
  const auto commit_timestamp = CommitTimestamp(desired_commit_timestamp);
  AppendToWal(durability::StorageGlobalOperation::UNIQUE_CONSTRAINT_DROP, label, properties, commit_timestamp);
  commit_log_->MarkFinished(commit_timestamp);
  last_commit_timestamp_ = commit_timestamp;
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
  return VerticesIterable(shard_->indices_.label_index.Vertices(label, view, &transaction_));
}

VerticesIterable Shard::Accessor::Vertices(LabelId label, PropertyId property, View view) {
  return VerticesIterable(shard_->indices_.label_property_index.Vertices(
      label, property, std::nullopt, std::nullopt, view, &transaction_, shard_->schema_validator_));
}

VerticesIterable Shard::Accessor::Vertices(LabelId label, PropertyId property, const PropertyValue &value, View view) {
  return VerticesIterable(shard_->indices_.label_property_index.Vertices(
      label, property, utils::MakeBoundInclusive(value), utils::MakeBoundInclusive(value), view, &transaction_,
      shard_->schema_validator_));
}

VerticesIterable Shard::Accessor::Vertices(LabelId label, PropertyId property,
                                           const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                           const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view) {
  return VerticesIterable(shard_->indices_.label_property_index.Vertices(
      label, property, lower_bound, upper_bound, view, &transaction_, shard_->schema_validator_));
}

Transaction Shard::CreateTransaction(IsolationLevel isolation_level) {
  // We acquire the transaction engine lock here because we access (and
  // modify) the transaction engine variables (`transaction_id` and
  // `timestamp`) below.
  uint64_t transaction_id{0};
  uint64_t start_timestamp{0};

  transaction_id = transaction_id_++;
  // Replica should have only read queries and the write queries
  // can come from main instance with any past timestamp.
  // To preserve snapshot isolation we set the start timestamp
  // of any query on replica to the last commited transaction
  // which is timestamp_ as only commit of transaction with writes
  // can change the value of it.
  if (replication_role_ == ReplicationRole::REPLICA) {
    start_timestamp = timestamp_;
  } else {
    start_timestamp = timestamp_++;
  }

  return {transaction_id, start_timestamp, isolation_level};
}

// `force` means there are no active transactions, so everything can be deleted without worrying about removing some
// data that is used by an active transaction
template <bool force>
void Shard::CollectGarbage() {
  if constexpr (force) {
    // TODO(antaljanosbenjamin): figure out whether is there any active transaction or not (probably accessors should
    // increment/decrement a counter). If there are no transactions, then garbage collection can be forced
    CollectGarbage<false>();
    return;
  }

  // Garbage collection must be performed in two phases. In the first phase,
  // deltas that won't be applied by any transaction anymore are unlinked from
  // the version chains. They cannot be deleted immediately, because there
  // might be a transaction that still needs them to terminate the version
  // chain traversal. They are instead marked for deletion and will be deleted
  // in the second GC phase in this GC iteration or some of the following
  // ones.

  uint64_t oldest_active_start_timestamp = commit_log_->OldestActive();
  // We don't move undo buffers of unlinked transactions to garbage_undo_buffers
  // list immediately, because we would have to repeatedly take
  // garbage_undo_buffers lock.
  std::list<std::pair<uint64_t, std::list<Delta>>> unlinked_undo_buffers;

  // We will only free vertices deleted up until now in this GC cycle, and we
  // will do it after cleaning-up the indices. That way we are sure that all
  // vertices that appear in an index also exist in main storage.

  // Flag that will be used to determine whether the Index GC should be run. It
  // should be run when there were any items that were cleaned up (there were
  // updates between this run of the GC and the previous run of the GC). This
  // eliminates high CPU usage when the GC doesn't have to clean up anything.
  bool run_index_cleanup = !committed_transactions_.empty() || !garbage_undo_buffers_.empty();

  while (true) {
    // We don't want to hold the lock on commited transactions for too long,
    // because that prevents other transactions from committing.
    Transaction *transaction{nullptr};
    {
      if (committed_transactions_.empty()) {
        break;
      }
      transaction = &committed_transactions_.front();
    }

    auto commit_timestamp = transaction->commit_timestamp->load(std::memory_order_acquire);
    if (commit_timestamp >= oldest_active_start_timestamp) {
      break;
    }

    // When unlinking a delta which is the first delta in its version chain,
    // special care has to be taken to avoid the following race condition:
    //
    // [Vertex] --> [Delta A]
    //
    //    GC thread: Delta A is the first in its chain, it must be unlinked from
    //               vertex and marked for deletion
    //    TX thread: Update vertex and add Delta B with Delta A as next
    //
    // [Vertex] --> [Delta B] <--> [Delta A]
    //
    //    GC thread: Unlink delta from Vertex
    //
    // [Vertex] --> (nullptr)
    //
    // When processing a delta that is the first one in its chain, we
    // obtain the corresponding vertex or edge lock, and then verify that this
    // delta still is the first in its chain.
    // When processing a delta that is in the middle of the chain we only
    // process the final delta of the given transaction in that chain. We
    // determine the owner of the chain (either a vertex or an edge), obtain the
    // corresponding lock, and then verify that this delta is still in the same
    // position as it was before taking the lock.
    //
    // Even though the delta chain is lock-free (both `next` and `prev`) the
    // chain should not be modified without taking the lock from the object that
    // owns the chain (either a vertex or an edge). Modifying the chain without
    // taking the lock will cause subtle race conditions that will leave the
    // chain in a broken state.
    // The chain can be only read without taking any locks.

    for (Delta &delta : transaction->deltas) {
      while (true) {
        auto prev = delta.prev.Get();
        switch (prev.type) {
          case PreviousPtr::Type::VERTEX: {
            Vertex *vertex = prev.vertex;
            if (vertex->delta != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            vertex->delta = nullptr;
            if (vertex->deleted) {
              InsertVertexPKIntoList(deleted_vertices_, vertex->keys.Keys());
            }
            break;
          }
          case PreviousPtr::Type::EDGE: {
            Edge *edge = prev.edge;
            if (edge->delta != &delta) {
              // Something changed, we're not the first delta in the chain
              // anymore.
              continue;
            }
            edge->delta = nullptr;
            if (edge->deleted) {
              deleted_edges_.push_back(edge->gid);
            }
            break;
          }
          case PreviousPtr::Type::DELTA: {
            if (prev.delta->timestamp->load(std::memory_order_acquire) == commit_timestamp) {
              // The delta that is newer than this one is also a delta from this
              // transaction. We skip the current delta and will remove it as a
              // part of the suffix later.
              break;
            }
            {
              // We need to find the parent object in order to be able to use
              // its lock.
              auto parent = prev;
              while (parent.type == PreviousPtr::Type::DELTA) {
                parent = parent.delta->prev.Get();
              }
              switch (parent.type) {
                case PreviousPtr::Type::VERTEX:
                case PreviousPtr::Type::EDGE:
                  break;
                case PreviousPtr::Type::DELTA:
                case PreviousPtr::Type::NULLPTR:
                  LOG_FATAL("Invalid database state!");
              }
            }
            Delta *prev_delta = prev.delta;
            prev_delta->next.store(nullptr, std::memory_order_release);
            break;
          }
          case PreviousPtr::Type::NULLPTR: {
            LOG_FATAL("Invalid pointer!");
          }
        }
        break;
      }
    }

    unlinked_undo_buffers.emplace_back(0, std::move(transaction->deltas));
    committed_transactions_.pop_front();
  }

  // After unlinking deltas from vertices, we refresh the indices. That way
  // we're sure that none of the vertices from `current_deleted_vertices`
  // appears in an index, and we can safely remove the from the main storage
  // after the last currently active transaction is finished.
  if (run_index_cleanup) {
    // This operation is very expensive as it traverses through all of the items
    // in every index every time.
    RemoveObsoleteEntries(&indices_, oldest_active_start_timestamp);
    constraints_.unique_constraints.RemoveObsoleteEntries(oldest_active_start_timestamp);
  }

  {
    uint64_t mark_timestamp = timestamp_;
    for (auto &[timestamp, undo_buffer] : unlinked_undo_buffers) {
      timestamp = mark_timestamp;
    }
    garbage_undo_buffers_.splice(garbage_undo_buffers_.end(), unlinked_undo_buffers);

    for (const auto &vertex : deleted_vertices_) {
      garbage_vertices_.emplace_back(mark_timestamp, vertex);
    }
  }

  // if force is set to true we can simply delete all the leftover undos because
  // no transaction is active
  if constexpr (force) {
    garbage_undo_buffers_.clear();
  } else {
    while (!garbage_undo_buffers_.empty() && garbage_undo_buffers_.front().first <= oldest_active_start_timestamp) {
      garbage_undo_buffers_.pop_front();
    }
  }

  {
    auto vertex_acc = vertices_.access();
    if constexpr (force) {
      // if force is set to true, then we have unique_lock and no transactions are active
      // so we can clean all of the deleted vertices
      while (!garbage_vertices_.empty()) {
        MG_ASSERT(vertex_acc.remove(garbage_vertices_.front().second), "Invalid database state!");
        garbage_vertices_.pop_front();
      }
    } else {
      while (!garbage_vertices_.empty() && garbage_vertices_.front().first < oldest_active_start_timestamp) {
        MG_ASSERT(vertex_acc.remove(garbage_vertices_.front().second), "Invalid database state!");
        garbage_vertices_.pop_front();
      }
    }
  }
  {
    auto edge_acc = edges_.access();
    for (auto edge : deleted_edges_) {
      MG_ASSERT(edge_acc.remove(edge), "Invalid database state!");
    }
  }
}

// tell the linker he can find the CollectGarbage definitions here
template void Shard::CollectGarbage<true>();
template void Shard::CollectGarbage<false>();

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
  auto current_commit_timestamp = transaction.commit_timestamp->load(std::memory_order_acquire);

  if (replication_role_ == ReplicationRole::MAIN) {
    replication_clients_.WithLock([&](auto &clients) {
      for (auto &client : clients) {
        client->StartTransactionReplication(wal_file_->SequenceNumber());
      }
    });
  }

  // Helper lambda that traverses the delta chain on order to find the first
  // delta that should be processed and then appends all discovered deltas.
  auto find_and_apply_deltas = [&](const auto *delta, const auto &parent, auto filter) {
    while (true) {
      auto *older = delta->next.load(std::memory_order_acquire);
      if (older == nullptr || older->timestamp->load(std::memory_order_acquire) != current_commit_timestamp) break;
      delta = older;
    }
    while (true) {
      if (filter(delta->action)) {
        wal_file_->AppendDelta(*delta, parent, final_commit_timestamp);
        replication_clients_.WithLock([&](auto &clients) {
          for (auto &client : clients) {
            client->IfStreamingTransaction(
                [&](auto &stream) { stream.AppendDelta(*delta, parent, final_commit_timestamp); });
          }
        });
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

  replication_clients_.WithLock([&](auto &clients) {
    for (auto &client : clients) {
      client->IfStreamingTransaction([&](auto &stream) { stream.AppendTransactionEnd(final_commit_timestamp); });
      client->FinalizeTransactionReplication();
    }
  });
}

void Shard::AppendToWal(durability::StorageGlobalOperation operation, LabelId label,
                        const std::set<PropertyId> &properties, uint64_t final_commit_timestamp) {
  if (!InitializeWalFile()) return;
  wal_file_->AppendOperation(operation, label, properties, final_commit_timestamp);
  {
    if (replication_role_ == ReplicationRole::MAIN) {
      replication_clients_.WithLock([&](auto &clients) {
        for (auto &client : clients) {
          client->StartTransactionReplication(wal_file_->SequenceNumber());
          client->IfStreamingTransaction(
              [&](auto &stream) { stream.AppendOperation(operation, label, properties, final_commit_timestamp); });
          client->FinalizeTransactionReplication();
        }
      });
    }
  }
  FinalizeWalFile();
}

utils::BasicResult<Shard::CreateSnapshotError> Shard::CreateSnapshot() {
  if (replication_role_ != ReplicationRole::MAIN) {
    return CreateSnapshotError::DisabledForReplica;
  }

  // Create the transaction used to create the snapshot.
  auto transaction = CreateTransaction(IsolationLevel::SNAPSHOT_ISOLATION);

  // Create snapshot.
  // durability::CreateSnapshot(&transaction, snapshot_directory_, wal_directory_,
  //                            config_.durability.snapshot_retention_count, &vertices_, &edges_,
  //                            &name_id_mapper_, &indices_, &constraints_, config_.items, schema_validator_,
  //                            uuid_, epoch_id_, epoch_history_, &file_retainer_);

  // Finalize snapshot transaction.
  commit_log_->MarkFinished(transaction.start_timestamp);
  return {};
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

void Shard::FreeMemory() {
  CollectGarbage<true>();

  // SkipList is already threadsafe
  vertices_.run_gc();
  edges_.run_gc();
  indices_.label_index.RunGC();
  indices_.label_property_index.RunGC();
}

uint64_t Shard::CommitTimestamp(const std::optional<uint64_t> desired_commit_timestamp) {
  if (!desired_commit_timestamp) {
    return timestamp_++;
  }
  timestamp_ = std::max(timestamp_, *desired_commit_timestamp + 1);
  return *desired_commit_timestamp;
}

bool Shard::IsVertexBelongToShard(const VertexId &vertex_id) const {
  return vertex_id.primary_label == primary_label_ && vertex_id.primary_key >= min_primary_key_ &&
         (!max_primary_key_.has_value() || vertex_id.primary_key < *max_primary_key_);
}

bool Shard::SetReplicaRole(io::network::Endpoint endpoint, const replication::ReplicationServerConfig &config) {
  // We don't want to restart the server if we're already a REPLICA
  if (replication_role_ == ReplicationRole::REPLICA) {
    return false;
  }

  replication_server_ = std::make_unique<ReplicationServer>(this, std::move(endpoint), config);

  replication_role_ = ReplicationRole::REPLICA;
  return true;
}

bool Shard::SetMainReplicationRole() {
  // We don't want to generate new epoch_id and do the
  // cleanup if we're already a MAIN
  if (replication_role_ == ReplicationRole::MAIN) {
    return false;
  }

  // Main instance does not need replication server
  // This should be always called first so we finalize everything
  replication_server_.reset(nullptr);

  if (wal_file_) {
    wal_file_->FinalizeWal();
    wal_file_.reset();
  }

  // Generate new epoch id and save the last one to the history.
  if (epoch_history_.size() == kEpochHistoryRetention) {
    epoch_history_.pop_front();
  }
  epoch_history_.emplace_back(std::move(epoch_id_), last_commit_timestamp_);
  epoch_id_ = utils::GenerateUUID();

  replication_role_ = ReplicationRole::MAIN;
  return true;
}

utils::BasicResult<Shard::RegisterReplicaError> Shard::RegisterReplica(
    std::string name, io::network::Endpoint endpoint, const replication::ReplicationMode replication_mode,
    const replication::ReplicationClientConfig &config) {
  MG_ASSERT(replication_role_ == ReplicationRole::MAIN, "Only main instance can register a replica!");

  const bool name_exists = replication_clients_.WithLock([&](auto &clients) {
    return std::any_of(clients.begin(), clients.end(), [&name](const auto &client) { return client->Name() == name; });
  });

  if (name_exists) {
    return RegisterReplicaError::NAME_EXISTS;
  }

  const auto end_point_exists = replication_clients_.WithLock([&endpoint](auto &clients) {
    return std::any_of(clients.begin(), clients.end(),
                       [&endpoint](const auto &client) { return client->Endpoint() == endpoint; });
  });

  if (end_point_exists) {
    return RegisterReplicaError::END_POINT_EXISTS;
  }

  MG_ASSERT(replication_mode == replication::ReplicationMode::SYNC || !config.timeout,
            "Only SYNC mode can have a timeout set");

  auto client = std::make_unique<ReplicationClient>(std::move(name), this, endpoint, replication_mode, config);
  if (client->State() == replication::ReplicaState::INVALID) {
    return RegisterReplicaError::CONNECTION_FAILED;
  }

  return replication_clients_.WithLock([&](auto &clients) -> utils::BasicResult<Shard::RegisterReplicaError> {
    // Another thread could have added a client with same name while
    // we were connecting to this client.
    if (std::any_of(clients.begin(), clients.end(),
                    [&](const auto &other_client) { return client->Name() == other_client->Name(); })) {
      return RegisterReplicaError::NAME_EXISTS;
    }

    if (std::any_of(clients.begin(), clients.end(),
                    [&client](const auto &other_client) { return client->Endpoint() == other_client->Endpoint(); })) {
      return RegisterReplicaError::END_POINT_EXISTS;
    }

    clients.push_back(std::move(client));
    return {};
  });
}

bool Shard::UnregisterReplica(const std::string_view name) {
  MG_ASSERT(replication_role_ == ReplicationRole::MAIN, "Only main instance can unregister a replica!");
  return replication_clients_.WithLock([&](auto &clients) {
    return std::erase_if(clients, [&](const auto &client) { return client->Name() == name; });
  });
}

std::optional<replication::ReplicaState> Shard::GetReplicaState(const std::string_view name) {
  return replication_clients_.WithLock([&](auto &clients) -> std::optional<replication::ReplicaState> {
    const auto client_it =
        std::find_if(clients.cbegin(), clients.cend(), [name](auto &client) { return client->Name() == name; });
    if (client_it == clients.cend()) {
      return std::nullopt;
    }
    return (*client_it)->State();
  });
}

ReplicationRole Shard::GetReplicationRole() const { return replication_role_; }

std::vector<Shard::ReplicaInfo> Shard::ReplicasInfo() {
  return replication_clients_.WithLock([](auto &clients) {
    std::vector<Shard::ReplicaInfo> replica_info;
    replica_info.reserve(clients.size());
    std::transform(clients.begin(), clients.end(), std::back_inserter(replica_info),
                   [](const auto &client) -> ReplicaInfo {
                     return {client->Name(), client->Mode(), client->Timeout(), client->Endpoint(), client->State()};
                   });
    return replica_info;
  });
}

void Shard::SetIsolationLevel(IsolationLevel isolation_level) { isolation_level_ = isolation_level; }

}  // namespace memgraph::storage::v3
