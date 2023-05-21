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

#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "utils/stat.hpp"
#include "utils/typeinfo.hpp"

namespace memgraph::storage {

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

auto AdvanceToVisibleVertex(utils::SkipList<Vertex>::Iterator it, utils::SkipList<Vertex>::Iterator end,
                            std::optional<VertexAccessor> *vertex, Transaction *tx, View view, Indices *indices,
                            Constraints *constraints, Config::Items config) {
  while (it != end) {
    *vertex = VertexAccessor::Create(&*it, tx, indices, constraints, config, view);
    if (!*vertex) {
      ++it;
      continue;
    }
    break;
  }
  return it;
}

AllVerticesIterable::Iterator::Iterator(AllVerticesIterable *self, utils::SkipList<Vertex>::Iterator it)
    : self_(self),
      it_(AdvanceToVisibleVertex(it, self->vertices_accessor_.end(), &self->vertex_, self->transaction_, self->view_,
                                 self->indices_, self_->constraints_, self->config_)) {}

VertexAccessor AllVerticesIterable::Iterator::operator*() const { return *self_->vertex_; }

AllVerticesIterable::Iterator &AllVerticesIterable::Iterator::operator++() {
  ++it_;
  it_ = AdvanceToVisibleVertex(it_, self_->vertices_accessor_.end(), &self_->vertex_, self_->transaction_, self_->view_,
                               self_->indices_, self_->constraints_, self_->config_);
  return *this;
}

VerticesIterable::VerticesIterable(AllVerticesIterable vertices) : type_(Type::ALL) {
  new (&all_vertices_) AllVerticesIterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(InMemoryLabelIndex::Iterable vertices) : type_(Type::BY_LABEL_IN_MEMORY) {
  new (&in_memory_vertices_by_label_) InMemoryLabelIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(InMemoryLabelPropertyIndex::Iterable vertices)
    : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY) {
  new (&in_memory_vertices_by_label_property_) InMemoryLabelPropertyIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(DiskLabelIndex::Iterable vertices) : type_(Type::BY_LABEL_PROPERTY_ON_DISK) {
  new (&disk_vertices_by_label_property_) DiskLabelIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(DiskLabelPropertyIndex::Iterable vertices) : type_(Type::BY_LABEL_PROPERTY_ON_DISK) {
  new (&disk_vertices_by_label_property_) DiskLabelPropertyIndex::Iterable(std::move(vertices));
}

VerticesIterable::VerticesIterable(VerticesIterable &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_vertices_) AllVerticesIterable(std::move(other.all_vertices_));
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_vertices_by_label_) InMemoryLabelIndex::Iterable(std::move(other.in_memory_vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_vertices_by_label_property_)
          InMemoryLabelPropertyIndex::Iterable(std::move(other.in_memory_vertices_by_label_property_));
      break;
    case Type::BY_LABEL_ON_DISK:
      new (&disk_vertices_by_label_) DiskLabelIndex::Iterable(std::move(other.disk_vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      new (&disk_vertices_by_label_property_)
          DiskLabelPropertyIndex::Iterable(std::move(other.disk_vertices_by_label_property_));
      break;
  }
}

VerticesIterable &VerticesIterable::operator=(VerticesIterable &&other) noexcept {
  switch (type_) {
    case Type::ALL:
      all_vertices_.AllVerticesIterable::~AllVerticesIterable();
      break;
    case Type::BY_LABEL_IN_MEMORY:
      in_memory_vertices_by_label_.InMemoryLabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      in_memory_vertices_by_label_property_.InMemoryLabelPropertyIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_ON_DISK:
      disk_vertices_by_label_.DiskLabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      disk_vertices_by_label_property_.DiskLabelPropertyIndex::Iterable::~Iterable();
      break;
  }
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_vertices_) AllVerticesIterable(std::move(other.all_vertices_));
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_vertices_by_label_) InMemoryLabelIndex::Iterable(std::move(other.in_memory_vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_vertices_by_label_property_)
          InMemoryLabelPropertyIndex::Iterable(std::move(other.in_memory_vertices_by_label_property_));
      break;
    case Type::BY_LABEL_ON_DISK:
      new (&disk_vertices_by_label_) DiskLabelIndex::Iterable(std::move(other.disk_vertices_by_label_));
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      new (&disk_vertices_by_label_property_)
          DiskLabelPropertyIndex::Iterable(std::move(other.disk_vertices_by_label_property_));
      break;
  }
  return *this;
}

VerticesIterable::~VerticesIterable() {
  switch (type_) {
    case Type::ALL:
      all_vertices_.AllVerticesIterable::~AllVerticesIterable();
      break;
    case Type::BY_LABEL_IN_MEMORY:
      in_memory_vertices_by_label_.InMemoryLabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      in_memory_vertices_by_label_property_.InMemoryLabelPropertyIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_ON_DISK:
      disk_vertices_by_label_.DiskLabelIndex::Iterable::~Iterable();
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      disk_vertices_by_label_property_.DiskLabelPropertyIndex::Iterable::~Iterable();
      break;
  }
}

VerticesIterable::Iterator VerticesIterable::begin() {
  switch (type_) {
    case Type::ALL:
      return Iterator(all_vertices_.begin());
    case Type::BY_LABEL_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_.begin());
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_property_.begin());
    case Type::BY_LABEL_ON_DISK:
      return Iterator(disk_vertices_by_label_.begin());
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      return Iterator(disk_vertices_by_label_property_.begin());
  }
}

VerticesIterable::Iterator VerticesIterable::end() {
  switch (type_) {
    case Type::ALL:
      return Iterator(all_vertices_.end());
    case Type::BY_LABEL_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_.end());
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      return Iterator(in_memory_vertices_by_label_property_.end());
    case Type::BY_LABEL_ON_DISK:
      return Iterator(disk_vertices_by_label_.end());
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      return Iterator(disk_vertices_by_label_property_.end());
  }
}

VerticesIterable::Iterator::Iterator(AllVerticesIterable::Iterator it) : type_(Type::ALL) {
  new (&all_it_) AllVerticesIterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(InMemoryLabelIndex::Iterable::Iterator it) : type_(Type::BY_LABEL_IN_MEMORY) {
  new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(InMemoryLabelPropertyIndex::Iterable::Iterator it)
    : type_(Type::BY_LABEL_PROPERTY_IN_MEMORY) {
  new (&in_memory_by_label_property_it_) InMemoryLabelPropertyIndex::Iterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(DiskLabelIndex::Iterable::Iterator it) : type_(Type::BY_LABEL_ON_DISK) {
  new (&disk_by_label_it_) DiskLabelIndex::Iterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(DiskLabelPropertyIndex::Iterable::Iterator it)
    : type_(Type::BY_LABEL_PROPERTY_ON_DISK) {
  new (&disk_by_label_property_it_) DiskLabelPropertyIndex::Iterable::Iterator(std::move(it));
}

VerticesIterable::Iterator::Iterator(const VerticesIterable::Iterator &other) : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(other.in_memory_by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_by_label_property_it_)
          InMemoryLabelPropertyIndex::Iterable::Iterator(other.in_memory_by_label_property_it_);
      break;
    case Type::BY_LABEL_ON_DISK:
      new (&disk_by_label_it_) DiskLabelIndex::Iterable::Iterator(other.disk_by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      new (&disk_by_label_property_it_) DiskLabelPropertyIndex::Iterable::Iterator(other.disk_by_label_property_it_);
      break;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator=(const VerticesIterable::Iterator &other) {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(other.all_it_);
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(other.in_memory_by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_by_label_property_it_)
          InMemoryLabelPropertyIndex::Iterable::Iterator(other.in_memory_by_label_property_it_);
      break;
    case Type::BY_LABEL_ON_DISK:
      new (&disk_by_label_it_) DiskLabelIndex::Iterable::Iterator(other.disk_by_label_it_);
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      new (&disk_by_label_property_it_) DiskLabelPropertyIndex::Iterable::Iterator(other.disk_by_label_property_it_);
      break;
  }
  return *this;
}

VerticesIterable::Iterator::Iterator(VerticesIterable::Iterator &&other) noexcept : type_(other.type_) {
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(std::move(other.all_it_));
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(std::move(other.in_memory_by_label_it_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_by_label_property_it_)
          InMemoryLabelPropertyIndex::Iterable::Iterator(std::move(other.in_memory_by_label_property_it_));
      break;
    case Type::BY_LABEL_ON_DISK:
      new (&disk_by_label_it_) DiskLabelIndex::Iterable::Iterator(std::move(other.disk_by_label_it_));
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      new (&disk_by_label_property_it_)
          DiskLabelPropertyIndex::Iterable::Iterator(std::move(other.disk_by_label_property_it_));
      break;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator=(VerticesIterable::Iterator &&other) noexcept {
  Destroy();
  type_ = other.type_;
  switch (other.type_) {
    case Type::ALL:
      new (&all_it_) AllVerticesIterable::Iterator(std::move(other.all_it_));
      break;
    case Type::BY_LABEL_IN_MEMORY:
      new (&in_memory_by_label_it_) InMemoryLabelIndex::Iterable::Iterator(std::move(other.in_memory_by_label_it_));
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      new (&in_memory_by_label_property_it_)
          InMemoryLabelPropertyIndex::Iterable::Iterator(std::move(other.in_memory_by_label_property_it_));
      break;
    case Type::BY_LABEL_ON_DISK:
      new (&disk_by_label_it_) DiskLabelIndex::Iterable::Iterator(std::move(other.disk_by_label_it_));
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      new (&disk_by_label_property_it_)
          DiskLabelPropertyIndex::Iterable::Iterator(std::move(other.disk_by_label_property_it_));
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
    case Type::BY_LABEL_IN_MEMORY:
      in_memory_by_label_it_.InMemoryLabelIndex::Iterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      in_memory_by_label_property_it_.InMemoryLabelPropertyIndex::Iterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_ON_DISK:
      disk_by_label_it_.DiskLabelIndex::Iterable::Iterator::~Iterator();
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      disk_by_label_property_it_.DiskLabelPropertyIndex::Iterable::Iterator::~Iterator();
      break;
  }
}

VertexAccessor VerticesIterable::Iterator::operator*() const {
  switch (type_) {
    case Type::ALL:
      return *all_it_;
    case Type::BY_LABEL_IN_MEMORY:
      return *in_memory_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      return *in_memory_by_label_property_it_;
    case Type::BY_LABEL_ON_DISK:
      return *disk_by_label_it_;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      return *disk_by_label_property_it_;
  }
}

VerticesIterable::Iterator &VerticesIterable::Iterator::operator++() {
  switch (type_) {
    case Type::ALL:
      ++all_it_;
      break;
    case Type::BY_LABEL_IN_MEMORY:
      ++in_memory_by_label_it_;
      break;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      ++in_memory_by_label_property_it_;
      break;
    case Type::BY_LABEL_ON_DISK:
      ++disk_by_label_it_;
      break;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      ++disk_by_label_property_it_;
      break;
  }
  return *this;
}

bool VerticesIterable::Iterator::operator==(const Iterator &other) const {
  switch (type_) {
    case Type::ALL:
      return all_it_ == other.all_it_;
    case Type::BY_LABEL_IN_MEMORY:
      return in_memory_by_label_it_ == other.in_memory_by_label_it_;
    case Type::BY_LABEL_PROPERTY_IN_MEMORY:
      return in_memory_by_label_property_it_ == other.in_memory_by_label_property_it_;
    case Type::BY_LABEL_ON_DISK:
      return disk_by_label_it_ == other.disk_by_label_it_;
    case Type::BY_LABEL_PROPERTY_ON_DISK:
      return disk_by_label_property_it_ == other.disk_by_label_property_it_;
  }
}

Storage::Accessor::Accessor(Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode)
    : storage_(storage),
      // The lock must be acquired before creating the transaction object to
      // prevent freshly created transactions from dangling in an active state
      // during exclusive operations.
      storage_guard_(storage_->main_lock_),
      transaction_(storage->CreateTransaction(isolation_level, storage_mode)),
      is_transaction_active_(true) {}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      storage_guard_(std::move(other.storage_guard_)),
      transaction_(std::move(other.transaction_)),
      commit_timestamp_(other.commit_timestamp_),
      is_transaction_active_(other.is_transaction_active_) {
  // Don't allow the other accessor to abort our transaction in destructor.
  other.is_transaction_active_ = false;
  other.commit_timestamp_.reset();
}

bool Storage::LockPath() {
  auto locker_accessor = global_locker_.Access();
  return locker_accessor.AddPath(config_.durability.storage_directory);
}

bool Storage::UnlockPath() {
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

// this should be handled on an above level of abstraction
const std::string &Storage::LabelToName(LabelId label) const { return name_id_mapper_.IdToName(label.AsUint()); }

// this should be handled on an above level of abstraction
const std::string &Storage::PropertyToName(PropertyId property) const {
  return name_id_mapper_.IdToName(property.AsUint());
}

// this should be handled on an above level of abstraction
const std::string &Storage::EdgeTypeToName(EdgeTypeId edge_type) const {
  return name_id_mapper_.IdToName(edge_type.AsUint());
}

// this should be handled on an above level of abstraction
LabelId Storage::NameToLabel(const std::string_view name) { return LabelId::FromUint(name_id_mapper_.NameToId(name)); }

// this should be handled on an above level of abstraction
PropertyId Storage::NameToProperty(const std::string_view name) {
  return PropertyId::FromUint(name_id_mapper_.NameToId(name));
}

// this should be handled on an above level of abstraction
EdgeTypeId Storage::NameToEdgeType(const std::string_view name) {
  return EdgeTypeId::FromUint(name_id_mapper_.NameToId(name));
}

const std::string &Storage::Accessor::LabelToName(LabelId label) const { return storage_->LabelToName(label); }

const std::string &Storage::Accessor::PropertyToName(PropertyId property) const {
  return storage_->PropertyToName(property);
}

const std::string &Storage::Accessor::EdgeTypeToName(EdgeTypeId edge_type) const {
  return storage_->EdgeTypeToName(edge_type);
}

LabelId Storage::Accessor::NameToLabel(const std::string_view name) { return storage_->NameToLabel(name); }

PropertyId Storage::Accessor::NameToProperty(const std::string_view name) { return storage_->NameToProperty(name); }

EdgeTypeId Storage::Accessor::NameToEdgeType(const std::string_view name) { return storage_->NameToEdgeType(name); }

std::optional<uint64_t> Storage::Accessor::GetTransactionId() const {
  if (is_transaction_active_) {
    return transaction_.transaction_id.load(std::memory_order_acquire);
  }
  return {};
}

void Storage::Accessor::AdvanceCommand() { ++transaction_.command_id; }

}  // namespace memgraph::storage
