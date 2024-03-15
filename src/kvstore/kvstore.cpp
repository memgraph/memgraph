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

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace memgraph::kvstore {

struct KVStore::impl {
  std::filesystem::path storage;
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::Options options;
};

KVStore::KVStore(std::filesystem::path storage) : pimpl_(std::make_unique<impl>()) {
  pimpl_->storage = storage;
  if (!utils::EnsureDir(pimpl_->storage))
    throw KVStoreError("Folder for the key-value store " + pimpl_->storage.string() + " couldn't be initialized!");
  pimpl_->options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(pimpl_->options, storage.c_str(), &db);
  if (!s.ok())
    throw KVStoreError("RocksDB couldn't be initialized inside " + storage.string() + " -- " +
                       std::string(s.ToString()));
  pimpl_->db.reset(db);
}

KVStore::KVStore(std::filesystem::path storage, rocksdb::Options db_options) : pimpl_(std::make_unique<impl>()) {
  pimpl_->storage = storage;
  pimpl_->options = std::move(db_options);
  if (!utils::EnsureDir(pimpl_->storage))
    throw KVStoreError("Folder for the key-value store " + pimpl_->storage.string() + " couldn't be initialized!");
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(pimpl_->options, storage.c_str(), &db);
  if (!s.ok())
    throw KVStoreError("RocksDB couldn't be initialized inside " + storage.string() + " -- " +
                       std::string(s.ToString()));
  pimpl_->db.reset(db);
}

KVStore::~KVStore() {
  if (pimpl_ == nullptr) return;
  spdlog::debug("Destroying KVStore at {}", pimpl_->storage.string());
  const auto sync = pimpl_->db->SyncWAL();
  if (!sync.ok()) spdlog::error("KVStore sync failed!");
  const auto close = pimpl_->db->Close();
  if (!close.ok()) spdlog::error("KVStore close failed!");
}

KVStore::KVStore(KVStore &&other) { pimpl_ = std::move(other.pimpl_); }

KVStore &KVStore::operator=(KVStore &&other) {
  pimpl_ = std::move(other.pimpl_);
  return *this;
}

bool KVStore::Put(std::string_view key, std::string_view value, rocksdb::WriteOptions options) {
  auto s = pimpl_->db->Put(options, key, value);
  return s.ok();
}

bool KVStore::PutMultiple(const std::map<std::string, std::string> &items, rocksdb::WriteOptions options) {
  rocksdb::WriteBatch batch;
  for (const auto &item : items) {
    batch.Put(item.first, item.second);
  }
  auto s = pimpl_->db->Write(options, &batch);
  return s.ok();
}

std::optional<std::string> KVStore::Get(std::string_view key, rocksdb::ReadOptions options) const noexcept {
  std::string value;
  auto s = pimpl_->db->Get(options, key, &value);
  if (!s.ok()) return std::nullopt;
  return value;
}

bool KVStore::Delete(std::string_view key, rocksdb::WriteOptions options) {
  auto s = pimpl_->db->Delete(options, key);
  return s.ok();
}

bool KVStore::DeleteMultiple(const std::vector<std::string> &keys, rocksdb::WriteOptions options) {
  rocksdb::WriteBatch batch;
  for (const auto &key : keys) {
    batch.Delete(key);
  }
  auto s = pimpl_->db->Write(options, &batch);
  return s.ok();
}

bool KVStore::DeletePrefix(const std::string &prefix, rocksdb::WriteOptions write_options,
                           rocksdb::ReadOptions read_options) {
  std::unique_ptr<rocksdb::Iterator> iter = std::unique_ptr<rocksdb::Iterator>(pimpl_->db->NewIterator(read_options));
  for (iter->Seek(prefix); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    if (!pimpl_->db->Delete(write_options, iter->key()).ok()) return false;
  }
  return true;
}

bool KVStore::PutAndDeleteMultiple(const std::map<std::string, std::string> &items,
                                   const std::vector<std::string> &keys, rocksdb::WriteOptions options) {
  rocksdb::WriteBatch batch;
  for (const auto &item : items) {
    batch.Put(item.first, item.second);
  }
  for (const auto &key : keys) {
    batch.Delete(key);
  }
  auto s = pimpl_->db->Write(options, &batch);
  return s.ok();
}

// iterator

struct KVStore::iterator::impl {
  const KVStore *kvstore;
  std::string prefix;
  std::unique_ptr<rocksdb::Iterator> it;
  std::pair<std::string, std::string> disk_prop;
};

KVStore::iterator::iterator(const KVStore *kvstore, const std::string &prefix, bool at_end,
                            rocksdb::ReadOptions options)
    : pimpl_(std::make_unique<impl>()) {
  pimpl_->kvstore = kvstore;
  pimpl_->prefix = prefix;
  pimpl_->it = std::unique_ptr<rocksdb::Iterator>(pimpl_->kvstore->pimpl_->db->NewIterator(options));
  pimpl_->it->Seek(pimpl_->prefix);
  if (!pimpl_->it->Valid() || !pimpl_->it->key().starts_with(pimpl_->prefix) || at_end) pimpl_->it = nullptr;
}

KVStore::iterator::iterator(KVStore::iterator &&other) { pimpl_ = std::move(other.pimpl_); }

KVStore::iterator::~iterator() = default;

KVStore::iterator &KVStore::iterator::operator=(KVStore::iterator &&other) {
  pimpl_ = std::move(other.pimpl_);
  return *this;
}

KVStore::iterator &KVStore::iterator::operator++() {
  pimpl_->it->Next();
  if (!pimpl_->it->Valid() || !pimpl_->it->key().starts_with(pimpl_->prefix)) pimpl_->it = nullptr;
  return *this;
}

bool KVStore::iterator::operator==(const iterator &other) const {
  return pimpl_->kvstore == other.pimpl_->kvstore && pimpl_->prefix == other.pimpl_->prefix &&
         pimpl_->it == other.pimpl_->it;
}

bool KVStore::iterator::operator!=(const iterator &other) const { return !(*this == other); }

KVStore::iterator::reference KVStore::iterator::operator*() {
  pimpl_->disk_prop = {pimpl_->it->key().ToString(), pimpl_->it->value().ToString()};
  return pimpl_->disk_prop;
}

KVStore::iterator::pointer KVStore::iterator::operator->() { return &**this; }

void KVStore::iterator::SetInvalid() { pimpl_->it = nullptr; }

bool KVStore::iterator::IsValid() { return pimpl_->it != nullptr; }

// TODO(ipaljak) The complexity of the size function should be at most
//               logarithmic.
size_t KVStore::Size(const std::string &prefix, rocksdb::ReadOptions options) const {
  size_t size = 0;
  for (auto it = this->begin(prefix, options); it != this->end(prefix, options); ++it) ++size;
  return size;
}

bool KVStore::CompactRange(const std::string &begin_prefix, const std::string &end_prefix) {
  rocksdb::CompactRangeOptions options;
  rocksdb::Slice begin(begin_prefix);
  rocksdb::Slice end(end_prefix);
  auto s = pimpl_->db->CompactRange(options, &begin, &end);
  return s.ok();
}

}  // namespace memgraph::kvstore
