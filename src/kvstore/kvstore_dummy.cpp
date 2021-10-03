// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "kvstore/kvstore.hpp"

#include "utils/file.hpp"
#include "utils/logging.hpp"

namespace kvstore {

struct KVStore::impl {};

KVStore::KVStore(std::filesystem::path storage) {}

KVStore::~KVStore() {}

bool KVStore::Put(const std::string &key, const std::string &value) {
  LOG_FATAL("Unsupported operation (KVStore::Put) -- this is a dummy kvstore");
}

bool KVStore::PutMultiple(const std::map<std::string, std::string> &items) {
  LOG_FATAL(
      "Unsupported operation (KVStore::PutMultiple) -- this is a "
      "dummy kvstore");
}

std::optional<std::string> KVStore::Get(const std::string &key) const noexcept {
  LOG_FATAL("Unsupported operation (KVStore::Get) -- this is a dummy kvstore");
}

bool KVStore::Delete(const std::string &key) {
  LOG_FATAL("Unsupported operation (KVStore::Delete) -- this is a dummy kvstore");
}

bool KVStore::DeleteMultiple(const std::vector<std::string> &keys) {
  LOG_FATAL(
      "Unsupported operation (KVStore::DeleteMultiple) -- this is "
      "a dummy kvstore");
}

bool KVStore::DeletePrefix(const std::string &prefix) {
  LOG_FATAL(
      "Unsupported operation (KVStore::DeletePrefix) -- this is a "
      "dummy kvstore");
}

bool KVStore::PutAndDeleteMultiple(const std::map<std::string, std::string> &items,
                                   const std::vector<std::string> &keys) {
  LOG_FATAL(
      "Unsupported operation (KVStore::PutAndDeleteMultiple) -- this is a "
      "dummy kvstore");
}

// iterator

struct KVStore::iterator::impl {};

KVStore::iterator::iterator(const KVStore *kvstore, const std::string &prefix, bool at_end) : pimpl_(new impl()) {}

KVStore::iterator::iterator(KVStore::iterator &&other) { pimpl_ = std::move(other.pimpl_); }

KVStore::iterator::~iterator() {}

KVStore::iterator &KVStore::iterator::operator=(KVStore::iterator &&other) {
  pimpl_ = std::move(other.pimpl_);
  return *this;
}

KVStore::iterator &KVStore::iterator::operator++() {
  LOG_FATAL(
      "Unsupported operation (&KVStore::iterator::operator++) -- "
      "this is a dummy kvstore");
}

bool KVStore::iterator::operator==(const iterator &other) const { return true; }

bool KVStore::iterator::operator!=(const iterator &other) const { return false; }

KVStore::iterator::reference KVStore::iterator::operator*() {
  LOG_FATAL(
      "Unsupported operation (KVStore::iterator::operator*)-- this "
      "is a dummy kvstore");
}

KVStore::iterator::pointer KVStore::iterator::operator->() {
  LOG_FATAL(
      "Unsupported operation (KVStore::iterator::operator->) -- "
      "this is a dummy kvstore");
}

void KVStore::iterator::SetInvalid() {}

bool KVStore::iterator::IsValid() { return false; }

size_t KVStore::Size(const std::string &prefix) { return 0; }

bool KVStore::CompactRange(const std::string &begin_prefix, const std::string &end_prefix) {
  LOG_FATAL(
      "Unsupported operation (KVStore::Compact) -- this is a "
      "dummy kvstore");
}

}  // namespace kvstore
