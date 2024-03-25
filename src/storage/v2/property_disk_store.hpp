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

#pragma once

#include <cstdint>
#include <cstring>
#include <json/json.hpp>
#include <map>

#include "kvstore/kvstore.hpp"
#include "slk/streams.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

#include "slk/serialization.hpp"
#include "storage/v2/replication/slk.hpp"

namespace memgraph::storage {

struct PdsItr {
  rocksdb::Iterator *itr_{};
  PropertyId pid_;
  Gid gid_;
  PropertyValue pv_{};

  PdsItr() {
    pid_ = PropertyId::FromUint(0);
    gid_ = Gid::FromUint(0);
  }
};

inline PropertyValue ToPV(std::string_view sv) {
  PropertyValue pv;
  slk::Reader reader((const uint8_t *)sv.data(), sv.size());
  slk::Load(&pv, &reader);
  return pv;
}

// class PDSWrapper {
//  public:
//   PDSWrapper() : itr_{nullptr}, pv_{nullptr} {}
//   PDSWrapper(std::unique_ptr<rocksdb::Iterator> itr) : itr_{std::move(itr)}, pv_{nullptr} {}
//   PDSWrapper(PropertyValue pv) : itr_{nullptr}, pv_{std::make_unique<PropertyValue>(std::move(pv))} {}

//   void reset() { itr_.reset(); }

//   operator bool() const { return (itr_ != nullptr && itr_->Valid()) || pv_ != nullptr; }
//   PropertyValue operator*() const {
//     if (itr_ != nullptr && itr_->Valid()) return ToPV(itr_->value().ToStringView());
//     if (pv_) return *pv_;
//     return {};
//   }

//  private:
//   std::unique_ptr<rocksdb::Iterator> itr_;
//   std::unique_ptr<PropertyValue> pv_;
// };

class PDS {
 public:
  static void Init(std::filesystem::path root) {
    if (ptr_ == nullptr) ptr_ = new PDS(root);
  }

  static void Deinit() { delete ptr_; }

  static PDS *get() {
    if (ptr_ == nullptr) {
      ptr_ = new PDS("/tmp");
    }
    return ptr_;
  }

  static std::string ToKey(Gid gid, PropertyId pid);

  static std::string ToKey2(Gid gid, PropertyId pid);

  static std::string Key2Key2(std::string_view key);

  static std::string ToPrefix(Gid gid);

  static std::string ToPrefix2(PropertyId pid);

  static Gid ToGid(std::string_view sv);

  static Gid ToGid2(std::string_view sv);

  static PropertyId ToPid(std::string_view sv);

  static std::string ToStr(const PropertyValue &pv);

  // PDSWrapper GetItr(Gid gid, PropertyId pid);

  std::optional<PropertyValue> Get(Gid gid, PropertyId pid, PdsItr *itr = nullptr);

  size_t GetSize(Gid gid, PropertyId pid, PdsItr *itr = nullptr);

  std::map<PropertyId, PropertyValue> Get(Gid gid, PdsItr *itr = nullptr);

  bool Set(Gid gid, PropertyId pid, const PropertyValue &pv, PdsItr *itr = nullptr);

  void Clear(Gid gid, PdsItr *itr = nullptr);

  bool Has(Gid gid, PropertyId pid, PdsItr *itr = nullptr);

  // kvstore::KVStore::iterator Itr() {}

 private:
  PDS(std::filesystem::path root);
  kvstore::KVStore kvstore_;
  rocksdb::ReadOptions r_options{};
  rocksdb::WriteOptions w_options{};
  static PDS *ptr_;
};

}  // namespace memgraph::storage
