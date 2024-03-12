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
#include <set>
#include <sstream>

#include "kvstore/kvstore.hpp"
#include "slk/streams.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"

#include "slk/serialization.hpp"
#include "storage/v2/replication/slk.hpp"

namespace memgraph::storage {

class PDS {
 public:
  static void Init(std::filesystem::path root) {
    if (ptr_ == nullptr) ptr_ = new PDS(root);
  }

  static PDS *get() {
    if (ptr_ == nullptr) {
      ptr_ = new PDS("/tmp");
    }
    return ptr_;
  }

  static std::string ToKey(Gid gid, PropertyId pid) {
    std::string key(sizeof(gid) + sizeof(pid), '\0');
    memcpy(key.data(), &gid, sizeof(gid));
    memcpy(&key[sizeof(gid)], &pid, sizeof(pid));
    return key;
  }

  static std::string ToPrefix(Gid gid) {
    std::string key(sizeof(gid), '\0');
    memcpy(key.data(), &gid, sizeof(gid));
    return key;
  }

  static Gid ToGid(std::string_view sv) {
    uint64_t gid;
    gid = *((uint64_t *)sv.data());
    return Gid::FromUint(gid);
  }

  static PropertyId ToPid(std::string_view sv) {
    uint32_t pid;
    pid = *((uint32_t *)&sv[sizeof(Gid)]);
    return PropertyId::FromUint(pid);
  }

  static PropertyValue ToPV(std::string_view sv) {
    PropertyValue pv;
    slk::Reader reader((const uint8_t *)sv.data(), sv.size());
    slk::Load(&pv, &reader);
    return pv;
  }

  static std::string ToStr(const PropertyValue &pv) {
    std::string val{};
    slk::Builder builder([&val](const uint8_t *data, size_t size, bool /*have_more*/) {
      const auto old_size = val.size();
      val.resize(old_size + size);
      memcpy(&val[old_size], data, size);
    });
    slk::Save(pv, &builder);
    builder.Finalize();
    return val;
  }

  std::optional<PropertyValue> Get(Gid gid, PropertyId pid) {
    const auto element = kvstore_.Get(ToKey(gid, pid));
    if (element) {
      return ToPV(*element);
    }
    return std::nullopt;
  }

  size_t GetSize(Gid gid, PropertyId pid) {
    const auto element = kvstore_.Get(ToKey(gid, pid));
    if (element) {
      return element->size();
    }
    return 0;
  }

  std::map<PropertyId, PropertyValue> Get(Gid gid) {
    std::map<PropertyId, PropertyValue> res;
    auto itr = kvstore_.begin(ToPrefix(gid));
    auto end = kvstore_.end(ToPrefix(gid));
    for (; itr != end; ++itr) {
      if (!itr.IsValid()) continue;
      res[ToPid(itr->first)] = ToPV(itr->second);
    }
    return res;
  }

  auto Set(Gid gid, PropertyId pid, const PropertyValue &pv) {
    if (pv.IsNull()) {
      return kvstore_.Delete(ToKey(gid, pid));
    }
    return kvstore_.Put(ToKey(gid, pid), ToStr(pv));
  }

  void Clear(Gid gid) { kvstore_.DeletePrefix(ToPrefix(gid)); }

  bool Has(Gid gid, PropertyId pid) { return kvstore_.Size(ToKey(gid, pid)) != 0; }

  // kvstore::KVStore::iterator Itr() {}

 private:
  PDS(std::filesystem::path root) : kvstore_{root / "pds"} {}
  kvstore::KVStore kvstore_;
  static PDS *ptr_;
};

}  // namespace memgraph::storage
