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

#include "storage/v2/property_disk_store.hpp"

#include <cstdint>
#include <memory>
#include <set>
#include <sstream>

#include <rocksdb/cache.h>
#include <rocksdb/compression_type.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/iterator.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/endian.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

PDS *PDS::ptr_ = nullptr;

std::string PDS::ToKey(Gid gid, PropertyId pid) {
  std::string key(sizeof(gid) + sizeof(pid), '\0');
  *(uint64_t *)key.data() = *reinterpret_cast<uint64_t *>(&gid);
  *(uint32_t *)&key[sizeof(gid)] = *reinterpret_cast<uint32_t *>(&pid);
  // memcpy(key.data(), &gid, sizeof(gid));
  // memcpy(&key[sizeof(gid)], &pid, sizeof(pid));
  return key;
}

std::string PDS::ToKey2(Gid gid, PropertyId pid) {
  std::string key(16, '\0');
  key[0] = 1;
  key[1] = 1;
  key[2] = 1;
  key[3] = 1;
  *(uint32_t *)&key[4] = *reinterpret_cast<uint32_t *>(&pid);
  *(uint64_t *)&key[8] = *reinterpret_cast<uint64_t *>(&gid);
  // memcpy(&key[4], &pid, sizeof(pid));
  // memcpy(&key[8], &gid, sizeof(gid));
  return key;
}

std::string PDS::Key2Key2(std::string_view key) { return ToKey2(ToGid(key), ToPid(key)); }

std::string PDS::ToPrefix(Gid gid) {
  std::string key(sizeof(gid), '\0');
  *(uint64_t *)&key[0] = *reinterpret_cast<uint64_t *>(&gid);
  // memcpy(key.data(), &gid, sizeof(gid));a_pid < b_pid
  return key;
}

std::string PDS::ToPrefix2(PropertyId pid) {
  std::string key(8, '\0');
  key[0] = 1;
  key[1] = 1;
  key[2] = 1;
  key[3] = 1;
  // memcpy(&key[4], &pid, sizeof(pid));
  *(uint32_t *)&key[4] = *reinterpret_cast<uint32_t *>(&pid);
  return key;
}

Gid PDS::ToGid(std::string_view sv) {
  // gid = *((uint64_t *)sv.data());
  return Gid::FromUint(*(uint64_t *)&sv[0]);
}

Gid PDS::ToGid2(std::string_view sv) {
  // memcpy(&gid, &sv[8], sizeof(gid));
  return Gid::FromUint(*(uint64_t *)&sv[8]);
}

PropertyId PDS::ToPid(std::string_view sv) {
  // memcpy(&pid, &sv[8], sizeof(pid));
  return PropertyId::FromUint(*(uint32_t *)&sv[8]);
}

std::string PDS::ToStr(const PropertyValue &pv) {
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

// PDSWrapper PDS::GetItr(Gid gid, PropertyId pid) {
//   // auto element = kvstore_.GetItr(ToKey2(gid, pid), r_options);
//   // return {std::move(element)};
//   return {};
// }

std::optional<PropertyValue> PDS::Get(Gid gid, PropertyId pid, PdsItr *itr) {
  if (itr == nullptr) {
    auto res = kvstore_.Get(ToKey2(gid, pid));
    if (res) {
      return ToPV(*res);
    }
    return {};
  }

  // auto newpref = ToPrefix2(pid);
  // // auto newpref = ToKey2(gid, pid);
  // // itr_ = kvstore_.GetItr(newpref, r_options);
  // if (pid_ != pid || !itr_ || !itr_->Valid()) {
  //   delete itr_;
  //   itr_ = kvstore_.GetItr(newpref, r_options);
  //   prefix_ = std::move(newpref);

  //   if (!itr_) return {};
  // }
  // // const auto element = kvstore_.Get(ToKey2(gid, pid), r_options);
  // else
  //   itr_->Next();
  // if (itr_ && itr_->Valid()) {
  //   return ToPV(itr_->value().ToStringView());
  // }

  auto clean = [&]() {
    // std::cout << "clean" << std::endl;
    delete itr->itr_;
    itr->itr_ = nullptr;
    itr->gid_ = Gid::FromUint(0);
    itr->pid_ = PropertyId::FromUint(0);
  };

  auto get_itr = [&]() {
    clean();
    // std::cout << "get_itr" << std::endl;
    itr->itr_ = kvstore_.GetItr(ToPrefix2(pid), r_options);
    if (!itr->itr_) return;
    itr->pid_ = pid;
    itr->gid_ = ToGid2(itr->itr_->key().ToStringView());
  };

  // std::cout << "pid: " << pid_.AsUint() << " new: " << pid.AsUint() << std::endl;
  // std::cout << "gid: " << gid_.AsUint() << " new: " << gid.AsUint() << std::endl;

  if (itr->pid_ == pid) {
    if (itr->gid_ == gid) {
      // std::cout << "val " << __LINE__ << std::endl;
      return itr->pv_;
    }
    if (itr->itr_) {
      if (itr->gid_ < gid) {
        // std::cout << "next1" << std::endl;
        itr->itr_->Next();
        if (!itr->itr_->Valid()) {
          clean();
          // std::cout << "nullopt " << __LINE__ << std::endl;
          return {};
        }
        itr->gid_ = ToGid2(itr->itr_->key().ToStringView());
        // std::cout << "gid: " << gid_.AsUint() << std::endl;
      }
      if (itr->gid_ == gid) {
        if (itr->itr_->Valid()) {
          itr->pv_ = ToPV(itr->itr_->value().ToStringView());
          // std::cout << "val " << __LINE__ << std::endl;
          return itr->pv_;
        }
      }
      // clean();
      // return {};
      // // Different gid; but save for later...
    }
  }

  // HACK to get iterator if at the beginning, not necessarily true.
  if (gid.AsUint() > 10) {
    // std::cout << "get" << std::endl;
    clean();
    auto res = kvstore_.Get(ToKey2(gid, pid));
    if (res) {
      itr->pid_ = pid;
      itr->gid_ = gid;
      itr->pv_ = ToPV(*res);
      // std::cout << "val " << __LINE__ << std::endl;
      return itr->pv_;
    }
    // std::cout << "nullopt " << __LINE__ << std::endl;
    return {};
  }

  get_itr();
  if (!itr->itr_) {
    clean();
    // std::cout << "nullopt " << __LINE__ << std::endl;
    return {};
  }
  while (itr->gid_ < gid) {
    // std::cout << "next2" << std::endl;
    itr->itr_->Next();
    if (!itr->itr_->Valid()) {
      clean();
      // std::cout << "nullopt " << __LINE__ << std::endl;
      return {};
    }
    itr->gid_ = ToGid2(itr->itr_->key().ToStringView());
    // std::cout << "gid: " << gid_.AsUint() << std::endl;
  }
  if (itr->gid_ == gid) {
    if (itr->itr_ && itr->itr_->Valid()) {
      itr->pv_ = ToPV(itr->itr_->value().ToStringView());
      // std::cout << "val " << __LINE__ << std::endl;
      return itr->pv_;
    }
    clean();
    // std::cout << "nullopt " << __LINE__ << std::endl;
    return {};
  }

  clean();
  // std::cout << "nullopt " << __LINE__ << std::endl;
  return std::nullopt;
}

size_t PDS::GetSize(Gid gid, PropertyId pid, PdsItr * /*itr*/) {
  const auto element = kvstore_.Get(ToKey2(gid, pid), r_options);
  if (element) {
    return element->size();
  }
  return 0;
}

std::map<PropertyId, PropertyValue> PDS::Get(Gid gid, PdsItr * /*itr*/) {
  std::map<PropertyId, PropertyValue> res;
  const auto prf = ToPrefix(gid);
  auto itr = kvstore_.begin(prf, r_options);
  auto end = kvstore_.end(prf, r_options);
  auto map_itr = res.begin();
  for (; itr != end; ++itr) {
    if (!itr.IsValid()) continue;
    map_itr = res.insert_or_assign(map_itr, ToPid(itr->first), ToPV(itr->second));
  }
  return res;
}

bool PDS::Set(Gid gid, PropertyId pid, const PropertyValue &pv, PdsItr * /*itr*/) {
  if (pv.IsNull()) {
    return kvstore_.Delete(ToKey(gid, pid), w_options);
  }
  // std::cout << "set " << pid.AsUint() << " - " << gid.AsUint() << std::endl;
  const bool res = kvstore_.Put(ToKey2(gid, pid), ToStr(pv), w_options);
  return kvstore_.Put(ToKey(gid, pid), ToStr(pv), w_options) && res;
}

void PDS::Clear(Gid gid, PdsItr * /*itr*/) {
  auto itr = kvstore_.begin(ToPrefix(gid), r_options);
  auto end = kvstore_.end(ToPrefix(gid), r_options);
  std::vector<std::string> keys, keys2;
  for (; itr != end; ++itr) {
    if (!itr.IsValid()) continue;
    keys.emplace_back(itr->first);
    keys2.emplace_back(Key2Key2(itr->first));
  }
  kvstore_.DeleteMultiple(keys, w_options);
  kvstore_.DeleteMultiple(keys2, w_options);
}

bool PDS::Has(Gid gid, PropertyId pid, PdsItr * /*itr*/) { return kvstore_.Size(ToKey(gid, pid), r_options) != 0; }

class PdsKeyComparator : public ::rocksdb::Comparator {
 public:
  ~PdsKeyComparator() {}

  PdsKeyComparator() {}

  // Three-way comparison function:
  // if a < b: negative result
  // if a > b: positive result
  // else: zero result
  virtual int Compare(const rocksdb::Slice &a, const rocksdb::Slice &b) const override {
    // Very specialized to the keys we currently have

    // TODO: Move to PDS
    auto ToPid2 = [](auto sv) { return PropertyId::FromUint(*(uint32_t *)&sv[4]); };

    if (a.size() == b.size()) {
      if (a.size() == 12) {  // Key1

        const auto a_gid = PDS::ToGid(a.ToStringView());  //*(uint64_t *)a.data();
        const auto b_gid = PDS::ToGid(b.ToStringView());  //*(uint64_t *)b.data();
        if (a_gid == b_gid) {
          // Same Gid, check Pid
          const auto a_pid = PDS::ToPid(a.ToStringView());  //*(uint32_t *)&a.data()[8];
          const auto b_pid = PDS::ToPid(b.ToStringView());  //*(uint32_t *)&b.data()[8];
          return int(a_pid != b_pid) * (1 - 2 * int(a_pid < b_pid));
          // if (a_pid == b_pid) return 0;
          // return a_pid < b_pid ? -1 : 1;
        }
        // Diff Gid, just check it
        return (1 - 2 * int(a_gid < b_gid));
        // return a_gid < b_gid ? -1 : 1;
      }
      if (a.size() == 16) {                           // Key2
        const auto a_pid = ToPid2(a.ToStringView());  //*(uint64_t *)a.data();
        const auto b_pid = ToPid2(b.ToStringView());  //*(uint64_t *)b.data();
        if (a_pid == b_pid) {
          // Same Pid, check Gid
          const auto a_gid = PDS::ToGid2(a.ToStringView());  //*(uint64_t *)&a.data()[8];
          const auto b_gid = PDS::ToGid2(b.ToStringView());  //*(uint64_t *)&b.data()[8];
          return int(a_gid != b_gid) * (1 - 2 * int(a_gid < b_gid));
          // if (a_gid == b_gid) return 0;
          // return a_gid < b_gid ? -1 : 1;
        }
        // Diff Pid, just check it
        return (1 - 2 * int(a_pid < b_pid));
        // return a_pid < b_pid ? -1 : 1;
      }
      LOG_FATAL("Unknown PDS key.");
    }
    // Different keys, just check size
    return (1 - 2 * int(a.size() < b.size()));
    // return a.size() < b.size() ? -1 : 1;
  };

  virtual bool Equal(const rocksdb::Slice &a, const rocksdb::Slice &b) const override { return Compare(a, b) == 0; }

  // Ignore the following methods for now:
  virtual const char *Name() const { return "PdsKeyComparator"; };

  virtual void FindShortestSeparator(std::string *, const rocksdb::Slice &) const override{};

  virtual void FindShortSuccessor(std::string *) const {};
};

const rocksdb::Comparator *pds_cmp() {
  static PdsKeyComparator cmp;
  return &cmp;
}

PDS::PDS(std::filesystem::path root)
    : kvstore_{root / "pds", std::invoke([]() {
                 rocksdb::Options options;
                 rocksdb::BlockBasedTableOptions table_options;
                 table_options.block_cache = rocksdb::NewLRUCache(256 * 1024 * 1024);
                 table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(8));
                 table_options.optimize_filters_for_memory = false;
                 table_options.enable_index_compression = false;
                 //  table_options.prepopulate_block_cache =
                 //      rocksdb::BlockBasedTableOptions::PrepopulateBlockCache::kFlushOnly;
                 options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
                 options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(8));
                 options.max_background_jobs = 4;
                 options.enable_pipelined_write = true;
                 options.avoid_unnecessary_blocking_io = true;

                 options.create_if_missing = true;

                 options.use_direct_io_for_flush_and_compaction = true;
                 options.use_direct_reads = true;

                 options.max_open_files = 256;

                 //  options.OptimizeLevelStyleCompaction(128 * 1024 * 1024);
                 //  options.memtable_factory.reset(new rocksdb::SkipListFactory(4));
                 options.allow_concurrent_memtable_write = false;
                 options.memtable_factory.reset(rocksdb::NewHashLinkListRepFactory());

                 options.comparator = pds_cmp();

                 return options;
               })} {
  // Setup read options
  r_options.async_io = true;
  r_options.adaptive_readahead = true;
  r_options.prefix_same_as_start = true;
  // r_options.read_tier = rocksdb::kMemtableTier;

  // Setup write options
  w_options.disableWAL = true;
}

}  // namespace memgraph::storage
