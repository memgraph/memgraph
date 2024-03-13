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

#include <rocksdb/compression_type.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>

namespace memgraph::storage {

PDS *PDS::ptr_ = nullptr;

PDS::PDS(std::filesystem::path root)
    : kvstore_{root / "pds", std::invoke([]() {
                 rocksdb::Options options;
                 rocksdb::BlockBasedTableOptions table_options;
                 table_options.block_cache = rocksdb::NewLRUCache(128 * 1024 * 1024);
                 table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(sizeof(storage::Gid)));
                 table_options.optimize_filters_for_memory = false;
                 table_options.enable_index_compression = false;
                 options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
                 options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(sizeof(storage::Gid)));
                 options.max_background_jobs = 4;
                 options.enable_pipelined_write = true;
                 options.avoid_unnecessary_blocking_io = true;

                 options.create_if_missing = true;

                 options.use_direct_io_for_flush_and_compaction = true;
                 options.use_direct_reads = true;

                 //  options.compression = rocksdb::kLZ4HCCompression;
                 return options;
               })} {}

}  // namespace memgraph::storage
