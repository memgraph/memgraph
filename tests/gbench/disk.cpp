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

#include <benchmark/benchmark.h>

#include <fmt/core.h>
#include <rocksdb/configurable.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/transaction_db.h>

#include <stdexcept>
#include <string>
#include <utils/rocksdb_serialization.hpp>
#include "storage/v2/id_types.hpp"

using namespace memgraph;

auto GenLabels(int n) {
  std::vector<storage::LabelId> ids;
  ids.reserve(n);
  for (int i = 0; i < n; ++i) ids.push_back(storage::LabelId::FromInt(i));
  return ids;
}

auto GenLabelsSerialized(int n) {
  std::string res;
  for (int i = 0; i < n; ++i) {
    res += std::to_string(i) + ",";
  }
  return res.substr(0, res.size() - 1);
}

auto GenLabelsSerialized2(int n) {
  std::string res;
  for (int i = 0; i < n; ++i) {
    res += std::to_string(i) + ",";
  }
  return res.substr(0, res.size() - 1);
}

static void TransformIDsToString(benchmark::State &state) {
  const auto labels = GenLabels(state.range());
  for (auto _ : state) {
    benchmark::DoNotOptimize(utils::TransformIDsToString(labels));
  }
}

static void TransformIDsToString2(benchmark::State &state) {
  const auto labels = GenLabels(state.range());
  for (auto _ : state) {
    benchmark::DoNotOptimize(utils::TransformIDsToString(0, labels));
  }
}

static void TransformFromStringLabels(benchmark::State &state) {
  const auto labels = utils::SerializeLabels(utils::TransformIDsToString(GenLabels(state.range())));
  for (auto _ : state) {
    benchmark::DoNotOptimize(utils::TransformFromStringLabels(utils::Split(labels, ",")));
  }
}

static void TransformFromStringLabels2(benchmark::State &state) {
  const auto labels = utils::TransformIDsToString(0, GenLabels(state.range()));
  for (auto _ : state) {
    benchmark::DoNotOptimize(utils::TransformFromStringLabels(labels));
  }
}

static void ExtractGidAndCompare(benchmark::State &state) {
  const std::string key = "1,2,3,4|1234";
  const auto gid1234 = storage::Gid::FromInt(1234);
  const auto gid1234567 = storage::Gid::FromInt(1234567);
  for (auto _ : state) {
    const auto gid = utils::ExtractGidFromKey(key);
    bool res_t = gid1234.ToString() == gid;
    bool res_f = gid1234567.ToString() == gid;
    benchmark::DoNotOptimize(res_t);
    benchmark::DoNotOptimize(res_f);
  }
}

static void ExtractGidAndCompare2(benchmark::State &state) {
  const std::string key = "1,2,3,4|1234";
  const auto gid1234 = storage::Gid::FromInt(1234);
  const auto gid1234567 = storage::Gid::FromInt(1234567);
  for (auto _ : state) {
    const auto gid = utils::ExtractGidFromKey(0, key);
    bool res_t = gid1234 == gid;
    bool res_f = gid1234567 == gid;
    benchmark::DoNotOptimize(res_t);
    benchmark::DoNotOptimize(res_f);
  }
}

constexpr std::string_view dir = "/tmp/disk_bench";

static void RocksDBWrite(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  rocksdb::WriteOptions wo;
  std::string key, val;

  // Just focus on the key size
  val = "temp val";

  unsigned long i = 0;

  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    key = std::move(oss.str().substr(0, state.range()));
    state.ResumeTiming();

    auto res = db->Put(wo, key, val);
    benchmark::DoNotOptimize(res);
  }

  db->Close();
  delete db;
}

static void RocksDBWriteTx(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::TransactionDB *db = nullptr;
  auto s = rocksdb::TransactionDB::Open(options, {}, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  rocksdb::WriteOptions wo;
  auto txOptions = rocksdb::TransactionOptions{.set_snapshot = true};
  std::string key, val;

  // Just focus on the key size
  val = "temp val";

  unsigned long i = 0;

  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    key = std::move(oss.str().substr(0, state.range()));
    state.ResumeTiming();

    auto tx = db->BeginTransaction(wo, txOptions);
    auto res = tx->Put(key, val);
    auto commit = tx->Commit();

    benchmark::DoNotOptimize(tx);
    benchmark::DoNotOptimize(res);
    benchmark::DoNotOptimize(commit);
  }

  db->Close();
  delete db;
}

static void RocksDBWriteTxJustPutAndCommit(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::TransactionDB *db = nullptr;
  auto s = rocksdb::TransactionDB::Open(options, {}, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  rocksdb::WriteOptions wo;
  auto txOptions = rocksdb::TransactionOptions{.set_snapshot = true};
  std::string key, val;

  // Just focus on the key size
  val = "temp val";

  unsigned long i = 0;

  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    key = std::move(oss.str().substr(0, state.range()));
    auto tx = db->BeginTransaction(wo, txOptions);
    state.ResumeTiming();

    auto res = tx->Put(key, val);
    auto commit = tx->Commit();

    benchmark::DoNotOptimize(res);
    benchmark::DoNotOptimize(commit);
  }

  db->Close();
  delete db;
}

static void RocksDBWriteTxJustPut(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::TransactionDB *db = nullptr;
  auto s = rocksdb::TransactionDB::Open(options, {}, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  rocksdb::WriteOptions wo;
  auto txOptions = rocksdb::TransactionOptions{.set_snapshot = true};
  std::string key, val;

  // Just focus on the key size
  val = "temp val";

  unsigned long i = 0;

  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    key = std::move(oss.str().substr(0, state.range()));
    auto tx = db->BeginTransaction(wo, txOptions);
    state.ResumeTiming();

    auto res = tx->Put(key, val);
    benchmark::DoNotOptimize(res);

    state.PauseTiming();
    auto commit = tx->Commit();
    state.ResumeTiming();
  }

  db->Close();
  delete db;
}

static void RocksDBRead(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    if (i >= 10000) i = 0;
    std::string key = std::move(oss.str().substr(0, state.range()));
    state.ResumeTiming();

    auto res = db->Get(ro, key, &val);
    benchmark::DoNotOptimize(res);
  }

  db->Close();
  delete db;
}

static void RocksDBReadLH(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.readahead_size = 64U << 10U;
  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    if (i >= 10000) i = 0;
    std::string key = std::move(oss.str().substr(0, state.range()));
    state.ResumeTiming();

    auto res = db->Get(ro, key, &val);
    benchmark::DoNotOptimize(res);
  }

  db->Close();
  delete db;
}

static void RocksDBReadTx(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::TransactionDB *db = nullptr;
  auto s = rocksdb::TransactionDB::Open(options, {}, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    if (i >= 10000) i = 0;
    std::string key = std::move(oss.str().substr(0, state.range()));
    state.ResumeTiming();

    auto tx = db->BeginTransaction(wo);
    auto res = tx->Get(ro, key, &val);
    auto commit = tx->Commit();

    benchmark::DoNotOptimize(tx);
    benchmark::DoNotOptimize(res);
    benchmark::DoNotOptimize(commit);
  }

  db->Close();
  delete db;
}

static void RocksDBReadTxJustGetAndCommit(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::TransactionDB *db = nullptr;
  auto s = rocksdb::TransactionDB::Open(options, {}, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    if (i >= 10000) i = 0;
    std::string key = std::move(oss.str().substr(0, state.range()));
    auto tx = db->BeginTransaction(wo);
    state.ResumeTiming();

    auto res = tx->Get(ro, key, &val);
    auto commit = tx->Commit();

    benchmark::DoNotOptimize(res);
    benchmark::DoNotOptimize(commit);
  }

  db->Close();
  delete db;
}

static void RocksDBReadTxJustGet(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::TransactionDB *db = nullptr;
  auto s = rocksdb::TransactionDB::Open(options, {}, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    state.PauseTiming();
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    if (i >= 10000) i = 0;
    std::string key = std::move(oss.str().substr(0, state.range()));
    auto tx = db->BeginTransaction(wo);
    state.ResumeTiming();

    auto res = tx->Get(ro, key, &val);
    benchmark::DoNotOptimize(res);

    state.PauseTiming();
    auto commit = tx->Commit();
    state.ResumeTiming();
  }

  db->Close();
  delete db;
}

static void RocksDBReadSeq(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    auto itr = db->NewIterator(ro);
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
  }

  db->Close();
  delete db;
}

static void RocksDBReadSeq2(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;
  wo.disableWAL = true;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;

  rocksdb::ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.async_io = true;
  ro.optimize_multiget_for_io = true;

  for (auto _ : state) {
    auto itr = db->NewIterator(ro);
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
  }

  db->Close();
  delete db;
}

static void RocksDBReadSeq3(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;

  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_size = 1;
  table_options.index_type = rocksdb::BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
  table_options.partition_filters = true;
  table_options.no_block_cache = true;
  table_options.cache_index_and_filter_blocks = false;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  options.table_factory.reset(NewBlockBasedTableFactory(table_options));

  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;

  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    auto itr = db->NewIterator(ro);
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
  }

  db->Close();
  delete db;
}

static void RocksDBReadSeqSmall(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    auto itr = db->NewIterator(ro);
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
  }

  db->Close();
  delete db;
}

static void RocksDBReadSeqBig(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val =
      "temp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp "
      "valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp valtemp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    auto itr = db->NewIterator(ro);
    for (itr->SeekToFirst(); itr->Valid(); itr->Next()) {
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
  }

  db->Close();
  delete db;
}

static void RocksDBReadJustNext(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.max_background_jobs = 12;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp valtemp valtemp valtemp valtemp valtemp valtemp valtemp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  for (auto _ : state) {
    state.PauseTiming();
    auto itr = db->NewIterator(ro);
    itr->SeekToFirst();
    while (itr->Valid()) {
      state.ResumeTiming();

      itr->Next();

      state.PauseTiming();
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
    state.ResumeTiming();
  }

  db->Close();
  delete db;
}

static void RocksDBReadJustNext2(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.max_background_jobs = 12;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp valtemp valtemp valtemp valtemp valtemp valtemp valtemp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  ro.adaptive_readahead = true;
  ro.readahead_size = 16 << 10;

  for (auto _ : state) {
    state.PauseTiming();
    auto itr = db->NewIterator(ro);
    itr->SeekToFirst();
    while (itr->Valid()) {
      state.ResumeTiming();

      itr->Next();

      state.PauseTiming();
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
    state.ResumeTiming();
  }

  db->Close();
  delete db;
}

static void RocksDBReadJustNext3(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.max_background_jobs = 12;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp valtemp valtemp valtemp valtemp valtemp valtemp valtemp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  ro.async_io = true;
  ro.background_purge_on_iterator_cleanup = true;

  for (auto _ : state) {
    state.PauseTiming();
    auto itr = db->NewIterator(ro);
    itr->SeekToFirst();
    while (itr->Valid()) {
      state.ResumeTiming();

      itr->Next();

      state.PauseTiming();
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
    state.ResumeTiming();
  }

  db->Close();
  delete db;
}

static void RocksDBReadJustNext4(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.max_background_jobs = 12;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp valtemp valtemp valtemp valtemp valtemp valtemp valtemp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  ro.verify_checksums = false;

  for (auto _ : state) {
    state.PauseTiming();
    auto itr = db->NewIterator(ro);
    itr->SeekToFirst();
    while (itr->Valid()) {
      state.ResumeTiming();

      itr->Next();

      state.PauseTiming();
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
    state.ResumeTiming();
  }

  db->Close();
  delete db;
}

static void RocksDBReadJustNext5(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.max_background_jobs = 12;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp valtemp valtemp valtemp valtemp valtemp valtemp valtemp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
  }

  i = 0;
  rocksdb::ReadOptions ro;
  ro.optimize_multiget_for_io = true;

  for (auto _ : state) {
    state.PauseTiming();
    auto itr = db->NewIterator(ro);
    itr->SeekToFirst();
    while (itr->Valid()) {
      state.ResumeTiming();

      itr->Next();

      state.PauseTiming();
      auto res = itr->value();
      benchmark::DoNotOptimize(res);
    }
    state.ResumeTiming();
  }

  db->Close();
  delete db;
}

static void RocksDBReadMultiGet(benchmark::State &state) {
  utils::DeleteDir(dir);
  utils::EnsureDir(dir);

  rocksdb::Options options;
  options.max_background_jobs = 12;
  options.create_if_missing = true;
  rocksdb::DB *db = nullptr;
  auto s = rocksdb::DB::Open(options, dir.data(), &db);
  if (!s.ok() || db == nullptr) throw std::runtime_error("RocksDB couldn't be initialized");

  // Just focus on the key size
  std::string val = "temp valtemp valtemp valtemp valtemp valtemp valtemp valtemp val";
  rocksdb::WriteOptions wo;

  unsigned long i = 0;
  std::vector<rocksdb::Slice> keys;
  std::vector<std::string> strs;
  keys.reserve(10000);
  for (int j = 0; j < 10000; ++j) {
    std::ostringstream oss;
    oss << std::setw(state.range()) << std::setfill('0') << i++;
    auto res = db->Put(wo, oss.str().substr(0, state.range()), val);
    strs.push_back(oss.str().substr(0, state.range()));
    keys.push_back(strs.back());
  }

  i = 0;
  rocksdb::ReadOptions ro;
  ro.async_io = true;
  ro.optimize_multiget_for_io = true;

  for (auto _ : state) {
    state.PauseTiming();
    std::vector<std::string> vals;
    vals.reserve(10000);
    state.ResumeTiming();
    auto sts = db->MultiGet(ro, keys, &vals);
    benchmark::DoNotOptimize(sts);
  }

  db->Close();
  delete db;
}

// Register the function as a benchmark
// BENCHMARK(TransformIDsToString)->Unit(benchmark::kNanosecond)->Args({1});
// BENCHMARK(TransformIDsToString)->Unit(benchmark::kNanosecond)->Args({10});
// BENCHMARK(TransformIDsToString)->Unit(benchmark::kNanosecond)->Args({100});
// BENCHMARK(TransformIDsToString2)->Unit(benchmark::kNanosecond)->Args({1});
// BENCHMARK(TransformIDsToString2)->Unit(benchmark::kNanosecond)->Args({10});
// BENCHMARK(TransformIDsToString2)->Unit(benchmark::kNanosecond)->Args({100});
// BENCHMARK(TransformFromStringLabels)->Unit(benchmark::kNanosecond)->Args({1});
// BENCHMARK(TransformFromStringLabels)->Unit(benchmark::kNanosecond)->Args({10});
// BENCHMARK(TransformFromStringLabels)->Unit(benchmark::kNanosecond)->Args({100});
// BENCHMARK(TransformFromStringLabels2)->Unit(benchmark::kNanosecond)->Args({1});
// BENCHMARK(TransformFromStringLabels2)->Unit(benchmark::kNanosecond)->Args({10});
// BENCHMARK(TransformFromStringLabels2)->Unit(benchmark::kNanosecond)->Args({100});
// BENCHMARK(ExtractGidAndCompare)->Unit(benchmark::kNanosecond);
// BENCHMARK(ExtractGidAndCompare2)->Unit(benchmark::kNanosecond);

// BENCHMARK(RocksDBWrite)->Unit(benchmark::kNanosecond)->Args({4});
// BENCHMARK(RocksDBWrite)->Unit(benchmark::kNanosecond)->Args({8});
// BENCHMARK(RocksDBWrite)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBWriteTx)->Unit(benchmark::kNanosecond)->Args({4});
// BENCHMARK(RocksDBWriteTx)->Unit(benchmark::kNanosecond)->Args({8});
// BENCHMARK(RocksDBWriteTx)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBWriteTxJustPutAndCommit)->Unit(benchmark::kNanosecond)->Args({4});
// BENCHMARK(RocksDBWriteTxJustPutAndCommit)->Unit(benchmark::kNanosecond)->Args({8});
// BENCHMARK(RocksDBWriteTxJustPutAndCommit)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBWriteTxJustPut)->Unit(benchmark::kNanosecond)->Args({4});
// BENCHMARK(RocksDBWriteTxJustPut)->Unit(benchmark::kNanosecond)->Args({8});
// BENCHMARK(RocksDBWriteTxJustPut)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBRead)->Unit(benchmark::kNanosecond)->Args({4});
// BENCHMARK(RocksDBRead)->Unit(benchmark::kNanosecond)->Args({8});
// BENCHMARK(RocksDBRead)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadLH)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadTx)->Unit(benchmark::kNanosecond)->Args({4});
// BENCHMARK(RocksDBReadTx)->Unit(benchmark::kNanosecond)->Args({8});
// BENCHMARK(RocksDBReadTx)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadTxJustGetAndCommit)->Unit(benchmark::kNanosecond)->Args({4});
// BENCHMARK(RocksDBReadTxJustGetAndCommit)->Unit(benchmark::kNanosecond)->Args({8});
// BENCHMARK(RocksDBReadTxJustGetAndCommit)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadTxJustGet)->Unit(benchmark::kNanosecond)->Args({4});
// BENCHMARK(RocksDBReadTxJustGet)->Unit(benchmark::kNanosecond)->Args({8});
// BENCHMARK(RocksDBReadTxJustGet)->Unit(benchmark::kNanosecond)->Args({16});

// BENCHMARK(RocksDBReadSeq)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadSeq2)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadSeq3)->Unit(benchmark::kNanosecond)->Args({16});

// BENCHMARK(RocksDBReadSeqSmall)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadSeqBig)->Unit(benchmark::kNanosecond)->Args({16});

// BENCHMARK(RocksDBReadJustNext)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadJustNext2)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadJustNext3)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadJustNext4)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadJustNext5)->Unit(benchmark::kNanosecond)->Args({16});
// BENCHMARK(RocksDBReadMultiGet)->Unit(benchmark::kNanosecond)->Args({16});

BENCHMARK_MAIN();
