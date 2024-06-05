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

#ifdef MG_ENTERPRISE

#include "nuraft/coordinator_log_store.hpp"
#include "coordination/coordinator_exceptions.hpp"
#include "utils/logging.hpp"

#include "kvstore/kvstore.hpp"

#include <ranges>
#include "json/json.hpp"

namespace memgraph::coordination {

using nuraft::buffer_serializer;
using nuraft::cs_new;

namespace {

constexpr std::string_view kLogEntryPrefix = "log_entry_";
constexpr std::string_view kLastLogEntry = "last_log_entry";
constexpr std::string_view kStartIdx = "start_idx";

constexpr std::string_view kLogStoreDurabilityVersion = "log_store_durability_version";
constexpr int kActiveVersion = 1;

const std::string kLogEntryDataKey = "data";
const std::string kLogEntryTermKey = "term";
const std::string kLogEntryValTypeKey = "val_type";

nuraft::log_val_type GetLogValType(int val_type) {
  switch (val_type) {
    case 1:
      return nuraft::log_val_type::app_log;
    case 2:
      return nuraft::log_val_type::conf;
    case 3:
      return nuraft::log_val_type::cluster_server;
    case 4:
      return nuraft::log_val_type::log_pack;
    case 5:
      return nuraft::log_val_type::snp_sync_req;
    case 6:
      return nuraft::log_val_type::custom;
    default:
      throw std::runtime_error("Invalid value type");
  }
}

int FromLogValType(nuraft::log_val_type val_type) {
  switch (val_type) {
    case nuraft::log_val_type::app_log:
      return 1;
    case nuraft::log_val_type::conf:
      return 2;
    case nuraft::log_val_type::cluster_server:
      return 3;
    case nuraft::log_val_type::log_pack:
      return 4;
    case nuraft::log_val_type::snp_sync_req:
      return 5;
    case nuraft::log_val_type::custom:
      return 6;
    default:
      throw std::runtime_error("Invalid log value type");
  }
}

ptr<log_entry> MakeClone(const ptr<log_entry> &entry) {
  return cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type(),
                           entry->get_timestamp());
}

bool StoreToDisk(const ptr<log_entry> &clone, const uint64_t slot, bool is_new_last_slot, kvstore::KVStore &kv_store) {
  buffer_serializer bs(clone->get_buf());  // data buff, nlohmann::json
  auto data_str = bs.get_str();
  spdlog::trace("Serializing log entry with id {}", std::to_string(clone->get_term()));
  spdlog::trace("Serializing log entry with id {}", data_str);
  auto clone_val = FromLogValType(clone->get_val_type());
  spdlog::trace("Clone val type {}", clone_val);
  auto const log_term_json = nlohmann::json(
      {{kLogEntryTermKey, clone->get_term()}, {kLogEntryDataKey, data_str}, {kLogEntryValTypeKey, clone_val}});
  spdlog::trace("Storing to log to disk {} with id {}", log_term_json.dump(), std::to_string(slot));
  MG_ASSERT(kv_store.Put("log_entry_" + std::to_string(slot), log_term_json.dump()), "Failed to store log to disk!");

  if (is_new_last_slot) {
    spdlog::trace("Storing last log entry to disk {}", std::to_string(slot));
    MG_ASSERT(kv_store.Put(kLastLogEntry, std::to_string(slot)), "Failed to store last log entry to disk!");
  }

  return true;
}

}  // namespace

CoordinatorLogStore::CoordinatorLogStore(std::shared_ptr<kvstore::KVStore> durability_store, LoggerWrapper logger)
    : durability_store_(std::move(durability_store)), logger_(logger) {
  ptr<buffer> buf = buffer::alloc(sizeof(uint64_t));
  logs_[0] = cs_new<log_entry>(0, buf);

  if (!durability_store_) {
    spdlog::warn("No durability directory provided, logs will not be persisted to disk");
    start_idx_ = 1;
    return;
  }

  int version{0};
  auto maybe_version = durability_store_->Get(kLogStoreDurabilityVersion);
  if (maybe_version.has_value()) {
    version = std::stoi(maybe_version.value());
  } else {
    logger_.Log(nuraft_log_level::INFO,
                "Assuming first start of log store with durability as version is missing, storing version 1.");
    MG_ASSERT(durability_store_->Put(kLogStoreDurabilityVersion, std::to_string(kActiveVersion)),
              "Failed to store version to disk");
    version = 1;
  }

  MG_ASSERT(version <= kActiveVersion && version > 0, "Unsupported version of log store with durability");

  auto const maybe_last_log_entry = durability_store_->Get(kLastLogEntry);
  auto const maybe_start_idx = durability_store_->Get(kStartIdx);
  if (!maybe_last_log_entry.has_value() || !maybe_start_idx.has_value()) {
    logger_.Log(nuraft_log_level::INFO,
                "No last log entry or start index found on disk, assuming first start of log store with durability");
    start_idx_ = 1;
    MG_ASSERT(durability_store_->Put(kStartIdx, std::to_string(start_idx_.load())),
              "Failed to store start index to disk");
    MG_ASSERT(durability_store_->Put(kLastLogEntry, std::to_string(start_idx_.load() - 1)),
              "Failed to store last log entry to disk");
    return;
  }

  uint64_t last_log_entry = std::stoull(maybe_last_log_entry.value());
  start_idx_ = std::stoull(maybe_start_idx.value());

  // Compaction might have happened so we might be missing some logs.
  for (auto const id : std::ranges::iota_view{start_idx_.load(), last_log_entry + 1}) {
    auto const entry = durability_store_->Get(std::string{kLogEntryPrefix} + std::to_string(id));

    MG_ASSERT(entry.has_value(), "Missing entry with id {} in range [{}:{}>", id, start_idx_.load(),
              last_log_entry + 1);

    auto const j = nlohmann::json::parse(entry.value());
    auto const term = j.at(kLogEntryTermKey).get<int>();
    auto const data = j.at(kLogEntryDataKey).get<std::string>();
    auto const value_type = j.at("val_type").get<int>();
    auto log_term_buffer = buffer::alloc(sizeof(uint32_t) + data.size());
    buffer_serializer bs{log_term_buffer};
    bs.put_str(data);
    logs_[id] = cs_new<log_entry>(term, log_term_buffer, GetLogValType(value_type));
    logger_.Log(nuraft_log_level::TRACE, fmt::format("Loaded entry from disk: ID {}, \n ENTRY {} ,\n DATA:{}, ",
                                                     std::string{j.dump()}, std::to_string(id), data));
  }
}

CoordinatorLogStore::~CoordinatorLogStore() = default;

auto CoordinatorLogStore::FindOrDefault_(uint64_t index) const -> ptr<log_entry> {
  auto entry = logs_.find(index);
  if (entry == logs_.end()) {
    entry = logs_.find(0);
  }
  return entry->second;
}

void CoordinatorLogStore::DeleteLogs(uint64_t start, uint64_t end) {
  for (uint64_t i = start; i <= end; i++) {
    auto const entry = logs_.find(i);
    if (entry == logs_.end()) {
      continue;
    }
    logs_.erase(entry);
    if (durability_store_) {
      MG_ASSERT(durability_store_->Delete(std::string{kLogEntryPrefix} + std::to_string(i)),
                "Failed to delete log entry from disk");
    }
  }
}

uint64_t CoordinatorLogStore::next_slot() const {
  auto lock = std::lock_guard{logs_lock_};
  return start_idx_ + logs_.size() - 1;
}

uint64_t CoordinatorLogStore::start_index() const { return start_idx_; }

ptr<log_entry> CoordinatorLogStore::last_entry() const {
  auto lock = std::lock_guard{logs_lock_};
  uint64_t const next_slot = start_idx_ + logs_.size() - 1;
  auto const last_src = FindOrDefault_(next_slot - 1);

  return MakeClone(last_src);
}

uint64_t CoordinatorLogStore::append(ptr<log_entry> &entry) {
  ptr<log_entry> clone = MakeClone(entry);
  auto lock = std::lock_guard{logs_lock_};
  uint64_t next_slot = start_idx_ + logs_.size() - 1;

  if (durability_store_) {
    StoreToDisk(clone, next_slot, next_slot == start_idx_ + logs_.size() - 1, *durability_store_);
  }

  logs_[next_slot] = clone;

  return next_slot;
}

void CoordinatorLogStore::write_at(uint64_t index, ptr<log_entry> &entry) {
  ptr<log_entry> clone = MakeClone(entry);

  // Discard all logs equal to or greater than `index.
  auto lock = std::lock_guard{logs_lock_};
  auto itr = logs_.lower_bound(index);
  while (itr != logs_.end()) {
    itr = logs_.erase(itr);
  }
  logs_[index] = clone;

  if (durability_store_) {
    StoreToDisk(clone, index, index >= start_idx_ - logs_.size() - 1, *durability_store_);
  }
}

ptr<std::vector<ptr<log_entry>>> CoordinatorLogStore::log_entries(uint64_t start, uint64_t end) {
  auto ret = cs_new<std::vector<ptr<log_entry>>>();
  ret->resize(end - start);

  for (uint64_t i = start, curr_index = 0; i < end; i++, curr_index++) {
    ptr<log_entry> src;
    {
      auto lock = std::lock_guard{logs_lock_};
      auto const entry = logs_.find(i);
      if (entry == logs_.end()) {
        spdlog::error("Could not find entry at index {}", i);
        return nullptr;
      }
      src = entry->second;
    }
    (*ret)[curr_index] = MakeClone(src);
  }
  return ret;
}

ptr<log_entry> CoordinatorLogStore::entry_at(uint64_t index) {
  auto lock = std::lock_guard{logs_lock_};
  ptr<log_entry> src = FindOrDefault_(index);
  return MakeClone(src);
}

uint64_t CoordinatorLogStore::term_at(uint64_t index) {
  auto lock = std::lock_guard{logs_lock_};
  return FindOrDefault_(index)->get_term();
}

ptr<buffer> CoordinatorLogStore::pack(uint64_t index, int32 cnt) {
  std::vector<ptr<buffer>> logs;

  size_t size_total = 0;
  uint64_t const end_index = index + cnt;
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Packing logs from {} to {}", index, end_index));
  for (uint64_t i = index; i < end_index; ++i) {
    ptr<log_entry> le = nullptr;
    {
      auto lock = std::lock_guard{logs_lock_};
      le = logs_[i];
    }
    MG_ASSERT(le.get(), "Could not find log entry at index {}", i);
    auto buf = le->serialize();
    size_total += buf->size();
    logs.push_back(buf);
  }

  auto buf_out = buffer::alloc(sizeof(int32) + cnt * sizeof(int32) + size_total);
  buf_out->pos(0);
  buf_out->put((int32)cnt);

  for (auto &entry : logs) {
    buf_out->put(static_cast<int32>(entry->size()));
    buf_out->put(*entry);
  }
  return buf_out;
}

void CoordinatorLogStore::apply_pack(uint64_t index, buffer &pack) {
  pack.pos(0);
  int32 const num_logs = pack.get_int();
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Applying pack for logs from {} to {}", index, index + num_logs));
  for (int32 i = 0; i < num_logs; ++i) {
    uint64_t cur_idx = index + i;
    int32 buf_size = pack.get_int();

    ptr<buffer> buf_local = buffer::alloc(buf_size);
    pack.get(buf_local);

    ptr<log_entry> le = log_entry::deserialize(*buf_local);
    {
      auto lock = std::lock_guard{logs_lock_};
      logs_[cur_idx] = le;
      if (durability_store_) {
        StoreToDisk(le, cur_idx, cur_idx >= start_idx_ + logs_.size() - 1, *durability_store_);
      }
    }
  }

  {
    auto lock = std::lock_guard{logs_lock_};
    auto const entry = logs_.upper_bound(0);
    if (entry != logs_.end()) {
      start_idx_ = entry->first;
      if (durability_store_) {
        MG_ASSERT(durability_store_->Put(kStartIdx, std::to_string(start_idx_.load())),
                  "Failed to store start index to disk");
      }
    } else {
      start_idx_ = 1;
    }
  }
}

// NOTE: Remove all logs up to given 'last_log_index' (inclusive).
bool CoordinatorLogStore::compact(uint64_t last_log_index) {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Compacting logs up to {}", last_log_index));
  auto lock = std::lock_guard{logs_lock_};
  for (uint64_t ii = start_idx_; ii <= last_log_index; ++ii) {
    auto const entry = logs_.find(ii);
    if (entry == logs_.end()) {
      continue;
    }
    logs_.erase(entry);
    if (durability_store_) {
      MG_ASSERT(durability_store_->Delete(std::string{kLogEntryPrefix} + std::to_string(ii)),
                "Failed to delete log entry from disk");
    }
  }

  if (start_idx_ <= last_log_index) {
    start_idx_ = last_log_index + 1;
    if (durability_store_) {
      MG_ASSERT(durability_store_->Put(kStartIdx, std::to_string(start_idx_.load())),
                "Failed to store start index to disk");
    }
  }
  return true;
}

bool CoordinatorLogStore::flush() {
  if (durability_store_) {
    return durability_store_->SyncWal();
  }
  return true;
}

}  // namespace memgraph::coordination
#endif
