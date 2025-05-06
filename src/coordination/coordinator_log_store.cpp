// Copyright 2025 Memgraph Ltd.
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

#include "coordination/coordinator_log_store.hpp"
#include "coordination/constants.hpp"
#include "coordination/coordinator_communication_config.hpp"
#include "coordination/utils.hpp"
#include "utils/logging.hpp"

#include "kvstore/kvstore.hpp"

#include <ranges>

#include <nlohmann/json.hpp>

namespace memgraph::coordination {
using nuraft::buffer_serializer;

namespace {
std::shared_ptr<log_entry> MakeClone(const std::shared_ptr<log_entry> &entry) {
  return std::make_shared<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type(),
                                     entry->get_timestamp());
}
}  // namespace

CoordinatorLogStore::CoordinatorLogStore(LoggerWrapper const logger, LogStoreDurability log_store_durability)
    : durability_(std::move(log_store_durability.durability_store_)), logger_(logger) {
  logger_.Log(nuraft_log_level::INFO, "Restoring coordinator log store with durability.");
  MG_ASSERT(HandleVersionMigration(log_store_durability.stored_log_store_version_),
            "Couldn't handle version migration in coordinator log store.");
}

bool CoordinatorLogStore::HandleVersionMigration(LogStoreVersion const stored_version) {
  if constexpr (kActiveVersion == LogStoreVersion::kV2) {
    if (stored_version == LogStoreVersion::kV1 || stored_version == LogStoreVersion::kV2) {
      auto const maybe_last_log_entry = durability_->Get(kLastLogEntry);
      auto const maybe_start_idx = durability_->Get(kStartIdx);
      bool is_first_start{false};
      if (!maybe_last_log_entry.has_value()) {
        logger_.Log(nuraft_log_level::INFO,
                    "No last log entry found on disk, assuming first start of log store with durability");
        is_first_start = true;
      }
      if (!maybe_start_idx.has_value()) {
        logger_.Log(nuraft_log_level::INFO,
                    "No last start index found on disk, assuming first start of log store with durability");
        is_first_start = true;
      }
      if (is_first_start) {
        start_idx_.store(1, std::memory_order_release);
        durability_->Put(kStartIdx, "1");
        durability_->Put(kLastLogEntry, "0");
        return true;
      }

      uint64_t const last_log_entry = std::stoull(maybe_last_log_entry.value());
      auto const durable_start_idx_value = std::stoull(maybe_start_idx.value());
      start_idx_.store(durable_start_idx_value, std::memory_order_release);

      // Compaction might have happened so we might be missing some logs.
      for (auto const id : std::ranges::iota_view{durable_start_idx_value, last_log_entry + 1}) {
        auto const entry = durability_->Get(fmt::format("{}{}", kLogEntryPrefix, id));

        if (!entry.has_value()) {
          logger_.Log(nuraft_log_level::TRACE, fmt::format("Missing entry with id {} in range [{}:{}]", id,
                                                           durable_start_idx_value, last_log_entry));
          continue;
        }

        try {
          auto const j = nlohmann::json::parse(entry.value());
          auto const term = j.at(kLogEntryTermKey).get<int>();
          auto const data = j.at(kLogEntryDataKey).get<std::string>();
          auto const value_type = j.at("val_type").get<int>();
          auto log_term_buffer = buffer::alloc(sizeof(uint32_t) + data.size());
          buffer_serializer bs{log_term_buffer};
          bs.put_str(data);
          logs_[id] = std::make_shared<log_entry>(term, log_term_buffer, static_cast<nuraft::log_val_type>(value_type));
          logger_.Log(nuraft_log_level::TRACE,
                      fmt::format("Loaded entry from disk: ID {}, \n ENTRY {} ,\n DATA:{}, ", j.dump(), id, data));
        } catch (std::exception const &e) {
          LOG_FATAL("Error occurred while parsing JSON {}", e.what());
        }
      }
      return true;
    }
  }

  return false;
}

CoordinatorLogStore::~CoordinatorLogStore() = default;

auto CoordinatorLogStore::FindOrDefault_(uint64_t index) const -> std::shared_ptr<log_entry> {
  auto entry = logs_.find(index);
  if (entry == logs_.end()) {
    logger_.Log(nuraft_log_level::TRACE, fmt::format("Couldn't find log with index {} in the log storage.", index));
    return std::make_shared<log_entry>(0, buffer::alloc(sizeof(uint64_t)));
  }
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Found log with index {} in the log storage.", index));
  return entry->second;
}

void CoordinatorLogStore::DeleteLogs(uint64_t start, uint64_t end) {
  for (uint64_t i = start; i <= end; i++) {
    auto const entry = logs_.find(i);
    if (entry == logs_.end()) {
      continue;
    }
    logs_.erase(entry);
    durability_->Delete(fmt::format("{}{}", kLogEntryPrefix, i));
  }
}

uint64_t CoordinatorLogStore::next_slot() const {
  auto lock = std::lock_guard{logs_lock_};
  return GetNextSlot();
}

auto CoordinatorLogStore::GetNextSlot() const -> uint64_t {
  return start_idx_.load(std::memory_order_acquire) + logs_.size();
}

uint64_t CoordinatorLogStore::start_index() const { return start_idx_.load(std::memory_order_acquire); }

std::shared_ptr<log_entry> CoordinatorLogStore::last_entry() const {
  auto lock = std::lock_guard{logs_lock_};
  uint64_t const next_slot = GetNextSlot();
  auto const last_src = FindOrDefault_(next_slot - 1);

  return MakeClone(last_src);
}

uint64_t CoordinatorLogStore::append(std::shared_ptr<log_entry> &entry) {
  auto const clone = MakeClone(entry);
  auto lock = std::lock_guard{logs_lock_};
  uint64_t const next_slot = GetNextSlot();

  bool constexpr is_entry_with_biggest_id{true};
  StoreEntryToDisk(clone, next_slot, is_entry_with_biggest_id);

  spdlog::trace("Appended log at index {} to the log storage.", next_slot);
  logs_[next_slot] = clone;

  return next_slot;
}

void CoordinatorLogStore::write_at(uint64_t index, std::shared_ptr<log_entry> &entry) {
  std::shared_ptr<log_entry> const clone = MakeClone(entry);

  // Discard all logs equal to or greater than `index.
  auto lock = std::lock_guard{logs_lock_};
  auto itr = logs_.lower_bound(index);
  while (itr != logs_.end()) {
    itr = logs_.erase(itr);
  }
  bool const is_entry_with_biggest_id = index == GetNextSlot();
  StoreEntryToDisk(clone, index, is_entry_with_biggest_id);
  if (index != 0) [[likely]] {
    logs_[index] = clone;
  }
}

std::shared_ptr<std::vector<std::shared_ptr<log_entry>>> CoordinatorLogStore::log_entries(uint64_t start,
                                                                                          uint64_t end) {
  auto ret = std::make_shared<std::vector<std::shared_ptr<log_entry>>>();
  ret->reserve(end - start);

  for (uint64_t i = start; i < end; i++) {
    std::shared_ptr<log_entry> src;
    {
      auto lock = std::lock_guard{logs_lock_};
      auto const entry = logs_.find(i);
      if (entry == logs_.end()) {
        spdlog::trace("Could not find entry at index {}", i);
        return nullptr;
      }
      src = entry->second;
    }
    ret->emplace_back(MakeClone(src));
  }
  return ret;
}

std::vector<std::pair<int64_t, std::shared_ptr<log_entry>>> CoordinatorLogStore::GetAllEntriesRange(
    uint64_t start, uint64_t end) const {
  std::vector<std::pair<int64_t, std::shared_ptr<log_entry>>> entries;
  entries.reserve(end - start);

  for (uint64_t i = start; i < end; i++) {
    std::shared_ptr<log_entry> src;
    {
      auto lock = std::lock_guard{logs_lock_};
      auto const entry = logs_.find(i);
      if (entry == logs_.end()) {
        spdlog::trace("Could not find entry at index {}", i);
        continue;
      }
      src = entry->second;
    }
    entries.emplace_back(i, MakeClone(src));
  }
  return entries;
}

std::shared_ptr<log_entry> CoordinatorLogStore::entry_at(uint64_t index) {
  auto lock = std::lock_guard{logs_lock_};
  std::shared_ptr<log_entry> const src = FindOrDefault_(index);
  return MakeClone(src);
}

uint64_t CoordinatorLogStore::term_at(uint64_t index) {
  auto lock = std::lock_guard{logs_lock_};
  return FindOrDefault_(index)->get_term();
}

std::shared_ptr<buffer> CoordinatorLogStore::pack(uint64_t index, int32 cnt) {
  std::vector<std::shared_ptr<buffer>> logs;

  size_t size_total = 0;
  uint64_t const end_index = index + cnt;
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Packing logs from {} to {}", index, end_index));
  for (uint64_t i = index; i < end_index; ++i) {
    std::shared_ptr<log_entry> le = nullptr;
    {
      auto lock = std::lock_guard{logs_lock_};
      if (auto elem = logs_.find(i); elem != logs_.end()) {
        le = elem->second;
      } else if (index == 0) {
        le = FindOrDefault_(0);
      }
    }
    MG_ASSERT(le != nullptr, "Could not find log entry at index {}", i);
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
    auto const cur_idx = index + i;
    auto const buf_size = pack.get_int();

    std::shared_ptr<buffer> buf_local = buffer::alloc(buf_size);
    pack.get(buf_local);

    std::shared_ptr<log_entry> const le = log_entry::deserialize(*buf_local);
    {
      auto lock = std::lock_guard{logs_lock_};
      // This needs to be before we update logs_ as we change NextSlot once we insert log into in-memory logs_
      bool const is_entry_with_biggest_id = cur_idx >= GetNextSlot();
      if (cur_idx != 0) [[likely]] {
        logs_[cur_idx] = le;
      }

      StoreEntryToDisk(le, cur_idx, is_entry_with_biggest_id);
    }
  }
  {
    auto lock = std::lock_guard{logs_lock_};
    if (auto const entry = logs_.upper_bound(0); entry != logs_.end()) {
      start_idx_.store(entry->first, std::memory_order_release);
      durability_->Put(kStartIdx, std::to_string(entry->first));
    } else {
      start_idx_.store(1, std::memory_order_release);
    }
  }
}

// NOTE: Remove all logs up to given 'last_log_index' (inclusive).
// NOTE: Remove all logs up to given 'last_log_index' (inclusive).
bool CoordinatorLogStore::compact(uint64_t last_log_index) {
  logger_.Log(nuraft_log_level::TRACE, fmt::format("Compacting logs up to {}", last_log_index));
  auto lock = std::lock_guard{logs_lock_};
  auto const old_start_idx = start_idx_.load(std::memory_order_acquire);
  for (uint64_t ii = old_start_idx; ii <= last_log_index; ++ii) {
    auto const entry = logs_.find(ii);
    if (entry == logs_.end()) {
      continue;
    }
    logs_.erase(entry);
    durability_->Delete(fmt::format("{}{}", kLogEntryPrefix, ii));
  }

  if (old_start_idx <= last_log_index) {
    auto const new_idx = last_log_index + 1;
    start_idx_.store(new_idx, std::memory_order_release);
    durability_->Put(kStartIdx, std::to_string(new_idx));
  }
  return true;
}

// Configuration logs are flushed immediately. This is called from NuRaft.
// Otherwise, the possibility of split brain occurs inside Raft cluster.
bool CoordinatorLogStore::flush() {
  spdlog::trace("Synced WAL to make Raft logs durable.");
  return durability_->SyncWal();
}

// Assumes durability exists
bool CoordinatorLogStore::StoreEntryToDisk(const std::shared_ptr<log_entry> &clone, uint64_t key_id,
                                           bool is_newest_entry) {
  auto const data_string = [&clone, logger = &logger_]() -> std::string {
    if (clone->get_val_type() != nuraft::log_val_type::app_log) {
      // this is only our log, others nuraft creates and we
      // don't have actions for them
      logger->Log(nuraft_log_level::TRACE, "Received non-application log, data will be empty string.");
      // behavior.
      return {};
    }
    logger->Log(nuraft_log_level::TRACE, "Received application log, serializing it.");
    buffer_serializer bs(clone->get_buf());  // data buff, nlohmann::json
    return bs.get_str();
  }();  // iile

  auto const clone_val = static_cast<int>(clone->get_val_type());
  logger_.Log(nuraft_log_level::TRACE,
              fmt::format("Storing entry to disk: ID {}, \n ENTRY {} \n type {}", key_id, data_string, clone_val));

  auto const log_term_json = nlohmann::json(
      {{kLogEntryTermKey, clone->get_term()}, {kLogEntryDataKey, data_string}, {kLogEntryValTypeKey, clone_val}});
  auto const log_term_str = log_term_json.dump();
  auto const key = fmt::format("{}{}", kLogEntryPrefix, key_id);
  durability_->Put(key, log_term_str);

  if (is_newest_entry) {
    logger_.Log(nuraft_log_level::TRACE, fmt::format("Storing newest entry to disk {}", key_id));
    durability_->Put(kLastLogEntry, std::to_string(key_id));
  }

  return true;
}
}  // namespace memgraph::coordination
#endif
