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

namespace memgraph::coordination {

using nuraft::cs_new;

namespace {

ptr<log_entry> MakeClone(const ptr<log_entry> &entry) {
  return cs_new<log_entry>(entry->get_term(), buffer::clone(entry->get_buf()), entry->get_val_type(),
                           entry->get_timestamp());
}

}  // namespace

CoordinatorLogStore::CoordinatorLogStore() : start_idx_(1) {
  ptr<buffer> buf = buffer::alloc(sizeof(uint64_t));
  logs_[0] = cs_new<log_entry>(0, buf);
}

CoordinatorLogStore::~CoordinatorLogStore() = default;

auto CoordinatorLogStore::FindOrDefault_(uint64_t index) const -> ptr<log_entry> {
  spdlog::debug("    find or default {}", index);
  auto entry = logs_.find(index);
  if (entry == logs_.end()) {
    entry = logs_.find(0);
  }
  return entry->second;
}

uint64_t CoordinatorLogStore::next_slot() const {
  auto lock = std::lock_guard{logs_lock_};
  auto slot = start_idx_ + logs_.size() - 1;
  spdlog::trace("next slot is {}", slot);
  return slot;
}

uint64_t CoordinatorLogStore::start_index() const { return start_idx_; }

ptr<log_entry> CoordinatorLogStore::last_entry() const {
  spdlog::trace("last entry called log store");
  auto lock = std::lock_guard{logs_lock_};

  uint64_t const last_idx = start_idx_ + logs_.size() - 1;
  auto const last_src = FindOrDefault_(last_idx - 1);

  return MakeClone(last_src);
}

uint64_t CoordinatorLogStore::append(ptr<log_entry> &entry) {
  ptr<log_entry> clone = MakeClone(entry);

  auto lock = std::lock_guard{logs_lock_};
  uint64_t next_slot = start_idx_ + logs_.size() - 1;
  spdlog::trace("append entry on slot {} in log store", next_slot);
  logs_[next_slot] = clone;

  return next_slot;
}

// TODO: (andi) I think this is used for resolving conflicts inside NuRaft, check...
// different compared to in_memory_log_store.cxx
void CoordinatorLogStore::write_at(uint64_t index, ptr<log_entry> &entry) {
  spdlog::trace("write ate {} index log store", index);
  ptr<log_entry> clone = MakeClone(entry);

  // Discard all logs equal to or greater than `index.
  auto lock = std::lock_guard{logs_lock_};
  auto itr = logs_.lower_bound(index);
  while (itr != logs_.end()) {
    itr = logs_.erase(itr);
  }
  logs_[index] = clone;
}

ptr<std::vector<ptr<log_entry>>> CoordinatorLogStore::log_entries(uint64_t start, uint64_t end) {
  auto ret = cs_new<std::vector<ptr<log_entry>>>();
  ret->resize(end - start);

  for (uint64_t i = start, curr_index = 0; i < end; i++, curr_index++) {
    ptr<log_entry> src = nullptr;
    {
      auto lock = std::lock_guard{logs_lock_};
      if (auto const entry = logs_.find(i); entry != logs_.end()) {
        src = entry->second;
      } else {
        throw RaftCouldNotFindEntryException("Could not find entry at index {}", i);
      }
    }
    (*ret)[curr_index] = MakeClone(src);
  }
  return ret;
}

ptr<log_entry> CoordinatorLogStore::entry_at(uint64_t index) {
  auto lock = std::lock_guard{logs_lock_};
  ptr<log_entry> src = FindOrDefault_(index);
  spdlog::trace("entry_at index  {} in log store", index);
  return MakeClone(src);
}

uint64_t CoordinatorLogStore::term_at(uint64_t index) {
  auto lock = std::lock_guard{logs_lock_};
  spdlog::trace("term_at {} in log store", index);
  return FindOrDefault_(index)->get_term();
}

ptr<buffer> CoordinatorLogStore::pack(uint64_t index, int32 cnt) {
  spdlog::trace(" pack log store index {}  cnt {}", index, cnt);
  std::vector<ptr<buffer>> logs;

  size_t size_total = 0;
  uint64_t const end_index = index + cnt;
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
  spdlog::trace("applied pack log store on index {}", index);
  pack.pos(0);
  int32 const num_logs = pack.get_int();

  for (int32 i = 0; i < num_logs; ++i) {
    uint64_t cur_idx = index + i;
    int32 buf_size = pack.get_int();

    ptr<buffer> buf_local = buffer::alloc(buf_size);
    pack.get(buf_local);

    ptr<log_entry> le = log_entry::deserialize(*buf_local);
    {
      auto lock = std::lock_guard{logs_lock_};
      logs_[cur_idx] = le;
    }
  }

  {
    auto lock = std::lock_guard{logs_lock_};
    auto const entry = logs_.upper_bound(0);
    if (entry != logs_.end()) {
      start_idx_ = entry->first;
    } else {
      start_idx_ = 1;
    }
  }
}

// NOTE: Remove all logs up to given 'last_log_index' (inclusive).
bool CoordinatorLogStore::compact(uint64_t last_log_index) {
  spdlog::trace("compact called  {} ", last_log_index);
  auto lock = std::lock_guard{logs_lock_};
  for (uint64_t ii = start_idx_; ii <= last_log_index; ++ii) {
    auto const entry = logs_.find(ii);
    if (entry != logs_.end()) {
      logs_.erase(entry);
    }
  }

  if (start_idx_ <= last_log_index) {
    start_idx_ = last_log_index + 1;
  }
  return true;
}

bool CoordinatorLogStore::flush() { return true; }

}  // namespace memgraph::coordination
#endif
