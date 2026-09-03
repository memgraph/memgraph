// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "auth/atomic_auth_overlay.hpp"

namespace memgraph::auth {

AtomicAuthOverlay::AtomicAuthOverlay(kvstore::KVStore &base) : base_(base) {}

std::optional<std::string> AtomicAuthOverlay::Get(std::string_view key) const {
  auto const key_str = std::string(key);

  if (auto it = write_set_.find(key_str); it != write_set_.end()) {
    return it->second;  // nullopt if tombstone
  }

  if (read_set_.find(key_str) == read_set_.end()) {
    auto val = base_.Get(key);
    read_set_.emplace(key_str, val);
  }
  return read_set_.at(key_str);
}

void AtomicAuthOverlay::Put(std::string_view key, std::string_view value) {
  auto const key_str = std::string(key);

  // Record in read-set if not already there (for conflict detection on existing keys)
  if (read_set_.find(key_str) == read_set_.end()) {
    read_set_.emplace(key_str, base_.Get(key));
  }

  write_set_[key_str] = std::string(value);
}

void AtomicAuthOverlay::Delete(std::string_view key) {
  auto const key_str = std::string(key);

  if (read_set_.find(key_str) == read_set_.end()) {
    read_set_.emplace(key_str, base_.Get(key));
  }

  write_set_[key_str] = std::nullopt;  // tombstone
}

void AtomicAuthOverlay::PutAndDeleteMultiple(std::map<std::string, std::string> const &puts,
                                             std::vector<std::string> const &deletes) {
  for (auto const &[key, value] : puts) {
    Put(key, value);
  }
  for (auto const &key : deletes) {
    Delete(key);
  }
}

bool AtomicAuthOverlay::PutMultiple(std::map<std::string, std::string> const &items) {
  for (auto const &[key, value] : items) {
    Put(key, value);
  }
  return true;
}

bool AtomicAuthOverlay::DeleteMultiple(std::vector<std::string> const &keys) {
  for (auto const &key : keys) {
    Delete(key);
  }
  return true;
}

size_t AtomicAuthOverlay::Size(std::string const &prefix) const {
  size_t count = 0;
  // This is an approximation: count base entries not tombstoned, plus new write-set entries
  for (auto it = base_.begin(prefix); it != base_.end(prefix); ++it) {
    auto ws = write_set_.find(it->first);
    if (ws == write_set_.end() || ws->second.has_value()) {
      ++count;
    }
  }
  for (auto it = write_set_.lower_bound(prefix); it != write_set_.end() && it->first.starts_with(prefix); ++it) {
    if (it->second.has_value() && !base_.Get(it->first).has_value()) {
      ++count;
    }
  }
  return count;
}

bool AtomicAuthOverlay::Flush() {
  // Validate read-set against current base state
  for (auto const &[key, snapshot_val] : read_set_) {
    auto current_val = base_.Get(key);
    if (current_val != snapshot_val) {
      return false;
    }
  }

  // Build puts and deletes for atomic KVStore write
  std::map<std::string, std::string> puts;
  std::vector<std::string> deletes;

  for (auto const &[key, val] : write_set_) {
    if (val.has_value()) {
      puts.emplace(key, *val);
    } else {
      deletes.emplace_back(key);
    }
  }

  if (!puts.empty() || !deletes.empty()) {
    if (!base_.PutAndDeleteMultiple(puts, deletes)) {
      return false;
    }
  }

  return true;
}

// --- Iterator ---

AtomicAuthOverlay::iterator::iterator(AtomicAuthOverlay const *overlay, std::string prefix, bool at_end)
    : overlay_(overlay),
      prefix_(std::move(prefix)),
      base_it_(overlay->base_.begin(prefix_)),
      base_end_(overlay->base_.end(prefix_)),
      at_end_(at_end) {
  if (!at_end_) {
    write_it_ = overlay_->write_set_.lower_bound(prefix_);
    write_end_ = overlay_->write_set_.end();
    Advance();
  }
}

void AtomicAuthOverlay::iterator::Advance() {
  current_.reset();

  while (!current_.has_value()) {
    bool have_base = (base_it_ != base_end_);
    bool have_write = (write_it_ != write_end_ && write_it_->first.starts_with(prefix_));

    if (!have_base && !have_write) {
      at_end_ = true;
      return;
    }

    if (have_base && have_write) {
      if (base_it_->first < write_it_->first) {
        // Base entry not overridden; check it's not deleted in write-set
        auto ws = overlay_->write_set_.find(base_it_->first);
        if (ws == overlay_->write_set_.end()) {
          current_ = *base_it_;
        }
        ++base_it_;
      } else if (base_it_->first > write_it_->first) {
        // Write-set entry with no base counterpart
        if (write_it_->second.has_value()) {
          current_ = std::make_pair(write_it_->first, *write_it_->second);
        }
        ++write_it_;
      } else {
        // Same key: write-set wins
        if (write_it_->second.has_value()) {
          current_ = std::make_pair(write_it_->first, *write_it_->second);
        }
        ++base_it_;
        ++write_it_;
      }
    } else if (have_base) {
      auto ws = overlay_->write_set_.find(base_it_->first);
      if (ws == overlay_->write_set_.end()) {
        current_ = *base_it_;
      }
      ++base_it_;
    } else {
      if (write_it_->second.has_value()) {
        current_ = std::make_pair(write_it_->first, *write_it_->second);
      }
      ++write_it_;
    }
  }
}

AtomicAuthOverlay::iterator &AtomicAuthOverlay::iterator::operator++() {
  Advance();
  return *this;
}

bool AtomicAuthOverlay::iterator::operator==(iterator const &other) const {
  if (at_end_ && other.at_end_) return true;
  if (at_end_ != other.at_end_) return false;
  return current_ == other.current_;
}

bool AtomicAuthOverlay::iterator::operator!=(iterator const &other) const { return !(*this == other); }

AtomicAuthOverlay::iterator::reference AtomicAuthOverlay::iterator::operator*() const { return *current_; }

AtomicAuthOverlay::iterator::pointer AtomicAuthOverlay::iterator::operator->() const { return &*current_; }

AtomicAuthOverlay::iterator AtomicAuthOverlay::begin(std::string const &prefix) const {
  return iterator(this, prefix, false);
}

AtomicAuthOverlay::iterator AtomicAuthOverlay::end(std::string const &prefix) const {
  return iterator(this, prefix, true);
}

}  // namespace memgraph::auth
