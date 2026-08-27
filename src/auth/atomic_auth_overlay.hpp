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

#pragma once

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "kvstore/kvstore.hpp"

namespace memgraph::auth {

/// A transactional overlay for KVStore that buffers reads and writes,
/// enabling optimistic concurrency control for auth transactions.
///
/// Reads are served from the write-set first, then from the base KVStore
/// (and recorded in the read-set for conflict detection). Writes are
/// buffered in the write-set and only flushed to the base on Flush().
///
/// Flush() validates the read-set against the current base state. If any
/// read key has been modified concurrently, Flush() returns false and
/// the base is left untouched.
class AtomicAuthOverlay {
 public:
  explicit AtomicAuthOverlay(kvstore::KVStore &base);

  std::optional<std::string> Get(std::string_view key);

  void Put(std::string_view key, std::string_view value);

  void Delete(std::string_view key);

  void PutAndDeleteMultiple(std::map<std::string, std::string> const &puts, std::vector<std::string> const &deletes);

  /// Merging iterator over base + write-set for a given prefix.
  class iterator {
   public:
    using value_type = std::pair<std::string, std::string>;
    using reference = value_type const &;
    using pointer = value_type const *;

    iterator &operator++();
    bool operator==(iterator const &other) const;
    bool operator!=(iterator const &other) const;
    reference operator*() const;
    pointer operator->() const;

   private:
    friend class AtomicAuthOverlay;
    iterator(AtomicAuthOverlay *overlay, std::string prefix, bool at_end);

    void Advance();

    AtomicAuthOverlay *overlay_;
    std::string prefix_;

    kvstore::KVStore::iterator base_it_;
    kvstore::KVStore::iterator base_end_;
    std::map<std::string, std::optional<std::string>>::const_iterator write_it_;
    std::map<std::string, std::optional<std::string>>::const_iterator write_end_;

    std::optional<value_type> current_;
    bool at_end_{false};
  };

  iterator begin(std::string const &prefix);
  iterator end(std::string const &prefix);

  /// Validate read-set against base and flush write-set.
  /// Returns true on success, false on conflict (base untouched).
  bool Flush();

 private:
  kvstore::KVStore &base_;

  /// key -> value at snapshot time (nullopt = did not exist)
  std::map<std::string, std::optional<std::string>, std::less<>> read_set_;

  /// key -> new value (nullopt = tombstone)
  std::map<std::string, std::optional<std::string>, std::less<>> write_set_;
};

}  // namespace memgraph::auth
