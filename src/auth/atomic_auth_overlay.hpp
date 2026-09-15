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

  std::optional<std::string> Get(std::string_view key) const;

  void Put(std::string_view key, std::string_view value);

  void Delete(std::string_view key);

  void PutAndDeleteMultiple(std::map<std::string, std::string> const &puts, std::vector<std::string> const &deletes);

  bool PutMultiple(std::map<std::string, std::string> const &items);

  bool DeleteMultiple(std::vector<std::string> const &keys);

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
    iterator(AtomicAuthOverlay const *overlay, std::string prefix, bool at_end);

    void Advance();

    AtomicAuthOverlay const *overlay_;
    std::string prefix_;

    kvstore::KVStore::iterator base_it_;
    kvstore::KVStore::iterator base_end_;
    std::map<std::string, std::optional<std::string>>::const_iterator write_it_;
    std::map<std::string, std::optional<std::string>>::const_iterator write_end_;

    std::optional<value_type> current_;
    bool at_end_{false};
  };

  iterator begin(std::string const &prefix) const;
  iterator end(std::string const &prefix) const;

  /// Narrow a just-completed scan to depending only on whether the prefix was inhabited. The caller says so after
  /// the fact, because only it knows it stopped early; a scan is recorded as depending on the whole key set until
  /// told otherwise.
  void ScanDependsOnEmptinessOnly(std::string const &prefix) const;

  /// Validate read-set against base and flush write-set.
  /// Returns true on success, false on conflict (base untouched).
  bool Flush();

 private:
  /// Records a base key a scan walked past, so a scan's dependency on the keys it saw is conflict-checked the same
  /// way a named read is. Without this a transaction can decide on which keys exist and leave no trace of it.
  void RecordScanned(std::string const &key, std::string const &value) const;

  kvstore::KVStore &base_;

  /// What a scan of a prefix concluded, and therefore what invalidates it.
  ///
  /// A scan that stops at the first key learns only whether the prefix is inhabited; demanding it account for every
  /// key would fail it against keys it was never obliged to look at. One that runs to exhaustion learns the key set
  /// and depends on all of it.
  struct ScanDependency {
    enum class Kind : uint8_t { kEmptiness, kKeySet };

    Kind kind;
    bool was_empty;
  };

  /// Prefixes this transaction has scanned, and what each scan depended on. Recording the keys seen is not enough on
  /// its own: a scan of an empty prefix records nothing, which is exactly the first-user case.
  mutable std::map<std::string, ScanDependency, std::less<>> scanned_prefixes_;

  /// key -> value at snapshot time (nullopt = did not exist). Mutable because recording a read is bookkeeping for
  /// conflict detection, not observable state: reads stay logically const so Auth's query methods can too.
  mutable std::map<std::string, std::optional<std::string>, std::less<>> read_set_;

  /// key -> new value (nullopt = tombstone)
  std::map<std::string, std::optional<std::string>, std::less<>> write_set_;
};

}  // namespace memgraph::auth
