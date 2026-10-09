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

#include <cstdint>
#include <map>
#include <optional>
#include <set>
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
/// the base is left untouched. A write the base fails to make throws
/// AuthException instead, since retrying cannot help.
class AtomicAuthOverlay {
 public:
  explicit AtomicAuthOverlay(kvstore::KVStore &base);

  std::optional<std::string> Get(std::string_view key) const;

  void Put(std::string_view key, std::string_view value);

  void Delete(std::string_view key);

  void PutAndDeleteMultiple(std::map<std::string, std::string> const &puts, std::vector<std::string> const &deletes);

  bool PutMultiple(std::map<std::string, std::string> const &items);

  bool DeleteMultiple(std::vector<std::string> const &keys);

  /// Merging iterator over base + write-set for a given prefix. A scan's body must not write under the prefix it
  /// scans: the check for keys a scan no longer finds runs when the scan ends, against the write-set as it is then.
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
    iterator(AtomicAuthOverlay const &overlay, std::string prefix, bool at_end);

    void Advance();

    AtomicAuthOverlay const *overlay_;
    std::string prefix_;

    kvstore::KVStore::iterator base_it_;
    kvstore::KVStore::iterator base_end_;
    std::map<std::string, std::optional<std::string>>::const_iterator write_it_;
    std::map<std::string, std::optional<std::string>>::const_iterator write_end_;

    std::optional<value_type> current_;
    bool at_end_{false};

    /// Set when this transaction has deleted a key under the prefix by the time the scan starts. Base being
    /// inhabited then no longer means the transaction's view is, so the scan's answer rests on the keys it yields
    /// from base, and each is observed as it is yielded: a scan narrowed to emptiness would otherwise depend on a
    /// key it never recorded.
    bool own_delete_under_prefix_{false};

    /// Base keys this scan has walked past, yielded or not, and the write-set keys it has yielded. Handed to the
    /// prefix's dependency if the scan reaches the end.
    std::set<std::string, std::less<>> seen_;

    /// Base entries this scan walked past and the transaction has not written, with the values it saw. Held here
    /// rather than in the read set until the scan reaches the end, because only then is it known to depend on them:
    /// a scan that stops early and is narrowed to emptiness never read these values and must not conflict on them
    /// changing. When the transaction has deleted a key under the prefix, the keys it yields are observed at once
    /// instead (`own_delete_under_prefix_`).
    std::map<std::string, std::string, std::less<>> walked_;
  };

  iterator begin(std::string const &prefix) const;
  iterator end(std::string const &prefix) const;

  /// Narrow a just-completed scan to depending only on whether the prefix was inhabited. The caller says so after
  /// the fact, because only it knows it stopped early; a scan is recorded as depending on the whole key set until
  /// told otherwise. Only a caller whose answer depends on nothing but that may narrow: afterwards no key or value
  /// the scan read is checked, except the base keys it yielded after the transaction had deleted a key under the
  /// prefix.
  void ScanDependsOnEmptinessOnly(std::string const &prefix) const;

  /// Whether this transaction wrote anything. A read-only transaction still validates what it read, but has
  /// nothing to make durable, replicate, or invalidate cached permissions over.
  bool HasWrites() const { return !write_set_.empty(); }

  /// Validate read-set against base and flush write-set.
  /// Returns true on success, false on conflict (base untouched). Throws AuthException if the base write fails.
  bool Flush();

 private:
  /// Adopts the base entries an exhaustive scan walked past, so its dependency on their values is conflict-checked
  /// the same way a named read is. Without this a transaction can decide on which keys exist and leave no trace of
  /// it. Only an exhaustive scan calls this: one that stopped early depends on the prefix, not on what is under it,
  /// beyond the keys it observed through `own_delete_under_prefix_`.
  /// A read key under `prefix` that the scan did not walk is observed as absent.
  void AdoptWalked(std::string_view prefix, std::map<std::string, std::string, std::less<>> const &walked) const;

  /// Records one observation of `key`, holding it to the first: a disagreement sets `saw_two_values_`.
  void Observe(std::string const &key, std::optional<std::string> const &value) const;

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

    /// Set once a scan of this prefix has run to exhaustion. That transaction now depends on the whole key set for
    /// the rest of its life, so a later short-circuiting scan of the same prefix cannot weaken it back.
    bool exhausted{false};

    /// The keys that exhaustive scan saw, as it saw them. `Flush` compares base against this rather than against
    /// the read set, because the read set keeps growing: a later short-circuiting scan of the same prefix walks
    /// past whatever has appeared since, and a key recorded then would otherwise look like one this scan covered.
    std::set<std::string, std::less<>> seen;
  };

  /// Prefixes this transaction has scanned, and what each scan depended on. Recording the keys seen is not enough on
  /// its own: a scan of an empty prefix records nothing, which is exactly the first-user case.
  mutable std::map<std::string, ScanDependency, std::less<>> scanned_prefixes_;

  /// key -> value at snapshot time (nullopt = did not exist). Mutable because recording a read is bookkeeping for
  /// conflict detection, not observable state: reads stay logically const so Auth's query methods can too.
  ///
  /// Holds the first observation of each key, and every later one must agree with it: a read, a value a scan
  /// walked past, or an exhaustive scan not finding the key. A key this transaction wrote is not observed by its
  /// scans, since they see the write instead. The same holds for a prefix: whether a scan finds it inhabited in
  /// base must agree with the first scan of it (`ScanDependency::was_empty`).
  mutable std::map<std::string, std::optional<std::string>, std::less<>> read_set_;

  /// Set when an observation disagrees with the first one in `read_set_`: the transaction has acted on two states
  /// of that key, and no later check can tell, since the key may change back.
  mutable bool saw_two_values_{false};

  /// key -> new value (nullopt = tombstone)
  std::map<std::string, std::optional<std::string>, std::less<>> write_set_;
};

}  // namespace memgraph::auth
