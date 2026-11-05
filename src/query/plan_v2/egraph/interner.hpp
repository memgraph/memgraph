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

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace memgraph::query::plan::v2 {

/// Interns keys to dense ids and keeps a reverse table indexed by that id. A
/// symbol whose e-nodes carry interned side-data holds one of these: the id
/// becomes the e-node's disambiguator, and the reverse table is how a later
/// pass recovers the data from it.
///
/// `intern` is the sole writer of both directions. That is what keeps the id
/// space dense and the reverse entry at index `id`, rather than leaving two
/// containers for each caller to keep in step.
///
/// `Payload` may borrow from the key - a pointer or a view, so a large constant
/// is not duplicated - in which case `Map` must keep its keys at a stable
/// address, because the reverse entry outlives the insertion that made it. An
/// owned payload places no such requirement on `Map`. The accompanying test
/// interns past any initial bucket count and reads every entry back, which is
/// what distinguishes a node-stable map from a flat one.
template <typename Map, typename Payload>
class Interner {
 public:
  using key_type = typename Map::key_type;

  /// Intern `key` and answer its stable id. `make_payload` is called with the
  /// stored key, and only when the key is new, so a borrowing payload may take
  /// its address.
  template <typename K, typename MakePayload>
  auto intern(K &&key, MakePayload &&make_payload) -> std::uint64_t {
    auto [it, inserted] = store_.try_emplace(key_type{std::forward<K>(key)}, reverse_.size());
    if (inserted) reverse_.emplace_back(std::forward<MakePayload>(make_payload)(it->first));
    return it->second;
  }

  /// The reverse entry for `id`, or nullptr when `id` was never interned.
  [[nodiscard]] auto find(std::uint64_t id) const -> Payload const * {
    return id < reverse_.size() ? &reverse_[id] : nullptr;
  }

  /// The whole reverse table, for a pass that indexes it by disambiguator.
  [[nodiscard]] auto reverse() const -> std::vector<Payload> const & { return reverse_; }

  [[nodiscard]] auto size() const -> std::size_t { return reverse_.size(); }

 private:
  Map store_;
  std::vector<Payload> reverse_;
};

}  // namespace memgraph::query::plan::v2
