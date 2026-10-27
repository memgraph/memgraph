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

#include "memory/query_memory_control.hpp"
#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/typed_value.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/hops_limit.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/vertices_iterable.hpp"
#include "storage/v2/view.hpp"
#include "utils/bound.hpp"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/pmr/unordered_set.hpp"
#include "utils/variant_helpers.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::storage {
enum class PointDistanceCondition : uint8_t;
enum class WithinBBoxCondition : uint8_t;
struct PointIterable;
struct TextSearchResult;
struct TextEdgeSearchResult;
struct VectorIndexInfo;
struct VectorEdgeIndexInfo;
}  // namespace memgraph::storage

namespace memgraph::query::plan {
using PointDistanceCondition = memgraph::storage::PointDistanceCondition;
using WithinBBoxCondition = memgraph::storage::WithinBBoxCondition;
}  // namespace memgraph::query::plan

enum class text_search_mode;

#include <algorithm>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <unordered_set>
#include <vector>

#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>

namespace memgraph::query {

class Graph;
class VirtualGraph;
class VirtualNode;

class SubgraphVertexAccessor final {
 public:
  query::VertexAccessor impl_;
  query::Graph *graph_;

  explicit SubgraphVertexAccessor(query::VertexAccessor impl, query::Graph *graph_) : impl_(impl), graph_(graph_) {}

  bool operator==(const SubgraphVertexAccessor &v) const noexcept {
    static_assert(noexcept(impl_ == v.impl_));
    return impl_ == v.impl_;
  }

  auto InEdges(storage::View view) const -> decltype(impl_.InEdges(view));

  auto OutEdges(storage::View view) const -> decltype(impl_.OutEdges(view));

  auto Labels(storage::View view) const { return impl_.Labels(view); }

  storage::Result<bool> AddLabel(storage::LabelId label) { return impl_.AddLabel(label); }

  storage::Result<bool> RemoveLabel(storage::LabelId label) { return impl_.RemoveLabel(label); }

  storage::Result<bool> HasLabel(storage::View view, storage::LabelId label) const {
    return impl_.HasLabel(view, label);
  }

  auto Properties(storage::View view) const { return impl_.Properties(view); }

  storage::Result<storage::PropertyValue> GetProperty(storage::View view, storage::PropertyId key) const;

  storage::Result<uint64_t> GetPropertySize(storage::PropertyId key, storage::View view) const {
    return impl_.GetPropertySize(key, view);
  }

  storage::Gid Gid() const noexcept { return impl_.Gid(); }

  storage::Result<size_t> InDegree(storage::View view) const { return impl_.InDegree(view); }

  storage::Result<size_t> OutDegree(storage::View view) const { return impl_.OutDegree(view); }

  storage::Result<storage::PropertyValue> SetProperty(storage::PropertyId key, const storage::PropertyValue &value) {
    return impl_.SetProperty(key, value);
  }

  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    return impl_.UpdateProperties(properties);
  }

  VertexAccessor GetVertexAccessor() const;
};
}  // namespace memgraph::query

namespace memgraph::query {

class VerticesIterable final {
  // Alternatives: storage scan, caller-materialized set, branch union scan, and a materialized (already filtered)
  // branch result; shared_ptr keeps that vector alive across copies of the iterable and its Iterator.
  std::variant<storage::VerticesIterable,
               std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                  utils::Allocator<VertexAccessor>> *,
               versioning::BranchContext::UnionVerticesIterable, std::shared_ptr<std::vector<VertexAccessor>>>
      iterable_;
  // Branch union-scan alternative only: stamped onto each yielded VertexAccessor so mutators can CowVertex through it.
  // The diff-engine accessor lives in BranchContext::current_diff_txn(), keeping accessors at one extra pointer.
  versioning::BranchContext *branch_ctx_{nullptr};

 public:
  class Iterator final {
    std::variant<storage::VerticesIterable::Iterator,
                 std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                    utils::Allocator<VertexAccessor>>::iterator,
                 versioning::BranchContext::UnionVerticesIterable::Iterator, std::vector<VertexAccessor>::iterator>
        it_;
    versioning::BranchContext *branch_ctx_{nullptr};
    std::shared_ptr<std::vector<VertexAccessor>> materialized_;

   public:
    Iterator(storage::VerticesIterable::Iterator it, versioning::BranchContext *ctx)
        : it_(std::move(it)), branch_ctx_(ctx) {}

    explicit Iterator(std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                         utils::Allocator<VertexAccessor>>::iterator it)
        : it_(it) {}

    Iterator(versioning::BranchContext::UnionVerticesIterable::Iterator it, versioning::BranchContext *ctx)
        : it_(std::move(it)), branch_ctx_(ctx) {}

    Iterator(std::vector<VertexAccessor>::iterator it, std::shared_ptr<std::vector<VertexAccessor>> materialized)
        : it_(it), materialized_(std::move(materialized)) {}

    VertexAccessor operator*() const {
      return std::visit(
          memgraph::utils::Overloaded{
              [this](const storage::VerticesIterable::Iterator &it) { return VertexAccessor(*it, branch_ctx_); },
              [](const std::unordered_set<VertexAccessor,
                                          std::hash<VertexAccessor>,
                                          std::equal_to<void>,
                                          utils::Allocator<VertexAccessor>>::iterator &it) {
                return VertexAccessor(*it);
              },
              [this](const versioning::BranchContext::UnionVerticesIterable::Iterator &it) {
                return VertexAccessor(*it, branch_ctx_);
              },
              [](const std::vector<VertexAccessor>::iterator &it) { return *it; }},
          it_);
    }

    Iterator &operator++() {
      std::visit([](auto &it_) { ++it_; }, it_);
      return *this;
    }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(other == *this); }
  };

  explicit VerticesIterable(storage::VerticesIterable iterable) : iterable_(std::move(iterable)) {}

  VerticesIterable(storage::VerticesIterable iterable, versioning::BranchContext *ctx)
      : iterable_(std::move(iterable)), branch_ctx_(ctx) {}

  explicit VerticesIterable(std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                               utils::Allocator<VertexAccessor>> *vertices)
      : iterable_(vertices) {}

  VerticesIterable(versioning::BranchContext::UnionVerticesIterable iterable, versioning::BranchContext *ctx)
      : iterable_(std::move(iterable)), branch_ctx_(ctx) {}

  explicit VerticesIterable(std::shared_ptr<std::vector<VertexAccessor>> materialized)
      : iterable_(std::move(materialized)) {}

  Iterator begin() {
    return std::visit(memgraph::utils::Overloaded{[this](storage::VerticesIterable &iterable_) {
                                                    return Iterator(iterable_.begin(), branch_ctx_);
                                                  },
                                                  [](std::unordered_set<VertexAccessor,
                                                                        std::hash<VertexAccessor>,
                                                                        std::equal_to<void>,
                                                                        utils::Allocator<VertexAccessor>> *iterable_) {
                                                    return Iterator(iterable_->begin());
                                                  },
                                                  [this](versioning::BranchContext::UnionVerticesIterable &iterable_) {
                                                    return Iterator(iterable_.begin(), branch_ctx_);
                                                  },
                                                  [](std::shared_ptr<std::vector<VertexAccessor>> &materialized) {
                                                    return Iterator(materialized->begin(), materialized);
                                                  }},
                      iterable_);
  }

  Iterator end() {
    return std::visit(
        memgraph::utils::Overloaded{
            [this](storage::VerticesIterable &iterable_) { return Iterator(iterable_.end(), branch_ctx_); },
            [](std::unordered_set<VertexAccessor,
                                  std::hash<VertexAccessor>,
                                  std::equal_to<void>,
                                  utils::Allocator<VertexAccessor>> *iterable_) { return Iterator(iterable_->end()); },
            [this](versioning::BranchContext::UnionVerticesIterable &iterable_) {
              return Iterator(iterable_.end(), branch_ctx_);
            },
            [](std::shared_ptr<std::vector<VertexAccessor>> &materialized) {
              return Iterator(materialized->end(), materialized);
            }},
        iterable_);
  }
};

template <typename storage_iterator>
struct query_vertex_iterator final {
  using value_type = VertexAccessor;

  explicit query_vertex_iterator(storage_iterator it) : it_(std::move(it)) {}

  query_vertex_iterator(query_vertex_iterator const &) = default;
  query_vertex_iterator(query_vertex_iterator &&) = default;
  query_vertex_iterator &operator=(query_vertex_iterator const &) = default;
  query_vertex_iterator &operator=(query_vertex_iterator &&) = default;

  VertexAccessor operator*() const { return VertexAccessor{*it_}; }

  auto operator++() -> query_vertex_iterator & {
    ++it_;
    return *this;
  }

  friend bool operator==(query_vertex_iterator const &, query_vertex_iterator const &) = default;

 private:
  storage_iterator it_;
};

template <typename storage_iterable>
struct query_iterable final {
  using iterator = query_vertex_iterator<typename storage_iterable::iterator>;

  explicit query_iterable(storage_iterable iterable) : iterable_(std::move(iterable)) {}

  query_iterable(query_iterable const &) = default;
  query_iterable(query_iterable &&) = default;
  query_iterable &operator=(query_iterable const &) = default;
  query_iterable &operator=(query_iterable &&) = default;
  ~query_iterable() = default;

  iterator begin() { return iterator{iterable_.begin()}; }

  iterator end() { return iterator{iterable_.end()}; }

 private:
  storage_iterable iterable_;
};

class VerticesChunkedIterable {
 public:
  storage::VerticesChunkedIterable chunks_;

  class Iterator {
   public:
    storage::VerticesChunkedIterable::Iterator it_;

    VertexAccessor operator*() const { return VertexAccessor(*it_); }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return it_ != other.it_; }

    Iterator &operator++() {
      ++it_;
      return *this;
    }
  };

  class Chunk {
    Iterator begin_;
    Iterator end_;

   public:
    explicit Chunk(auto &&chunk) : begin_{chunk.begin()}, end_{chunk.end()} {}

    Iterator begin() { return begin_; }

    Iterator end() { return end_; }
  };

  Chunk get_chunk(size_t id) { return Chunk{chunks_.get_chunk(id)}; }

  size_t size() const { return chunks_.size(); }
};

class EdgesChunkedIterable {
 public:
  storage::EdgesChunkedIterable chunks_;

  class Iterator {
   public:
    storage::EdgesChunkedIterable::Iterator it_;

    EdgeAccessor operator*() const { return EdgeAccessor(*it_); }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return it_ != other.it_; }

    Iterator &operator++() {
      ++it_;
      return *this;
    }
  };

  class Chunk {
    Iterator begin_;
    Iterator end_;

   public:
    explicit Chunk(auto &&chunk) : begin_{chunk.begin()}, end_{chunk.end()} {}

    Iterator begin() { return begin_; }

    Iterator end() { return end_; }
  };

  Chunk get_chunk(size_t id) { return Chunk{chunks_.get_chunk(id)}; }

  size_t size() const { return chunks_.size(); }
};

class EdgesIterable final {
  // 3rd alternative: materialized, already-filtered branch scan result; its EdgeAccessors are branch-stamped at
  // materialization, so unlike VerticesIterable no branch_ctx_ member is needed.
  std::variant<
      storage::EdgesIterable,
      std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>, utils::Allocator<EdgeAccessor>> *,
      std::shared_ptr<std::vector<EdgeAccessor>>>
      iterable_;

 public:
  class Iterator final {
    std::variant<storage::EdgesIterable::Iterator,
                 std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                    utils::Allocator<EdgeAccessor>>::iterator,
                 std::vector<EdgeAccessor>::iterator>
        it_;
    std::shared_ptr<std::vector<EdgeAccessor>> materialized_;

   public:
    explicit Iterator(storage::EdgesIterable::Iterator it) : it_(std::move(it)) {}

    explicit Iterator(std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                         utils::Allocator<EdgeAccessor>>::iterator it)
        : it_(it) {}

    Iterator(std::vector<EdgeAccessor>::iterator it, std::shared_ptr<std::vector<EdgeAccessor>> materialized)
        : it_(it), materialized_(std::move(materialized)) {}

    EdgeAccessor operator*() const {
      return std::visit(
          memgraph::utils::Overloaded{
              [](const storage::EdgesIterable::Iterator &it) { return EdgeAccessor(*it); },
              [](const std::unordered_set<EdgeAccessor,
                                          std::hash<EdgeAccessor>,
                                          std::equal_to<void>,
                                          utils::Allocator<EdgeAccessor>>::iterator &it) { return EdgeAccessor(*it); },
              [](const std::vector<EdgeAccessor>::iterator &it) { return *it; }},
          it_);
    }

    Iterator &operator++() {
      std::visit([](auto &it_) { ++it_; }, it_);
      return *this;
    }

    bool operator==(const Iterator &other) const { return it_ == other.it_; }

    bool operator!=(const Iterator &other) const { return !(other == *this); }
  };

  explicit EdgesIterable(storage::EdgesIterable iterable) : iterable_(std::move(iterable)) {}

  explicit EdgesIterable(std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                            utils::Allocator<EdgeAccessor>> *edges)
      : iterable_(edges) {}

  explicit EdgesIterable(std::shared_ptr<std::vector<EdgeAccessor>> materialized)
      : iterable_(std::move(materialized)) {}

  Iterator begin() {
    return std::visit(
        memgraph::utils::Overloaded{
            [](storage::EdgesIterable &iterable_) { return Iterator(iterable_.begin()); },
            [](std::unordered_set<EdgeAccessor,
                                  std::hash<EdgeAccessor>,
                                  std::equal_to<void>,
                                  utils::Allocator<EdgeAccessor>> *iterable_) { return Iterator(iterable_->begin()); },
            [](std::shared_ptr<std::vector<EdgeAccessor>> &materialized) {
              return Iterator(materialized->begin(), materialized);
            }},
        iterable_);
  }

  Iterator end() {
    return std::visit(
        memgraph::utils::Overloaded{
            [](storage::EdgesIterable &iterable_) { return Iterator(iterable_.end()); },
            [](std::unordered_set<EdgeAccessor,
                                  std::hash<EdgeAccessor>,
                                  std::equal_to<void>,
                                  utils::Allocator<EdgeAccessor>> *iterable_) { return Iterator(iterable_->end()); },
            [](std::shared_ptr<std::vector<EdgeAccessor>> &materialized) {
              return Iterator(materialized->end(), materialized);
            }},
        iterable_);
  }
};

using PointIterable = query_iterable<storage::PointIterable>;

/// Query-layer chunk collection that wraps storage layer chunk collection.
/// This follows the same pattern as other query layer iterables.
class VerticesChunkCollection final {
  std::vector<std::pair<VerticesIterable::Iterator, VerticesIterable::Iterator>> chunks_;

 public:
  explicit VerticesChunkCollection(
      std::vector<std::pair<VerticesIterable::Iterator, VerticesIterable::Iterator>> &&chunks)
      : chunks_(std::move(chunks)) {}

  size_t size() const { return chunks_.size(); }

  bool empty() const { return chunks_.empty(); }

  auto begin() { return chunks_.begin(); }

  auto end() { return chunks_.end(); }

  const auto &operator[](size_t index) const { return chunks_[index]; }

  auto &operator[](size_t index) { return chunks_[index]; }
};

class DbAccessor final {
  storage::Storage::Accessor *accessor_;
  // Non-null iff this belongs to a checked-out branch's transaction, where `accessor_` is the branch's diff-engine
  // accessor. Non-const: BranchContext's resolve/COW methods mutate it.
  versioning::BranchContext *branch_ctx_{nullptr};

  static bool VertexHasLabel(VertexAccessor &v, storage::View view, storage::LabelId label) {
    auto has_label = v.HasLabel(view, label);
    return has_label.has_value() && *has_label;
  }

  // Full scan of the branch union, O(graph) regardless of selectivity; fallback when an index is not ready.
  template <typename Pred>
  auto MaterializeFilteredBranchScan(storage::View view, Pred &&predicate)
      -> std::shared_ptr<std::vector<VertexAccessor>> {
    auto result = std::make_shared<std::vector<VertexAccessor>>();
    for (auto raw : branch_ctx_->Vertices(view)) {
      VertexAccessor candidate(raw, branch_ctx_);
      if (predicate(candidate)) {
        result->push_back(candidate);
      }
    }
    return result;
  }

  // Index-scan overlay: the fork index minus touched vertices, merged in index order with the touched set rechecked on
  // the reconstructed vertex. O(log V + fork matches + touched), never O(V).
  using BranchKey = std::vector<storage::PropertyValue>;

  // Deduplicated by gid; fork gids are held as fork accessors. A deleted fork vertex may be a candidate: the recheck
  // fails with DELETED_OBJECT and drops it.
  struct BranchTouchedSet {
    explicit BranchTouchedSet(versioning::BranchContext *ctx) : ctx_(ctx) {}

    versioning::BranchContext *ctx_;
    std::unordered_set<storage::Gid> seen;
    std::vector<storage::VertexAccessor> items;

    void Add(storage::VertexAccessor const &raw) {
      if (seen.insert(raw.Gid()).second) items.push_back(ctx_->ForkIdentity(raw));
    }
  };

  static std::optional<BranchKey> ReadBranchKey(VertexAccessor &v, storage::View view,
                                                std::span<storage::PropertyPath const> properties) {
    BranchKey key;
    key.reserve(properties.size());
    for (auto const &path : properties) {
      auto top = v.GetProperty(view, path.front());
      if (!top.has_value()) return std::nullopt;
      if (path.size() == 1) {
        key.push_back(std::move(*top));
      } else {
        const auto *nested = storage::ReadNestedPropertyValue(*top, path.as_span().subspan(1));
        if (nested == nullptr) return std::nullopt;
        key.push_back(*nested);
      }
    }
    return key;
  }

  // Mirrors the storage index: `property_ranges` constrains only a leading prefix of the key, IS_NOT_NULL excludes
  // null, the planner's value predicate applies to the leading property, and an all-null key is never indexed.
  static bool BranchKeyInRange(BranchKey const &key, std::span<storage::PropertyValueRange const> property_ranges) {
    for (size_t i = 0; i < property_ranges.size() && i < key.size(); ++i) {
      auto const &range = property_ranges[i];
      if (!range.IsValueInRange(key[i])) return false;
      if (range.type_ == storage::PropertyRangeType::IS_NOT_NULL && key[i].IsNull()) return false;
      if (i == 0) {
        if (auto const &pred = range.GetValuePredicate(); pred && !(*pred)(key[0])) return false;
      }
    }
    return !std::ranges::all_of(key, [](auto const &v) { return v.IsNull(); });
  }

  // Untouched fork candidates keep stream order; those with an overlay join `touched`. A clear `branched()` bit is a
  // lock-free fast path; `label_touched` (single-label scans) replaces the coarse any-label filter.
  std::vector<storage::VertexAccessor> SplitBranchForkStream(
      storage::VerticesIterable fork_stream, BranchTouchedSet &touched,
      versioning::BranchCore::LabelTouchedSet const *label_touched = nullptr) {
    std::vector<storage::VertexAccessor> untouched;
    for (auto raw : fork_stream) {
      auto const label_changed = [&](storage::Gid gid) {
        return label_touched != nullptr ? label_touched->contains(gid.AsUint()) : branch_ctx_->MayHaveLabelChange(gid);
      };
      if (!raw.vertex_->branched() || !(label_changed(raw.Gid()) || branch_ctx_->MayHavePropertyChange(raw.Gid()))) {
        untouched.push_back(raw);
        continue;
      }
      if (branch_ctx_->HasVertexOverlay(raw.Gid())) {
        touched.Add(raw);
      } else {
        untouched.push_back(raw);
      }
    }
    return untouched;
  }

  template <typename ReadKey, typename Accept>
  VerticesIterable MergeBranchOverlay(std::vector<storage::VertexAccessor> untouched, BranchTouchedSet &touched,
                                      storage::IndexOrder order, ReadKey &&read_key, Accept &&accept) {
    auto result = std::make_shared<std::vector<VertexAccessor>>();
    if (touched.items.empty()) {
      result->reserve(untouched.size());
      for (auto &raw : untouched) result->emplace_back(raw, branch_ctx_);
      return VerticesIterable(std::move(result));
    }

    struct Keyed {
      BranchKey key;
      VertexAccessor vertex;
    };

    std::vector<Keyed> diff;
    diff.reserve(touched.items.size());
    for (auto &raw : touched.items) {
      VertexAccessor candidate(raw, branch_ctx_);
      auto key = read_key(candidate);
      if (!key || !accept(candidate, *key)) continue;
      diff.push_back(Keyed{std::move(*key), candidate});
    }
    bool const asc = order == storage::IndexOrder::ASC;
    auto before = [asc](BranchKey const &ak, storage::Gid ag, BranchKey const &bk, storage::Gid bg) {
      if (ak < bk) return asc;
      if (bk < ak) return !asc;
      return asc ? ag.AsUint() < bg.AsUint() : ag.AsUint() > bg.AsUint();
    };
    std::sort(diff.begin(), diff.end(), [&](Keyed const &a, Keyed const &b) {
      return before(a.key, a.vertex.Gid(), b.key, b.vertex.Gid());
    });

    result->reserve(untouched.size() + diff.size());
    size_t d = 0;
    for (auto &raw : untouched) {
      VertexAccessor fork_vertex(raw, branch_ctx_);
      if (d < diff.size()) {
        auto const fork_key = read_key(fork_vertex).value_or(BranchKey{});
        auto const fork_gid = fork_vertex.Gid();
        while (d < diff.size() && before(diff[d].key, diff[d].vertex.Gid(), fork_key, fork_gid)) {
          result->push_back(diff[d++].vertex);
        }
      }
      result->push_back(fork_vertex);
    }
    for (; d < diff.size(); ++d) result->push_back(diff[d].vertex);
    return VerticesIterable(std::move(result));
  }

  // A range that rejects null anchors the match: a vertex satisfying it has a non-null key, so "all-null" cannot be
  // the reason a fork vertex failed.
  static bool RangeExcludesNull(storage::PropertyValueRange const &range) {
    return range.type_ == storage::PropertyRangeType::IS_NOT_NULL ||
           (range.type_ == storage::PropertyRangeType::BOUNDED && (range.lower_ || range.upper_));
  }

  // Adds the fork vertices named by a touched set in gid order (stable merge). The set is a superset of the changed
  // vertices, so every candidate is rechecked on its reconstructed state.
  void AddTouchedForkVertices(BranchTouchedSet &touched, versioning::BranchCore::LabelTouchedSet const &gids) {
    std::vector<uint64_t> sorted(gids.begin(), gids.end());
    std::ranges::sort(sorted);
    for (auto const gid : sorted) {
      if (auto fork = branch_ctx_->FindForkVertex(storage::Gid::FromUint(gid))) touched.Add(*fork);
    }
  }

  template <typename Key, typename Extract, typename Less>
  VerticesIterable MaterializeSortedBranchScan(storage::View view, Extract &&extract, Less &&less) {
    using Keyed = std::pair<Key, VertexAccessor>;
    std::vector<Keyed> keyed;
    for (auto raw : branch_ctx_->Vertices(view)) {
      VertexAccessor candidate(raw, branch_ctx_);
      auto key = extract(candidate);
      if (!key) continue;
      keyed.emplace_back(std::move(*key), candidate);
    }
    std::stable_sort(
        keyed.begin(), keyed.end(), [&](Keyed const &a, Keyed const &b) { return less(a.first, b.first); });
    auto result = std::make_shared<std::vector<VertexAccessor>>();
    result->reserve(keyed.size());
    for (auto &entry : keyed) result->push_back(entry.second);
    return VerticesIterable(std::move(result));
  }

  // Full-scan fallback for a label+property scan, used when main's or the diff engine's index is not ready.
  VerticesIterable MaterializeSortedBranchLabelPropertyScan(
      storage::View view, storage::LabelId label, std::span<storage::PropertyPath const> properties,
      std::span<storage::PropertyValueRange const> property_ranges, storage::IndexOrder order) {
    return MaterializeSortedBranchScan<BranchKey>(
        view,
        [&](VertexAccessor &v) -> std::optional<BranchKey> {
          if (!VertexHasLabel(v, view, label)) return std::nullopt;
          auto key = ReadBranchKey(v, view, properties);
          if (!key || !BranchKeyInRange(*key, property_ranges)) return std::nullopt;
          return key;
        },
        [order](BranchKey const &a, BranchKey const &b) { return order == storage::IndexOrder::ASC ? a < b : b < a; });
  }

  // Nothing relevant changed on the branch (caller checked its touched snapshots and deletes) and the diff engine's
  // index has no hit: the answer is main's index stream at fork, in index order, yielded lazily. `index_size()` is the
  // diff engine's index skiplist size (counts uncommitted/un-GC'd entries, so 0 means nothing visible to anyone); it is
  // only evaluated when the diff engine has vertices.
  template <typename IndexSize, typename DiffScan, typename ForkScan>
  std::optional<VerticesIterable> LazyForkOnlyScan(IndexSize &&index_size, DiffScan &&diff_scan, ForkScan &&fork_scan) {
    if (accessor_->ApproximateVertexCount() != 0 && index_size() != 0) {
      auto diff = diff_scan();
      if (diff.begin() != diff.end()) return std::nullopt;
    }
    return VerticesIterable(fork_scan(), branch_ctx_);
  }

  VerticesIterable BranchLabelPropertyScan(storage::View view, storage::LabelId label,
                                           std::span<storage::PropertyPath const> properties,
                                           std::span<storage::PropertyValueRange const> property_ranges,
                                           storage::IndexOrder order) {
    auto &historical = branch_ctx_->historical();
    bool const overlay_ready =
        historical.LabelPropertyIndexReady(label, properties) && accessor_->LabelPropertyIndexReady(label, properties);
    if (!overlay_ready) {
      return MaterializeSortedBranchLabelPropertyScan(view, label, properties, property_ranges, order);
    }
    if (std::ranges::any_of(property_ranges,
                            [](auto const &r) { return r.type_ == storage::PropertyRangeType::INVALID; })) {
      return VerticesIterable(std::make_shared<std::vector<VertexAccessor>>());
    }

    // A null-excluding range anchors the match, so only constrained positions can have turned into a match; otherwise
    // every key position is enumerated.
    bool const anchored = std::ranges::any_of(property_ranges, RangeExcludesNull);
    auto const key_enumerated = [&](size_t i) { return !anchored || i < property_ranges.size(); };

    // Lock-free pre-check (gates are set before commit; clear = empty snapshot). The label snapshot also merges the
    // deleted gids, hence the delete check.
    bool maybe_touched = branch_ctx_->MayHaveLabelTouched(label) || branch_ctx_->MayHaveAnyVertexDeleted();
    for (size_t i = 0; !maybe_touched && i < properties.size(); ++i) {
      if (key_enumerated(i)) maybe_touched = branch_ctx_->MayHavePropertyTouched(properties[i].front());
    }
    if (!maybe_touched) {
      if (auto lazy = LazyForkOnlyScan(
              [&] { return accessor_->ApproximateVertexCount(label, properties); },
              [&] { return accessor_->Vertices(label, properties, property_ranges, view, order); },
              [&] { return historical.Vertices(label, properties, property_ranges, storage::View::OLD, order); })) {
        return std::move(*lazy);
      }
    }

    // One snapshot up front: any txn visible to this scan recorded its gids before this point.
    auto const label_touched = branch_ctx_->LabelTouchedSnapshot(label);
    std::vector<std::shared_ptr<const versioning::BranchCore::LabelTouchedSet>> property_touched;
    property_touched.reserve(properties.size());
    for (size_t i = 0; i < properties.size(); ++i) {
      if (!key_enumerated(i)) continue;
      property_touched.push_back(branch_ctx_->PropertyTouchedSnapshot(properties[i].front()));
    }

    // The label snapshot includes every deleted fork gid, so an empty one also means no deletes.
    if (label_touched->empty() && std::ranges::all_of(property_touched, [](auto const &t) { return t->empty(); })) {
      if (auto lazy = LazyForkOnlyScan(
              [&] { return accessor_->ApproximateVertexCount(label, properties); },
              [&] { return accessor_->Vertices(label, properties, property_ranges, view, order); },
              [&] { return historical.Vertices(label, properties, property_ranges, storage::View::OLD, order); })) {
        return std::move(*lazy);
      }
    }

    BranchTouchedSet touched{branch_ctx_};

    // Candidates the fork index cannot contain: branch-native hits, vertices that gained `label` or SET a key property.
    for (auto raw : accessor_->Vertices(label, properties, property_ranges, view, order)) touched.Add(raw);
    AddTouchedForkVertices(touched, *label_touched);
    for (auto const &t : property_touched) AddTouchedForkVertices(touched, *t);

    auto untouched =
        SplitBranchForkStream(historical.Vertices(label, properties, property_ranges, storage::View::OLD, order),
                              touched,
                              label_touched.get());
    return MergeBranchOverlay(
        std::move(untouched),
        touched,
        order,
        [&](VertexAccessor &v) { return ReadBranchKey(v, view, properties); },
        [&](VertexAccessor &v, BranchKey const &key) {
          return VertexHasLabel(v, view, label) && BranchKeyInRange(key, property_ranges);
        });
  }

  // Global vertex-property scan overlay (ascending by value). `fork_scan`/`diff_scan` open the scan on main@fork and
  // the diff engine (native vertices only); `in_range` is the predicate for the reconstructed vertex.
  template <typename ForkScan, typename DiffScan, typename InRange>
  VerticesIterable BranchPropertyScan(storage::View view, storage::PropertyId property, ForkScan &&fork_scan,
                                      DiffScan &&diff_scan, InRange &&in_range) {
    if (!branch_ctx_->historical().VertexPropertyIndexReady(property) ||
        !accessor_->VertexPropertyIndexReady(property)) {
      return MaterializeSortedBranchScan<storage::PropertyValue>(
          view,
          [&](VertexAccessor &v) -> std::optional<storage::PropertyValue> {
            auto prop = v.GetProperty(view, property);
            if (!prop.has_value() || prop->IsNull() || !in_range(*prop)) return std::nullopt;
            return std::move(*prop);
          },
          [](storage::PropertyValue const &a, storage::PropertyValue const &b) { return a < b; });
    }
    // Lock-free pre-check first (gate set before commit; clear = empty snapshot), then the snapshot.
    if (!branch_ctx_->MayHavePropertyTouched(property) && !branch_ctx_->MayHaveAnyVertexDeleted()) {
      if (auto lazy =
              LazyForkOnlyScan([&] { return accessor_->ApproximateVertexCount(property); }, diff_scan, fork_scan)) {
        return std::move(*lazy);
      }
    }
    auto const property_touched = branch_ctx_->PropertyTouchedSnapshot(property);
    if (property_touched->empty() && !branch_ctx_->MayHaveAnyVertexDeleted()) {
      if (auto lazy =
              LazyForkOnlyScan([&] { return accessor_->ApproximateVertexCount(property); }, diff_scan, fork_scan)) {
        return std::move(*lazy);
      }
    }
    BranchTouchedSet touched{branch_ctx_};
    for (auto raw : diff_scan()) touched.Add(raw);
    AddTouchedForkVertices(touched, *property_touched);
    auto untouched = SplitBranchForkStream(fork_scan(), touched);
    return MergeBranchOverlay(
        std::move(untouched),
        touched,
        storage::IndexOrder::ASC,
        [&](VertexAccessor &v) -> std::optional<BranchKey> {
          auto value = v.GetProperty(view, property);
          if (!value.has_value()) return std::nullopt;
          BranchKey key;
          key.push_back(std::move(*value));
          return key;
        },
        [&](VertexAccessor &, BranchKey const &key) { return !key[0].IsNull() && in_range(key[0]); });
  }

  // Walks every branch edge once via each vertex's OUT adjacency (an edge lives on its FROM vertex; IN would double-
  // count). O(graph); `fn(raw_edge)` returns true to stop.
  template <typename Fn>
  void ForEachBranchEdge(storage::View view, Fn &&fn) {
    for (auto raw_v : branch_ctx_->Vertices(view)) {
      for (auto &raw_e : branch_ctx_->ResolveEdges(raw_v.Gid(), storage::EdgeDirection::OUT, view)) {
        if (fn(raw_e)) return;
      }
    }
  }

  template <typename Pred>
  auto MaterializeFilteredBranchEdgeScan(storage::View view, Pred &&predicate)
      -> std::shared_ptr<std::vector<EdgeAccessor>> {
    auto result = std::make_shared<std::vector<EdgeAccessor>>();
    ForEachBranchEdge(view, [&](auto &raw_e) {
      EdgeAccessor candidate(raw_e, branch_ctx_);
      if (predicate(candidate)) result->push_back(candidate);
      return false;
    });
    return result;
  }

  // Full-scan fallback for an edge-property scan the planner may treat as ordered: ascending by `property`, gid ties.
  template <typename Pred>
  auto MaterializeSortedBranchEdgePropertyScan(storage::View view, storage::PropertyId property, Pred &&predicate)
      -> std::shared_ptr<std::vector<EdgeAccessor>> {
    using KeyedEdge = std::pair<storage::PropertyValue, EdgeAccessor>;
    std::vector<KeyedEdge> keyed;
    ForEachBranchEdge(view, [&](auto &raw_e) {
      EdgeAccessor candidate(raw_e, branch_ctx_);
      if (!predicate(candidate)) return false;
      auto prop = candidate.GetProperty(view, property);
      if (prop.has_value()) keyed.emplace_back(std::move(*prop), candidate);
      return false;
    });
    std::stable_sort(keyed.begin(), keyed.end(), [](KeyedEdge const &a, KeyedEdge const &b) {
      if (a.first < b.first) return true;
      if (b.first < a.first) return false;
      return a.second.Gid().AsUint() < b.second.Gid().AsUint();
    });
    auto result = std::make_shared<std::vector<EdgeAccessor>>();
    result->reserve(keyed.size());
    for (auto &entry : keyed) result->push_back(entry.second);
    return result;
  }

  // Ascending by `property` (planner elides OrderBy): merges the diff index (native edges), the fork index minus hidden
  // or touched edges, and touched fork edges re-read via the overlay. O(log E + fork matches + touched).
  template <typename InRange>
  auto MergeBranchEdgePropertyScan(storage::View view, storage::PropertyId property,
                                   std::optional<storage::EdgeTypeId> edge_type, storage::EdgesIterable diff_stream,
                                   storage::EdgesIterable hist_stream, InRange &&in_range)
      -> std::shared_ptr<std::vector<EdgeAccessor>> {
    using KeyedEdge = std::pair<storage::PropertyValue, EdgeAccessor>;
    auto const by_value = [](KeyedEdge const &a, KeyedEdge const &b) { return a.first < b.first; };

    // One snapshot up front: any txn visible to this scan recorded its edges before this point.
    auto const touched = branch_ctx_->EdgePropertyTouchedSnapshot(property);

    std::vector<KeyedEdge> native;
    for (auto raw : diff_stream) {
      auto prop = raw.GetProperty(property, view);
      if (!prop.has_value()) continue;
      native.emplace_back(std::move(*prop), EdgeAccessor(raw, branch_ctx_));
    }

    std::vector<KeyedEdge> untouched;
    for (auto raw : hist_stream) {
      if (touched->contains(raw.Gid().AsUint()) || branch_ctx_->IsForkEdgeHidden(raw, view)) continue;
      auto prop = raw.GetProperty(property, storage::View::OLD);
      if (!prop.has_value()) continue;
      untouched.emplace_back(std::move(*prop), EdgeAccessor(raw, branch_ctx_));
    }

    std::vector<KeyedEdge> changed;
    for (auto const &[edge_gid, from_gid] : *touched) {
      auto fork = branch_ctx_->FindForkEdge(storage::Gid::FromUint(edge_gid), storage::Gid::FromUint(from_gid));
      if (!fork || (edge_type && fork->EdgeType() != *edge_type) || branch_ctx_->IsForkEdgeHidden(*fork, view))
        continue;
      EdgeAccessor candidate(*fork, branch_ctx_);
      auto prop = candidate.GetProperty(view, property);
      if (!prop.has_value() || prop->IsNull() || !in_range(*prop)) continue;
      changed.emplace_back(std::move(*prop), std::move(candidate));
    }
    std::ranges::sort(changed, [&](KeyedEdge const &a, KeyedEdge const &b) {
      if (by_value(a, b)) return true;
      if (by_value(b, a)) return false;
      return a.second.Gid().AsUint() < b.second.Gid().AsUint();
    });

    std::vector<KeyedEdge> fork_side;
    fork_side.reserve(untouched.size() + changed.size());
    std::ranges::merge(untouched, changed, std::back_inserter(fork_side), by_value);
    std::vector<KeyedEdge> merged;
    merged.reserve(native.size() + fork_side.size());
    std::ranges::merge(native, fork_side, std::back_inserter(merged), by_value);

    auto result = std::make_shared<std::vector<EdgeAccessor>>();
    result->reserve(merged.size());
    for (auto &entry : merged) result->push_back(std::move(entry.second));
    return result;
  }

  // The diff engine's native check sees only its own adjacency (for a fork vertex, just its anchor's branch-created
  // edges, not fork edges), so consult the branch's full adjacency. Edges this same statement deletes
  // (`co_deleted_edge_gids`) do not count against the vertex.
  bool BranchVertexHasUncountedEdges(storage::Gid vertex_gid,
                                     const std::unordered_set<storage::Gid> &co_deleted_edge_gids) {
    auto has_uncounted = [&](storage::EdgeDirection direction) {
      auto edges = branch_ctx_->ResolveEdges(vertex_gid, direction, storage::View::NEW);
      return std::ranges::any_of(edges, [&](auto &e) { return !co_deleted_edge_gids.contains(e.Gid()); });
    };
    return has_uncounted(storage::EdgeDirection::OUT) || has_uncounted(storage::EdgeDirection::IN);
  }

  // DELETE on a branch: diff-engine objects are deleted normally; fork vertices/edges get the overlay delete flag
  // (OverlayMarkDeleted/DeleteForkEdge), so DETACH DELETE of a fork vertex deletes only its diff-engine edges for real.
  storage::Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>> BranchDetachDelete(
      std::vector<VertexAccessor> nodes, std::vector<EdgeAccessor> edges, bool detach) {
    using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;
    auto &ctx = *branch_ctx_;
    constexpr auto kView = storage::View::NEW;

    std::erase_if(nodes, [&](const VertexAccessor &node) {
      return ctx.IsForkVertexGid(node.Gid()) ? ctx.IsVertexDeleted(node.Gid(), kView)
                                             : !accessor_->FindVertex(node.Gid(), kView).has_value();
    });
    std::erase_if(edges, [&](const EdgeAccessor &edge) {
      return ctx.IsForkEdgeGid(edge.Gid())
                 ? ctx.IsForkEdgeHidden(edge.impl_, kView)
                 : !ctx.FindDiffEdge(edge.Gid(), edge.impl_.from_vertex_->gid, kView).has_value();
    });

    // Plain DELETE fails like main if a visible edge remains (edges this statement deletes excluded); checked before
    // any write.
    if (!detach) {
      std::unordered_set<storage::Gid> co_deleted_edges;
      for (const auto &edge : edges) co_deleted_edges.insert(edge.Gid());
      for (const auto &node : nodes) {
        if (BranchVertexHasUncountedEdges(node.Gid(), co_deleted_edges)) {
          return std::unexpected{storage::Error::VERTEX_HAS_EDGES};
        }
      }
    }

    std::vector<VertexAccessor> deleted_vertices;
    std::vector<EdgeAccessor> deleted_edges;
    std::vector<EdgeAccessor> diff_edges;  // handed to the diff-engine delete
    std::unordered_set<storage::Gid> seen_edges;

    for (auto &edge : edges) {
      if (!seen_edges.insert(edge.Gid()).second) continue;
      if (ctx.IsForkEdgeGid(edge.Gid())) {
        auto deleted = ctx.DeleteForkEdge(edge.impl_);
        if (!deleted) return std::unexpected{deleted.error()};
        if (*deleted) deleted_edges.push_back(edge);
      } else {
        diff_edges.push_back(edge);
      }
    }

    std::vector<VertexAccessor *> diff_nodes;
    std::vector<VertexAccessor *> fork_nodes;
    for (auto &node : nodes) {
      if (!ctx.IsForkVertexGid(node.Gid())) {
        auto cowed = ctx.CowVertex(node.Gid());
        if (!cowed) throw QueryRuntimeException(cowed.error().message);
        node.impl_ = *cowed;
        diff_nodes.push_back(&node);
        continue;
      }
      fork_nodes.push_back(&node);
      if (!detach) continue;
      // All incident edges are enumerated before any delete flag is written, so an edge between two deleted vertices
      // is seen from both ends and counted once.
      for (auto direction : {storage::EdgeDirection::OUT, storage::EdgeDirection::IN}) {
        for (auto &raw : ctx.ResolveEdges(node.Gid(), direction, kView)) {
          if (!seen_edges.insert(raw.Gid()).second) continue;
          EdgeAccessor incident(raw, branch_ctx_);
          if (raw.storage_ == &ctx.diff_engine()) {
            diff_edges.push_back(std::move(incident));
          } else {
            if (!ctx.CanDeleteForkEdge(raw.Gid())) return std::unexpected{storage::Error::SERIALIZATION_ERROR};
            deleted_edges.push_back(std::move(incident));
          }
        }
      }
    }

    if (!diff_nodes.empty() || !diff_edges.empty()) {
      std::vector<storage::VertexAccessor *> nodes_impl;
      std::vector<storage::EdgeAccessor *> edges_impl;
      nodes_impl.reserve(diff_nodes.size());
      edges_impl.reserve(diff_edges.size());
      for (auto *node : diff_nodes) nodes_impl.push_back(&node->impl_);
      for (auto &edge : diff_edges) edges_impl.push_back(&edge.impl_);
      auto res = accessor_->DetachDelete(std::move(nodes_impl), std::move(edges_impl), detach);
      if (!res) return std::unexpected{res.error()};
      if (*res) {
        for (const auto &v : (*res)->first) deleted_vertices.emplace_back(v, branch_ctx_);
        for (const auto &e : (*res)->second) deleted_edges.emplace_back(e, branch_ctx_);
      }
    }

    for (auto *node : fork_nodes) {
      auto marked = ctx.OverlayMarkDeleted(node->Gid());
      if (!marked) return std::unexpected{marked.error()};
      if (!*marked) continue;
      deleted_vertices.push_back(*node);
    }

    return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
  }

 public:
  explicit DbAccessor(storage::Storage::Accessor *accessor, versioning::BranchContext *branch_ctx = nullptr)
      : accessor_(accessor), branch_ctx_(branch_ctx) {}

  // Non-null iff this accessor serves a checked-out branch's transaction (never on main or a USING VERSION 'main'
  // query).
  versioning::BranchContext *branch_context() const { return branch_ctx_; }

  void SetParallelExecution() { accessor_->GetTransaction()->SetParallelExecution(); }

  bool CheckIndicesAreReady(storage::IndicesCollection const &required_indices) const {
    return accessor_->CheckIndicesAreReady(required_indices);
  }

  auto type() const { return accessor_->type(); }

  std::optional<VertexAccessor> FindVertex(storage::Gid gid, storage::View view) {
    if (branch_ctx_ != nullptr) {
      auto maybe_vertex = branch_ctx_->ResolveVertex(gid, view);
      if (maybe_vertex) return VertexAccessor(*maybe_vertex, branch_ctx_);
      return std::nullopt;
    }
    auto maybe_vertex = accessor_->FindVertex(gid, view);
    if (maybe_vertex) return VertexAccessor(*maybe_vertex);
    return std::nullopt;
  }

  // On a branch `accessor_` is the diff engine only: look up diff-first, else union scan O(V+E).
  // Prefer the 3-argument overload when the FROM vertex is known.
  std::optional<EdgeAccessor> FindEdge(storage::Gid gid, storage::View view) {
    if (branch_ctx_ != nullptr) {
      if (auto diff_edge = branch_ctx_->FindDiffEdge(gid, view)) return EdgeAccessor(*diff_edge, branch_ctx_);
      std::optional<EdgeAccessor> found;
      ForEachBranchEdge(view, [&](auto &e) {
        if (e.Gid() != gid) return false;
        found.emplace(e, branch_ctx_);
        return true;
      });
      return found;
    }
    auto maybe_edge = accessor_->FindEdge(gid, view);
    if (maybe_edge) return EdgeAccessor(*maybe_edge);
    return std::nullopt;
  }

  // Walks only the FROM vertex's OUT adjacency, no full scan.
  std::optional<EdgeAccessor> FindEdge(storage::Gid edge_gid, storage::Gid from_vertex_gid, storage::View view) {
    if (branch_ctx_ != nullptr) {
      for (auto &e : branch_ctx_->ResolveEdges(from_vertex_gid, storage::EdgeDirection::OUT, view)) {
        if (e.Gid() == edge_gid) return EdgeAccessor(e, branch_ctx_);
      }
      return std::nullopt;
    }
    auto maybe_edge = accessor_->FindEdge(edge_gid, from_vertex_gid, view);
    if (maybe_edge) return EdgeAccessor(*maybe_edge);
    return std::nullopt;
  }

  void FinalizeTransaction() { accessor_->FinalizeTransaction(); }

  void TrackCurrentThreadAllocations() {
    auto *tracker = &accessor_->GetTransactionMemoryTracker();
    DMG_ASSERT(tracker, "Query memory tracker must be set before tracking allocations");
    memgraph::memory::StartTrackingCurrentThread(tracker);
  }

  static void UntrackCurrentThreadAllocations() { memgraph::memory::StopTrackingCurrentThread(); }

  auto &GetTransactionMemoryTracker() { return accessor_->GetTransactionMemoryTracker(); }

  auto GetStartTimestamp() { return accessor_->GetStartTimestamp(); }

  bool TransactionHasSerializationError() const { return accessor_->TransactionHasSerializationError(); }

  VerticesIterable Vertices(storage::View view) {
    if (branch_ctx_ != nullptr) {
      // No deletes and no branch-native vertices (skiplist size counts uncommitted/un-GC'd ones, so 0 means none
      // visible to anyone): the union is exactly the fork stream, yielded lazily without the per-vertex merge.
      if (!branch_ctx_->MayHaveAnyVertexDeleted() && accessor_->ApproximateVertexCount() == 0) {
        return VerticesIterable(branch_ctx_->historical().Vertices(storage::View::OLD), branch_ctx_);
      }
      return VerticesIterable(branch_ctx_->Vertices(view), branch_ctx_);
    }
    return VerticesIterable(accessor_->Vertices(view));
  }

  // Union of the fork and diff label indexes, unsorted; full-scan fallback if main dropped the index after the fork.
  // Fork candidates with an overlay are re-checked on the reconstructed vertex.
  VerticesIterable Vertices(storage::View view, storage::LabelId label) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.LabelIndexReady(label)) {
        return VerticesIterable(MaterializeFilteredBranchScan(
            view, [label, view](VertexAccessor &v) { return VertexHasLabel(v, view, label); }));
      }

      // One snapshot up front: any txn visible to this scan recorded its gids before this point.
      auto const label_touched = branch_ctx_->LabelTouchedSnapshot(label);

      // Nothing touched and no branch-native hits: the fork stream is the answer, yielded lazily.
      if (label_touched->empty()) {
        auto diff = accessor_->Vertices(label, view);
        if (diff.begin() == diff.end()) {
          return VerticesIterable(historical.Vertices(label, storage::View::OLD), branch_ctx_);
        }
      }

      auto result = std::make_shared<std::vector<VertexAccessor>>();
      result->reserve(historical.ApproximateVertexCount(label));

      // (a) Fork stream: a candidate the branch changed is re-checked on the reconstructed vertex (removed label or
      // delete flag drops it).
      for (auto raw : historical.Vertices(label, storage::View::OLD)) {
        VertexAccessor candidate(raw, branch_ctx_);
        if (raw.vertex_->branched() && label_touched->contains(raw.Gid().AsUint()) &&
            branch_ctx_->HasVertexOverlay(raw.Gid()) && !VertexHasLabel(candidate, view, label)) {
          continue;
        }
        result->push_back(candidate);
      }

      for (auto raw : accessor_->Vertices(label, view)) result->push_back(VertexAccessor(raw, branch_ctx_));

      // (c) Fork vertices that may have gained `label`, in gid order; ones that already had it were emitted by (a).
      std::vector<uint64_t> gained(label_touched->begin(), label_touched->end());
      std::ranges::sort(gained);
      for (auto const gid : gained) {
        auto const g = storage::Gid::FromUint(gid);
        if (branch_ctx_->IsVertexDeleted(g, view)) continue;
        auto fork = branch_ctx_->FindForkVertex(g);
        if (!fork) continue;
        auto had = fork->HasLabel(label, storage::View::OLD);
        if (had.has_value() && *had) continue;
        VertexAccessor candidate(*fork, branch_ctx_);
        if (VertexHasLabel(candidate, view, label)) result->push_back(candidate);
      }

      return VerticesIterable(std::move(result));
    }
    return VerticesIterable(accessor_->Vertices(label, view));
  }

  VerticesIterable Vertices(storage::View view, storage::LabelId label,
                            std::span<storage::PropertyPath const> properties,
                            std::span<storage::PropertyValueRange const> property_ranges,
                            storage::IndexOrder order = storage::IndexOrder::ASC) {
    if (branch_ctx_ != nullptr) {
      return BranchLabelPropertyScan(view, label, properties, property_ranges, order);
    }
    return VerticesIterable(accessor_->Vertices(label, properties, property_ranges, view, order));
  }

  VerticesIterable Vertices(storage::View view, storage::PropertyId property) {
    if (branch_ctx_ != nullptr) {
      return BranchPropertyScan(
          view,
          property,
          [&] { return branch_ctx_->historical().Vertices(property, storage::View::OLD); },
          [&] { return accessor_->Vertices(property, view); },
          [](storage::PropertyValue const &) { return true; });
    }
    return VerticesIterable(accessor_->Vertices(property, view));
  }

  VerticesIterable Vertices(storage::View view, storage::PropertyId property, storage::PropertyValue const &value) {
    if (branch_ctx_ != nullptr) {
      return BranchPropertyScan(
          view,
          property,
          [&] { return branch_ctx_->historical().Vertices(property, value, storage::View::OLD); },
          [&] { return accessor_->Vertices(property, value, view); },
          [&value](storage::PropertyValue const &v) { return v == value; });
    }
    return VerticesIterable(accessor_->Vertices(property, value, view));
  }

  VerticesIterable Vertices(storage::View view, storage::PropertyId property,
                            std::optional<utils::Bound<storage::PropertyValue>> const &lower,
                            std::optional<utils::Bound<storage::PropertyValue>> const &upper) {
    if (branch_ctx_ != nullptr) {
      auto range = storage::PropertyValueRange::Bounded(lower, upper);
      return BranchPropertyScan(
          view,
          property,
          [&] { return branch_ctx_->historical().Vertices(property, lower, upper, storage::View::OLD); },
          [&] { return accessor_->Vertices(property, lower, upper, view); },
          [&range](storage::PropertyValue const &v) { return range.IsValueInRange(v); });
    }
    return VerticesIterable(accessor_->Vertices(property, lower, upper, view));
  }

  // Chunked scans bypass the branch union and would return only the diff engine's contents; rejected until a union
  // chunked scan exists. Serial scans are unaffected.
  void RejectBranchChunkedVertexScan() const {
    if (branch_ctx_ != nullptr) throw NotYetImplemented("Parallel (chunked) vertex scans on a versioned branch");
  }

  void RejectBranchChunkedEdgeScan() const {
    if (branch_ctx_ != nullptr) throw NotYetImplemented("Parallel (chunked) edge scans on a versioned branch");
  }

  VerticesChunkedIterable ChunkedVertices(storage::View view, size_t num_chunks) {
    RejectBranchChunkedVertexScan();
    return VerticesChunkedIterable{accessor_->ChunkedVertices(view, num_chunks)};
  }

  VerticesChunkedIterable ChunkedVertices(storage::View view, storage::LabelId label, size_t num_chunks) {
    RejectBranchChunkedVertexScan();
    return VerticesChunkedIterable{accessor_->ChunkedVertices(label, view, num_chunks)};
  }

  VerticesChunkedIterable ChunkedVertices(storage::View view, storage::LabelId label,
                                          std::span<storage::PropertyPath const> properties,
                                          std::span<storage::PropertyValueRange const> property_ranges,
                                          size_t num_chunks, storage::IndexOrder order = storage::IndexOrder::ASC) {
    RejectBranchChunkedVertexScan();
    return VerticesChunkedIterable{
        accessor_->ChunkedVertices(label, properties, property_ranges, view, num_chunks, order)};
  }

  VerticesChunkedIterable ChunkedVertices(storage::View view, storage::PropertyId property, size_t num_chunks) {
    RejectBranchChunkedVertexScan();
    return VerticesChunkedIterable{accessor_->ChunkedVertices(property, view, num_chunks)};
  }

  VerticesChunkedIterable ChunkedVertices(storage::View view, storage::PropertyId property,
                                          const storage::PropertyValue &value, size_t num_chunks) {
    RejectBranchChunkedVertexScan();
    return VerticesChunkedIterable{accessor_->ChunkedVertices(property, value, view, num_chunks)};
  }

  VerticesChunkedIterable ChunkedVertices(storage::View view, storage::PropertyId property,
                                          const std::optional<utils::Bound<storage::PropertyValue>> &lower_bound,
                                          const std::optional<utils::Bound<storage::PropertyValue>> &upper_bound,
                                          size_t num_chunks) {
    RejectBranchChunkedVertexScan();
    return VerticesChunkedIterable{accessor_->ChunkedVertices(property, lower_bound, upper_bound, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::EdgeTypeId edge_type, size_t num_chunks) {
    RejectBranchChunkedEdgeScan();
    return EdgesChunkedIterable{accessor_->ChunkedEdges(edge_type, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                                    size_t num_chunks) {
    RejectBranchChunkedEdgeScan();
    return EdgesChunkedIterable{accessor_->ChunkedEdges(edge_type, property, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                                    const std::optional<utils::Bound<storage::PropertyValue>> &lower_bound,
                                    const std::optional<utils::Bound<storage::PropertyValue>> &upper_bound,
                                    size_t num_chunks) {
    RejectBranchChunkedEdgeScan();
    return EdgesChunkedIterable{
        accessor_->ChunkedEdges(edge_type, property, lower_bound, upper_bound, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::PropertyId property, size_t num_chunks) {
    RejectBranchChunkedEdgeScan();
    return EdgesChunkedIterable{accessor_->ChunkedEdges(property, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::PropertyId property,
                                    const storage::PropertyValue value, size_t num_chunks) {
    RejectBranchChunkedEdgeScan();
    return EdgesChunkedIterable{accessor_->ChunkedEdges(property, value, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::PropertyId property,
                                    const std::optional<utils::Bound<storage::PropertyValue>> &lower_bound,
                                    const std::optional<utils::Bound<storage::PropertyValue>> &upper_bound,
                                    size_t num_chunks) {
    RejectBranchChunkedEdgeScan();
    return EdgesChunkedIterable{accessor_->ChunkedEdges(property, lower_bound, upper_bound, view, num_chunks)};
  }

  auto PointVertices(storage::LabelId label, storage::PropertyId property, storage::CoordinateReferenceSystem crs,
                     TypedValue const &point_value, TypedValue const &boundary_value,
                     plan::PointDistanceCondition condition) -> PointIterable;

  auto PointVertices(storage::LabelId label, storage::PropertyId property, storage::CoordinateReferenceSystem crs,
                     TypedValue const &bottom_left, TypedValue const &top_right, plan::WithinBBoxCondition condition)
      -> PointIterable;

  // On a branch `accessor_` is the diff engine only, so Edges() overloads merge it with main@fork's index via
  // historical(). Edge-type scan: unsorted union (no property key); a fork candidate is skipped if the branch deleted
  // it (fork edges never change type or endpoints).
  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgeTypeIndexReady(edge_type)) {
        // main dropped this index after the fork: full-scan fallback.
        return EdgesIterable(MaterializeFilteredBranchEdgeScan(
            view, [edge_type](EdgeAccessor &e) { return e.EdgeType() == edge_type; }));
      }

      auto result = std::make_shared<std::vector<EdgeAccessor>>();

      for (auto raw : accessor_->Edges(edge_type, view)) {
        result->push_back(EdgeAccessor(raw, branch_ctx_));
      }

      for (auto raw : historical.Edges(edge_type, storage::View::OLD)) {
        if (branch_ctx_->IsForkEdgeHidden(raw, view)) continue;
        result->push_back(EdgeAccessor(raw, branch_ctx_));
      }

      return EdgesIterable(std::move(result));
    }
    return EdgesIterable(accessor_->Edges(edge_type, view));
  }

  // Presence scan; the full-scan fallback (main dropped the index after the fork) sorts ascending by `property`
  // because the planner elides OrderBy on a branch regardless of main's index.
  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgeTypePropertyIndexReady(edge_type, property)) {
        return EdgesIterable(
            MaterializeSortedBranchEdgePropertyScan(view, property, [edge_type, property, view](EdgeAccessor &e) {
              if (e.EdgeType() != edge_type) return false;
              auto prop = e.GetProperty(view, property);
              return prop.has_value() && !prop->IsNull();
            }));
      }
      return EdgesIterable(MergeBranchEdgePropertyScan(view,
                                                       property,
                                                       edge_type,
                                                       accessor_->Edges(edge_type, property, view),
                                                       historical.Edges(edge_type, property, storage::View::OLD),
                                                       [](storage::PropertyValue const &) { return true; }));
    }
    return EdgesIterable(accessor_->Edges(edge_type, property, view));
  }

  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                      const storage::PropertyValue value) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgeTypePropertyIndexReady(edge_type, property)) {
        return EdgesIterable(
            MaterializeFilteredBranchEdgeScan(view, [edge_type, property, value, view](EdgeAccessor &e) {
              if (e.EdgeType() != edge_type) return false;
              auto prop = e.GetProperty(view, property);
              return prop.has_value() && *prop == value;
            }));
      }
      return EdgesIterable(
          MergeBranchEdgePropertyScan(view,
                                      property,
                                      edge_type,
                                      accessor_->Edges(edge_type, property, value, view),
                                      historical.Edges(edge_type, property, value, storage::View::OLD),
                                      [&value](storage::PropertyValue const &v) { return v == value; }));
    }
    return EdgesIterable(accessor_->Edges(edge_type, property, value, view));
  }

  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                      const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                      const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgeTypePropertyIndexReady(edge_type, property)) {
        auto range = storage::PropertyValueRange::Bounded(lower, upper);
        return EdgesIterable(MaterializeSortedBranchEdgePropertyScan(
            view, property, [edge_type, property, range, view](EdgeAccessor &e) {
              if (e.EdgeType() != edge_type) return false;
              auto prop = e.GetProperty(view, property);
              return prop.has_value() && !prop->IsNull() && range.IsValueInRange(*prop);
            }));
      }
      auto const range = storage::PropertyValueRange::Bounded(lower, upper);
      return EdgesIterable(
          MergeBranchEdgePropertyScan(view,
                                      property,
                                      edge_type,
                                      accessor_->Edges(edge_type, property, lower, upper, view),
                                      historical.Edges(edge_type, property, lower, upper, storage::View::OLD),
                                      [&range](storage::PropertyValue const &v) { return range.IsValueInRange(v); }));
    }
    return EdgesIterable(accessor_->Edges(edge_type, property, lower, upper, view));
  }

  EdgesIterable Edges(storage::View view, storage::PropertyId property) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgePropertyIndexReady(property)) {
        return EdgesIterable(MaterializeSortedBranchEdgePropertyScan(view, property, [property, view](EdgeAccessor &e) {
          auto prop = e.GetProperty(view, property);
          return prop.has_value() && !prop->IsNull();
        }));
      }
      return EdgesIterable(MergeBranchEdgePropertyScan(view,
                                                       property,
                                                       std::nullopt,
                                                       accessor_->Edges(property, view),
                                                       historical.Edges(property, storage::View::OLD),
                                                       [](storage::PropertyValue const &) { return true; }));
    }
    return EdgesIterable(accessor_->Edges(property, view));
  }

  EdgesIterable Edges(storage::View view, storage::PropertyId property, const storage::PropertyValue value) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgePropertyIndexReady(property)) {
        return EdgesIterable(MaterializeFilteredBranchEdgeScan(view, [property, value, view](EdgeAccessor &e) {
          auto prop = e.GetProperty(view, property);
          return prop.has_value() && *prop == value;
        }));
      }
      return EdgesIterable(
          MergeBranchEdgePropertyScan(view,
                                      property,
                                      std::nullopt,
                                      accessor_->Edges(property, value, view),
                                      historical.Edges(property, value, storage::View::OLD),
                                      [&value](storage::PropertyValue const &v) { return v == value; }));
    }
    return EdgesIterable(accessor_->Edges(property, value, view));
  }

  EdgesIterable Edges(storage::View view, storage::PropertyId property,
                      const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                      const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgePropertyIndexReady(property)) {
        auto range = storage::PropertyValueRange::Bounded(lower, upper);
        return EdgesIterable(
            MaterializeSortedBranchEdgePropertyScan(view, property, [property, range, view](EdgeAccessor &e) {
              auto prop = e.GetProperty(view, property);
              return prop.has_value() && !prop->IsNull() && range.IsValueInRange(*prop);
            }));
      }
      auto const range = storage::PropertyValueRange::Bounded(lower, upper);
      return EdgesIterable(
          MergeBranchEdgePropertyScan(view,
                                      property,
                                      std::nullopt,
                                      accessor_->Edges(property, lower, upper, view),
                                      historical.Edges(property, lower, upper, storage::View::OLD),
                                      [&range](storage::PropertyValue const &v) { return range.IsValueInRange(v); }));
    }
    return EdgesIterable(accessor_->Edges(property, lower, upper, view));
  }

  // On a branch the gid comes from main's counter (BranchContext::CreateNativeVertex), disjoint from every fork and
  // main gid.
  VertexAccessor InsertVertex() {
    if (branch_ctx_ != nullptr) {
      return VertexAccessor(branch_ctx_->CreateNativeVertex(), branch_ctx_);
    }
    return VertexAccessor(accessor_->CreateVertex());
  }

  // CreateEdge MG_ASSERTs both endpoints are resident in the diff engine, so CowVertex them first (idempotent) and
  // redirect a native `from`/`to` to its diff-engine vertex so outstanding accessors stay consistent with the new edge.
  storage::Result<EdgeAccessor> InsertEdge(VertexAccessor *from, VertexAccessor *to,
                                           const storage::EdgeTypeId &edge_type) {
    if (branch_ctx_ != nullptr) {
      auto from_diff = branch_ctx_->CowVertex(from->Gid());
      if (!from_diff) throw QueryRuntimeException(from_diff.error().message);
      auto to_diff = branch_ctx_->CowVertex(to->Gid());
      if (!to_diff) throw QueryRuntimeException(to_diff.error().message);

      // Fork endpoints stay fork accessors; the diff engine only needs their stubs as the edge's anchors.
      if (!branch_ctx_->IsForkVertexGid(from->Gid())) from->impl_ = *from_diff;
      if (!branch_ctx_->IsForkVertexGid(to->Gid())) to->impl_ = *to_diff;

      auto maybe_edge = branch_ctx_->CreateNativeEdge(&*from_diff, &*to_diff, edge_type);
      if (!maybe_edge) return std::unexpected{maybe_edge.error()};

      // Both endpoints' adjacency change is recorded here, after the write.
      branch_ctx_->RecordEdgeChange(from->Gid());
      branch_ctx_->RecordEdgeChange(to->Gid());
      return EdgeAccessor(*maybe_edge, branch_ctx_);
    }

    auto maybe_edge = accessor_->CreateEdge(&from->impl_, &to->impl_, edge_type);
    if (!maybe_edge) return std::unexpected{maybe_edge.error()};
    return EdgeAccessor(*maybe_edge);
  }

  // mgp C-API single-object deletes on a branch; the Cypher clause goes through DetachDelete below.
  storage::Result<std::optional<EdgeAccessor>> RemoveEdge(EdgeAccessor *edge) {
    if (branch_ctx_ != nullptr) {
      auto res = BranchDetachDelete({}, {*edge}, false);
      if (!res) return std::unexpected{res.error()};
      if (!*res || (*res)->second.empty()) return std::optional<EdgeAccessor>{};
      return std::make_optional<EdgeAccessor>((*res)->second.front());
    }
    auto res = accessor_->DeleteEdge(&edge->impl_);
    if (!res) {
      return std::unexpected{res.error()};
    }

    const auto &value = res.value();
    if (!value) {
      return std::optional<EdgeAccessor>{};
    }

    return std::make_optional<EdgeAccessor>(*value);
  }

  storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachRemoveVertex(
      VertexAccessor *vertex_accessor) {
    using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;

    if (branch_ctx_ != nullptr) {
      auto res = BranchDetachDelete({*vertex_accessor}, {}, true);
      if (!res) return std::unexpected{res.error()};
      if (!*res || (*res)->first.empty()) return std::optional<ReturnType>{};
      return std::make_optional<ReturnType>((*res)->first.front(), std::move((*res)->second));
    }

    auto res = accessor_->DetachDeleteVertex(&vertex_accessor->impl_);
    if (!res) {
      return std::unexpected{res.error()};
    }

    const auto &value = res.value();
    if (!value) {
      return std::optional<ReturnType>{};
    }

    const auto &[vertex, edges] = *value;

    std::vector<EdgeAccessor> deleted_edges;
    deleted_edges.reserve(edges.size());
    std::ranges::transform(
        edges, std::back_inserter(deleted_edges), [](const auto &deleted_edge) { return EdgeAccessor{deleted_edge}; });

    return std::make_optional<ReturnType>(vertex, std::move(deleted_edges));
  }

  storage::Result<std::optional<VertexAccessor>> RemoveVertex(VertexAccessor *vertex_accessor) {
    if (branch_ctx_ != nullptr) {
      auto res = BranchDetachDelete({*vertex_accessor}, {}, false);
      if (!res) return std::unexpected{res.error()};
      if (!*res || (*res)->first.empty()) return std::optional<VertexAccessor>{};
      return std::make_optional<VertexAccessor>((*res)->first.front());
    }
    auto res = accessor_->DeleteVertex(&vertex_accessor->impl_);
    if (!res) {
      return std::unexpected{res.error()};
    }

    const auto &value = res.value();
    if (!value) {
      return std::optional<VertexAccessor>{};
    }

    return std::make_optional<VertexAccessor>(*value);
  }

  storage::Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>> DetachDelete(
      std::vector<VertexAccessor> nodes, std::vector<EdgeAccessor> edges, bool detach) {
    using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;

    if (branch_ctx_ != nullptr) return BranchDetachDelete(std::move(nodes), std::move(edges), detach);

    std::vector<storage::VertexAccessor *> nodes_impl;
    std::vector<storage::EdgeAccessor *> edges_impl;

    nodes_impl.reserve(nodes.size());
    edges_impl.reserve(edges.size());

    for (auto &vertex_accessor : nodes) {
      nodes_impl.push_back(&vertex_accessor.impl_);
    }

    for (auto &edge_accessor : edges) {
      edges_impl.push_back(&edge_accessor.impl_);
    }

    auto res = accessor_->DetachDelete(std::move(nodes_impl), std::move(edges_impl), detach);
    if (!res) {
      return std::unexpected{res.error()};
    }

    const auto &value = res.value();
    if (!value) {
      return std::optional<ReturnType>{};
    }

    const auto &[val_vertices, val_edges] = *value;

    std::vector<VertexAccessor> deleted_vertices;
    std::vector<EdgeAccessor> deleted_edges;

    deleted_vertices.reserve(val_vertices.size());
    deleted_edges.reserve(val_edges.size());

    std::ranges::transform(val_vertices, std::back_inserter(deleted_vertices), [](const auto &deleted_vertex) {
      return VertexAccessor{deleted_vertex};
    });
    std::ranges::transform(val_edges, std::back_inserter(deleted_edges), [](const auto &deleted_edge) {
      return EdgeAccessor{deleted_edge};
    });

    return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
  }

  storage::PropertyId NameToProperty(const std::string_view name) { return accessor_->NameToProperty(name); }

  std::optional<storage::PropertyId> NameToPropertyIfExists(std::string_view name) const {
    return accessor_->NameToPropertyIfExists(name);
  }

  storage::LabelId NameToLabel(const std::string_view name) { return accessor_->NameToLabel(name); }

  storage::EdgeTypeId NameToEdgeType(const std::string_view name) { return accessor_->NameToEdgeType(name); }

  const std::string &PropertyToName(storage::PropertyId prop) const { return accessor_->PropertyToName(prop); }

  const std::string &LabelToName(storage::LabelId label) const { return accessor_->LabelToName(label); }

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const { return accessor_->EdgeTypeToName(type); }

  std::string DatabaseName() const { return accessor_->id(); }

  auto DatabaseNameView() const { return accessor_->id_view(); }

  void AdvanceCommand() { accessor_->AdvanceCommand(); }

  std::expected<void, storage::StorageManipulationError> Commit(storage::CommitArgs commit_args) {
    if (branch_ctx_ != nullptr) return branch_ctx_->CommitTransaction(std::move(commit_args));
    return accessor_->PrepareForCommitPhase(std::move(commit_args));
  }

  // Branch only: checks main's existence and type constraints on the reconstructed view of every touched vertex, before
  // the diff engine's commit and outside its engine lock; on violation the caller aborts.
  std::expected<void, storage::ConstraintViolation> ValidateBranchConstraints();

  std::expected<void, storage::StorageManipulationError> PeriodicCommit(storage::CommitArgs commit_args) {
    if (auto valid = ValidateBranchConstraints(); !valid) {
      accessor_->Abort();
      return std::unexpected{storage::StorageManipulationError{valid.error()}};
    }
    if (branch_ctx_ != nullptr) return branch_ctx_->PeriodicCommitTransaction(std::move(commit_args));
    return accessor_->PeriodicCommit(std::move(commit_args));
  }

  void Abort() { accessor_->Abort(); }

  storage::StorageMode GetStorageMode() const noexcept { return accessor_->GetPinnedStorageMode(); }

  bool LabelIndexReady(storage::LabelId label) const { return accessor_->LabelIndexReady(label); }

  bool LabelPropertyIndexReady(storage::LabelId label, std::span<storage::PropertyPath const> properties) const {
    return accessor_->LabelPropertyIndexReady(label, properties);
  }

  auto RelevantLabelPropertiesIndicesInfo(std::span<storage::LabelId const> labels,
                                          std::span<storage::PropertyPath const> properties) const
      -> std::vector<storage::LabelPropertiesIndicesInfo> {
    return accessor_->RelevantLabelPropertiesIndicesInfo(labels, properties);
  }

  bool EdgeTypeIndexReady(storage::EdgeTypeId edge_type) const { return accessor_->EdgeTypeIndexReady(edge_type); }

  bool EdgeTypePropertyIndexReady(storage::EdgeTypeId edge_type, storage::PropertyId property) const {
    return accessor_->EdgeTypePropertyIndexReady(edge_type, property);
  }

  bool EdgePropertyIndexReady(storage::PropertyId property) const {
    return accessor_->EdgePropertyIndexReady(property);
  }

  bool VertexPropertyIndexReady(storage::PropertyId property) const {
    return accessor_->VertexPropertyIndexReady(property);
  }

  bool TextIndexExists(const std::string &index_name) const;

  std::vector<storage::TextSearchResult> TextIndexSearch(const std::string &index_name, const std::string &search_query,
                                                         text_search_mode search_mode,
                                                         const storage::TextSearchConfig &config) const;

  std::string TextIndexAggregate(const std::string &index_name, const std::string &search_query,
                                 const std::string &aggregation_query) const;

  std::string TextEdgeIndexAggregate(const std::string &index_name, const std::string &search_query,
                                     const std::string &aggregation_query);

  std::vector<storage::TextEdgeSearchResult> SearchEdgeTextIndex(const std::string &index_name,
                                                                 const std::string &search_query,
                                                                 text_search_mode search_mode,
                                                                 const storage::TextSearchConfig &config) const;

  bool PointIndexExists(storage::LabelId label, storage::PropertyId prop) const;

  std::vector<std::tuple<storage::VertexAccessor, double, double>> VectorIndexSearchOnNodes(
      const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector);

  std::vector<std::tuple<storage::EdgeAccessor, double, double>> VectorIndexSearchOnEdges(
      const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector);

  std::vector<storage::VectorIndexInfo> ListAllVectorIndices() const;

  std::vector<storage::VectorEdgeIndexInfo> ListAllVectorEdgeIndices() const;

  std::optional<storage::LabelIndexStats> GetIndexStats(const storage::LabelId &label) const {
    return accessor_->GetIndexStats(label);
  }

  std::optional<storage::LabelPropertyIndexStats> GetIndexStats(
      const storage::LabelId &label, std::span<storage::PropertyPath const> properties) const {
    return accessor_->GetIndexStats(label, properties);
  }

  std::vector<std::pair<storage::LabelId, std::vector<storage::PropertyPath>>> DeleteLabelPropertyIndexStats(
      const storage::LabelId &label) {
    return accessor_->DeleteLabelPropertyIndexStats(label);
  }

  bool DeleteLabelIndexStats(const storage::LabelId &label) { return accessor_->DeleteLabelIndexStats(label); }

  void SetIndexStats(const storage::LabelId &label, const storage::LabelIndexStats &stats) {
    accessor_->SetIndexStats(label, stats);
  }

  void SetIndexStats(const storage::LabelId &label, std::span<storage::PropertyPath const> properties,
                     const storage::LabelPropertyIndexStats &stats) {
    accessor_->SetIndexStats(label, properties, stats);
  }

  int64_t VerticesCount() const { return accessor_->ApproximateVertexCount(); }

  int64_t VerticesCount(storage::LabelId label) const { return accessor_->ApproximateVertexCount(label); }

  int64_t VerticesCount(storage::LabelId label, std::span<storage::PropertyPath const> properties) const {
    return accessor_->ApproximateVertexCount(label, properties);
  }

  int64_t VerticesCount(storage::LabelId label, std::span<storage::PropertyPath const> properties,
                        std::span<storage::PropertyValue const> values) const {
    return accessor_->ApproximateVertexCount(label, properties, values);
  }

  // TODO: rename to ApproximateVertexCount?
  int64_t VerticesCount(storage::LabelId label, std::span<storage::PropertyPath const> properties,
                        std::span<storage::PropertyValueRange const> bounds) const {
    return accessor_->ApproximateVertexCount(label, properties, bounds);
  }

  int64_t VerticesCount(storage::PropertyId property) const { return accessor_->ApproximateVertexCount(property); }

  int64_t VerticesCount(storage::PropertyId property, storage::PropertyValue const &value) const {
    return accessor_->ApproximateVertexCount(property, value);
  }

  int64_t VerticesCount(storage::PropertyId property, std::optional<utils::Bound<storage::PropertyValue>> const &lower,
                        std::optional<utils::Bound<storage::PropertyValue>> const &upper) const {
    return accessor_->ApproximateVertexCount(property, lower, upper);
  }

  std::optional<uint64_t> VerticesPointCount(storage::LabelId label, storage::PropertyId property) const {
    return accessor_->ApproximateVerticesPointCount(label, property);
  }

  std::optional<uint64_t> VerticesVectorCount(std::string_view index_name) const {
    return accessor_->ApproximateVerticesVectorCount(index_name);
  }

  std::optional<uint64_t> VerticesTextCount(std::string_view index_name) const {
    return accessor_->ApproximateVerticesTextCount(index_name);
  }

  std::optional<uint64_t> EdgesTextCount(std::string_view index_name) const {
    return accessor_->ApproximateEdgesTextCount(index_name);
  }

  int64_t EdgesCount() const { return accessor_->ApproximateEdgeCount(); }

  int64_t EdgesCount(storage::EdgeTypeId edge_type) const { return accessor_->ApproximateEdgeCount(edge_type); }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property) const {
    return accessor_->ApproximateEdgeCount(edge_type, property);
  }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property,
                     const storage::PropertyValue &value) const {
    return accessor_->ApproximateEdgeCount(edge_type, property, value);
  }

  int64_t EdgesCount(storage::EdgeTypeId edge_type, storage::PropertyId property,
                     const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                     const std::optional<utils::Bound<storage::PropertyValue>> &upper) const {
    return accessor_->ApproximateEdgeCount(edge_type, property, lower, upper);
  }

  int64_t EdgesCount(storage::PropertyId property) const { return accessor_->ApproximateEdgeCount(property); }

  int64_t EdgesCount(storage::PropertyId property, const storage::PropertyValue &value) const {
    return accessor_->ApproximateEdgeCount(property, value);
  }

  int64_t EdgesCount(storage::PropertyId property, const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                     const std::optional<utils::Bound<storage::PropertyValue>> &upper) const {
    return accessor_->ApproximateEdgeCount(property, lower, upper);
  }

  storage::IndicesInfo ListAllIndices() const { return accessor_->ListAllIndices(); }

  storage::ConstraintsInfo ListAllConstraints() const { return accessor_->ListAllConstraints(); }

  void DropAllIndexes() { accessor_->DropAllIndexes(); }

  void DropAllConstraints() { accessor_->DropAllConstraints(); }

  std::string id() const { return accessor_->id(); }

  auto id_view() const { return accessor_->id_view(); }

  std::expected<void, storage::StorageIndexDefinitionError> CreateIndex(
      storage::LabelId label, storage::CheckCancelFunction cancel_check = storage::neverCancel) {
    return accessor_->CreateIndex(label, cancel_check);
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateIndex(
      storage::LabelId label, std::vector<storage::PropertyPath> &&properties,
      storage::IndexOrder order = storage::IndexOrder::ASC,
      storage::CheckCancelFunction cancel_check = storage::neverCancel) {
    return accessor_->CreateIndex(label, std::move(properties), order, std::move(cancel_check));
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateIndex(
      storage::EdgeTypeId edge_type, storage::CheckCancelFunction cancel_check = storage::neverCancel) {
    return accessor_->CreateIndex(edge_type, std::move(cancel_check));
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateIndex(
      storage::EdgeTypeId edge_type, storage::PropertyId property,
      storage::CheckCancelFunction cancel_check = storage::neverCancel) {
    return accessor_->CreateIndex(edge_type, property, std::move(cancel_check));
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateGlobalEdgeIndex(
      storage::PropertyId property, storage::CheckCancelFunction cancel_check = storage::neverCancel) {
    return accessor_->CreateGlobalEdgeIndex(property, std::move(cancel_check));
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropIndex(storage::LabelId label) {
    return accessor_->DropIndex(label);
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropIndex(
      storage::LabelId label, std::vector<storage::PropertyPath> &&properties,
      std::optional<storage::IndexOrder> order = std::nullopt) {
    return accessor_->DropIndex(label, std::move(properties), order);
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropIndex(storage::EdgeTypeId edge_type) {
    return accessor_->DropIndex(edge_type);
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropIndex(storage::EdgeTypeId edge_type,
                                                                      storage::PropertyId property) {
    return accessor_->DropIndex(edge_type, property);
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropGlobalEdgeIndex(storage::PropertyId property) {
    return accessor_->DropGlobalEdgeIndex(property);
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateGlobalVertexIndex(
      storage::PropertyId property, storage::CheckCancelFunction cancel_check = storage::neverCancel) {
    return accessor_->CreateGlobalVertexIndex(property, std::move(cancel_check));
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropGlobalVertexIndex(storage::PropertyId property) {
    return accessor_->DropGlobalVertexIndex(property);
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreatePointIndex(storage::LabelId label,
                                                                             storage::PropertyId property) {
    return accessor_->CreatePointIndex(label, property);
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropPointIndex(storage::LabelId label,
                                                                           storage::PropertyId property) {
    return accessor_->DropPointIndex(label, property);
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateTextIndex(
      const storage::TextIndexSpec &text_index_info) {
    return accessor_->CreateTextIndex(text_index_info);
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropTextIndex(const std::string &index_name) {
    return accessor_->DropTextIndex(index_name);
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateTextEdgeIndex(
      const storage::TextEdgeIndexSpec &text_edge_index_info) {
    return accessor_->CreateTextEdgeIndex(text_edge_index_info);
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateVectorIndex(storage::VectorIndexSpec spec) {
    return accessor_->CreateVectorIndex(std::move(spec));
  }

  std::expected<utils::small_vector<uint64_t>, storage::StorageIndexDefinitionError> GetVectorIndexIdsForVertex(
      storage::Vertex *vertex, storage::PropertyId property) {
    return accessor_->GetVectorIndexIdsForVertex(vertex, property);
  }

  std::expected<void, storage::StorageIndexDefinitionError> DropVectorIndex(std::string_view index_name) {
    return accessor_->DropVectorIndex(index_name);
  }

  std::expected<void, storage::StorageIndexDefinitionError> CreateVectorEdgeIndex(storage::VectorEdgeIndexSpec spec) {
    return accessor_->CreateVectorEdgeIndex(std::move(spec));
  }

  std::expected<void, storage::StorageExistenceConstraintDefinitionError> CreateExistenceConstraint(
      storage::LabelId label, storage::PropertyId property) {
    return accessor_->CreateExistenceConstraint(label, property);
  }

  std::expected<void, storage::StorageExistenceConstraintDroppingError> DropExistenceConstraint(
      storage::LabelId label, storage::PropertyId property) {
    return accessor_->DropExistenceConstraint(label, property);
  }

  std::expected<storage::UniqueConstraints::CreationStatus, storage::StorageUniqueConstraintDefinitionError>
  CreateUniqueConstraint(storage::LabelId label, const std::set<storage::PropertyId> &properties) {
    return accessor_->CreateUniqueConstraint(label, properties);
  }

  storage::UniqueConstraints::DeletionStatus DropUniqueConstraint(storage::LabelId label,
                                                                  const std::set<storage::PropertyId> &properties) {
    return accessor_->DropUniqueConstraint(label, properties);
  }

  std::expected<void, storage::StorageExistenceConstraintDefinitionError> CreateTypeConstraint(
      storage::LabelId label, storage::PropertyId property, storage::TypeConstraintKind type) {
    return accessor_->CreateTypeConstraint(label, property, type);
  }

  std::expected<void, storage::StorageExistenceConstraintDroppingError> DropTypeConstraint(
      storage::LabelId label, storage::PropertyId property, storage::TypeConstraintKind type) {
    return accessor_->DropTypeConstraint(label, property, type);
  }

  void DropGraph() { return accessor_->DropGraph(); }

  auto CreateEnum(std::string_view name, std::span<std::string const> values)
      -> std::expected<storage::EnumTypeId, storage::EnumStorageError> {
    return accessor_->CreateEnum(name, values);
  }

  auto ShowEnums() { return accessor_->ShowEnums(); }

  auto GetEnumValue(std::string_view name, std::string_view value) const
      -> std::expected<storage::Enum, storage::EnumStorageError> {
    return accessor_->GetEnumValue(name, value);
  }

  auto GetEnumValue(std::string_view enum_str) -> std::expected<storage::Enum, storage::EnumStorageError> {
    return accessor_->GetEnumValue(enum_str);
  }

  auto EnumToName(storage::Enum value) const -> std::expected<std::string, storage::EnumStorageError> {
    return accessor_->GetEnumStoreShared().ToString(value);
  }

  auto EnumAlterAdd(std::string_view name, std::string_view value)
      -> std::expected<storage::Enum, storage::EnumStorageError> {
    return accessor_->EnumAlterAdd(name, value);
  }

  auto EnumAlterUpdate(std::string_view name, std::string_view old_value, std::string_view new_value)
      -> std::expected<storage::Enum, storage::EnumStorageError> {
    return accessor_->EnumAlterUpdate(name, old_value, new_value);
  }

  void SetLabelDescription(std::span<std::string const> labels, std::string_view desc) {
    accessor_->SetLabelDescription(labels, desc);
  }

  bool DeleteLabelDescription(std::span<std::string const> labels) { return accessor_->DeleteLabelDescription(labels); }

  std::optional<std::string> GetLabelDescription(std::span<std::string const> labels) const {
    return accessor_->GetLabelDescription(labels);
  }

  void SetEdgeTypeDescription(std::string_view name, std::string_view desc) {
    accessor_->SetEdgeTypeDescription(name, desc);
  }

  bool DeleteEdgeTypeDescription(std::string_view name) { return accessor_->DeleteEdgeTypeDescription(name); }

  std::optional<std::string> GetEdgeTypeDescription(std::string_view name) const {
    return accessor_->GetEdgeTypeDescription(name);
  }

  void SetLabelPropertyDescription(std::span<std::string const> label_qualifier, std::string_view prop_name,
                                   std::string_view desc) {
    accessor_->SetLabelPropertyDescription(label_qualifier, prop_name, desc);
  }

  bool DeleteLabelPropertyDescription(std::span<std::string const> label_qualifier, std::string_view prop_name) {
    return accessor_->DeleteLabelPropertyDescription(label_qualifier, prop_name);
  }

  std::optional<std::string> GetLabelPropertyDescription(std::span<std::string const> label_qualifier,
                                                         std::string_view prop_name) const {
    return accessor_->GetLabelPropertyDescription(label_qualifier, prop_name);
  }

  void SetEdgeTypePropertyDescription(std::string_view edge_type_name, std::string_view prop_name,
                                      std::string_view desc) {
    accessor_->SetEdgeTypePropertyDescription(edge_type_name, prop_name, desc);
  }

  bool DeleteEdgeTypePropertyDescription(std::string_view edge_type_name, std::string_view prop_name) {
    return accessor_->DeleteEdgeTypePropertyDescription(edge_type_name, prop_name);
  }

  std::optional<std::string> GetEdgeTypePropertyDescription(std::string_view edge_type_name,
                                                            std::string_view prop_name) const {
    return accessor_->GetEdgeTypePropertyDescription(edge_type_name, prop_name);
  }

  void SetPropertyDescription(std::string_view prop_name, std::string_view desc) {
    accessor_->SetPropertyDescription(prop_name, desc);
  }

  bool DeletePropertyDescription(std::string_view prop_name) { return accessor_->DeletePropertyDescription(prop_name); }

  std::optional<std::string> GetPropertyDescription(std::string_view prop_name) const {
    return accessor_->GetPropertyDescription(prop_name);
  }

  void SetPropertyValueDescription(std::string_view prop_name, storage::ExternalPropertyValue const &value,
                                   std::string_view desc) {
    accessor_->SetPropertyValueDescription(prop_name, value, desc);
  }

  bool DeletePropertyValueDescription(std::string_view prop_name, storage::ExternalPropertyValue const &value) {
    return accessor_->DeletePropertyValueDescription(prop_name, value);
  }

  std::optional<std::string> GetPropertyValueDescription(std::string_view prop_name,
                                                         storage::ExternalPropertyValue const &value) const {
    return accessor_->GetPropertyValueDescription(prop_name, value);
  }

  void SetEdgeTypePatternDescription(std::span<std::string const> from_labels, std::string_view edge_type_name,
                                     std::span<std::string const> to_labels, std::string_view desc) {
    accessor_->SetEdgeTypePatternDescription(from_labels, edge_type_name, to_labels, desc);
  }

  bool DeleteEdgeTypePatternDescription(std::span<std::string const> from_labels, std::string_view edge_type_name,
                                        std::span<std::string const> to_labels) {
    return accessor_->DeleteEdgeTypePatternDescription(from_labels, edge_type_name, to_labels);
  }

  std::optional<std::string> GetEdgeTypePatternDescription(std::span<std::string const> from_labels,
                                                           std::string_view edge_type_name,
                                                           std::span<std::string const> to_labels) const {
    return accessor_->GetEdgeTypePatternDescription(from_labels, edge_type_name, to_labels);
  }

  void SetEdgeTypePatternPropertyDescription(std::span<std::string const> from_labels, std::string_view edge_type_name,
                                             std::span<std::string const> to_labels, std::string_view prop_name,
                                             std::string_view desc) {
    accessor_->SetEdgeTypePatternPropertyDescription(from_labels, edge_type_name, to_labels, prop_name, desc);
  }

  bool DeleteEdgeTypePatternPropertyDescription(std::span<std::string const> from_labels,
                                                std::string_view edge_type_name, std::span<std::string const> to_labels,
                                                std::string_view prop_name) {
    return accessor_->DeleteEdgeTypePatternPropertyDescription(from_labels, edge_type_name, to_labels, prop_name);
  }

  std::optional<std::string> GetEdgeTypePatternPropertyDescription(std::span<std::string const> from_labels,
                                                                   std::string_view edge_type_name,
                                                                   std::span<std::string const> to_labels,
                                                                   std::string_view prop_name) const {
    return accessor_->GetEdgeTypePatternPropertyDescription(from_labels, edge_type_name, to_labels, prop_name);
  }

  void SetDatabaseDescription(std::string_view desc) { accessor_->SetDatabaseDescription(desc); }

  bool DeleteDatabaseDescription() { return accessor_->DeleteDatabaseDescription(); }

  std::optional<std::string> GetDatabaseDescription() const { return accessor_->GetDatabaseDescription(); }

  std::vector<storage::DescriptionEntry> GetAllDescriptions() const { return accessor_->GetAllDescriptions(); }

  auto GetStorageAccessor() const -> storage::Storage::Accessor * { return accessor_; }

#ifdef MG_ENTERPRISE
  // TTL operations - pushed into accessor
  void StartTtl() { accessor_->StartTtl(); }

  void ConfigureTtl(const storage::ttl::TtlInfo &ttl_info) { accessor_->ConfigureTtl(ttl_info); }

  void DisableTtl() { accessor_->DisableTtl(); }

  void StopTtl() { accessor_->StopTtl(); }

  storage::ttl::TtlInfo GetTtlConfig() const { return accessor_->GetTtlConfig(); }
#endif
};

class SubgraphDbAccessor final {
  DbAccessor db_accessor_;
  Graph *graph_;

 public:
  explicit SubgraphDbAccessor(DbAccessor db_accessor, Graph *graph);

  static SubgraphDbAccessor *MakeSubgraphDbAccessor(DbAccessor *db_accessor, Graph *graph);

  void TrackThreadAllocations(const char *thread_id);

  void TrackCurrentThreadAllocations();

  void UntrackThreadAllocations(const char *thread_id);

  void UntrackCurrentThreadAllocations();

  storage::PropertyId NameToProperty(std::string_view name);

  storage::LabelId NameToLabel(std::string_view name);

  storage::EdgeTypeId NameToEdgeType(std::string_view name);

  const std::string &PropertyToName(storage::PropertyId prop) const;

  const std::string &LabelToName(storage::LabelId label) const;

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const;

  storage::Result<std::optional<EdgeAccessor>> RemoveEdge(EdgeAccessor *edge);

  storage::Result<EdgeAccessor> InsertEdge(SubgraphVertexAccessor *from, SubgraphVertexAccessor *to,
                                           const storage::EdgeTypeId &edge_type);

  storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachRemoveVertex(
      SubgraphVertexAccessor *vertex_accessor);

  storage::Result<std::optional<VertexAccessor>> RemoveVertex(SubgraphVertexAccessor *vertex_accessor);

  SubgraphVertexAccessor InsertVertex();

  VerticesIterable Vertices(storage::View view);

  std::optional<VertexAccessor> FindVertex(storage::Gid gid, storage::View view);

  std::optional<EdgeAccessor> FindEdge(storage::Gid gid, storage::View view);

  std::optional<EdgeAccessor> FindEdge(storage::Gid edge_gid, storage::Gid from_vertex_gid, storage::View view);

  Graph *getGraph();

  storage::StorageMode GetStorageMode() const noexcept;

  DbAccessor *GetAccessor();
};

class VirtualGraphDbAccessor final {
  DbAccessor db_accessor_;
  VirtualGraph *graph_;

 public:
  explicit VirtualGraphDbAccessor(DbAccessor db_accessor, VirtualGraph *graph);

  void TrackCurrentThreadAllocations();

  void UntrackCurrentThreadAllocations();

  storage::PropertyId NameToProperty(std::string_view name);

  storage::LabelId NameToLabel(std::string_view name);

  storage::EdgeTypeId NameToEdgeType(std::string_view name);

  const std::string &PropertyToName(storage::PropertyId prop) const;

  const std::string &LabelToName(storage::LabelId label) const;

  const std::string &EdgeTypeToName(storage::EdgeTypeId type) const;

  [[nodiscard]] std::shared_ptr<const VirtualNode> FindNode(storage::Gid synthetic_gid) const;

  [[nodiscard]] VirtualGraph *getGraph() const;

  storage::StorageMode GetStorageMode() const noexcept;

  DbAccessor *GetAccessor();
};

}  // namespace memgraph::query
