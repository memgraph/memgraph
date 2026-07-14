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
#include "query/hops_limit.hpp"
#include "query/typed_value.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/common_function_signatures.hpp"
#include "storage/v2/constraints/type_constraints.hpp"
#include "storage/v2/edge_accessor.hpp"
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
#include <memory>
#include <optional>
#include <ranges>
#include <span>
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

  // Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-1 knock-on fix: query::VertexAccessor::
  // UpdateProperties dropped `const` (see its own doc-comment) so it could CowIfNeeded() on a
  // branch -- `impl_` here is a by-value member (not a pointer), so a `const` SubgraphVertexAccessor
  // method can no longer call it either. Mirrors SetProperty below, already non-const.
  storage::Result<std::vector<std::tuple<storage::PropertyId, storage::PropertyValue, storage::PropertyValue>>>
  UpdateProperties(std::map<storage::PropertyId, storage::PropertyValue> &properties) {
    return impl_.UpdateProperties(properties);
  }

  VertexAccessor GetVertexAccessor() const;
};
}  // namespace memgraph::query

namespace memgraph::query {

class VerticesIterable final {
  // Graph Versioning v1 (lazy diff-context, slice E-1): the 3rd alternative -- a branch-checkout's
  // gid-ordered union scan (versioning::BranchContext::UnionVerticesIterable, branch_engine.hpp) --
  // is added ALONGSIDE the two pre-existing alternatives below (a plain storage-level scan, or a
  // caller-materialized set), not in place of either. Only DbAccessor::Vertices (below) ever
  // constructs this alternative, and only when a branch context is active.
  // Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-3(b) FIX (adversarial-review): the 4th
  // alternative is a fully-materialized, already-filtered result set for the branch label/property
  // scan workaround (see DbAccessor::Vertices(view, label[, properties, property_ranges]) below) --
  // a checked-out branch's diff engine has no label/property index reconciled with historical_, so
  // those scans fall back to filtering the (already-existing) unfiltered union scan by label/
  // property instead of seeking an index. Filtering eagerly into an owned std::shared_ptr<vector<>>
  // (rather than lazily skipping non-matches during iteration, which would need a predicate-aware
  // variant of BranchContext::UnionVerticesIterable::Iterator) trades some memory/time for
  // correctness and keeps the existing union-scan machinery untouched -- "at minimum correct even
  // if slower" per the adversarial-review fix. shared_ptr (not a raw owned member here) so the
  // materialized vector's lifetime is independent of how many copies of this VerticesIterable (or
  // of Iterator, which also holds a copy -- see below) get made; VertexAccessor is NOT subject to
  // the mgp_vertex 64-byte C-API budget concern that shaped its own single-pointer design, since
  // VerticesIterable/Iterator never appear inside mgp_vertex.
  std::variant<storage::VerticesIterable,
               std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                  utils::Allocator<VertexAccessor>> *,
               versioning::BranchContext::UnionVerticesIterable, std::shared_ptr<std::vector<VertexAccessor>>>
      iterable_;
  // Only meaningful for the 3rd (branch) alternative -- stamped onto every yielded VertexAccessor
  // so its mutators can later CowVertex through the SAME context (see query::VertexAccessor's own
  // doc-comment). NON-const: ResolveVertex/Vertices/CowVertex open accessors against the diff
  // engine and are therefore non-const member functions of BranchContext -- a const pointer here
  // could not call them. nullptr for the other two alternatives.
  //
  // SINGLE-POINTER COLLAPSE (mg_procedure_impl.hpp's kMaxMgpVertexSize budget, R6): there used to
  // be a SECOND pointer here (the diff-engine accessor), duplicated onto every yielded
  // VertexAccessor too -- that overflowed mgp_vertex's 64-byte C-API size budget and grew the hot
  // accessor for every caller, branch or not. `BranchContext` now holds that accessor itself
  // (`current_diff_txn()`, branch_engine.hpp) in a single per-query slot -- safe because a
  // checked-out branch is exclusive single-writer, so at most one query ever runs against a given
  // BranchContext at a time. `branch_ctx_` is therefore the ONLY extra pointer needed here.
  versioning::BranchContext *branch_ctx_{nullptr};

 public:
  class Iterator final {
    std::variant<storage::VerticesIterable::Iterator,
                 std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                    utils::Allocator<VertexAccessor>>::iterator,
                 versioning::BranchContext::UnionVerticesIterable::Iterator, std::vector<VertexAccessor>::iterator>
        it_;
    versioning::BranchContext *branch_ctx_{nullptr};
    // HIGH-3(b): keeps the materialized vector (4th alternative above) alive independently of the
    // parent VerticesIterable -- cheap (one atomic refcount bump), and removes any doubt about
    // Iterator outliving the VerticesIterable it was created from.
    std::shared_ptr<std::vector<VertexAccessor>> materialized_;

   public:
    explicit Iterator(storage::VerticesIterable::Iterator it) : it_(std::move(it)) {}

    explicit Iterator(std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                         utils::Allocator<VertexAccessor>>::iterator it)
        : it_(it) {}

    Iterator(versioning::BranchContext::UnionVerticesIterable::Iterator it, versioning::BranchContext *ctx)
        : it_(std::move(it)), branch_ctx_(ctx) {}

    Iterator(std::vector<VertexAccessor>::iterator it, std::shared_ptr<std::vector<VertexAccessor>> materialized)
        : it_(it), materialized_(std::move(materialized)) {}

    // All four alternatives return the SAME type (query::VertexAccessor, std::visit requires a
    // common return type across every Overloaded branch) -- the branch alternative additionally
    // stamps ctx (2-arg VertexAccessor ctor) so a later mutation on the yielded accessor can
    // CowVertex through `branch_ctx_->current_diff_txn()` (see query::VertexAccessor's own
    // doc-comment). The materialized (4th) alternative already holds fully-stamped VertexAccessor
    // values (branch_ctx_ set at materialization time, see DbAccessor::MaterializeFilteredBranchScan
    // below) so it's just a dereference.
    VertexAccessor operator*() const {
      return std::visit(
          memgraph::utils::Overloaded{[](const storage::VerticesIterable::Iterator &it) { return VertexAccessor(*it); },
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

  explicit VerticesIterable(std::unordered_set<VertexAccessor, std::hash<VertexAccessor>, std::equal_to<void>,
                                               utils::Allocator<VertexAccessor>> *vertices)
      : iterable_(vertices) {}

  VerticesIterable(versioning::BranchContext::UnionVerticesIterable iterable, versioning::BranchContext *ctx)
      : iterable_(std::move(iterable)), branch_ctx_(ctx) {}

  // HIGH-3(b): the materialized/filtered branch label-or-property scan alternative -- see
  // DbAccessor::MaterializeFilteredBranchScan.
  explicit VerticesIterable(std::shared_ptr<std::vector<VertexAccessor>> materialized)
      : iterable_(std::move(materialized)) {}

  Iterator begin() {
    return std::visit(
        memgraph::utils::Overloaded{[](storage::VerticesIterable &iterable_) { return Iterator(iterable_.begin()); },
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
            [](storage::VerticesIterable &iterable_) { return Iterator(iterable_.end()); },
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
  // Graph Versioning v1 (lazy diff-context, slice E-2d): the 3rd alternative -- a fully-materialized,
  // already-filtered result set for the branch edge-type/property index-scan workaround (see
  // DbAccessor::Edges(...) overloads below and MaterializeFilteredBranchEdgeScan) -- mirrors
  // VerticesIterable's own materialized (4th) alternative byte-for-byte. shared_ptr (not an owned
  // member) so the materialized vector's lifetime is independent of how many copies of this
  // EdgesIterable (or of Iterator, which also holds a copy) get made. Unlike VerticesIterable, no
  // `branch_ctx_` member is needed here: every EdgeAccessor is already fully branch-stamped at
  // materialization time (MaterializeFilteredBranchEdgeScan stamps `branch_ctx_` into each candidate
  // before filtering), so there is no lazy union-scan alternative on the edge side that would need a
  // context pointer to re-stamp yielded values.
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
    // Graph Versioning v1 (lazy diff-context, slice E-2d): keeps the materialized vector (3rd
    // alternative above) alive independently of the parent EdgesIterable -- mirrors
    // VerticesIterable::Iterator::materialized_ exactly (cheap atomic refcount bump; removes any
    // doubt about an Iterator outliving the EdgesIterable it was created from).
    std::shared_ptr<std::vector<EdgeAccessor>> materialized_;

   public:
    explicit Iterator(storage::EdgesIterable::Iterator it) : it_(std::move(it)) {}

    explicit Iterator(std::unordered_set<EdgeAccessor, std::hash<EdgeAccessor>, std::equal_to<void>,
                                         utils::Allocator<EdgeAccessor>>::iterator it)
        : it_(it) {}

    Iterator(std::vector<EdgeAccessor>::iterator it, std::shared_ptr<std::vector<EdgeAccessor>> materialized)
        : it_(it), materialized_(std::move(materialized)) {}

    // The materialized (3rd) alternative already holds fully-stamped EdgeAccessor values
    // (branch_ctx_ set at materialization time, see DbAccessor::MaterializeFilteredBranchEdgeScan)
    // so it's just a dereference -- no re-stamping needed, unlike VerticesIterable's lazy branch
    // alternative.
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

  // Graph Versioning v1 (lazy diff-context, slice E-2d): the materialized/filtered branch edge-type/
  // property scan alternative -- see DbAccessor::MaterializeFilteredBranchEdgeScan.
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
  // Graph Versioning v1 (lazy diff-context, slice E-1): non-null iff this DbAccessor belongs to a
  // checked-out branch's query transaction -- in which case `accessor_` IS the branch's diff-engine
  // accessor for this transaction (see CurrentDB::SetupDatabaseTransaction, interpreter.cpp: it
  // opens `db_transactional_accessor_` on `branch_context_->diff_engine()`, not on main, whenever a
  // branch is active). nullptr on `main` -- every method below that checks it therefore costs
  // exactly one extra pointer compare on the non-branch path (R6/zero-cost-when-off). NON-const:
  // ResolveVertex/Vertices/CowVertex open accessors against the diff engine and are therefore
  // non-const member functions of BranchContext -- a const pointer here could not call them (the
  // context genuinely is mutated by a COW).
  versioning::BranchContext *branch_ctx_{nullptr};

  // Graph Versioning v1 (lazy diff-context, slice E-1) HIGH-3(b) helpers (adversarial-review fix):
  // only ever called when branch_ctx_ != nullptr, from the label/property Vertices() overloads
  // below.

  // HasLabel through the query-layer VertexAccessor (not the raw storage one) so it benefits from
  // the HIGH-2 self-correcting-read fix -- a not-yet-resolved historical `v` still reads correctly
  // even if some other reference to the same gid triggered a COW earlier in this same scan.
  static bool VertexHasLabel(VertexAccessor &v, storage::View view, storage::LabelId label) {
    auto has_label = v.HasLabel(view, label);
    return has_label.has_value() && *has_label;
  }

  // Eagerly filters branch_ctx_->Vertices(view) (the unfiltered gid-ordered union scan) by
  // `predicate`, materializing every match into an owned vector -- see VerticesIterable's own
  // doc-comment on why eager materialization (not a lazy predicate-skipping iterator) was chosen
  // here. O(fork-state graph size) -- a full scan -- regardless of how selective `predicate` is;
  // flagged as a slower-but-correct fallback, not a permanent substitute for a real branch-aware
  // index (out of scope for this slice).
  auto MaterializeFilteredBranchScan(storage::View view, std::function<bool(VertexAccessor &)> predicate)
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

  // Graph Versioning v1 (lazy diff-context, slice E-2d): edge-side mirror of
  // MaterializeFilteredBranchScan above -- eagerly filters every edge reachable via the branch's
  // union vertex scan (branch_ctx_->Vertices) by walking each vertex's OUT adjacency
  // (branch_ctx_->ResolveEdges) exactly once. OUT-only is deliberate, not an oversight: every edge is
  // stored on exactly its FROM-vertex's adjacency list, so enumerating every vertex's OUT edges once
  // visits every (historical ∪ diff, tombstone-aware) edge exactly once -- also enumerating IN edges
  // would double-count. O(fork-state graph size) full scan regardless of predicate selectivity, the
  // same "slower but correct" fallback MaterializeFilteredBranchScan already accepted for vertices --
  // there is no branch-aware edge-type/property index to seek instead (out of scope for this slice).
  auto MaterializeFilteredBranchEdgeScan(storage::View view, std::function<bool(EdgeAccessor &)> predicate)
      -> std::shared_ptr<std::vector<EdgeAccessor>> {
    auto result = std::make_shared<std::vector<EdgeAccessor>>();
    for (auto raw_v : branch_ctx_->Vertices(view)) {
      VertexAccessor v(raw_v, branch_ctx_);
      for (auto &raw_e : branch_ctx_->ResolveEdges(v.Gid(), storage::EdgeDirection::OUT, view, {})) {
        EdgeAccessor candidate(raw_e, branch_ctx_);
        if (predicate(candidate)) {
          result->push_back(candidate);
        }
      }
    }
    return result;
  }

  // Graph Versioning v1, Slice 3b/Part B: edge-type+PROPERTY analogue of the vertex label-property
  // merge (`Vertices(view, label, properties, ranges, order)` above) combined with the plain
  // edge-type merge's tombstone/COW skip (`Edges(view, edge_type)` below). The storage-layer
  // edge-property index scans (unlike the vertex label-property ones) take no `IndexOrder` parameter
  // at all -- they are ascending-only -- and the planner elides the explicit OrderBy for
  // ScanAllByEdgeTypeProperty/…PropertyValue/…PropertyRange exactly as it does for the label-property
  // vertex scan, trusting index order. So this merge must also produce rows ASCENDING by `property`,
  // not gid-ordered ones. Two streams, both already filtered to (edge_type [, value/range]) by the
  // caller's chosen storage-layer scan overload:
  //   (a) `diff_stream` -- the diff engine's own mirrored edge-type+property index scan (Part A,
  //       branch_engine.cpp, always populates it once a branch exists), and
  //   (b) `hist_stream` -- main@fork's edge-type+property index as of the fork timestamp, via
  //       `historical()`; a main-stream candidate is skipped if the branch tombstoned it, OR if the
  //       diff engine already holds that gid (`FindDiffEdge(g, View::NEW)` -- a COW): the diff stream
  //       in (a) already represents that gid at its authoritative/new value (diff wins on overlap).
  // `storage::EdgesIterable` is move-only (see edges_iterable.hpp) so both streams are taken BY VALUE
  // -- callers pass the storage scan's return value directly (a prvalue), which guaranteed copy
  // elision (C++17) direct-initializes these parameters with no copy/move call at all.
  auto MergeBranchEdgePropertyScan(storage::View view, storage::PropertyId property, storage::EdgesIterable diff_stream,
                                   storage::EdgesIterable hist_stream) -> std::shared_ptr<std::vector<EdgeAccessor>> {
    using KeyedEdge = std::pair<storage::PropertyValue, EdgeAccessor>;
    std::vector<KeyedEdge> keyed;

    // (a) Diff-engine matches.
    for (auto raw : diff_stream) {
      EdgeAccessor e(raw, branch_ctx_);
      auto prop_res = e.GetProperty(view, property);
      if (!prop_res.has_value()) continue;  // defensive: an index candidate should always have it
      keyed.emplace_back(std::move(*prop_res), e);
    }

    // (b) Main@fork matches, gated by the tombstone/COW skip -- diff wins on any overlap.
    for (auto raw : hist_stream) {
      EdgeAccessor mc(raw, branch_ctx_);
      auto const g = mc.Gid();
      if (branch_ctx_->IsEdgeTombstoned(g)) continue;
      if (branch_ctx_->FindDiffEdge(g, storage::View::NEW).has_value()) continue;
      auto prop_res = mc.GetProperty(storage::View::OLD, property);
      if (!prop_res.has_value()) continue;  // defensive: an index candidate should always have it
      keyed.emplace_back(std::move(*prop_res), mc);
    }

    // Explicit sort ASCENDING by the indexed property, reusing storage::PropertyValue's own
    // `operator<=>` (defined in property_value.cppm) -- EXACTLY the comparator the storage index
    // itself uses, so this matches native (ascending-only) index order bit-for-bit.
    std::stable_sort(
        keyed.begin(), keyed.end(), [](const KeyedEdge &a, const KeyedEdge &b) { return a.first < b.first; });

    auto result = std::make_shared<std::vector<EdgeAccessor>>();
    result->reserve(keyed.size());
    for (auto &entry : keyed) result->push_back(entry.second);
    return result;
  }

  // Graph Versioning v1, slice E-4: branch-aware VERTEX_HAS_EDGES guard for a plain (non-cascading)
  // vertex delete (RemoveVertex/DetachDelete-with-detach=false below). `accessor_->DeleteVertex`'s
  // own native check (storage.cpp's TryDeleteVertices) only ever inspects the DIFF ENGINE's own
  // physical adjacency (`vertex_ptr->in_edges`/`out_edges`) -- for a COW'd fork vertex that is EMPTY
  // (CowVertex copies props+labels only, NOT adjacency, see its own doc-comment, branch_engine.cpp),
  // so the native check would silently "succeed" deleting a fork vertex that still has fork-resident
  // incident edges, leaving them dangling (pointing at a now-deleted vertex, resurfaced the next time
  // ResolveEdges runs). Consult the branch's FULL adjacency (historical_ UNION diff_engine_, via
  // ResolveEdges) instead. `co_deleted_edge_gids` excludes edges THIS SAME delete statement is ALSO
  // deleting (mirrors main's own PrepareDeletableEdges/DetachRemainingEdges accounting, storage.cpp)
  // -- they are gone by the time the vertex delete "happens", so they must not count against it.
  bool BranchVertexHasUncountedEdges(storage::Gid vertex_gid,
                                     const std::unordered_set<storage::Gid> &co_deleted_edge_gids) {
    auto has_uncounted = [&](storage::EdgeDirection direction) {
      auto edges = branch_ctx_->ResolveEdges(vertex_gid, direction, storage::View::NEW, {});
      return std::ranges::any_of(edges, [&](auto &e) { return !co_deleted_edge_gids.contains(e.Gid()); });
    };
    return has_uncounted(storage::EdgeDirection::OUT) || has_uncounted(storage::EdgeDirection::IN);
  }

 public:
  explicit DbAccessor(storage::Storage::Accessor *accessor, versioning::BranchContext *branch_ctx = nullptr)
      : accessor_(accessor), branch_ctx_(branch_ctx) {}

  void SetParallelExecution() { accessor_->GetTransaction()->SetParallelExecution(); }

  bool CheckIndicesAreReady(storage::IndicesCollection const &required_indices) const {
    return accessor_->CheckIndicesAreReady(required_indices);
  }

  auto type() const { return accessor_->type(); }

  std::optional<VertexAccessor> FindVertex(storage::Gid gid, storage::View view) {
    if (branch_ctx_ != nullptr) {
      // Diff-engine-first, falling back to the branch's historical (fork-state) base -- see
      // BranchContext::ResolveVertex's own doc-comment. ResolveVertex reads the diff-engine
      // accessor from branch_ctx_->current_diff_txn() (== accessor_, set once per query by
      // CurrentDB::SetupDatabaseTransaction) rather than taking it as a parameter here.
      auto maybe_vertex = branch_ctx_->ResolveVertex(gid, view);
      if (maybe_vertex) return VertexAccessor(*maybe_vertex, branch_ctx_);
      return std::nullopt;
    }
    auto maybe_vertex = accessor_->FindVertex(gid, view);
    if (maybe_vertex) return VertexAccessor(*maybe_vertex);
    return std::nullopt;
  }

  // Graph Versioning v1 (lazy diff-context, slice E-2a) HIGH FIX (adversarial-review, ROUND 2): was
  // UNGUARDED -- `accessor_->FindEdge` on a branch is the diff-engine's accessor ONLY (see
  // `branch_ctx_`'s own doc-comment above), so any pre-fork (historical) edge silently "didn't
  // exist" on a branch. Reachable via `MATCH ()-[e]-() WHERE id(e)=<gid>` (rewritten to a
  // ScanAllByEdgeId unconditionally) and the `mgp_graph_get_edge_by_id` C-API.
  //
  // ROUND 1 of this fix routed through `BranchContext::ResolveEdge` (diff-first, falling back to
  // `historical_->FindEdge`) -- adversarially verified WRONG: finding an edge by bare gid against
  // `historical_` (a HistoricalAccess accessor) is not the simple diff-engine-mirroring point lookup
  // ResolveVertex gets away with -- it depends on `properties_on_edges`/gid-storage-layout and
  // HistoricalAccess's own scan semantics in ways that don't just work by calling the same
  // `FindEdge(gid, view)` two of the storage layer already exposes elsewhere. That round REJECTED
  // cleanly instead (NotYetImplemented) rather than get it subtly wrong.
  //
  // SLICE E-2d IMPLEMENTATION: diff-first via `BranchContext::FindDiffEdge` (reliable, no
  // `historical_` dependency -- anything already COW'd/created into the diff engine is a direct
  // O(1) hit), falling back to a full union-vertex scan (`branch_ctx_->Vertices`) + per-vertex OUT-
  // adjacency walk (`ResolveEdges`) when the diff engine doesn't have it. Every edge lives on
  // exactly its FROM-vertex's OUT list, so walking every vertex's OUT edges once via the same
  // already-trusted `ResolveEdges` primitive the delete cascade and VERTEX_HAS_EDGES guard rely on
  // is guaranteed to surface it if it's visible at all, historical or diff-resident. O(V+E) worst
  // case for this fallback -- a full scan, same "slower but correct" trade-off
  // MaterializeFilteredBranchScan already accepted for the vertex-side label/property scans; there
  // is no branch-aware edge-by-id index to seek instead (out of scope for this slice). The
  // 3-argument overload below is strictly cheaper when the caller already has the endpoint, so
  // prefer it there.
  std::optional<EdgeAccessor> FindEdge(storage::Gid gid, storage::View view) {
    if (branch_ctx_ != nullptr) {
      if (auto diff_edge = branch_ctx_->FindDiffEdge(gid, view)) {
        return EdgeAccessor(*diff_edge, branch_ctx_);
      }
      for (auto raw_v : branch_ctx_->Vertices(view)) {
        VertexAccessor v(raw_v, branch_ctx_);
        for (auto &e : branch_ctx_->ResolveEdges(v.Gid(), storage::EdgeDirection::OUT, view, {})) {
          if (e.Gid() == gid) return EdgeAccessor(e, branch_ctx_);
        }
      }
      return std::nullopt;
    }
    auto maybe_edge = accessor_->FindEdge(gid, view);
    if (maybe_edge) return EdgeAccessor(*maybe_edge);
    return std::nullopt;
  }

  // HIGH FIX (adversarial-review, ROUND 2): same gap as the overload above -- FIXED in slice E-2d.
  // PREFERRED overload whenever the caller already has the FROM-vertex gid (e.g. an already-resolved
  // pattern edge): no full scan needed, just that one vertex's OUT adjacency via `ResolveEdges` --
  // an edge lives on exactly its FROM-vertex's OUT list, so this is a reliable, non-scanning lookup.
  std::optional<EdgeAccessor> FindEdge(storage::Gid edge_gid, storage::Gid from_vertex_gid, storage::View view) {
    if (branch_ctx_ != nullptr) {
      for (auto &e : branch_ctx_->ResolveEdges(from_vertex_gid, storage::EdgeDirection::OUT, view, {})) {
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

  // Graph Versioning v1 (lazy diff-context, slice E-1): the plain ScanAll AND (as of the HIGH-3(b)
  // adversarial-review fix, below) the label/property-indexed overloads are all branch-aware now.
  // A checked-out branch's diff engine has no label/property index population reconciled with
  // historical_ -- so the indexed overloads cannot seek an index like they do on main; instead they
  // fall back to filtering the union scan (see MaterializeFilteredBranchScan). Slower than an index
  // seek (O(fork-state graph size) instead of O(matches)), but CORRECT -- the prior version of this
  // code silently used accessor_->Vertices(label, ...), which hit the diff engine's own, always-
  // empty index and silently missed every historical (non-COW'd) vertex, e.g. `MATCH (n:A) RETURN
  // n` after a branch SET on an existing vertex would come back empty.
  VerticesIterable Vertices(storage::View view) {
    if (branch_ctx_ != nullptr) {
      return VerticesIterable(branch_ctx_->Vertices(view), branch_ctx_);
    }
    return VerticesIterable(accessor_->Vertices(view));
  }

  // Graph Versioning v1, Task 4: label-only scan is now INDEX-ACCELERATED, mirroring the plain
  // edge-type merge (`Edges(view, edge_type)` above) rather than the label-PROPERTY merge
  // (`Vertices(view, label, properties, ranges, order)` above): a label index carries no property
  // key, so both streams are already just "every vertex with this label" in whatever order their
  // respective index storage yields them -- a plain gid-order SET-UNION, no merge-sort needed
  // (unlike the property-ordered merge, which must explicitly sort to match the planner's elided
  // OrderBy). Two streams:
  //   (a) the diff engine's own label index (`accessor_->Vertices(label, view)` -- Part A,
  //       branch_engine.cpp, mirrors main's label index definitions onto the diff engine, so this
  //       is always populated once a branch exists), and
  //   (b) main@fork's label index as of the fork timestamp, via `historical()` -- falling back (B1)
  //       to the existing full filter-scan (MaterializeFilteredBranchScan) if main DROPPED the
  //       index after the fork (largely unreachable in practice, since label indexes are covered by
  //       the same global schema gate as everything else Part A mirrors, but defense-in-depth
  //       against a mid-flight drop).
  // A main-stream candidate is skipped if the branch tombstoned it (branch-deleted fork vertex), OR
  // if the diff engine already holds that gid (`accessor_->FindVertex(g, View::NEW)` -- a COW): the
  // diff stream in (a) already represents that gid at its authoritative/new value, so counting it
  // again from (b) would double-count it.
  VerticesIterable Vertices(storage::View view, storage::LabelId label) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.LabelIndexReady(label)) {
        // B1: main dropped this label index after the fork -- fall back to the existing full
        // filter-scan (correct, just O(graph) instead of O(matches)).
        return VerticesIterable(MaterializeFilteredBranchScan(
            view, [label, view](VertexAccessor &v) { return VertexHasLabel(v, view, label); }));
      }

      auto result = std::make_shared<std::vector<VertexAccessor>>();

      // (a) Diff stream: the diff engine's own (branch-native + COW'd) label index scan.
      for (auto raw : accessor_->Vertices(label, view)) {
        result->push_back(VertexAccessor(raw, branch_ctx_));
      }

      // (b) Main@fork stream, gated by the tombstone/COW skip -- diff wins on any overlap.
      for (auto raw : historical.Vertices(label, storage::View::OLD)) {
        VertexAccessor mc(raw, branch_ctx_);
        auto const g = mc.Gid();
        if (branch_ctx_->IsVertexTombstoned(g)) continue;
        if (accessor_->FindVertex(g, storage::View::NEW).has_value()) continue;
        result->push_back(mc);
      }

      // No sort: label indexes carry no property order, so gid/unspecified order matches main's
      // own label scan semantics.
      return VerticesIterable(std::move(result));
    }
    return VerticesIterable(accessor_->Vertices(label, view));
  }

  VerticesIterable Vertices(storage::View view, storage::LabelId label,
                            std::span<storage::PropertyPath const> properties,
                            std::span<storage::PropertyValueRange const> property_ranges,
                            storage::IndexOrder order = storage::IndexOrder::ASC) {
    if (branch_ctx_ != nullptr) {
      // ORDERED single/composite/nested label-property path (Slice 2/Part B). The planner has
      // ELIDED an explicit OrderBy here -- it trusts index order -- so this path must return
      // PROPERTY-ordered rows, not gid-ordered ones. The merge key is a
      // `std::vector<PropertyValue>` -- one value per entry in `properties`, in `properties` order --
      // compared LEXICOGRAPHICALLY; the single-property case is just the 1-element-vector special
      // case of the same key, and a NESTED path (`path.size() > 1`, e.g. `CREATE INDEX ON
      // :L(a.b.c)`) contributes the value found by walking the REMAINING path ids through nested
      // Maps via `storage::ReadNestedPropertyValue` (see `extract_key` below) -- so no separate code
      // path is needed for single, composite, or nested. Part A (branch_engine.cpp) mirrors single,
      // composite, AND nested label-property indexes onto the diff engine, so this merge handles all
      // three uniformly. Two match streams are merged then explicitly sorted (the merge itself is
      // not in any single order):
      //   (a) the diff engine's own mirrored index (Part A guarantees it always exists for single,
      //       composite, and nested label-property indexes), and
      //   (b) main@fork's index as of the fork timestamp, via `historical()` -- falling back (B1) to
      //       a full historical scan + manual predicate if main DROPPED the index after the fork
      //       (a historical index scan against a dropped index would be wrong/unsafe).
      // A main-stream candidate is skipped if the branch tombstoned it, OR if the diff engine already
      // physically holds that gid (a COW) -- that membership probe is deliberately
      // predicate-INDEPENDENT (checked via `accessor_->FindVertex`, not by re-testing the range), so
      // a COW that moved the value out of range is still correctly suppressed here: the diff-engine
      // stream in (a) already represents that gid at its authoritative/new value (or excludes it, if
      // the COW moved it out of range) -- both matches were already applied over there.
      using KeyedVertex = std::pair<std::vector<storage::PropertyValue>, VertexAccessor>;
      std::vector<KeyedVertex> keyed;

      // Builds the composite key for `v` by reading each `properties[i]` at `readview`. For a
      // single-level path (`path.size() == 1`) this is just the top-level property value, exactly as
      // before. For a nested path (`path.size() > 1`, e.g. `a.b.c`), the top-level value read for `a`
      // is walked down the REMAINING path ids (`b`, `c`) through nested Maps via
      // `storage::ReadNestedPropertyValue` (storage/v2/property_value.cppm) -- it returns nullptr if
      // any level along the way is not a Map or the key is missing, in which case this vertex is
      // skipped (treated as "property absent for this vertex", same as a missing top-level property).
      // Returns nullopt if ANY property in `properties` is absent/unreadable for this vertex,
      // signaling the caller to skip it -- mirrors the old single-property `!prop_res.has_value()`
      // skip, generalized across the whole key and across nesting depth. Shared across the
      // diff-engine stream, the historical-index stream, and the B1 historical-full-scan fallback
      // below, so the nested-aware extraction logic exists exactly once.
      auto extract_key = [&properties](VertexAccessor &v,
                                       storage::View readview) -> std::optional<std::vector<storage::PropertyValue>> {
        std::vector<storage::PropertyValue> key;
        key.reserve(properties.size());
        for (const auto &path : properties) {
          auto top = v.GetProperty(readview, path.front());
          if (!top.has_value()) return std::nullopt;  // defensive: an index candidate should always have it
          if (path.size() == 1) {
            key.push_back(std::move(*top));
            continue;
          }
          const auto *nested = storage::ReadNestedPropertyValue(*top, path.as_span().subspan(1));
          if (nested == nullptr) return std::nullopt;  // not a Map at some level along the path, or key missing
          key.push_back(*nested);
        }
        return key;
      };

      // (a) Diff-engine matches.
      for (auto raw : accessor_->Vertices(label, properties, property_ranges, view, order)) {
        VertexAccessor candidate(raw, branch_ctx_);
        auto key = extract_key(candidate, view);
        if (!key.has_value()) continue;
        keyed.emplace_back(std::move(*key), candidate);
      }

      // (b) Main@fork matches, gated by the tombstone/COW skip above.
      auto &historical = branch_ctx_->historical();
      auto consider_main_candidate =
          [this](std::vector<KeyedVertex> &out, VertexAccessor mc, std::vector<storage::PropertyValue> key) {
            auto const g = mc.Gid();
            if (branch_ctx_->IsVertexTombstoned(g)) return;
            if (accessor_->FindVertex(g, storage::View::NEW).has_value()) return;
            out.emplace_back(std::move(key), mc);
          };

      if (historical.LabelPropertyIndexReady(label, properties)) {
        for (auto raw : historical.Vertices(label, properties, property_ranges, storage::View::OLD, order)) {
          VertexAccessor mc(raw, branch_ctx_);
          auto key = extract_key(mc, storage::View::OLD);
          if (!key.has_value()) continue;
          consider_main_candidate(keyed, mc, std::move(*key));
        }
      } else {
        // B1: main dropped this label-property index after the fork -- fall back to a full
        // historical scan + manual predicate; the explicit sort below makes the resulting unordered
        // full scan fine. The range check reuses the same nested-aware `extract_key` used for the
        // sort key above, so single, composite, and nested paths are all extracted through exactly
        // one code path -- a candidate must satisfy ALL of properties[i]/property_ranges[i] (same
        // semantics as the diff-engine's own composite/nested index scan).
        for (auto raw : historical.Vertices(storage::View::OLD)) {
          VertexAccessor mc(raw, branch_ctx_);
          if (!VertexHasLabel(mc, storage::View::OLD, label)) continue;
          auto key = extract_key(mc, storage::View::OLD);
          if (!key.has_value()) continue;
          bool in_range = true;
          for (size_t i = 0; i < properties.size(); ++i) {
            if (!property_ranges[i].IsValueInRange((*key)[i])) {
              in_range = false;
              break;
            }
          }
          if (!in_range) continue;
          consider_main_candidate(keyed, mc, std::move(*key));
        }
      }

      // Explicit sort by the composite key: `std::vector<PropertyValue>`'s own `operator<` performs
      // an element-wise LEXICOGRAPHIC comparison (built on `storage::PropertyValue`'s `operator<=>`,
      // defined in property_value.cppm) -- EXACTLY the comparator the storage index itself uses to
      // order IndexOrderedValues for a composite or nested index, so this matches native index order
      // bit-for-bit (and, for a single property, degrades to the prior 1-element-vector comparison).
      std::stable_sort(keyed.begin(), keyed.end(), [order](const KeyedVertex &a, const KeyedVertex &b) {
        return order == storage::IndexOrder::ASC ? a.first < b.first : b.first < a.first;
      });

      auto result = std::make_shared<std::vector<VertexAccessor>>();
      result->reserve(keyed.size());
      for (auto &entry : keyed) result->push_back(entry.second);
      return VerticesIterable(std::move(result));
    }
    return VerticesIterable(accessor_->Vertices(label, properties, property_ranges, view, order));
  }

  // Graph Versioning v1 (lazy diff-context) DEFENSIVE HARDENING (adversarial-review): every
  // `ChunkedVertices(...)`/`ChunkedEdges(...)` overload below backs the enterprise `ScanParallel*`
  // operators (src/query/plan/operator.cpp) and, unlike their serial `Vertices(...)`/`Edges(...)`
  // counterparts, goes straight to `accessor_` UNCONDITIONALLY -- on a checked-out branch `accessor_`
  // is the diff engine ONLY (see `branch_ctx_`'s own doc-comment above), so a parallel/chunked scan
  // would silently return JUST the diff engine's own (structurally incomplete, historical-edges/
  // vertices-absent) contents. Same gap HIGH-3(b) closed for the serial `Vertices()`/`Edges()` paths
  // (`MaterializeFilteredBranchScan`/`MaterializeFilteredBranchEdgeScan`) -- but a correct chunked
  // UNION scan (reconciling `historical_` and the diff engine across parallel chunks) is a
  // meaningfully larger effort, deferred to its own slice. Reject cleanly (`NotYetImplemented`)
  // rather than return incomplete results in the meantime. Single-threaded ScanAll/ScanAllByLabel/
  // edge scans are unaffected and remain correct -- they route through the union-aware `Vertices()`/
  // `Edges()` above -- so the planner falling back to those on a branch is the right behavior; only
  // the enterprise `ScanParallel*` path that reaches these `Chunked*` overloads is guarded here.
  VerticesChunkedIterable ChunkedVertices(storage::View view, size_t num_chunks) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) vertex scans on a versioned branch");
    }
    return VerticesChunkedIterable{accessor_->ChunkedVertices(view, num_chunks)};
  }

  VerticesChunkedIterable ChunkedVertices(storage::View view, storage::LabelId label, size_t num_chunks) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) vertex scans on a versioned branch");
    }
    return VerticesChunkedIterable{accessor_->ChunkedVertices(label, view, num_chunks)};
  }

  VerticesChunkedIterable ChunkedVertices(storage::View view, storage::LabelId label,
                                          std::span<storage::PropertyPath const> properties,
                                          std::span<storage::PropertyValueRange const> property_ranges,
                                          size_t num_chunks, storage::IndexOrder order = storage::IndexOrder::ASC) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) vertex scans on a versioned branch");
    }
    return VerticesChunkedIterable{
        accessor_->ChunkedVertices(label, properties, property_ranges, view, num_chunks, order)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::EdgeTypeId edge_type, size_t num_chunks) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) edge scans on a versioned branch");
    }
    return EdgesChunkedIterable{accessor_->ChunkedEdges(edge_type, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                                    size_t num_chunks) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) edge scans on a versioned branch");
    }
    return EdgesChunkedIterable{accessor_->ChunkedEdges(edge_type, property, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                                    const std::optional<utils::Bound<storage::PropertyValue>> &lower_bound,
                                    const std::optional<utils::Bound<storage::PropertyValue>> &upper_bound,
                                    size_t num_chunks) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) edge scans on a versioned branch");
    }
    return EdgesChunkedIterable{
        accessor_->ChunkedEdges(edge_type, property, lower_bound, upper_bound, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::PropertyId property, size_t num_chunks) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) edge scans on a versioned branch");
    }
    return EdgesChunkedIterable{accessor_->ChunkedEdges(property, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::PropertyId property,
                                    const storage::PropertyValue value, size_t num_chunks) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) edge scans on a versioned branch");
    }
    return EdgesChunkedIterable{accessor_->ChunkedEdges(property, value, view, num_chunks)};
  }

  EdgesChunkedIterable ChunkedEdges(storage::View view, storage::PropertyId property,
                                    const std::optional<utils::Bound<storage::PropertyValue>> &lower_bound,
                                    const std::optional<utils::Bound<storage::PropertyValue>> &upper_bound,
                                    size_t num_chunks) {
    if (branch_ctx_ != nullptr) {
      throw NotYetImplemented("Parallel (chunked) edge scans on a versioned branch");
    }
    return EdgesChunkedIterable{accessor_->ChunkedEdges(property, lower_bound, upper_bound, view, num_chunks)};
  }

  auto PointVertices(storage::LabelId label, storage::PropertyId property, storage::CoordinateReferenceSystem crs,
                     TypedValue const &point_value, TypedValue const &boundary_value,
                     plan::PointDistanceCondition condition) -> PointIterable;

  auto PointVertices(storage::LabelId label, storage::PropertyId property, storage::CoordinateReferenceSystem crs,
                     TypedValue const &bottom_left, TypedValue const &top_right, plan::WithinBBoxCondition condition)
      -> PointIterable;

  // Graph Versioning v1 (lazy diff-context, slice E-2a) MED FIX (adversarial-review): every
  // `Edges(...)` overload below is an edge-TYPE-and/or-PROPERTY-INDEX scan (there is no non-indexed
  // "all edges" primitive on this class, unlike the plain `Vertices(view)` full scan) -- exactly the
  // class of gap HIGH-3(b) already closed on the vertex side (`Vertices(view, label)`,
  // MaterializeFilteredBranchScan): `accessor_` on a branch is the diff engine ONLY, so
  // `accessor_->Edges(...)` would silently return JUST the diff engine's own (structurally
  // incomplete, unreconciled-with-`historical_`) index contents, missing every still-historical
  // edge. Dead/unreachable TODAY only because the diff engine never has an edge-type/property index
  // to begin with (nothing creates one on a branch yet) -- but silently wrong the moment that
  // changes, so guard now rather than wait for a silent-data-loss bug report.
  //
  // SLICE E-2d IMPLEMENTATION (edge_type+property / +value / +range overloads below):
  // each of THOSE overloads builds a predicate over the dimensions it cares about (property
  // presence / property value / property range) and hands it to `MaterializeFilteredBranchEdgeScan`
  // -- the edge-side mirror of `MaterializeFilteredBranchScan`, union-materialize instead of an
  // index seek, same "eager, full-scan, but correct" trade-off (no branch-aware edge index to seek
  // instead, out of scope for this slice). A property read that comes back without a value, or with
  // a Null value, is treated as "does not match" by every predicate below -- matching main's own
  // index semantics: an edge missing (or nulled-out on) the property is never a member of a
  // property index.
  //
  // Slice 3a/Part B: the plain edge_type-only overload immediately below is now
  // INDEX-ACCELERATED, mirroring the vertex label-property Part B merge above (Vertices(view,
  // label, properties, ranges, order)) but WITHOUT that overload's explicit sort: an edge-type
  // index carries no property key, so both streams are already just "every edge of this type" in
  // whatever order their respective index storage yields them -- a plain gid-order SET-UNION, no
  // merge-sort needed. Two streams:
  //   (a) the diff engine's own edge-type index (`accessor_->Edges(edge_type, view)` -- Part A,
  //       branch_engine.cpp, mirrors main's edge-type index definitions onto the diff engine in
  //       full-edge mode, so this is always populated once a branch exists), and
  //   (b) main@fork's edge-type index as of the fork timestamp, via `historical()` -- falling back
  //       (B1) to the existing full filter-scan if main DROPPED the index after the fork (largely
  //       unreachable in practice, since edge-type indexes are covered by the same global schema
  //       gate as everything else Part A mirrors, but defense-in-depth against a mid-flight drop).
  // A main-stream candidate is skipped if the branch tombstoned it (branch-deleted fork edge), OR
  // if the diff engine already holds that gid (`FindDiffEdge(g, View::NEW)` -- a COW): the diff
  // stream in (a) already represents that gid, so counting it again from (b) would double-count it.
  // Full-edge-mode only: an edge-type index does not exist at all in light-edge mode, so this path
  // is only ever reached when Part A actually mirrored one.
  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgeTypeIndexReady(edge_type)) {
        // B1: main dropped this edge-type index after the fork -- fall back to the existing
        // full filter-scan (correct, just O(graph) instead of O(matches)).
        return EdgesIterable(MaterializeFilteredBranchEdgeScan(
            view, [edge_type](EdgeAccessor &e) { return e.EdgeType() == edge_type; }));
      }

      auto result = std::make_shared<std::vector<EdgeAccessor>>();

      // (a) Diff stream: the diff engine's own (branch-native + COW'd) edge-type index scan.
      for (auto raw : accessor_->Edges(edge_type, view)) {
        result->push_back(EdgeAccessor(raw, branch_ctx_));
      }

      // (b) Main@fork stream, gated by the tombstone/COW skip -- diff wins on any overlap.
      for (auto raw : historical.Edges(edge_type, storage::View::OLD)) {
        EdgeAccessor mc(raw, branch_ctx_);
        auto const g = mc.Gid();
        if (branch_ctx_->IsEdgeTombstoned(g)) continue;
        if (branch_ctx_->FindDiffEdge(g, storage::View::NEW).has_value()) continue;
        result->push_back(mc);
      }

      // No sort: edge-type indexes carry no property order, so gid/unspecified order matches
      // main's own edge-type scan semantics.
      return EdgesIterable(std::move(result));
    }
    return EdgesIterable(accessor_->Edges(edge_type, view));
  }

  // Slice 3b/Part B: PRESENCE (property IS NOT NULL) edge-type+property scan, index-accelerated like
  // the plain edge-type overload above -- merges the diff engine's own mirrored edge-type+property
  // index (Part A always mirrors it) with main@fork's index via `historical()` in
  // `MergeBranchEdgePropertyScan`, which also applies the tombstone/COW skip and returns rows sorted
  // ASCENDING by `property` (the planner elides the OrderBy here, trusting index order -- verified:
  // `MATCH ()-[r:T]->() WHERE r.p>0 RETURN r ORDER BY r.p` plans a bare ScanAllByEdgeTypePropertyRange
  // with no OrderBy operator on main). B1: if main dropped this index after the fork, fall back to the
  // existing (gid-order) full filter-scan verbatim -- largely unreachable via the global schema gate
  // Part A relies on, and the sort is preserved because the planner only elides the OrderBy when the
  // index is actually selected, which requires readiness.
  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgeTypePropertyIndexReady(edge_type, property)) {
        return EdgesIterable(MaterializeFilteredBranchEdgeScan(view, [edge_type, property, view](EdgeAccessor &e) {
          if (e.EdgeType() != edge_type) return false;
          auto prop = e.GetProperty(view, property);
          return prop.has_value() && !prop->IsNull();
        }));
      }
      return EdgesIterable(MergeBranchEdgePropertyScan(view,
                                                       property,
                                                       accessor_->Edges(edge_type, property, view),
                                                       historical.Edges(edge_type, property, storage::View::OLD)));
    }
    return EdgesIterable(accessor_->Edges(edge_type, property, view));
  }

  // Slice 3b/Part B: EQUALITY edge-type+property scan -- same index-accelerated merge as the
  // presence overload above, ASC-ordered by `property` (planner elides the OrderBy, same
  // verification), B1 fallback to the existing filter-scan verbatim when main dropped the index.
  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                      const storage::PropertyValue value) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgeTypePropertyIndexReady(edge_type, property)) {
        // Copy `value` by VALUE into the predicate closure -- same defensive reasoning as the
        // label/property vertex scan above (see its own doc-comment): the predicate is invoked
        // eagerly, during THIS call, never stored past it, but the copy is cheap and avoids relying
        // on the caller's `value` reference outliving this call.
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
                                      accessor_->Edges(edge_type, property, value, view),
                                      historical.Edges(edge_type, property, value, storage::View::OLD)));
    }
    return EdgesIterable(accessor_->Edges(edge_type, property, value, view));
  }

  // Slice 3b/Part B: RANGE edge-type+property scan -- same index-accelerated merge as the presence
  // overload above, ASC-ordered by `property` (planner elides the OrderBy, same verification), B1
  // fallback to the existing filter-scan verbatim when main dropped the index.
  EdgesIterable Edges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                      const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                      const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgeTypePropertyIndexReady(edge_type, property)) {
        // Copy lower/upper by VALUE (via PropertyValueRange::Bounded) into the predicate closure --
        // same defensive reasoning as the label/property vertex scan above.
        auto range = storage::PropertyValueRange::Bounded(lower, upper);
        return EdgesIterable(
            MaterializeFilteredBranchEdgeScan(view, [edge_type, property, range, view](EdgeAccessor &e) {
              if (e.EdgeType() != edge_type) return false;
              auto prop = e.GetProperty(view, property);
              return prop.has_value() && !prop->IsNull() && range.IsValueInRange(*prop);
            }));
      }
      return EdgesIterable(
          MergeBranchEdgePropertyScan(view,
                                      property,
                                      accessor_->Edges(edge_type, property, lower, upper, view),
                                      historical.Edges(edge_type, property, lower, upper, storage::View::OLD)));
    }
    return EdgesIterable(accessor_->Edges(edge_type, property, lower, upper, view));
  }

  // Slice 3c/Part B: PRESENCE (property IS NOT NULL) GLOBAL edge-property scan (no edge_type) --
  // reuses the exact same index-accelerated merge as the edge-type+property presence overload above
  // (`MergeBranchEdgePropertyScan`), just seeking main's GLOBAL edge-property index instead of the
  // edge-type+property one: merges the diff engine's own mirrored global edge-property index (Part A
  // always mirrors it) with main@fork's index via `historical()`, tombstone/COW skip applied inside
  // the helper, rows returned ASCENDING by `property` (the planner elides the OrderBy here too,
  // trusting index order, exactly as for the edge-type+property case). B1: if main dropped this
  // index after the fork, fall back to the existing (gid-order) full filter-scan verbatim.
  EdgesIterable Edges(storage::View view, storage::PropertyId property) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgePropertyIndexReady(property)) {
        return EdgesIterable(MaterializeFilteredBranchEdgeScan(view, [property, view](EdgeAccessor &e) {
          auto prop = e.GetProperty(view, property);
          return prop.has_value() && !prop->IsNull();
        }));
      }
      return EdgesIterable(MergeBranchEdgePropertyScan(
          view, property, accessor_->Edges(property, view), historical.Edges(property, storage::View::OLD)));
    }
    return EdgesIterable(accessor_->Edges(property, view));
  }

  // Slice 3c/Part B: EQUALITY GLOBAL edge-property scan -- same index-accelerated merge as the
  // presence overload above, ASC-ordered by `property` (planner elides the OrderBy, same
  // verification), B1 fallback to the existing filter-scan verbatim when main dropped the index.
  EdgesIterable Edges(storage::View view, storage::PropertyId property, const storage::PropertyValue value) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgePropertyIndexReady(property)) {
        // Copy `value` by VALUE into the predicate closure -- same defensive reasoning as the
        // edge-type+property equality overload above (see its own doc-comment): the predicate is
        // invoked eagerly, during THIS call, never stored past it, but the copy is cheap and avoids
        // relying on the caller's `value` reference outliving this call.
        return EdgesIterable(MaterializeFilteredBranchEdgeScan(view, [property, value, view](EdgeAccessor &e) {
          auto prop = e.GetProperty(view, property);
          return prop.has_value() && *prop == value;
        }));
      }
      return EdgesIterable(MergeBranchEdgePropertyScan(view,
                                                       property,
                                                       accessor_->Edges(property, value, view),
                                                       historical.Edges(property, value, storage::View::OLD)));
    }
    return EdgesIterable(accessor_->Edges(property, value, view));
  }

  // Slice 3c/Part B: RANGE GLOBAL edge-property scan -- same index-accelerated merge as the presence
  // overload above, ASC-ordered by `property` (planner elides the OrderBy, same verification), B1
  // fallback to the existing filter-scan verbatim when main dropped the index.
  EdgesIterable Edges(storage::View view, storage::PropertyId property,
                      const std::optional<utils::Bound<storage::PropertyValue>> &lower,
                      const std::optional<utils::Bound<storage::PropertyValue>> &upper) {
    if (branch_ctx_ != nullptr) {
      auto &historical = branch_ctx_->historical();
      if (!historical.EdgePropertyIndexReady(property)) {
        // Copy lower/upper by VALUE (via PropertyValueRange::Bounded) into the predicate closure --
        // same defensive reasoning as the edge-type+property range overload above.
        auto range = storage::PropertyValueRange::Bounded(lower, upper);
        return EdgesIterable(MaterializeFilteredBranchEdgeScan(view, [property, range, view](EdgeAccessor &e) {
          auto prop = e.GetProperty(view, property);
          return prop.has_value() && !prop->IsNull() && range.IsValueInRange(*prop);
        }));
      }
      return EdgesIterable(MergeBranchEdgePropertyScan(view,
                                                       property,
                                                       accessor_->Edges(property, lower, upper, view),
                                                       historical.Edges(property, lower, upper, storage::View::OLD)));
    }
    return EdgesIterable(accessor_->Edges(property, lower, upper, view));
  }

  // Branch-native create: `accessor_->CreateVertex()` already targets the diff engine when
  // branch_ctx_ is set (see branch_ctx_'s own doc-comment) -- stock CreateVertex, just stamped
  // with the context so subsequent mutations/identity comparisons on the returned accessor are
  // branch-aware. The gid-watermark reservation (branch_engine.cpp) keeps this auto-assigned gid
  // disjoint from anything historical_ could ever contain.
  VertexAccessor InsertVertex() {
    if (branch_ctx_ != nullptr) {
      return VertexAccessor(accessor_->CreateVertex(), branch_ctx_);
    }
    return VertexAccessor(accessor_->CreateVertex());
  }

  // Graph Versioning v1 (lazy diff-context, slice E-2a) COW-BOTH-ENDPOINTS: `accessor_` IS the
  // branch's diff-engine accessor for this transaction on a checked-out branch (see `branch_ctx_`'s
  // own doc-comment above) -- so `accessor_->CreateEdge` itself needs no branch-specific dispatch,
  // UNLIKE InsertVertex/CreateVertex above. The catch: `CreateEdge` (InMemoryAccessor::CreateEdgeEx's
  // public, auto-gid sibling) MG_ASSERTs both endpoint VertexAccessors' `transaction_ == &this
  // transaction`, i.e. both must already be PHYSICALLY resident in the diff engine -- a
  // not-yet-touched endpoint (still resolving through the read-only `historical_`) would trip that
  // assert. `CowVertex` (idempotent -- a no-op if either endpoint was already COW'd, e.g. by an
  // earlier `MATCH`/`SET` in the same statement) brings both into the diff engine first, mirroring
  // `VertexAccessor::CowIfNeeded`'s own mutator idiom one level up (there is no analogous
  // `EdgeAccessor::CowIfNeeded` to call THROUGH here -- the edge doesn't exist yet, this call is
  // what creates it). `from`/`to` are redirected to their diff-engine copies too (not just used
  // locally) so the caller's own accessors -- and, per the HIGH-2 self-correcting-read precedent, any
  // other outstanding copy of the same gid -- stay consistent with the just-created edge's actual
  // endpoints.
  storage::Result<EdgeAccessor> InsertEdge(VertexAccessor *from, VertexAccessor *to,
                                           const storage::EdgeTypeId &edge_type) {
    if (branch_ctx_ != nullptr) {
      auto from_diff = branch_ctx_->CowVertex(from->Gid());
      if (!from_diff) throw QueryRuntimeException(from_diff.error().message);
      auto to_diff = branch_ctx_->CowVertex(to->Gid());
      if (!to_diff) throw QueryRuntimeException(to_diff.error().message);

      from->impl_ = *from_diff;
      to->impl_ = *to_diff;

      auto maybe_edge = accessor_->CreateEdge(&*from_diff, &*to_diff, edge_type);
      if (!maybe_edge) return std::unexpected{maybe_edge.error()};
      return EdgeAccessor(*maybe_edge, branch_ctx_);
    }

    auto maybe_edge = accessor_->CreateEdge(&from->impl_, &to->impl_, edge_type);
    if (!maybe_edge) return std::unexpected{maybe_edge.error()};
    return EdgeAccessor(*maybe_edge);
  }

  // Graph Versioning v1, slice E-4: DELETE on a branch. A diff-engine DeleteEdge/DeleteVertex/
  // DetachDelete produces the exact deltas CaptureBranchCommit already encodes into WalEdgeDelete/
  // WalVertexDelete (verified: capture needed ZERO changes for this, see branch_engine.hpp's own
  // E-4 scope note) -- so the mechanism is: COW the target(s) into the diff engine first (mirrors
  // VertexAccessor::CowIfNeeded/EdgeAccessor::CowEdgeIfNeeded's own mutator idiom, and InsertEdge's
  // COW-both-endpoints idiom above -- a not-yet-touched FORK object's impl_ still resolves through
  // the read-only historical_, and accessor_ (the diff engine) can only delete an object PHYSICALLY
  // its own: PrepareDeletableNodes/PrepareDeletableEdges MG_ASSERT the transaction pointer matches),
  // run the real diff-engine delete, then record the gid(s) in BranchContext's tombstone set(s) --
  // see `tombstoned_vertices_`/`tombstoned_edges_`'s own doc-comment (branch_engine.hpp) for why a
  // bare diff-engine miss alone is not enough to hide a deleted fork object from the read path.
  //
  // DETACH DELETE's automatic cascade (detach=true) IS implemented (slice E-4 increment 5) -- see
  // DetachDelete/DetachRemoveVertex below. A COW'd fork vertex's diff-engine adjacency starts EMPTY
  // (CowVertex copies props+labels only, not adjacency), so the diff engine's OWN native cascade
  // (storage.cpp's PrepareDeletableEdges, which reads the physical in_edges/out_edges off each
  // node) would silently miss every fork-resident incident edge if left as-is. The fix: before
  // calling the native cascade, pre-COW every incident edge of every target vertex via
  // `branch_ctx_->ResolveEdges(gid, direction, View::NEW, {})` (both OUT and IN) followed by
  // `branch_ctx_->CowEdge(...)` for each -- CowEdge is idempotent (a no-op re-COW for an
  // already-diff-resident edge, e.g. one also passed as a named edge or shared between two target
  // vertices/a self-loop), so this is safe to run unconditionally per target vertex. Once every
  // incident edge is diff-resident, the native cascade's adjacency read is complete and correct.
  storage::Result<std::optional<EdgeAccessor>> RemoveEdge(EdgeAccessor *edge) {
    if (branch_ctx_ != nullptr) {
      // Bug fix (double-delete-on-a-branch crash): a target already tombstoned on this branch is
      // already deleted -- deleting it again must be a no-op (matches `main`'s own double-delete
      // semantics), NOT a re-COW. Re-COWing an already-tombstoned edge is exactly what used to crash
      // (CowEdge's own idempotency check only sees a diff-engine miss at View::NEW, indistinguishable
      // from "never COW'd", so it fell through to CreateEdgeEx at a gid the diff engine's skiplist
      // still physically occupies -- MG_ASSERT in storage.cpp). Must be checked BEFORE CowEdge.
      if (branch_ctx_->IsEdgeTombstoned(edge->Gid())) return std::optional<EdgeAccessor>{};

      auto cowed = branch_ctx_->CowEdge(edge->impl_);
      if (!cowed) throw QueryRuntimeException(cowed.error().message);
      edge->impl_ = *cowed;

      auto res = accessor_->DeleteEdge(&edge->impl_);
      if (!res) return std::unexpected{res.error()};

      const auto &value = res.value();
      if (!value) return std::optional<EdgeAccessor>{};

      branch_ctx_->TombstoneEdge(edge->Gid());
      return std::make_optional<EdgeAccessor>(*value, branch_ctx_);
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

  // Graph Versioning v1, slice E-4 increment 5: DETACH DELETE for a SINGLE vertex (mgp C-API's
  // `mgp_graph_detach_delete_vertex` only -- the Cypher `DETACH DELETE` clause always routes through
  // the batch `DetachDelete` below, query::plan::Delete::DeleteCursor::Pull, operator.cpp) always
  // implies the automatic incident-edge cascade -- see RemoveEdge's own doc-comment above for the
  // general mechanism (pre-COW every incident edge via ResolveEdges + CowEdge, then run the native
  // cascade). Plain (non-cascading) delete is also supported -- RemoveVertex below, and DetachDelete
  // below with detach=false.
  storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> DetachRemoveVertex(
      VertexAccessor *vertex_accessor) {
    using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;

    if (branch_ctx_ != nullptr) {
      // Bug fix (double-delete-on-a-branch crash): see RemoveEdge's own comment above for the full
      // rationale -- an already-tombstoned vertex is already deleted; deleting it again is a no-op,
      // not a re-COW (CowVertex's own idempotency check cannot tell "already deleted" apart from
      // "never COW'd", and would otherwise crash the same way CowEdge does).
      if (branch_ctx_->IsVertexTombstoned(vertex_accessor->Gid())) return std::optional<ReturnType>{};

      auto cowed = branch_ctx_->CowVertex(vertex_accessor->Gid());
      if (!cowed) throw QueryRuntimeException(cowed.error().message);
      vertex_accessor->impl_ = *cowed;

      for (auto direction : {storage::EdgeDirection::OUT, storage::EdgeDirection::IN}) {
        for (auto &incident_edge :
             branch_ctx_->ResolveEdges(vertex_accessor->Gid(), direction, storage::View::NEW, {})) {
          auto cowed_edge = branch_ctx_->CowEdge(incident_edge);
          if (!cowed_edge) throw QueryRuntimeException(cowed_edge.error().message);
        }
      }

      auto res = accessor_->DetachDeleteVertex(&vertex_accessor->impl_);
      if (!res) return std::unexpected{res.error()};

      const auto &value = res.value();
      if (!value) return std::optional<ReturnType>{};

      const auto &[vertex, edges] = *value;

      branch_ctx_->TombstoneVertex(vertex.Gid());

      std::vector<EdgeAccessor> deleted_edges;
      deleted_edges.reserve(edges.size());
      for (const auto &deleted_edge : edges) {
        branch_ctx_->TombstoneEdge(deleted_edge.Gid());
        deleted_edges.emplace_back(deleted_edge, branch_ctx_);
      }

      return std::make_optional<ReturnType>(VertexAccessor(vertex, branch_ctx_), std::move(deleted_edges));
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

  // Graph Versioning v1, slice E-4: see RemoveEdge's own doc-comment above for the COW + tombstone
  // mechanism. Plain (non-cascading) delete -- `no incident edges` is enforced via
  // BranchVertexHasUncountedEdges (its own doc-comment above) rather than relying on
  // accessor_->DeleteVertex's native check, which is blind to fork-resident edges on a COW'd vertex.
  storage::Result<std::optional<VertexAccessor>> RemoveVertex(VertexAccessor *vertex_accessor) {
    if (branch_ctx_ != nullptr) {
      // Bug fix (double-delete-on-a-branch crash): see RemoveEdge's own comment above.
      if (branch_ctx_->IsVertexTombstoned(vertex_accessor->Gid())) return std::optional<VertexAccessor>{};

      auto cowed = branch_ctx_->CowVertex(vertex_accessor->Gid());
      if (!cowed) throw QueryRuntimeException(cowed.error().message);
      vertex_accessor->impl_ = *cowed;

      if (BranchVertexHasUncountedEdges(vertex_accessor->Gid(), {})) {
        return std::unexpected{storage::Error::VERTEX_HAS_EDGES};
      }

      auto res = accessor_->DeleteVertex(&vertex_accessor->impl_);
      if (!res) return std::unexpected{res.error()};

      const auto &value = res.value();
      if (!value) return std::optional<VertexAccessor>{};

      branch_ctx_->TombstoneVertex(vertex_accessor->Gid());
      return std::make_optional<VertexAccessor>(*value, branch_ctx_);
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

  // Graph Versioning v1, slice E-4 (increment 5 adds the detach=true cascade): EVERY Cypher
  // `DELETE`/`DETACH DELETE` clause -- single node, single edge, or an arbitrary batch of both --
  // routes through THIS one method (query::plan::Delete::DeleteCursor::Pull, operator.cpp, always
  // calls `dba.DetachDelete(nodes, edges, detach_)` regardless of clause shape); RemoveVertex/
  // RemoveEdge above are only reached via the mgp C-API. See RemoveEdge's own doc-comment for the
  // general COW + tombstone mechanism. `detach=false` (plain DELETE of any mix of named nodes and/or
  // edges): every node/edge is COW'd, then a branch-aware VERTEX_HAS_EDGES check runs
  // (BranchVertexHasUncountedEdges, excluding edges THIS SAME statement is also deleting), then the
  // real diff-engine DetachDelete runs, then every gid it actually deleted is tombstoned. `detach=true`
  // (cascading DETACH DELETE): the VERTEX_HAS_EDGES guard is skipped (detach means "cascade", never
  // "reject") -- instead every named edge and every target node is COW'd, then EVERY incident edge of
  // EVERY target node (both `storage::EdgeDirection::OUT` and `IN`, via `branch_ctx_->ResolveEdges`)
  // is pre-COW'd into the diff engine (`branch_ctx_->CowEdge`, idempotent -- safe against
  // already-COW'd/named/shared/self-loop edges), so the native diff-engine cascade's adjacency read
  // (PrepareDeletableEdges, storage.cpp) is complete rather than silently missing fork-resident edges.
  storage::Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>> DetachDelete(
      std::vector<VertexAccessor> nodes, std::vector<EdgeAccessor> edges, bool detach) {
    using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;

    if (branch_ctx_ != nullptr) {
      // Bug fix (double-delete-on-a-branch crash): filter out any target already tombstoned on this
      // branch BEFORE any COW or native delete touches it -- see RemoveEdge's own comment above for
      // the full rationale. This is what makes `MATCH (:A)-[r:R]->(:B) DETACH DELETE r DELETE r` (two
      // separate Delete operators, each calling DetachDelete once) a no-op on its second call instead
      // of a crash: the first call tombstones r; the second call's `edges` vector still holds r's
      // (now stale, fork/historical-pointing) EdgeAccessor, which this strips out here so neither the
      // COW loop below nor the native `accessor_->DetachDelete` ever sees it again -- matching main's
      // own "deleting an already-deleted target is a no-op" semantics (the actually-deleted set
      // reported back to the caller is derived from what the native delete returns, so a skipped
      // target is correctly absent from it, not double-counted). The vertex analogue
      // (`DETACH DELETE n DELETE n`) is covered by the `nodes` filter for the identical reason.
      std::erase_if(edges, [this](const EdgeAccessor &edge) { return branch_ctx_->IsEdgeTombstoned(edge.Gid()); });
      std::erase_if(nodes, [this](const VertexAccessor &node) { return branch_ctx_->IsVertexTombstoned(node.Gid()); });

      if (detach) {
        for (auto &edge : edges) {
          auto cowed = branch_ctx_->CowEdge(edge.impl_);
          if (!cowed) throw QueryRuntimeException(cowed.error().message);
          edge.impl_ = *cowed;
        }
        for (auto &node : nodes) {
          auto cowed = branch_ctx_->CowVertex(node.Gid());
          if (!cowed) throw QueryRuntimeException(cowed.error().message);
          node.impl_ = *cowed;
        }

        // Pre-COW every incident edge of every target node so the native diff-engine cascade's
        // adjacency read (PrepareDeletableEdges, storage.cpp) sees fork-resident edges too --
        // CowEdge is idempotent, so re-COWing a named/shared/self-loop edge here is a safe no-op.
        for (auto &node : nodes) {
          for (auto direction : {storage::EdgeDirection::OUT, storage::EdgeDirection::IN}) {
            for (auto &incident_edge : branch_ctx_->ResolveEdges(node.Gid(), direction, storage::View::NEW, {})) {
              auto cowed_edge = branch_ctx_->CowEdge(incident_edge);
              if (!cowed_edge) throw QueryRuntimeException(cowed_edge.error().message);
            }
          }
        }

        std::vector<storage::VertexAccessor *> nodes_impl;
        std::vector<storage::EdgeAccessor *> edges_impl;
        nodes_impl.reserve(nodes.size());
        edges_impl.reserve(edges.size());
        for (auto &vertex_accessor : nodes) nodes_impl.push_back(&vertex_accessor.impl_);
        for (auto &edge_accessor : edges) edges_impl.push_back(&edge_accessor.impl_);

        auto res = accessor_->DetachDelete(std::move(nodes_impl), std::move(edges_impl), detach);
        if (!res) return std::unexpected{res.error()};

        const auto &value = res.value();
        if (!value) return std::optional<ReturnType>{};

        const auto &[val_vertices, val_edges] = *value;

        std::vector<VertexAccessor> deleted_vertices;
        std::vector<EdgeAccessor> deleted_edges;
        deleted_vertices.reserve(val_vertices.size());
        deleted_edges.reserve(val_edges.size());

        for (const auto &deleted_vertex : val_vertices) {
          branch_ctx_->TombstoneVertex(deleted_vertex.Gid());
          deleted_vertices.emplace_back(deleted_vertex, branch_ctx_);
        }
        for (const auto &deleted_edge : val_edges) {
          branch_ctx_->TombstoneEdge(deleted_edge.Gid());
          deleted_edges.emplace_back(deleted_edge, branch_ctx_);
        }

        return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
      }

      for (auto &node : nodes) {
        auto cowed = branch_ctx_->CowVertex(node.Gid());
        if (!cowed) throw QueryRuntimeException(cowed.error().message);
        node.impl_ = *cowed;
      }
      for (auto &edge : edges) {
        auto cowed = branch_ctx_->CowEdge(edge.impl_);
        if (!cowed) throw QueryRuntimeException(cowed.error().message);
        edge.impl_ = *cowed;
      }

      std::unordered_set<storage::Gid> co_deleted_edges;
      for (auto &edge : edges) co_deleted_edges.insert(edge.Gid());
      for (auto &node : nodes) {
        if (BranchVertexHasUncountedEdges(node.Gid(), co_deleted_edges)) {
          return std::unexpected{storage::Error::VERTEX_HAS_EDGES};
        }
      }

      std::vector<storage::VertexAccessor *> nodes_impl;
      std::vector<storage::EdgeAccessor *> edges_impl;
      nodes_impl.reserve(nodes.size());
      edges_impl.reserve(edges.size());
      for (auto &vertex_accessor : nodes) nodes_impl.push_back(&vertex_accessor.impl_);
      for (auto &edge_accessor : edges) edges_impl.push_back(&edge_accessor.impl_);

      auto res = accessor_->DetachDelete(std::move(nodes_impl), std::move(edges_impl), detach);
      if (!res) return std::unexpected{res.error()};

      const auto &value = res.value();
      if (!value) return std::optional<ReturnType>{};

      const auto &[val_vertices, val_edges] = *value;

      std::vector<VertexAccessor> deleted_vertices;
      std::vector<EdgeAccessor> deleted_edges;
      deleted_vertices.reserve(val_vertices.size());
      deleted_edges.reserve(val_edges.size());

      for (const auto &deleted_vertex : val_vertices) {
        branch_ctx_->TombstoneVertex(deleted_vertex.Gid());
        deleted_vertices.emplace_back(deleted_vertex, branch_ctx_);
      }
      for (const auto &deleted_edge : val_edges) {
        branch_ctx_->TombstoneEdge(deleted_edge.Gid());
        deleted_edges.emplace_back(deleted_edge, branch_ctx_);
      }

      return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
    }

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
    return accessor_->PrepareForCommitPhase(std::move(commit_args));
  }

  std::expected<void, storage::StorageManipulationError> PeriodicCommit(storage::CommitArgs commit_args) {
    return accessor_->PeriodicCommit(std::move(commit_args));
  }

  void Abort() { accessor_->Abort(); }

  storage::StorageMode GetStorageMode() const noexcept { return accessor_->GetCreationStorageMode(); }

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
