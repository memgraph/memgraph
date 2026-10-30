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

#include "query/db_accessor.hpp"
#ifdef MG_ENTERPRISE
#include <algorithm>
#include <iterator>
#include <tuple>
#include <unordered_set>
#endif
#include <cppitertools/filter.hpp>
#include <cppitertools/imap.hpp>
#include "query/graph.hpp"
#include "query/virtual_graph.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/text_index_utils.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/storage_mode.hpp"

namespace memgraph::query {
SubgraphDbAccessor::SubgraphDbAccessor(query::DbAccessor db_accessor, Graph *graph)
    : db_accessor_(db_accessor), graph_(graph) {}

void SubgraphDbAccessor::TrackCurrentThreadAllocations() { return db_accessor_.TrackCurrentThreadAllocations(); }

void SubgraphDbAccessor::UntrackCurrentThreadAllocations() { return db_accessor_.UntrackCurrentThreadAllocations(); }

storage::PropertyId SubgraphDbAccessor::NameToProperty(const std::string_view name) {
  return db_accessor_.NameToProperty(name);
}

storage::LabelId SubgraphDbAccessor::NameToLabel(const std::string_view name) { return db_accessor_.NameToLabel(name); }

storage::EdgeTypeId SubgraphDbAccessor::NameToEdgeType(const std::string_view name) {
  return db_accessor_.NameToEdgeType(name);
}

const std::string &SubgraphDbAccessor::PropertyToName(storage::PropertyId prop) const {
  return db_accessor_.PropertyToName(prop);
}

const std::string &SubgraphDbAccessor::LabelToName(storage::LabelId label) const {
  return db_accessor_.LabelToName(label);
}

const std::string &SubgraphDbAccessor::EdgeTypeToName(storage::EdgeTypeId type) const {
  return db_accessor_.EdgeTypeToName(type);
}

storage::Result<std::optional<EdgeAccessor>> SubgraphDbAccessor::RemoveEdge(EdgeAccessor *edge) {
  if (!this->graph_->ContainsEdge(*edge)) {
    throw std::logic_error{"Projected graph must contain edge!"};
  }
  auto result = db_accessor_.RemoveEdge(edge);
  if (!result || !*result) {
    return result;
  }
  return this->graph_->RemoveEdge(*edge);
}

storage::Result<EdgeAccessor> SubgraphDbAccessor::InsertEdge(SubgraphVertexAccessor *from, SubgraphVertexAccessor *to,
                                                             const storage::EdgeTypeId &edge_type) {
  VertexAccessor *from_impl = &from->impl_;
  VertexAccessor *to_impl = &to->impl_;
  if (!this->graph_->ContainsVertex(*from_impl) || !this->graph_->ContainsVertex(*to_impl)) {
    throw std::logic_error{"Projected graph must contain both vertices to insert edge!"};
  }
  auto result = db_accessor_.InsertEdge(from_impl, to_impl, edge_type);
  if (!result) {
    return result;
  }
  this->graph_->InsertEdge(*result);
  return result;
}

storage::Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>>
SubgraphDbAccessor::DetachRemoveVertex(  // NOLINT(readability-convert-member-functions-to-static)
    SubgraphVertexAccessor *) {          // NOLINT(hicpp-named-parameter)
  throw std::logic_error{
      "Vertex holds only partial information about edges. Cannot detach delete safely while using projected graph."};
}

storage::Result<std::optional<VertexAccessor>> SubgraphDbAccessor::RemoveVertex(
    SubgraphVertexAccessor *subgraphvertex_accessor) {
  VertexAccessor *vertex_accessor = &subgraphvertex_accessor->impl_;
  if (!this->graph_->ContainsVertex(*vertex_accessor)) {
    throw std::logic_error{"Projected graph must contain vertex!"};
  }
  auto result = db_accessor_.RemoveVertex(vertex_accessor);
  if (!result || !*result) {
    return result;
  }
  return this->graph_->RemoveVertex(*vertex_accessor);
}

SubgraphVertexAccessor SubgraphDbAccessor::InsertVertex() {
  VertexAccessor vertex = db_accessor_.InsertVertex();
  this->graph_->InsertVertex(vertex);
  return SubgraphVertexAccessor(vertex, this->getGraph());
}

VerticesIterable SubgraphDbAccessor::Vertices(storage::View) {  // NOLINT(hicpp-named-parameter)
  return VerticesIterable(&graph_->vertices());
}

std::optional<VertexAccessor> SubgraphDbAccessor::FindVertex(storage::Gid gid, storage::View view) {
  std::optional<VertexAccessor> maybe_vertex = db_accessor_.FindVertex(gid, view);
  if (maybe_vertex && this->graph_->ContainsVertex(*maybe_vertex)) {
    return *maybe_vertex;
  }
  return std::nullopt;
}

std::optional<EdgeAccessor> SubgraphDbAccessor::FindEdge(storage::Gid edge_gid, storage::Gid from_vertex_gid,
                                                         storage::View view) {
  std::optional<EdgeAccessor> maybe_edge = db_accessor_.FindEdge(edge_gid, from_vertex_gid, view);
  if (maybe_edge && this->graph_->ContainsEdge(*maybe_edge)) {
    return maybe_edge;
  }
  return std::nullopt;
}

query::Graph *SubgraphDbAccessor::getGraph() { return graph_; }

storage::StorageMode SubgraphDbAccessor::GetStorageMode() const noexcept { return db_accessor_.GetStorageMode(); }

DbAccessor *SubgraphDbAccessor::GetAccessor() { return &db_accessor_; }

VirtualGraphDbAccessor::VirtualGraphDbAccessor(query::DbAccessor db_accessor, VirtualGraph *graph)
    : db_accessor_(db_accessor), graph_(graph) {}

void VirtualGraphDbAccessor::TrackCurrentThreadAllocations() { db_accessor_.TrackCurrentThreadAllocations(); }

void VirtualGraphDbAccessor::
    UntrackCurrentThreadAllocations() {  // NOLINT(readability-convert-member-functions-to-static)
  query::DbAccessor::UntrackCurrentThreadAllocations();
}

storage::PropertyId VirtualGraphDbAccessor::NameToProperty(const std::string_view name) {
  return db_accessor_.NameToProperty(name);
}

storage::LabelId VirtualGraphDbAccessor::NameToLabel(const std::string_view name) {
  return db_accessor_.NameToLabel(name);
}

storage::EdgeTypeId VirtualGraphDbAccessor::NameToEdgeType(const std::string_view name) {
  return db_accessor_.NameToEdgeType(name);
}

const std::string &VirtualGraphDbAccessor::PropertyToName(storage::PropertyId prop) const {
  return db_accessor_.PropertyToName(prop);
}

const std::string &VirtualGraphDbAccessor::LabelToName(storage::LabelId label) const {
  return db_accessor_.LabelToName(label);
}

const std::string &VirtualGraphDbAccessor::EdgeTypeToName(storage::EdgeTypeId type) const {
  return db_accessor_.EdgeTypeToName(type);
}

std::shared_ptr<const VirtualNode> VirtualGraphDbAccessor::FindNode(storage::Gid synthetic_gid) const {
  return graph_->FindNode(synthetic_gid);
}

VirtualGraph *VirtualGraphDbAccessor::getGraph() const { return graph_; }

storage::StorageMode VirtualGraphDbAccessor::GetStorageMode() const noexcept { return db_accessor_.GetStorageMode(); }

DbAccessor *VirtualGraphDbAccessor::GetAccessor() { return &db_accessor_; }

VertexAccessor SubgraphVertexAccessor::GetVertexAccessor() const { return impl_; }

storage::Result<storage::PropertyValue> SubgraphVertexAccessor::GetProperty(storage::View view,
                                                                            storage::PropertyId key) const {
  return impl_.GetProperty(view, key);
}

namespace {

/// Narrows an expansion to the edges `graph` holds, keeping it a range that builds its accessors
/// when they are looked at.
auto KeptIn(Graph const *graph) {
  return [graph](storage::EdgesVertexAccessorResult &&result) -> ExpandedEdgesResult {
    auto const &graph_edges = graph->edges();
    auto kept = result.edges.Filtered(
        [&graph_edges](storage::EdgeAccessor const &edge) { return graph_edges.contains(EdgeAccessor{edge}); });
    return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(kept)}, .expanded_count = result.expanded_count};
  };
}
#ifdef MG_ENTERPRISE

/// On a branch the storage adjacency is one-sided and holds raw stubs, so the expansion goes through the branch-aware
/// query accessor and only membership is filtered here (by gid, so stub-backed and fork copies of an edge match).
auto KeptInBranch(Graph const *graph) {
  return [graph](ExpandedEdgesResult &&result) -> ExpandedEdgesResult {
    auto const &graph_edges = graph->edges();
    std::vector<EdgeAccessor> kept;
    for (auto const &edge : result.edges) {
      if (graph_edges.contains(edge)) kept.push_back(edge);
    }
    return ExpandedEdgesResult{.edges = ExpandedEdges{std::move(kept)}, .expanded_count = result.expanded_count};
  };
}
#endif

}  // namespace

storage::Result<ExpandedEdgesResult> SubgraphVertexAccessor::OutEdges(storage::View view) const {
#ifdef MG_ENTERPRISE
  if (impl_.branch_ctx_ != nullptr) [[unlikely]]
    return impl_.OutEdges(view).transform(KeptInBranch(graph_));
#endif
  return impl_.impl_.OutEdges(view, {}).transform(KeptIn(graph_));
}

storage::Result<ExpandedEdgesResult> SubgraphVertexAccessor::InEdges(storage::View view) const {
#ifdef MG_ENTERPRISE
  if (impl_.branch_ctx_ != nullptr) [[unlikely]]
    return impl_.InEdges(view).transform(KeptInBranch(graph_));
#endif
  return impl_.impl_.InEdges(view, {}).transform(KeptIn(graph_));
}

#ifdef MG_ENTERPRISE
std::optional<VertexAccessor> DbAccessor::BranchFindVertex(storage::Gid gid, storage::View view) {
  auto maybe_vertex = branch_ctx_->ResolveVertex(gid, view);
  if (maybe_vertex) return VertexAccessor(*maybe_vertex, branch_ctx_);
  return std::nullopt;
}

namespace {
// Every branch edge once, through its FROM vertex's OUT adjacency, O(V+E); stops when `fn(edge)` returns true.
template <typename Fn>
void ForEachBranchEdge(versioning::BranchContext &ctx, storage::View view, Fn &&fn) {
  for (auto vertex : ctx.Vertices(view)) {
    for (auto &edge : ctx.ResolveEdges(vertex.Gid(), storage::EdgeDirection::OUT, view)) {
      if (fn(edge)) return;
    }
  }
}
}  // namespace

std::optional<EdgeAccessor> DbAccessor::BranchFindEdge(storage::Gid gid, storage::View view) {
  if (auto diff_edge = branch_ctx_->FindDiffEdge(gid, view)) return EdgeAccessor(*diff_edge, branch_ctx_);
  std::optional<EdgeAccessor> found;
  ForEachBranchEdge(*branch_ctx_, view, [&](storage::EdgeAccessor &edge) {
    if (edge.Gid() == gid) found.emplace(edge, branch_ctx_);
    return found.has_value();
  });
  return found;
}

std::optional<EdgeAccessor> DbAccessor::BranchFindEdge(storage::Gid edge_gid, storage::Gid from_vertex_gid,
                                                       storage::View view) {
  for (auto &edge : branch_ctx_->ResolveEdges(from_vertex_gid, storage::EdgeDirection::OUT, view)) {
    if (edge.Gid() == edge_gid) return EdgeAccessor(edge, branch_ctx_);
  }
  return std::nullopt;
}

VerticesIterable DbAccessor::BranchVertices(storage::View view) {
  // No deletes and no branch-native vertex (the skiplist size counts uncommitted and un-GC'd ones too, so 0 means none
  // visible to anyone): the union is exactly main@fork's stream, yielded without the per-vertex merge. OLD only: a NEW
  // scan must see deletes the same query makes while it iterates, which the union checks per vertex.
  if (view == storage::View::OLD && !branch_ctx_->MayHaveAnyVertexDeleted() &&
      accessor_->ApproximateVertexCount() == 0) {
    return {branch_ctx_->historical().Vertices(storage::View::OLD), branch_ctx_};
  }
  return {branch_ctx_->Vertices(view), branch_ctx_};
}

bool BranchScanBefore(BranchScanPosition const &a, BranchScanPosition const &b, bool descending) {
  if (a.key != b.key) return descending ? b.key < a.key : a.key < b.key;
  return std::tie(a.native, a.address) < std::tie(b.native, b.address);
}

template <typename T>
BranchScan<T>::BranchScan(std::vector<T> elements, Live live)
    : elements_(std::move(elements)), live_(std::move(live)), epoch_seen_(live_->epoch()) {}

template <typename T>
size_t BranchScan<T>::NextLive(size_t from) {
  auto &live = *live_;
  if (auto const epoch = live.epoch(); epoch != epoch_seen_) {
    epoch_seen_ = epoch;
    elements_ = live.rescan();
    from = 0;
    if (cursor_) {
      auto const after = std::ranges::partition_point(elements_, [&](T &element) {
        auto const position = live.locate(element);
        DMG_ASSERT(position, "A re-materialized branch scan holds an element it does not match");
        return !BranchScanBefore(*cursor_, *position, live.descending);
      });
      from = static_cast<size_t>(after - elements_.begin());
    }
  }
  for (; from < elements_.size(); ++from) {
    if (auto position = live.locate(elements_[from])) {
      cursor_ = std::move(*position);
      return from;
    }
  }
  return kEnd;
}

template class BranchScan<VertexAccessor>;
template class BranchScan<EdgeAccessor>;

namespace {
using Materialized = std::vector<VertexAccessor>;
using VertexScan = BranchScan<VertexAccessor>;
using TouchedGids = versioning::BranchCore::LabelTouchedSet;
// A vertex's index key: one value per indexed property path.
using BranchKey = std::vector<storage::PropertyValue>;
using ScanInput = versioning::BranchContext::ScanInput;

bool VertexHasLabel(VertexAccessor &vertex, storage::View view, storage::LabelId label) {
  auto has_label = vertex.HasLabel(view, label);
  return has_label.has_value() && *has_label;
}

BranchScanPosition VertexPosition(versioning::BranchContext &ctx, VertexAccessor const &vertex, BranchKey key) {
  return {.key = std::move(key),
          .native = !ctx.IsForkVertexGid(vertex.Gid()),
          .address = reinterpret_cast<uintptr_t>(vertex.impl_.vertex_)};
}

// Wraps a vertex scan that `build` materializes in index order. `key_of` is the vertex's key while the scan still
// matches it at `view`; `epoch` sums the scan epochs of the inputs that can bring a vertex in.
template <typename Build, typename KeyOf, typename Epoch>
VerticesIterable MakeVertexScan(versioning::BranchContext &ctx, storage::View view, Build build, KeyOf key_of,
                                Epoch epoch, storage::IndexOrder order = storage::IndexOrder::ASC) {
  if (view == storage::View::OLD) return VerticesIterable(std::make_shared<VertexScan>(build()));
  // As main's scans: an object created after the scan started is never yielded.
  auto const gid_floor = ctx.core()->main().vertex_id_.load(std::memory_order_acquire);
  VertexScan::Live live{.rescan =
                            [build, gid_floor] {
                              auto elements = build();
                              std::erase_if(elements,
                                            [&](VertexAccessor const &v) { return v.Gid().AsUint() >= gid_floor; });
                              return elements;
                            },
                        .locate = [&ctx, key_of](VertexAccessor &v) -> std::optional<BranchScanPosition> {
                          auto key = key_of(v);
                          if (!key) return std::nullopt;
                          return VertexPosition(ctx, v, std::move(*key));
                        },
                        .epoch = std::move(epoch),
                        .descending = order == storage::IndexOrder::DESC};
  return VerticesIterable(std::make_shared<VertexScan>(build(), std::move(live)));
}

// Full scan of the branch in index order, O(V log V); the fallback for an index not visible at the fork. `key_of` is
// the vertex's key if it is in the scan.
template <typename KeyOf>
Materialized MaterializeFilteredScan(versioning::BranchContext &ctx, storage::View view, KeyOf &&key_of,
                                     storage::IndexOrder order = storage::IndexOrder::ASC) {
  std::vector<std::pair<BranchScanPosition, VertexAccessor>> keyed;
  for (auto raw : ctx.Vertices(view)) {
    VertexAccessor candidate(raw, &ctx);
    if (auto key = key_of(candidate)) keyed.emplace_back(VertexPosition(ctx, candidate, std::move(*key)), candidate);
  }
  bool const descending = order == storage::IndexOrder::DESC;
  std::ranges::sort(keyed,
                    [&](auto const &a, auto const &b) { return BranchScanBefore(a.first, b.first, descending); });
  Materialized result;
  result.reserve(keyed.size());
  for (auto &entry : keyed) result.push_back(entry.second);
  return result;
}

std::optional<BranchKey> ReadBranchKey(VertexAccessor &vertex, storage::View view,
                                       std::span<storage::PropertyPath const> properties) {
  BranchKey key;
  key.reserve(properties.size());
  for (auto const &path : properties) {
    auto top = vertex.GetProperty(view, path.front());
    if (!top.has_value()) return std::nullopt;
    if (path.size() == 1) {
      key.push_back(std::move(*top));
      continue;
    }
    auto const *nested = storage::ReadNestedPropertyValue(*top, path.as_span().subspan(1));
    if (nested == nullptr) return std::nullopt;
    key.push_back(*nested);
  }
  return key;
}

// A single-property range as the storage scan applies it: bounds, IS_NOT_NULL and the planner's value predicate.
bool BranchValueInRange(storage::PropertyValueRange const &range, storage::PropertyValue const &value) {
  if (!range.IsValueInRange(value)) return false;
  if (range.type_ == storage::PropertyRangeType::IS_NOT_NULL && value.IsNull()) return false;
  auto const &pred = range.GetValuePredicate();
  return !pred || (*pred)(value);
}

// As the storage index: the ranges constrain a leading prefix of the key, the value predicate applies to the leading
// property, and an all-null key is never indexed.
bool BranchKeyInRange(BranchKey const &key, std::span<storage::PropertyValueRange const> property_ranges) {
  for (size_t i = 0; i < property_ranges.size() && i < key.size(); ++i) {
    auto const &range = property_ranges[i];
    if (!range.IsValueInRange(key[i])) return false;
    if (range.type_ == storage::PropertyRangeType::IS_NOT_NULL && key[i].IsNull()) return false;
    if (i == 0) {
      if (auto const &pred = range.GetValuePredicate(); pred && !(*pred)(key[0])) return false;
    }
  }
  return !std::ranges::all_of(key, [](auto const &value) { return value.IsNull(); });
}

// A range that rejects null anchors the match: a vertex satisfying it has a non-null key there.
bool RangeExcludesNull(storage::PropertyValueRange const &range) {
  return range.type_ == storage::PropertyRangeType::IS_NOT_NULL ||
         (range.type_ == storage::PropertyRangeType::BOUNDED && (range.lower_ || range.upper_));
}

// Candidates the fork index cannot answer for, deduplicated by gid and held as fork accessors. A deleted fork vertex
// may be one: its recheck fails and drops it.
class TouchedSet {
 public:
  explicit TouchedSet(versioning::BranchContext &ctx) : ctx_(&ctx) {}

  void Add(storage::VertexAccessor const &raw) {
    if (seen_.insert(raw.Gid()).second) items_.push_back(ctx_->ForkIdentity(raw));
  }

  // The fork vertices named by a touched-gid snapshot. The snapshot is a superset of the changed vertices; every
  // candidate is rechecked on its branch state.
  void AddForkVertices(TouchedGids const &gids) {
    for (auto const gid : gids) {
      if (auto fork = ctx_->FindForkVertex(storage::Gid::FromUint(gid))) Add(*fork);
    }
  }

  std::vector<storage::VertexAccessor> const &items() const { return items_; }

 private:
  versioning::BranchContext *ctx_;
  std::unordered_set<storage::Gid> seen_;
  std::vector<storage::VertexAccessor> items_;
};

// Splits main@fork's index stream: a vertex with a branch overlay that may change its match joins `touched`, the rest
// keep stream order. A clear branched() bit is the lock-free fast path; `label_touched` (single-label scans) replaces
// the coarse any-label filter.
std::vector<storage::VertexAccessor> SplitForkStream(versioning::BranchContext &ctx,
                                                     storage::VerticesIterable fork_stream, TouchedSet &touched,
                                                     TouchedGids const *label_touched = nullptr) {
  std::vector<storage::VertexAccessor> untouched;
  for (auto raw : fork_stream) {
    if (raw.vertex_->branched()) {
      auto const gid = raw.Gid();
      auto const label_changed =
          label_touched != nullptr ? label_touched->contains(gid.AsUint()) : ctx.MayHaveLabelChange(gid);
      if ((label_changed || ctx.MayHavePropertyChange(gid)) && ctx.HasVertexOverlay(gid)) {
        touched.Add(raw);
        continue;
      }
    }
    untouched.push_back(raw);
  }
  return untouched;
}

// Merges the untouched fork stream (already in index order) with the touched candidates that `key_of` keeps, in index
// order. O(fork matches + touched log touched).
template <typename ReadKey, typename KeyOf>
Materialized MergeOverlay(versioning::BranchContext &ctx, std::vector<storage::VertexAccessor> const &untouched,
                          TouchedSet const &touched, storage::IndexOrder order, ReadKey &&read_key, KeyOf &&key_of) {
  Materialized result;
  if (touched.items().empty()) {
    result.reserve(untouched.size());
    for (auto const &raw : untouched) result.emplace_back(raw, &ctx);
    return result;
  }

  std::vector<std::pair<BranchScanPosition, VertexAccessor>> changed;
  changed.reserve(touched.items().size());
  for (auto const &raw : touched.items()) {
    VertexAccessor candidate(raw, &ctx);
    if (auto key = key_of(candidate)) changed.emplace_back(VertexPosition(ctx, candidate, std::move(*key)), candidate);
  }
  bool const descending = order == storage::IndexOrder::DESC;
  std::ranges::sort(changed,
                    [&](auto const &a, auto const &b) { return BranchScanBefore(a.first, b.first, descending); });

  result.reserve(untouched.size() + changed.size());
  size_t next = 0;
  for (auto const &raw : untouched) {
    VertexAccessor fork_vertex(raw, &ctx);
    if (next < changed.size()) {
      auto const position = VertexPosition(ctx, fork_vertex, read_key(fork_vertex).value_or(BranchKey{}));
      while (next < changed.size() && BranchScanBefore(changed[next].first, position, descending)) {
        result.push_back(changed[next++].second);
      }
    }
    result.push_back(fork_vertex);
  }
  for (; next < changed.size(); ++next) result.push_back(changed[next].second);
  return result;
}

// Nothing relevant changed on the branch (the caller checked) and the diff engine's index has no hit: the answer is
// main@fork's index stream, yielded lazily. `index_size()` is the diff index's skiplist size, which counts uncommitted
// and un-GC'd entries too, so 0 means no hit for anyone.
template <typename IndexSize, typename DiffScan, typename ForkScan>
std::optional<VerticesIterable> LazyForkOnlyScan(storage::Storage::Accessor &diff, versioning::BranchContext &ctx,
                                                 IndexSize &&index_size, DiffScan &&diff_scan, ForkScan &&fork_scan) {
  if (diff.ApproximateVertexCount() != 0 && index_size() != 0) {
    auto hits = diff_scan();
    if (hits.begin() != hits.end()) return std::nullopt;
  }
  return VerticesIterable(fork_scan(), &ctx);
}

// Global vertex-property scan, ascending by value. `fork_scan`/`diff_scan` open the scan on main@fork and on the diff
// engine; `in_range` is the predicate on a candidate's branch value.
template <typename ForkScan, typename DiffScan, typename InRange>
VerticesIterable BranchPropertyScan(storage::Storage::Accessor &diff, versioning::BranchContext &ctx,
                                    storage::View view, storage::PropertyId property, ForkScan fork_scan,
                                    DiffScan diff_scan, InRange in_range) {
  auto const key_of = [view, property, in_range](VertexAccessor &v) -> std::optional<BranchKey> {
    auto value = v.GetProperty(view, property);
    if (!value.has_value() || value->IsNull() || !in_range(*value)) return std::nullopt;
    return BranchKey{std::move(*value)};
  };
  auto const epoch = [&ctx, property] { return ctx.ScanEpoch(ScanInput::kProperty, property.AsUint()); };
  if (!ctx.historical().VertexPropertyIndexReady(property) || !diff.VertexPropertyIndexReady(property)) {
    return MakeVertexScan(
        ctx, view, [&ctx, view, key_of] { return MaterializeFilteredScan(ctx, view, key_of); }, key_of, epoch);
  }
  auto &core = *ctx.core();
  auto const index_size = [&] { return diff.ApproximateVertexCount(property); };
  // OLD only (a NEW scan must see the writes the same query makes while it iterates). The lock-free gate first (set
  // before commit; clear means an empty snapshot), then the snapshot.
  if (view == storage::View::OLD && !ctx.MayHaveAnyVertexDeleted()) {
    bool diff_has_hit = false;
    if (!core.MayHavePropertyTouched(property)) {
      if (auto lazy = LazyForkOnlyScan(diff, ctx, index_size, diff_scan, fork_scan)) return std::move(*lazy);
      diff_has_hit = true;
    }
    if (!diff_has_hit && core.PropertyTouchedSnapshot(property)->empty()) {
      if (auto lazy = LazyForkOnlyScan(diff, ctx, index_size, diff_scan, fork_scan)) return std::move(*lazy);
    }
  }
  auto const build = [&ctx, view, property, fork_scan, diff_scan, key_of] {
    TouchedSet touched{ctx};
    for (auto raw : diff_scan()) touched.Add(raw);
    touched.AddForkVertices(*ctx.core()->PropertyTouchedSnapshot(property));
    auto const untouched = SplitForkStream(ctx, fork_scan(), touched);
    return MergeOverlay(
        ctx,
        untouched,
        touched,
        storage::IndexOrder::ASC,
        [&](VertexAccessor &v) -> std::optional<BranchKey> {
          auto value = v.GetProperty(view, property);
          if (!value.has_value()) return std::nullopt;
          return BranchKey{std::move(*value)};
        },
        key_of);
  };
  return MakeVertexScan(ctx, view, build, key_of, epoch);
}
}  // namespace

// Unsorted (index order is by address): main@fork's label index, minus fork vertices the branch removed the label from
// or deleted, plus fork vertices that gained the label, then the diff engine's hits (branch-native vertices).
VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  auto const key_of = [view, label](VertexAccessor &v) -> std::optional<BranchKey> {
    if (!VertexHasLabel(v, view, label)) return std::nullopt;
    return BranchKey{};
  };
  auto const epoch = [&ctx, label] { return ctx.ScanEpoch(ScanInput::kLabel, label.AsUint()); };
  if (!ctx.historical().LabelIndexReady(label)) {
    return MakeVertexScan(
        ctx, view, [&ctx, view, key_of] { return MaterializeFilteredScan(ctx, view, key_of); }, key_of, epoch);
  }

  // OLD only: a NEW scan must see the writes the same query makes while it iterates.
  if (view == storage::View::OLD && ctx.core()->LabelTouchedSnapshot(label)->empty()) {
    auto native = accessor_->Vertices(label, view);
    if (native.begin() == native.end()) return {ctx.historical().Vertices(label, storage::View::OLD), branch_ctx_};
  }

  auto const build = [this, view, label] {
    auto &ctx = *branch_ctx_;
    auto &historical = ctx.historical();
    // One snapshot up front: every transaction visible to this scan recorded its gids before this point. It also holds
    // every deleted fork gid.
    auto const label_touched = ctx.core()->LabelTouchedSnapshot(label);
    Materialized fork_part;
    fork_part.reserve(historical.ApproximateVertexCount(label));
    for (auto raw : historical.Vertices(label, storage::View::OLD)) {
      VertexAccessor candidate(raw, branch_ctx_);
      if (raw.vertex_->branched() && label_touched->contains(raw.Gid().AsUint()) && ctx.HasVertexOverlay(raw.Gid()) &&
          !VertexHasLabel(candidate, view, label)) {
        continue;
      }
      fork_part.push_back(candidate);
    }

    // Fork vertices that gained the label, merged in by address; those that had it at the fork came from main's index.
    auto const by_address = [](VertexAccessor const &a, VertexAccessor const &b) {
      return a.impl_.vertex_ < b.impl_.vertex_;
    };
    auto const fork_end = fork_part.size();
    for (auto const gid : *label_touched) {
      auto const fork_gid = storage::Gid::FromUint(gid);
      if (ctx.IsVertexDeleted(fork_gid, view)) continue;
      auto fork = ctx.FindForkVertex(fork_gid);
      if (!fork) continue;
      auto had = fork->HasLabel(label, storage::View::OLD);
      if (had.has_value() && *had) continue;
      VertexAccessor candidate(*fork, branch_ctx_);
      if (VertexHasLabel(candidate, view, label)) fork_part.push_back(candidate);
    }
    std::sort(fork_part.begin() + static_cast<std::ptrdiff_t>(fork_end), fork_part.end(), by_address);
    std::inplace_merge(
        fork_part.begin(), fork_part.begin() + static_cast<std::ptrdiff_t>(fork_end), fork_part.end(), by_address);

    for (auto raw : accessor_->Vertices(label, view)) fork_part.emplace_back(raw, branch_ctx_);
    return fork_part;
  };
  return MakeVertexScan(ctx, view, build, key_of, epoch);
}

VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::LabelId label,
                                            std::span<storage::PropertyPath const> properties_span,
                                            std::span<storage::PropertyValueRange const> ranges_span,
                                            storage::IndexOrder order) {
  auto &ctx = *branch_ctx_;
  auto &core = *ctx.core();
  // Copies: a NEW scan re-materializes after the planner's ranges are gone.
  std::vector<storage::PropertyPath> const properties(properties_span.begin(), properties_span.end());
  std::vector<storage::PropertyValueRange> const property_ranges(ranges_span.begin(), ranges_span.end());
  auto const key_of = [view, label, properties, property_ranges](VertexAccessor &v) -> std::optional<BranchKey> {
    if (!VertexHasLabel(v, view, label)) return std::nullopt;
    auto key = ReadBranchKey(v, view, properties);
    if (!key || !BranchKeyInRange(*key, property_ranges)) return std::nullopt;
    return key;
  };
  auto const epoch = [&ctx, label, properties] {
    auto sum = ctx.ScanEpoch(ScanInput::kLabel, label.AsUint());
    for (auto const &path : properties) sum += ctx.ScanEpoch(ScanInput::kProperty, path.front().AsUint());
    return sum;
  };
  if (!ctx.historical().LabelPropertyIndexReady(label, properties) ||
      !accessor_->LabelPropertyIndexReady(label, properties)) {
    return MakeVertexScan(
        ctx,
        view,
        [&ctx, view, key_of, order] { return MaterializeFilteredScan(ctx, view, key_of, order); },
        key_of,
        epoch,
        order);
  }
  if (std::ranges::any_of(property_ranges,
                          [](auto const &range) { return range.type_ == storage::PropertyRangeType::INVALID; })) {
    return VerticesIterable(std::make_shared<VertexScan>(Materialized{}));
  }

  // With a null-excluding range only the constrained positions can turn a vertex into a match; otherwise every key
  // position counts. Unenumerated trailing positions keep main's order on the fast path, which is safe because ORDER BY
  // elimination needs a range per sort column.
  bool const anchored = std::ranges::any_of(property_ranges, RangeExcludesNull);
  auto const key_enumerated = [anchored, n = property_ranges.size()](size_t i) { return !anchored || i < n; };

  auto const diff_scan = [this, label, properties, property_ranges, view, order] {
    return accessor_->Vertices(label, properties, property_ranges, view, order);
  };
  auto const fork_scan = [&ctx, label, properties, property_ranges, order] {
    return ctx.historical().Vertices(label, properties, property_ranges, storage::View::OLD, order);
  };

  // OLD only (a NEW scan must see the writes the same query makes while it iterates). The lock-free gates first (set
  // before commit; clear means an empty snapshot). The label snapshot also holds the deleted gids, hence the delete
  // check.
  if (view == storage::View::OLD) {
    auto const index_size = [&] { return accessor_->ApproximateVertexCount(label, properties); };
    bool maybe_touched = core.MayHaveLabelTouched(label) || ctx.MayHaveAnyVertexDeleted();
    for (size_t i = 0; !maybe_touched && i < properties.size(); ++i) {
      if (key_enumerated(i)) maybe_touched = core.MayHavePropertyTouched(properties[i].front());
    }
    // A diff-index hit here is final: the snapshot path would hit too.
    bool diff_has_hit = false;
    if (!maybe_touched) {
      if (auto lazy = LazyForkOnlyScan(*accessor_, ctx, index_size, diff_scan, fork_scan)) return std::move(*lazy);
      diff_has_hit = true;
    }
    auto const untouched_by_branch = [&] {
      if (!core.LabelTouchedSnapshot(label)->empty()) return false;
      for (size_t i = 0; i < properties.size(); ++i) {
        if (key_enumerated(i) && !core.PropertyTouchedSnapshot(properties[i].front())->empty()) return false;
      }
      return true;
    };
    if (!diff_has_hit && untouched_by_branch()) {
      if (auto lazy = LazyForkOnlyScan(*accessor_, ctx, index_size, diff_scan, fork_scan)) return std::move(*lazy);
    }
  }

  // Candidates main's index cannot hold: branch-native hits and fork vertices that gained the label or set a key.
  auto const build = [&ctx, view, label, properties, order, key_enumerated, diff_scan, fork_scan, key_of] {
    auto &core = *ctx.core();
    auto const label_touched = core.LabelTouchedSnapshot(label);
    TouchedSet touched{ctx};
    for (auto raw : diff_scan()) touched.Add(raw);
    touched.AddForkVertices(*label_touched);
    for (size_t i = 0; i < properties.size(); ++i) {
      if (key_enumerated(i)) touched.AddForkVertices(*core.PropertyTouchedSnapshot(properties[i].front()));
    }
    auto const untouched = SplitForkStream(ctx, fork_scan(), touched, label_touched.get());
    return MergeOverlay(
        ctx, untouched, touched, order, [&](VertexAccessor &v) { return ReadBranchKey(v, view, properties); }, key_of);
  };
  return MakeVertexScan(ctx, view, build, key_of, epoch, order);
}

VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::PropertyId property) {
  return BranchPropertyScan(
      *accessor_,
      *branch_ctx_,
      view,
      property,
      [this, property] { return branch_ctx_->historical().Vertices(property, storage::View::OLD); },
      [this, property, view] { return accessor_->Vertices(property, view); },
      [](storage::PropertyValue const &) { return true; });
}

VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::PropertyId property,
                                            std::optional<utils::Bound<storage::PropertyValue>> const &lower,
                                            std::optional<utils::Bound<storage::PropertyValue>> const &upper) {
  auto const range = storage::PropertyValueRange::Bounded(lower, upper);
  return BranchPropertyScan(
      *accessor_,
      *branch_ctx_,
      view,
      property,
      [this, property, lower, upper] {
        return branch_ctx_->historical().Vertices(property, lower, upper, storage::View::OLD);
      },
      [this, property, lower, upper, view] { return accessor_->Vertices(property, lower, upper, view); },
      [range](storage::PropertyValue const &value) { return range.IsValueInRange(value); });
}

VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::PropertyId property,
                                            storage::PropertyValueRange const &range) {
  return BranchPropertyScan(
      *accessor_,
      *branch_ctx_,
      view,
      property,
      [this, property, range] { return branch_ctx_->historical().Vertices(property, range, storage::View::OLD); },
      [this, property, range, view] { return accessor_->Vertices(property, range, view); },
      [range](storage::PropertyValue const &value) { return BranchValueInRange(range, value); });
}

namespace {
using MaterializedEdges = std::vector<EdgeAccessor>;
using EdgeScan = BranchScan<EdgeAccessor>;
using KeyedEdge = std::pair<BranchScanPosition, EdgeAccessor>;

// Edge indexes need properties on edges, so `edge_` is the Edge pointer: main's order after the key.
BranchScanPosition EdgePosition(versioning::BranchContext &ctx, EdgeAccessor const &edge, BranchKey key) {
  return {.key = std::move(key),
          .native = !ctx.IsForkEdgeGid(edge.Gid()),
          .address = reinterpret_cast<uintptr_t>(edge.impl_.edge_.ptr)};
}

bool EdgeVisible(versioning::BranchContext &ctx, EdgeAccessor const &edge, storage::View view) {
  return ctx.IsForkEdgeGid(edge.Gid()) ? !ctx.IsForkEdgeHidden(edge.impl_, view) : edge.IsVisible(view);
}

// As MakeVertexScan, for edges.
template <typename Build, typename KeyOf, typename Epoch>
EdgesIterable MakeEdgeScan(versioning::BranchContext &ctx, storage::View view, Build build, KeyOf key_of, Epoch epoch) {
  if (view == storage::View::OLD) return EdgesIterable(std::make_shared<EdgeScan>(build()));
  auto const gid_floor = ctx.core()->main().edge_id_.load(std::memory_order_acquire);
  EdgeScan::Live live{.rescan =
                          [build, gid_floor] {
                            auto elements = build();
                            std::erase_if(elements,
                                          [&](EdgeAccessor const &e) { return e.Gid().AsUint() >= gid_floor; });
                            return elements;
                          },
                      .locate = [&ctx, key_of](EdgeAccessor &e) -> std::optional<BranchScanPosition> {
                        auto key = key_of(e);
                        if (!key) return std::nullopt;
                        return EdgePosition(ctx, e, std::move(*key));
                      },
                      .epoch = std::move(epoch),
                      .descending = false};
  return EdgesIterable(std::make_shared<EdgeScan>(build(), std::move(live)));
}

void SortByPosition(std::vector<KeyedEdge> &edges) {
  std::ranges::sort(edges, [](KeyedEdge const &a, KeyedEdge const &b) { return BranchScanBefore(a.first, b.first); });
}

MaterializedEdges FromKeyed(std::vector<KeyedEdge> &&keyed) {
  MaterializedEdges result;
  result.reserve(keyed.size());
  for (auto &entry : keyed) result.push_back(std::move(entry.second));
  return result;
}

// Every branch edge `key_of` keeps, in index order; the fallback for an edge index not visible at the fork.
template <typename KeyOf>
MaterializedEdges MaterializeEdgeScan(versioning::BranchContext &ctx, storage::View view, KeyOf &&key_of) {
  std::vector<KeyedEdge> keyed;
  ForEachBranchEdge(ctx, view, [&](storage::EdgeAccessor &raw) {
    EdgeAccessor candidate(raw, &ctx);
    if (auto key = key_of(candidate)) keyed.emplace_back(EdgePosition(ctx, candidate, std::move(*key)), candidate);
    return false;
  });
  SortByPosition(keyed);
  return FromKeyed(std::move(keyed));
}

// The edge's branch value of `property` while the scan keeps it: visible, of `edge_type` if given, non-null, in range.
auto EdgePropertyKeyOf(storage::View view, storage::PropertyId property, std::optional<storage::EdgeTypeId> edge_type,
                       storage::PropertyValueRange range) {
  return [view, property, edge_type, range](EdgeAccessor &e) -> std::optional<BranchKey> {
    if (edge_type && e.EdgeType() != *edge_type) return std::nullopt;
    auto value = e.GetProperty(view, property);
    if (!value.has_value() || value->IsNull() || !BranchValueInRange(range, *value)) return std::nullopt;
    return BranchKey{std::move(*value)};
  };
}

// Merges, in index order, main@fork's index minus edges the branch hid or whose property it wrote, those written fork
// edges re-read on the branch, and the diff engine's index (branch-native edges). O(log E + fork matches + touched).
template <typename KeyOf>
MaterializedEdges MergeEdgePropertyScan(versioning::BranchContext &ctx, storage::View view,
                                        storage::PropertyId property, storage::EdgesIterable diff_stream,
                                        storage::EdgesIterable fork_stream, KeyOf &&key_of) {
  // One snapshot up front: every transaction visible to this scan recorded its edges before this point.
  auto const touched = ctx.core()->EdgePropertyTouchedSnapshot(property);

  std::vector<KeyedEdge> fork_side;
  for (auto raw : fork_stream) {
    if (touched->contains(raw.Gid().AsUint()) || ctx.IsForkEdgeHidden(raw, view)) continue;
    auto value = raw.GetProperty(property, storage::View::OLD);
    EdgeAccessor edge(raw, &ctx);
    if (value.has_value()) fork_side.emplace_back(EdgePosition(ctx, edge, BranchKey{std::move(*value)}), edge);
  }

  std::vector<KeyedEdge> changed;
  for (auto const &[edge_gid, from_gid] : *touched) {
    auto fork = ctx.FindForkEdge(storage::Gid::FromUint(edge_gid), storage::Gid::FromUint(from_gid));
    if (!fork) continue;
    EdgeAccessor candidate(*fork, &ctx);
    if (auto key = key_of(candidate)) changed.emplace_back(EdgePosition(ctx, candidate, std::move(*key)), candidate);
  }
  SortByPosition(changed);
  auto const middle = static_cast<std::ptrdiff_t>(fork_side.size());
  fork_side.insert(fork_side.end(), std::make_move_iterator(changed.begin()), std::make_move_iterator(changed.end()));
  auto const before = [](KeyedEdge const &a, KeyedEdge const &b) { return BranchScanBefore(a.first, b.first); };
  std::inplace_merge(fork_side.begin(), fork_side.begin() + middle, fork_side.end(), before);

  std::vector<KeyedEdge> native;
  for (auto raw : diff_stream) {
    auto value = raw.GetProperty(property, view);
    EdgeAccessor edge(raw, &ctx);
    if (value.has_value()) native.emplace_back(EdgePosition(ctx, edge, BranchKey{std::move(*value)}), edge);
  }
  std::vector<KeyedEdge> merged;
  merged.reserve(native.size() + fork_side.size());
  std::ranges::merge(fork_side, native, std::back_inserter(merged), before);
  return FromKeyed(std::move(merged));
}
}  // namespace

// Unsorted (index order is by address): main@fork's index minus the edges the branch hid, then the diff engine's hits
// (branch-native edges). A fork edge never changes type and an edge created after the scan started is never yielded,
// so nothing can join a scan in flight.
EdgesIterable DbAccessor::BranchEdges(storage::View view, storage::EdgeTypeId edge_type) {
  auto &ctx = *branch_ctx_;
  auto const key_of = [&ctx, view, edge_type](EdgeAccessor &e) -> std::optional<BranchKey> {
    if (e.EdgeType() != edge_type || !EdgeVisible(ctx, e, view)) return std::nullopt;
    return BranchKey{};
  };
  auto const no_input = [] { return uint64_t{0}; };
  if (!ctx.historical().EdgeTypeIndexReady(edge_type)) {
    return MakeEdgeScan(
        ctx, view, [&ctx, view, key_of] { return MaterializeEdgeScan(ctx, view, key_of); }, key_of, no_input);
  }
  auto const build = [this, view, edge_type] {
    auto &ctx = *branch_ctx_;
    MaterializedEdges result;
    for (auto raw : ctx.historical().Edges(edge_type, storage::View::OLD)) {
      if (!ctx.IsForkEdgeHidden(raw, view)) result.emplace_back(raw, branch_ctx_);
    }
    for (auto raw : accessor_->Edges(edge_type, view)) result.emplace_back(raw, branch_ctx_);
    return result;
  };
  return MakeEdgeScan(ctx, view, build, key_of, no_input);
}

EdgesIterable DbAccessor::BranchEdges(storage::View view, storage::EdgeTypeId edge_type, storage::PropertyId property,
                                      storage::PropertyValueRange const &range) {
  auto &ctx = *branch_ctx_;
  auto const key_of = EdgePropertyKeyOf(view, property, edge_type, range);
  auto const epoch = [&ctx, property] { return ctx.ScanEpoch(ScanInput::kProperty, property.AsUint()); };
  if (!ctx.historical().EdgeTypePropertyIndexReady(edge_type, property)) {
    return MakeEdgeScan(
        ctx, view, [&ctx, view, key_of] { return MaterializeEdgeScan(ctx, view, key_of); }, key_of, epoch);
  }
  auto const build = [this, view, edge_type, property, range, key_of] {
    return MergeEdgePropertyScan(*branch_ctx_,
                                 view,
                                 property,
                                 accessor_->Edges(edge_type, property, range, view),
                                 branch_ctx_->historical().Edges(edge_type, property, range, storage::View::OLD),
                                 key_of);
  };
  return MakeEdgeScan(ctx, view, build, key_of, epoch);
}

EdgesIterable DbAccessor::BranchEdges(storage::View view, storage::PropertyId property,
                                      storage::PropertyValueRange const &range) {
  auto &ctx = *branch_ctx_;
  auto const key_of = EdgePropertyKeyOf(view, property, std::nullopt, range);
  auto const epoch = [&ctx, property] { return ctx.ScanEpoch(ScanInput::kProperty, property.AsUint()); };
  if (!ctx.historical().EdgePropertyIndexReady(property)) {
    return MakeEdgeScan(
        ctx, view, [&ctx, view, key_of] { return MaterializeEdgeScan(ctx, view, key_of); }, key_of, epoch);
  }
  auto const build = [this, view, property, range, key_of] {
    return MergeEdgePropertyScan(*branch_ctx_,
                                 view,
                                 property,
                                 accessor_->Edges(property, range, view),
                                 branch_ctx_->historical().Edges(property, range, storage::View::OLD),
                                 key_of);
  };
  return MakeEdgeScan(ctx, view, build, key_of, epoch);
}

namespace {
// The diff engine's own adjacency check sees only branch-created edges (for a fork vertex, those on its anchor), so the
// branch's full adjacency is consulted. Edges the same statement deletes (`co_deleted`) do not count.
bool BranchVertexHasUncountedEdges(versioning::BranchContext &ctx, storage::Gid vertex_gid,
                                   const std::unordered_set<storage::Gid> &co_deleted) {
  auto has_uncounted = [&](storage::EdgeDirection direction) {
    auto edges = ctx.ResolveEdges(vertex_gid, direction, storage::View::NEW);
    return std::ranges::any_of(edges, [&](auto const &e) { return !co_deleted.contains(e.Gid()); });
  };
  return has_uncounted(storage::EdgeDirection::OUT) || has_uncounted(storage::EdgeDirection::IN);
}
}  // namespace

storage::Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>>
DbAccessor::BranchDetachDelete(std::vector<VertexAccessor> nodes, std::vector<EdgeAccessor> edges, bool detach) {
  using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;
  auto &ctx = *branch_ctx_;
  constexpr auto kView = storage::View::NEW;

  // Already deleted (by this statement or an earlier one): a no-op, as on main.
  std::erase_if(nodes, [&](const VertexAccessor &node) {
    return ctx.IsForkVertexGid(node.Gid()) ? ctx.IsVertexDeleted(node.Gid(), kView)
                                           : !accessor_->FindVertex(node.Gid(), kView).has_value();
  });
  std::erase_if(edges, [&](const EdgeAccessor &edge) {
    return ctx.IsForkEdgeGid(edge.Gid()) ? ctx.IsForkEdgeHidden(edge.impl_, kView)
                                         : !ctx.FindDiffEdge(edge.Gid(), edge.impl_.from_vertex_->gid, kView);
  });

  // A plain DELETE fails like main's while a visible edge remains; checked before any write.
  if (!detach) {
    std::unordered_set<storage::Gid> co_deleted;
    for (const auto &edge : edges) co_deleted.insert(edge.Gid());
    for (const auto &node : nodes) {
      if (BranchVertexHasUncountedEdges(ctx, node.Gid(), co_deleted)) {
        return std::unexpected{storage::Error::VERTEX_HAS_EDGES};
      }
    }
  }

  std::vector<VertexAccessor> deleted_vertices;
  std::vector<EdgeAccessor> deleted_edges;
  std::vector<EdgeAccessor> diff_edges;  // handed to the diff engine's delete
  std::unordered_set<storage::Gid> seen_edges;

  for (auto &edge : edges) {
    if (!seen_edges.insert(edge.Gid()).second) continue;
    if (!ctx.IsForkEdgeGid(edge.Gid())) {
      diff_edges.push_back(edge);
      continue;
    }
    auto deleted = ctx.DeleteForkEdge(edge.impl_);
    if (!deleted) return std::unexpected{deleted.error()};
    if (*deleted) deleted_edges.push_back(edge);
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
    // Every incident edge is listed before any delete flag is written, so an edge between two deleted vertices is seen
    // from both ends and counted once.
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
    if (*marked) deleted_vertices.push_back(*node);
  }

  return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
}

storage::Result<EdgeAccessor> DbAccessor::BranchInsertEdge(const VertexAccessor &from, const VertexAccessor &to,
                                                           storage::EdgeTypeId edge_type) {
  auto &ctx = *branch_ctx_;
  auto from_diff = ctx.CowVertex(from.Gid());
  if (!from_diff) throw QueryRuntimeException(from_diff.error().message);
  auto to_diff = ctx.CowVertex(to.Gid());
  if (!to_diff) throw QueryRuntimeException(to_diff.error().message);

  auto edge = ctx.CreateNativeEdge(&*from_diff, &*to_diff, edge_type);
  if (!edge) return std::unexpected{edge.error()};
  // Turns off both endpoints' unchanged-adjacency fast path.
  ctx.RecordEdgeChange(from.Gid());
  ctx.RecordEdgeChange(to.Gid());
  return EdgeAccessor(*edge, branch_ctx_);
}

#endif
bool DbAccessor::TextIndexExists(const std::string &index_name) const { return accessor_->TextIndexExists(index_name); }

std::vector<storage::TextSearchResult> DbAccessor::TextIndexSearch(const std::string &index_name,
                                                                   const std::string &search_query,
                                                                   text_search_mode search_mode,
                                                                   const storage::TextSearchConfig &config) const {
  return accessor_->TextIndexSearch(index_name, search_query, search_mode, config);
}

std::string DbAccessor::TextIndexAggregate(const std::string &index_name, const std::string &search_query,
                                           const std::string &aggregation_query) const {
  return accessor_->TextIndexAggregate(index_name, search_query, aggregation_query);
}

std::string DbAccessor::TextEdgeIndexAggregate(const std::string &index_name, const std::string &search_query,
                                               const std::string &aggregation_query) {
  return accessor_->TextEdgeIndexAggregate(index_name, search_query, aggregation_query);
}

std::vector<storage::TextEdgeSearchResult> DbAccessor::SearchEdgeTextIndex(
    const std::string &index_name, const std::string &search_query, text_search_mode search_mode,
    const storage::TextSearchConfig &config) const {
  return accessor_->SearchEdgeTextIndex(index_name, search_query, search_mode, config);
}

bool DbAccessor::PointIndexExists(storage::LabelId label, storage::PropertyId prop) const {
  return accessor_->PointIndexExists(label, prop);
}

std::vector<std::tuple<storage::VertexAccessor, double, double>> DbAccessor::VectorIndexSearchOnNodes(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  return accessor_->VectorIndexSearchOnNodes(index_name, number_of_results, vector);
}

std::vector<std::tuple<storage::EdgeAccessor, double, double>> DbAccessor::VectorIndexSearchOnEdges(
    const std::string &index_name, uint64_t number_of_results, const std::vector<float> &vector) {
  return accessor_->VectorIndexSearchOnEdges(index_name, number_of_results, vector);
}

std::vector<storage::VectorIndexInfo> DbAccessor::ListAllVectorIndices() const {
  return accessor_->ListAllVectorIndices();
}

std::vector<storage::VectorEdgeIndexInfo> DbAccessor::ListAllVectorEdgeIndices() const {
  return accessor_->ListAllVectorEdgeIndices();
}

auto DbAccessor::PointVertices(storage::LabelId label, storage::PropertyId property,
                               storage::CoordinateReferenceSystem crs, TypedValue const &point_value,
                               TypedValue const &boundary_value, plan::PointDistanceCondition condition)
    -> PointIterable {
  return PointIterable(accessor_->PointVertices(label,
                                                property,
                                                crs,
                                                point_value.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                boundary_value.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                condition));
}

auto DbAccessor::PointVertices(storage::LabelId label, storage::PropertyId property,
                               storage::CoordinateReferenceSystem crs, TypedValue const &bottom_left,
                               TypedValue const &top_right, plan::WithinBBoxCondition condition) -> PointIterable {
  return PointIterable(accessor_->PointVertices(label,
                                                property,
                                                crs,
                                                bottom_left.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                top_right.ToPropertyValue(accessor_->GetNameIdMapper()),
                                                condition));
}
}  // namespace memgraph::query
