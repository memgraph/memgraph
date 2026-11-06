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
#include <span>
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

// Branch storage adjacency holds stub-attached natives and delete markers, so expand via the branch-aware accessor and
// filter membership only: by gid and transaction, which match since a fork edge is always the historical txn's.
auto KeptInBranch(Graph const *graph) {
  return [graph](ExpandedEdgesResult &&result) -> ExpandedEdgesResult {
    auto const &graph_edges = graph->edges();
    std::vector<storage::EdgeAccessor> kept;
    for (auto const &edge : result.edges) {
      if (graph_edges.contains(edge)) kept.push_back(edge.impl_);
    }
    return ExpandedEdgesResult{.edges = ExpandedEdges{storage::ExpandedEdges{std::move(kept)}},
                               .expanded_count = result.expanded_count};
  };
}
#endif

}  // namespace

storage::Result<ExpandedEdgesResult> SubgraphVertexAccessor::OutEdges(storage::View view) const {
#ifdef MG_ENTERPRISE
  if (impl_.branch_ctx() != nullptr) [[unlikely]]
    return impl_.OutEdges(view).transform(KeptInBranch(graph_));
#endif
  return impl_.impl_.OutEdges(view, {}).transform(KeptIn(graph_));
}

storage::Result<ExpandedEdgesResult> SubgraphVertexAccessor::InEdges(storage::View view) const {
#ifdef MG_ENTERPRISE
  if (impl_.branch_ctx() != nullptr) [[unlikely]]
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

std::optional<EdgeAccessor> DbAccessor::BranchFindEdge(storage::Gid gid, storage::View view) {
  if (auto edge = branch_ctx_->ResolveEdge(gid, view)) return EdgeAccessor(*edge, branch_ctx_);
  return std::nullopt;
}

std::optional<EdgeAccessor> DbAccessor::BranchFindEdge(storage::Gid edge_gid, storage::Gid from_vertex_gid,
                                                       storage::View view) {
  if (auto edge = branch_ctx_->ResolveEdge(edge_gid, from_vertex_gid, view)) return EdgeAccessor(*edge, branch_ctx_);
  return std::nullopt;
}

VerticesIterable DbAccessor::BranchVertices(storage::View view) {
  // No deletes and an empty diff skiplist (its size counts uncommitted and un-GC'd vertices): the union is exactly
  // main@fork's stream. OLD only: a NEW scan must see deletes this query makes mid-iteration, checked per vertex.
  if (view == storage::View::OLD && !branch_ctx_->MayHaveAnyVertexDeleted() &&
      accessor_->ApproximateVertexCount() == 0) {
    return VerticesIterable(branch_ctx_->historical().Vertices(storage::View::OLD));
  }
  return VerticesIterable(branch_ctx_->Vertices(view));
}

namespace {
using VertexScan = BranchScan<VertexAccessor>;
// A vertex's index key: one value per indexed property path (empty for a label scan).
using BranchKey = std::vector<storage::PropertyValue>;

bool VertexHasLabel(VertexAccessor &vertex, storage::View view, storage::LabelId label) {
  auto has_label = vertex.HasLabel(view, label);
  return has_label.has_value() && *has_label;
}

// Only View::NEW checks the elements again as the scan steps (see BranchScan).
VerticesIterable YieldScan(storage::View view, std::vector<VertexAccessor> elements,
                           VertexScan::StillMatches still_matches) {
  if (view != storage::View::NEW) still_matches = {};
  return VerticesIterable(std::make_shared<VertexScan>(std::move(elements), std::move(still_matches)));
}

// Full scan, O(V log V): the fallback when the index is not ready on main@fork or the diff engine. `key_of` owns what
// it uses, as the scan keeps a copy. Matches come back in key order (the planner elides ORDER BY over an index), ties
// in gid order.
template <typename KeyOf>
VerticesIterable FallbackScan(versioning::BranchContext &ctx, storage::View view, KeyOf key_of,
                              storage::IndexOrder order = storage::IndexOrder::ASC) {
  std::vector<std::pair<BranchKey, VertexAccessor>> keyed;
  for (auto raw : ctx.Vertices(view)) {
    VertexAccessor candidate(raw, &ctx);
    if (auto key = key_of(candidate)) keyed.emplace_back(std::move(*key), candidate);
  }
  bool const asc = order == storage::IndexOrder::ASC;
  std::ranges::stable_sort(keyed,
                           [asc](auto const &a, auto const &b) { return asc ? a.first < b.first : b.first < a.first; });
  std::vector<VertexAccessor> result;
  result.reserve(keyed.size());
  for (auto &entry : keyed) result.push_back(entry.second);
  return YieldScan(view, std::move(result), [key_of](VertexAccessor &v) { return key_of(v).has_value(); });
}

// A key from `get_property` (a vertex's value of a property, or an error), one value per indexed path. A missing part
// (top-level or nested) is null, as in main's index; none only if a read fails (the vertex is deleted on the branch).
template <typename GetProperty>
std::optional<BranchKey> ReadKeyWith(GetProperty &&get_property, std::span<storage::PropertyPath const> properties) {
  BranchKey key;
  key.reserve(properties.size());
  for (auto const &path : properties) {
    auto top = get_property(path.front());
    if (!top.has_value()) return std::nullopt;
    if (path.size() == 1) {
      key.push_back(std::move(*top));
      continue;
    }
    auto const *nested = storage::ReadNestedPropertyValue(*top, path.as_span().subspan(1));
    key.push_back(nested == nullptr ? storage::PropertyValue() : *nested);
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

// As the storage index: the ranges constrain a leading prefix of the key, each with its value predicate; an all-null
// key is never indexed.
bool BranchKeyInRange(BranchKey const &key, std::span<storage::PropertyValueRange const> property_ranges) {
  for (size_t i = 0; i < property_ranges.size() && i < key.size(); ++i) {
    if (!BranchValueInRange(property_ranges[i], key[i])) return false;
  }
  return !std::ranges::all_of(key, [](auto const &value) { return value.IsNull(); });
}

// An index scan on a branch: F, main@fork's index (View::OLD), merged lazily with D, the diff engine's index
// (branch-native objects, plus stubs of fork objects whose key the branch changed). One owner per object, by its key
// K_b on the branch and K_f at the fork: F yields it iff it matches and K_b == K_f; D yields a native hit iff it
// matches, a stub hit iff it matches and K_b != K_f. A keyed scan merges by (key, direction), F first on ties; a
// keyless one (label) yields F, then D. View::NEW instead reads D in full on open and F skips its gids, then re-checks
// D rows and branch-written F rows as it steps. Under READ_COMMITTED a commit moving a key mid-scan can yield an object
// twice or not at all, as main's index does.
template <typename Q, typename Raw, typename Iterable>
class IndexStream final : public BranchStream<Q> {
 public:
  struct Spec {
    storage::View view;
    bool keyed;
    storage::IndexOrder order;
    // The key of `q` on the branch at `view` if it matches the scan, else none. Owns what it uses.
    std::function<std::optional<BranchKey>(Q &)> branch_key;
    // The key of a fork object at the fork (not range-checked), none if it has no key there.
    std::function<std::optional<BranchKey>(Raw const &)> fork_key;
    // Whether the branch may have changed a fork row's match or key (a filter probe: false is exact, true is not).
    std::function<bool(Raw const &)> may_have_changed;
    // Whether a fork row may have been written by the branch since the scan opened (View::NEW re-checks such rows).
    std::function<bool(Raw const &)> written;
    // Whether `gid` can name a fork object; `find_fork` is the fork object a D hit stands for (its stub), if any.
    std::function<bool(storage::Gid)> is_fork_candidate;
    std::function<std::optional<Raw>(Raw const &)> find_fork;
  };

  IndexStream(versioning::BranchContext &ctx, Spec spec, Iterable fork, Iterable diff)
      : ctx_(&ctx),
        spec_(std::move(spec)),
        fork_(std::move(fork)),
        diff_(std::move(diff)),
        f_it_(fork_.begin()),
        f_end_(fork_.end()),
        d_it_(diff_.begin()),
        d_end_(diff_.end()) {
    if (spec_.view == storage::View::NEW) CollectDiffHits();
  }

  std::optional<Q> Next() override {
    for (;;) {
      if (!f_head_ && !f_done_) PullFork();
      if (!d_head_ && !d_done_ && (spec_.keyed || f_done_)) PullDiff();
      bool take_diff = false;
      if (!f_head_ && !d_head_) return std::nullopt;
      if (!f_head_) {
        take_diff = true;
      } else if (d_head_ && spec_.keyed) {
        take_diff = DiffBeforeFork();
      }
      auto &head = take_diff ? d_head_ : f_head_;
      auto item = std::move(head->item);
      auto const fork_row = head->fork.has_value() && spec_.written(*head->fork);
      head.reset();
      // Only a row the branch wrote can have changed since the scan opened.
      if (spec_.view == storage::View::NEW && (take_diff || fork_row) && !spec_.branch_key(item)) continue;
      return item;
    }
  }

  uint64_t SourceRows() const override { return source_rows_; }

 private:
  struct Head {
    Q item;
    std::optional<Raw> fork;  // set for a row of F
    std::optional<BranchKey> key;
  };

  bool ForkRowOwned(Raw const &raw) {
    if (spec_.view == storage::View::NEW) return !diff_owned_.contains(raw.Gid());
    if (!spec_.may_have_changed(raw)) return true;
    Q candidate(raw, ctx_);
    auto const branch_key = spec_.branch_key(candidate);
    if (!branch_key) return false;
    auto const fork_key = spec_.fork_key(raw);
    return fork_key && *fork_key == *branch_key;
  }

  std::optional<Head> DiffHit(Raw const &raw) {
    if (!spec_.is_fork_candidate(raw.Gid())) {
      Q native(raw, ctx_);
      auto key = spec_.branch_key(native);
      if (!key) return std::nullopt;
      return Head{std::move(native), std::nullopt, std::move(key)};
    }
    auto fork = spec_.find_fork(raw);
    if (!fork) return std::nullopt;
    Q candidate(*fork, ctx_);
    auto key = spec_.branch_key(candidate);
    if (!key) return std::nullopt;
    if (auto const fork_key = spec_.fork_key(*fork); fork_key && *fork_key == *key) return std::nullopt;
    return Head{std::move(candidate), std::nullopt, std::move(key)};
  }

  void PullFork() {
    while (f_it_ != f_end_) {
      Raw const raw = *f_it_;
      ++f_it_;
      ++source_rows_;
      if (!ForkRowOwned(raw)) continue;
      f_head_.emplace(Head{Q(raw, ctx_), raw, std::nullopt});
      return;
    }
    f_done_ = true;
  }

  void PullDiff() {
    if (spec_.view == storage::View::NEW) {
      if (d_pos_ < d_hits_.size()) {
        d_head_ = std::move(d_hits_[d_pos_++]);
      } else {
        d_done_ = true;
      }
      return;
    }
    while (d_it_ != d_end_) {
      Raw const raw = *d_it_;
      ++d_it_;
      ++source_rows_;
      if (auto hit = DiffHit(raw)) {
        d_head_ = std::move(hit);
        return;
      }
    }
    d_done_ = true;
  }

  void CollectDiffHits() {
    for (; d_it_ != d_end_; ++d_it_) {
      ++source_rows_;
      auto hit = DiffHit(*d_it_);
      if (!hit) continue;
      if (spec_.is_fork_candidate(hit->item.Gid())) diff_owned_.insert(hit->item.Gid());
      d_hits_.push_back(std::move(*hit));
    }
  }

  bool DiffBeforeFork() {
    if (!f_head_->key) f_head_->key = spec_.fork_key(*f_head_->fork).value_or(BranchKey{});
    return spec_.order == storage::IndexOrder::ASC ? *d_head_->key < *f_head_->key : *f_head_->key < *d_head_->key;
  }

  versioning::BranchContext *ctx_;
  Spec spec_;
  // Their iterators point into them, so they are never moved.
  Iterable fork_;
  Iterable diff_;
  Iterable::Iterator f_it_;
  Iterable::Iterator f_end_;
  Iterable::Iterator d_it_;
  Iterable::Iterator d_end_;
  std::optional<Head> f_head_;
  std::optional<Head> d_head_;
  bool f_done_{false};
  bool d_done_{false};
  // View::NEW: D's hits at the scan's start, and the fork gids among them.
  std::vector<Head> d_hits_;
  size_t d_pos_{0};
  std::unordered_set<storage::Gid> diff_owned_;
  uint64_t source_rows_{0};
};

using VertexIndexStream = IndexStream<VertexAccessor, storage::VertexAccessor, storage::VerticesIterable>;

// The vertex scan's hooks: `label_in_key` says whether a label's membership is part of the key, `key_properties` are
// the root properties of the key's parts.
VertexIndexStream::Spec VertexStreamSpec(
    versioning::BranchContext &ctx, storage::View view, bool keyed, storage::IndexOrder order, bool label_in_key,
    std::vector<storage::PropertyId> key_properties,
    std::function<std::optional<BranchKey>(VertexAccessor &)> branch_key,
    std::function<std::optional<BranchKey>(storage::VertexAccessor const &)> fork_key) {
  auto *const core = std::addressof(*ctx.core());
  return {.view = view,
          .keyed = keyed,
          .order = order,
          .branch_key = std::move(branch_key),
          .fork_key = std::move(fork_key),
          .may_have_changed =
              [core, label_in_key, key_properties = std::move(key_properties)](storage::VertexAccessor const &raw) {
                if (!raw.vertex_->branched()) return false;
                auto const gid = raw.Gid();
                return core->MayHaveVertexDeleted(gid) || (label_in_key && core->MayHaveLabelChange(gid)) ||
                       std::ranges::any_of(key_properties,
                                           [&](auto key) { return core->MayHavePropertyFieldChange(gid, key); });
              },
          .written = [](storage::VertexAccessor const &raw) { return raw.vertex_->branched(); },
          .is_fork_candidate = [&ctx](storage::Gid gid) { return ctx.IsForkCandidateVertex(gid); },
          .find_fork = [&ctx](storage::VertexAccessor const &raw) { return ctx.FindForkVertex(raw.Gid()); }};
}

VerticesIterable StreamScan(versioning::BranchContext &ctx, VertexIndexStream::Spec spec,
                            storage::VerticesIterable fork, storage::VerticesIterable diff) {
  return VerticesIterable(std::make_shared<VertexScan>(
      std::make_unique<VertexIndexStream>(ctx, std::move(spec), std::move(fork), std::move(diff))));
}

// Nothing relevant changed on the branch (the caller checked) and the diff index has no hit: main@fork's index stream,
// lazily. View::OLD only: the stream is never re-checked, and a NEW scan sees the running statement's writes.
// `index_size()` is the diff index's skiplist size, which counts uncommitted and un-GC'd entries too, so 0 means no hit
// for anyone.
template <typename IndexSize, typename DiffScan, typename ForkScan>
std::optional<VerticesIterable> LazyForkOnlyScan(storage::Storage::Accessor &diff, IndexSize &&index_size,
                                                 DiffScan &&diff_scan, ForkScan &&fork_scan) {
  if (diff.ApproximateVertexCount() != 0 && index_size() != 0) {
    auto hits = diff_scan();
    if (hits.begin() != hits.end()) return std::nullopt;
  }
  return VerticesIterable(fork_scan());
}

// Global vertex-property scan, ascending by value.
template <typename ForkScan, typename DiffScan, typename InRange>
VerticesIterable BranchPropertyScan(storage::Storage::Accessor &diff, versioning::BranchContext &ctx,
                                    storage::View view, storage::PropertyId property, ForkScan &&fork_scan,
                                    DiffScan &&diff_scan, InRange in_range) {
  auto read_key = [view, property, in_range](VertexAccessor &v) -> std::optional<BranchKey> {
    auto value = v.GetProperty(view, property);
    if (!value.has_value() || value->IsNull() || !in_range(*value)) return std::nullopt;
    return BranchKey{std::move(*value)};
  };
  if (!ctx.historical().VertexPropertyIndexReady(property) || !diff.VertexPropertyIndexReady(property)) {
    return FallbackScan(ctx, view, read_key);
  }
  if (view == storage::View::OLD && !ctx.core()->MayHavePropertyTouched(property) && !ctx.MayHaveAnyVertexDeleted()) {
    auto const index_size = [&] { return diff.ApproximateVertexCount(property); };
    if (auto lazy = LazyForkOnlyScan(diff, index_size, diff_scan, fork_scan)) return std::move(*lazy);
  }
  auto fork_key = [property](storage::VertexAccessor const &v) -> std::optional<BranchKey> {
    auto value = v.GetProperty(property, storage::View::OLD);
    if (!value.has_value() || value->IsNull()) return std::nullopt;
    return BranchKey{std::move(*value)};
  };
  return StreamScan(
      ctx,
      VertexStreamSpec(
          ctx, view, true, storage::IndexOrder::ASC, false, {property}, std::move(read_key), std::move(fork_key)),
      fork_scan(),
      diff_scan());
}
}  // namespace

// Unsorted: main@fork's rows, then the diff engine's (see VertexIndexStream).
VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::LabelId label) {
  auto &ctx = *branch_ctx_;
  auto &historical = ctx.historical();
  auto has_label = [view, label](VertexAccessor &v) -> std::optional<BranchKey> {
    if (!VertexHasLabel(v, view, label)) return std::nullopt;
    return BranchKey{};
  };
  if (!historical.LabelIndexReady(label) || !accessor_->LabelIndexReady(label)) {
    return FallbackScan(ctx, view, has_label);
  }

  auto const fork_scan = [&] { return historical.Vertices(label, storage::View::OLD); };
  auto const diff_scan = [&] { return accessor_->Vertices(label, view); };

  if (view == storage::View::OLD && !ctx.core()->MayHaveLabelTouched(label) && !ctx.MayHaveAnyVertexDeleted()) {
    auto const index_size = [&] { return accessor_->ApproximateVertexCount(label); };
    if (auto lazy = LazyForkOnlyScan(*accessor_, index_size, diff_scan, fork_scan)) return std::move(*lazy);
  }

  auto fork_has_label = [label](storage::VertexAccessor const &v) -> std::optional<BranchKey> {
    auto has = v.HasLabel(label, storage::View::OLD);
    if (!has.has_value() || !*has) return std::nullopt;
    return BranchKey{};
  };
  return StreamScan(
      ctx,
      VertexStreamSpec(
          ctx, view, false, storage::IndexOrder::ASC, true, {}, std::move(has_label), std::move(fork_has_label)),
      fork_scan(),
      diff_scan());
}

VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::LabelId label,
                                            std::span<storage::PropertyPath const> properties,
                                            std::span<storage::PropertyValueRange const> property_ranges,
                                            storage::IndexOrder order) {
  auto &ctx = *branch_ctx_;
  auto &core = *ctx.core();
  auto &historical = ctx.historical();
  auto match_key = [view,
                    label,
                    owned_properties = std::vector<storage::PropertyPath>(properties.begin(), properties.end()),
                    owned_ranges =
                        std::vector<storage::PropertyValueRange>(property_ranges.begin(), property_ranges.end())](
                       VertexAccessor &v) -> std::optional<BranchKey> {
    if (!VertexHasLabel(v, view, label)) return std::nullopt;
    auto key = ReadKeyWith([&](storage::PropertyId id) { return v.GetProperty(view, id); }, owned_properties);
    if (!key || !BranchKeyInRange(*key, owned_ranges)) return std::nullopt;
    return key;
  };
  if (!historical.LabelPropertyIndexReady(label, properties) ||
      !accessor_->LabelPropertyIndexReady(label, properties)) {
    return FallbackScan(ctx, view, match_key, order);
  }

  auto const index_size = [&] { return accessor_->ApproximateVertexCount(label, properties); };
  auto const diff_scan = [&] { return accessor_->Vertices(label, properties, property_ranges, view, order); };
  auto const fork_scan = [&] {
    return historical.Vertices(label, properties, property_ranges, storage::View::OLD, order);
  };

  std::vector<storage::PropertyId> key_properties;
  key_properties.reserve(properties.size());
  for (auto const &path : properties) key_properties.push_back(path.front());

  bool maybe_touched = core.MayHaveLabelTouched(label) || ctx.MayHaveAnyVertexDeleted();
  for (size_t i = 0; !maybe_touched && i < key_properties.size(); ++i) {
    maybe_touched = core.MayHavePropertyTouched(key_properties[i]);
  }
  if (view == storage::View::OLD && !maybe_touched) {
    if (auto lazy = LazyForkOnlyScan(*accessor_, index_size, diff_scan, fork_scan)) return std::move(*lazy);
  }

  auto fork_key = [label, owned_properties = std::vector<storage::PropertyPath>(properties.begin(), properties.end())](
                      storage::VertexAccessor const &v) -> std::optional<BranchKey> {
    auto has = v.HasLabel(label, storage::View::OLD);
    if (!has.has_value() || !*has) return std::nullopt;
    return ReadKeyWith([&](storage::PropertyId id) { return v.GetProperty(id, storage::View::OLD); }, owned_properties);
  };
  return StreamScan(
      ctx,
      VertexStreamSpec(
          ctx, view, true, order, true, std::move(key_properties), std::move(match_key), std::move(fork_key)),
      fork_scan(),
      diff_scan());
}

VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::PropertyId property) {
  return BranchPropertyScan(
      *accessor_,
      *branch_ctx_,
      view,
      property,
      [&] { return branch_ctx_->historical().Vertices(property, storage::View::OLD); },
      [&] { return accessor_->Vertices(property, view); },
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
      [&] { return branch_ctx_->historical().Vertices(property, lower, upper, storage::View::OLD); },
      [&] { return accessor_->Vertices(property, lower, upper, view); },
      [range](storage::PropertyValue const &value) { return range.IsValueInRange(value); });
}

VerticesIterable DbAccessor::BranchVertices(storage::View view, storage::PropertyId property,
                                            storage::PropertyValueRange const &range) {
  return BranchPropertyScan(
      *accessor_,
      *branch_ctx_,
      view,
      property,
      [&] { return branch_ctx_->historical().Vertices(property, range, storage::View::OLD); },
      [&] { return accessor_->Vertices(property, range, view); },
      [range](storage::PropertyValue const &value) { return BranchValueInRange(range, value); });
}

storage::Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>>
DbAccessor::BranchDetachDelete(std::vector<VertexAccessor> nodes, std::vector<EdgeAccessor> edges, bool detach) {
  using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;
  auto &ctx = *branch_ctx_;
  constexpr auto kView = storage::View::NEW;
  auto const is_native = [&](const auto &object) { return object.impl_.storage_ == &ctx.diff_engine(); };

  // A vertex already deleted (by this statement or an earlier one) is a no-op, as on main.
  std::erase_if(nodes, [&](const VertexAccessor &node) {
    return is_native(node) ? !accessor_->FindVertex(node.Gid(), kView).has_value()
                           : ctx.IsForkVertexDeleted(node.impl_, kView);
  });

  // Dedups the explicit edges and seeds seen_edges, so a deleted vertex does not take them again below.
  std::unordered_set<storage::Gid> seen_edges;
  std::erase_if(edges, [&](const EdgeAccessor &edge) { return !seen_edges.insert(edge.Gid()).second; });

  // The edges each fork vertex loses with it, listed before any write: a plain DELETE fails like main's while a visible
  // edge is left, and an edge between two deleted vertices is taken by the first one.
  struct ForkNode {
    VertexAccessor *node;
    std::vector<EdgeAccessor> fork_edges;
  };

  std::vector<ForkNode> fork_nodes;
  std::vector<storage::EdgeAccessor> native_edges;
  std::vector<storage::VertexAccessor *> native_nodes;
  for (auto &edge : edges) {
    if (is_native(edge)) native_edges.push_back(edge.impl_);
  }
  for (auto &node : nodes) {
    if (is_native(node)) {
      native_nodes.push_back(&node.impl_);
      continue;
    }
    auto &fork_node = fork_nodes.emplace_back(&node);
    for (auto const direction : {storage::EdgeDirection::OUT, storage::EdgeDirection::IN}) {
      auto incident =
          direction == storage::EdgeDirection::OUT ? ctx.OutEdges(node.impl_, kView) : ctx.InEdges(node.impl_, kView);
      if (!incident) return std::unexpected{incident.error()};
      for (auto &raw : *incident) {
        if (!seen_edges.insert(raw.Gid()).second) continue;
        if (!detach) return std::unexpected{storage::Error::VERTEX_HAS_EDGES};
        if (raw.storage_ == &ctx.diff_engine()) {
          native_edges.push_back(raw);
        } else {
          fork_node.fork_edges.emplace_back(raw, branch_ctx_);
        }
      }
    }
  }

  std::vector<VertexAccessor> deleted_vertices;
  std::vector<EdgeAccessor> deleted_edges;

  for (auto &edge : edges) {
    if (is_native(edge)) continue;
    auto deleted = ctx.DeleteForkEdge(edge.impl_);
    if (!deleted) return std::unexpected{deleted.error()};
    if (*deleted) deleted_edges.push_back(edge);
  }

  if (!native_nodes.empty() || !native_edges.empty()) {
    std::vector<storage::EdgeAccessor *> native_edge_ptrs;
    native_edge_ptrs.reserve(native_edges.size());
    for (auto &edge : native_edges) native_edge_ptrs.push_back(&edge);
    auto res = accessor_->DetachDelete(std::move(native_nodes), std::move(native_edge_ptrs), detach);
    if (!res) return std::unexpected{res.error()};
    if (*res) {
      for (const auto &v : (*res)->first) deleted_vertices.emplace_back(v, branch_ctx_);
      for (const auto &e : (*res)->second) deleted_edges.emplace_back(e, branch_ctx_);
    }
  }

  for (auto &[node, fork_edges] : fork_nodes) {
    auto deleted = ctx.DeleteForkVertex(node->Gid());
    if (!deleted) return std::unexpected{deleted.error()};
    if (!*deleted) continue;
    deleted_vertices.push_back(*node);
    std::ranges::move(fork_edges, std::back_inserter(deleted_edges));
  }

  return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
}

storage::Result<EdgeAccessor> DbAccessor::BranchInsertEdge(const VertexAccessor &from, const VertexAccessor &to,
                                                           storage::EdgeTypeId edge_type) {
  auto &ctx = *branch_ctx_;
  // BranchWriteError's message is dropped on purpose: DELETED_OBJECT makes CreateEdge emit main's own text.
  auto from_diff = ctx.EnsureDiffVertex(from.Gid());
  if (!from_diff) return std::unexpected{storage::Error::DELETED_OBJECT};
  auto to_diff = ctx.EnsureDiffVertex(to.Gid());
  if (!to_diff) return std::unexpected{storage::Error::DELETED_OBJECT};

  auto edge = ctx.CreateNativeEdge(*from_diff, *to_diff, edge_type);
  if (!edge) return std::unexpected{edge.error()};
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
