// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "spdlog/spdlog.h"

#include "storage/v2/disk/name_id_mapper.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_histogram.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/stat.hpp"
#include "utils/timer.hpp"
#include "utils/typeinfo.hpp"
#include "utils/uuid.hpp"

namespace memgraph::metrics {
extern const Event SnapshotCreationLatency_us;

extern const Event ActiveLabelIndices;
extern const Event ActiveLabelPropertyIndices;
}  // namespace memgraph::metrics

namespace memgraph::storage {

class InMemoryStorage;

using OOMExceptionEnabler = utils::MemoryTracker::OutOfMemoryExceptionEnabler;

Storage::Storage(Config config, StorageMode storage_mode)
    : name_id_mapper_(std::invoke([config, storage_mode]() -> std::unique_ptr<NameIdMapper> {
        if (storage_mode == StorageMode::ON_DISK_TRANSACTIONAL) {
          return std::make_unique<DiskNameIdMapper>(config.disk.name_id_mapper_directory,
                                                    config.disk.id_name_mapper_directory);
        }
        return std::make_unique<NameIdMapper>();
      })),
      config_(config),
      isolation_level_(config.transaction.isolation_level),
      storage_mode_(storage_mode),
      indices_(&constraints_, config, storage_mode),
      constraints_(config, storage_mode),
      id_(config.name),
      replication_state_(config_.durability.restore_replication_state_on_startup,
                         config_.durability.storage_directory) {}

Storage::Accessor::Accessor(Storage *storage, IsolationLevel isolation_level, StorageMode storage_mode)
    : storage_(storage),
      // The lock must be acquired before creating the transaction object to
      // prevent freshly created transactions from dangling in an active state
      // during exclusive operations.
      storage_guard_(storage_->main_lock_),
      transaction_(storage->CreateTransaction(isolation_level, storage_mode)),
      is_transaction_active_(true),
      creation_storage_mode_(storage_mode) {}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      storage_guard_(std::move(other.storage_guard_)),
      transaction_(std::move(other.transaction_)),
      commit_timestamp_(other.commit_timestamp_),
      is_transaction_active_(other.is_transaction_active_),
      creation_storage_mode_(other.creation_storage_mode_) {
  // Don't allow the other accessor to abort our transaction in destructor.
  other.is_transaction_active_ = false;
  other.commit_timestamp_.reset();
}

IndicesInfo Storage::ListAllIndices() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {indices_.label_index_->ListIndices(), indices_.label_property_index_->ListIndices()};
}

ConstraintsInfo Storage::ListAllConstraints() const {
  std::shared_lock<utils::RWLock> storage_guard_(main_lock_);
  return {constraints_.existence_constraints_->ListConstraints(), constraints_.unique_constraints_->ListConstraints()};
}

/// Main lock is taken by the caller.
void Storage::SetStorageMode(StorageMode storage_mode) {
  std::unique_lock main_guard{main_lock_};
  MG_ASSERT(
      (storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL || storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL) &&
      (storage_mode == StorageMode::IN_MEMORY_ANALYTICAL || storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL));
  if (storage_mode_ != storage_mode) {
    storage_mode_ = storage_mode;
    FreeMemory(std::move(main_guard));
  }
}

IsolationLevel Storage::GetIsolationLevel() const noexcept { return isolation_level_; }

StorageMode Storage::GetStorageMode() const { return storage_mode_; }

utils::BasicResult<Storage::SetIsolationLevelError> Storage::SetIsolationLevel(IsolationLevel isolation_level) {
  std::unique_lock main_guard{main_lock_};
  if (storage_mode_ == storage::StorageMode::IN_MEMORY_ANALYTICAL) {
    return Storage::SetIsolationLevelError::DisabledForAnalyticalMode;
  }

  isolation_level_ = isolation_level;
  return {};
}

StorageMode Storage::Accessor::GetCreationStorageMode() const { return creation_storage_mode_; }

std::optional<uint64_t> Storage::Accessor::GetTransactionId() const {
  if (is_transaction_active_) {
    return transaction_.transaction_id.load(std::memory_order_acquire);
  }
  return {};
}

void Storage::Accessor::AdvanceCommand() {
  transaction_.manyDeltasCache.Clear();  // TODO: Just invalidate the View::OLD cache, NEW should still be fine
  ++transaction_.command_id;
}

Result<std::optional<VertexAccessor>> Storage::Accessor::DeleteVertex(VertexAccessor *vertex) {
  auto res = DetachDelete({vertex}, {}, false);

  if (res.HasError()) {
    return res.GetError();
  }

  const auto &value = res.GetValue();
  if (!value) {
    return std::optional<VertexAccessor>{};
  }

  const auto &[vertices, edges] = *value;

  MG_ASSERT(vertices.size() <= 1, "The number of deleted vertices is not less or equal to 1!");
  MG_ASSERT(edges.empty(), "Deleting a vertex without detaching should not have resulted in deleting any edges!");

  if (vertices.empty()) {
    return std::optional<VertexAccessor>{};
  }

  return std::make_optional<VertexAccessor>(vertices[0]);
}

Result<std::optional<std::pair<VertexAccessor, std::vector<EdgeAccessor>>>> Storage::Accessor::DetachDeleteVertex(
    VertexAccessor *vertex) {
  using ReturnType = std::pair<VertexAccessor, std::vector<EdgeAccessor>>;

  auto res = DetachDelete({vertex}, {}, true);

  if (res.HasError()) {
    return res.GetError();
  }

  auto &value = res.GetValue();
  if (!value) {
    return std::optional<ReturnType>{};
  }

  auto &[vertices, edges] = *value;

  MG_ASSERT(vertices.size() <= 1, "The number of detach deleted vertices is not less or equal to 1!");

  return std::make_optional<ReturnType>(vertices[0], std::move(edges));
}

Result<std::optional<EdgeAccessor>> Storage::Accessor::DeleteEdge(EdgeAccessor *edge) {
  auto res = DetachDelete({}, {edge}, false);

  if (res.HasError()) {
    return res.GetError();
  }

  const auto &value = res.GetValue();
  if (!value) {
    return std::optional<EdgeAccessor>{};
  }

  const auto &[vertices, edges] = *value;

  MG_ASSERT(vertices.empty(), "Deleting an edge should not have deleted a vertex!");
  MG_ASSERT(edges.size() <= 1, "Deleted edges need to be less or equal to 1!");

  if (edges.empty()) {
    return std::optional<EdgeAccessor>{};
  }

  return std::make_optional<EdgeAccessor>(edges[0]);
}

Result<std::optional<std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>>>
Storage::Accessor::DetachDelete(std::vector<VertexAccessor *> nodes, std::vector<EdgeAccessor *> edges, bool detach) {
  using ReturnType = std::pair<std::vector<VertexAccessor>, std::vector<EdgeAccessor>>;

  // Gather nodes for deletion and check write conflicts
  auto maybe_nodes_to_delete = PrepareNodesForDeletion(nodes);
  if (maybe_nodes_to_delete.HasError()) {
    return maybe_nodes_to_delete.GetError();
  }

  std::unordered_set<Vertex *> &nodes_to_delete = maybe_nodes_to_delete.GetValue();

  auto edge_deletion_info = PrepareEdgesForDeletion(nodes_to_delete, edges, detach);

  std::set<EdgeRef> deleted_edges_set;
  std::vector<EdgeAccessor> deleted_edges;

  // Detach nodes which need to be deleted
  if (detach) {
    auto maybe_cleared_edges = ClearEdgesOnVertices(nodes_to_delete, deleted_edges_set);
    if (maybe_cleared_edges.HasError()) {
      return maybe_cleared_edges.GetError();
    }

    deleted_edges = *maybe_cleared_edges.GetValue();
  }

  // Detach nodes on the other end, which don't need deletion, by passing once through their vectors
  auto another_edges = DetachEdgesFromNodes(std::move(edge_deletion_info), deleted_edges_set);
  deleted_edges.insert(deleted_edges.end(), another_edges.begin(), another_edges.end());

  auto maybe_deleted_vertices = TryDeleteVertices(nodes_to_delete);
  if (maybe_deleted_vertices.HasError()) {
    return maybe_deleted_vertices.GetError();
  }

  auto deleted_vertices = maybe_deleted_vertices.GetValue();

  return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
}

Result<std::unordered_set<Vertex *>> Storage::Accessor::PrepareNodesForDeletion(
    const std::vector<VertexAccessor *> &vertices) {
  std::unordered_set<Vertex *> nodes_to_delete{};
  for (const auto &vertex : vertices) {
    MG_ASSERT(vertex->transaction_ == &transaction_,
              "VertexAccessor must be from the same transaction as the storage "
              "accessor when deleting a vertex!");
    auto *vertex_ptr = vertex->vertex_;

    {
      auto vertex_lock = std::unique_lock{vertex_ptr->lock};

      if (vertex_ptr->deleted) {
        continue;
      }
    }

    nodes_to_delete.insert(vertex_ptr);
  }

  return nodes_to_delete;
}

Result<std::vector<VertexAccessor>> Storage::Accessor::TryDeleteVertices(const std::unordered_set<Vertex *> &vertices) {
  std::vector<VertexAccessor> deleted_vertices;
  deleted_vertices.reserve(vertices.size());

  for (auto *vertex_ptr : vertices) {
    auto vertex_lock = std::unique_lock{vertex_ptr->lock};

    if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

    MG_ASSERT(!vertex_ptr->deleted, "Invalid database state!");

    if (!vertex_ptr->in_edges.empty() || !vertex_ptr->out_edges.empty()) {
      return Error::VERTEX_HAS_EDGES;
    }

    CreateAndLinkDelta(&transaction_, vertex_ptr, Delta::RecreateObjectTag());
    vertex_ptr->deleted = true;

    deleted_vertices.emplace_back(vertex_ptr, &transaction_, &storage_->indices_, &storage_->constraints_,
                                  storage_->config_.items, true);
  }

  return deleted_vertices;
}

EdgeInfoForDeletion Storage::Accessor::PrepareEdgesForDeletion(const std::unordered_set<Vertex *> &vertices,
                                                               const std::vector<EdgeAccessor *> &edges,
                                                               bool detach) noexcept {
  std::set<EdgeRef> deleted_edges_set;
  std::set<Vertex *> partial_src_vertices;
  std::set<Vertex *> partial_dest_vertices;
  std::set<EdgeRef> src_edge_refs;
  std::set<EdgeRef> dest_edge_refs;
  uint64_t total_edges_to_delete = 0;

  auto try_adding_partial_delete_vertices = [&vertices, &total_edges_to_delete](auto &partial_delete_vertices,
                                                                                auto &edge_refs, auto &item) {
    auto [edge_type, opposing_vertex, edge] = item;
    if (!vertices.contains(opposing_vertex)) {
      partial_delete_vertices.insert(opposing_vertex);
      edge_refs.insert(edge);
      total_edges_to_delete++;
    }
  };

  // add nodes which need to be detached on the other end of the edge
  if (detach) {
    for (auto *vertex_ptr : vertices) {
      for (const auto &item : vertex_ptr->in_edges) {
        try_adding_partial_delete_vertices(partial_src_vertices, src_edge_refs, item);
      }
      for (const auto &item : vertex_ptr->out_edges) {
        try_adding_partial_delete_vertices(partial_dest_vertices, dest_edge_refs, item);
      }
    }
  }

  // also add edges which we want to delete from the query
  for (const auto &edge_accessor : edges) {
    partial_src_vertices.insert(edge_accessor->from_vertex_);
    partial_dest_vertices.insert(edge_accessor->to_vertex_);

    src_edge_refs.insert(edge_accessor->edge_);
    dest_edge_refs.insert(edge_accessor->edge_);
    total_edges_to_delete++;
  }

  return EdgeInfoForDeletion{.partial_src_edges = std::move(src_edge_refs),
                             .partial_dest_edges = std::move(dest_edge_refs),
                             .partial_src_vertices = std::move(partial_src_vertices),
                             .partial_dest_vertices = std::move(partial_dest_vertices),
                             .total_edges_to_delete = total_edges_to_delete};
}

Result<std::optional<std::vector<EdgeAccessor>>> Storage::Accessor::ClearEdgesOnVertices(
    const std::unordered_set<Vertex *> &vertices, std::set<EdgeRef> &deleted_edges_set) {
  using ReturnType = std::vector<EdgeAccessor>;
  std::vector<EdgeAccessor> deleted_edges{};

  auto clear_edges = [this, &deleted_edges, &deleted_edges_set](
                         auto *vertex_ptr, auto *edges_collection, auto delta,
                         auto reverse_vertex_order) -> Result<std::optional<ReturnType>> {
    while (!edges_collection->empty()) {
      auto [edge_type, opposing_vertex, edge_ref] = *edges_collection->rbegin();
      std::unique_lock<utils::RWSpinLock> guard;
      if (storage_->config_.items.properties_on_edges) {
        auto edge_ptr = edge_ref.ptr;
        guard = std::unique_lock{edge_ptr->lock};

        if (!PrepareForWrite(&transaction_, edge_ptr)) return Error::SERIALIZATION_ERROR;
      }

      std::unique_lock<utils::RWSpinLock> guard_vertex{vertex_ptr->lock, std::defer_lock};
      std::unique_lock<utils::RWSpinLock> guard_opposing_vertex{opposing_vertex->lock, std::defer_lock};

      // Obtain the locks by `gid` order to avoid lock cycles.
      if (vertex_ptr->gid < opposing_vertex->gid) {
        guard_vertex.lock();
        guard_opposing_vertex.lock();
      } else if (vertex_ptr->gid > opposing_vertex->gid) {
        guard_opposing_vertex.lock();
        guard_vertex.lock();
      } else {
        // The vertices are the same vertex, only lock one.
        guard_vertex.lock();
      }

      if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;
      MG_ASSERT(!vertex_ptr->deleted, "Invalid database state!");

      if (opposing_vertex != vertex_ptr) {
        if (!PrepareForWrite(&transaction_, opposing_vertex)) return Error::SERIALIZATION_ERROR;
        MG_ASSERT(!opposing_vertex->deleted, "Invalid database state!");
      }

      edges_collection->pop_back();
      if (storage_->config_.items.properties_on_edges) {
        auto *edge_ptr = edge_ref.ptr;
        MarkEdgeAsDeleted(edge_ptr);
      }

      if (!deleted_edges_set.insert(edge_ref).second) {
        auto *from_vertex = reverse_vertex_order ? vertex_ptr : opposing_vertex;
        auto *to_vertex = reverse_vertex_order ? opposing_vertex : vertex_ptr;
        deleted_edges.emplace_back(edge_ref, edge_type, from_vertex, to_vertex, &transaction_, &storage_->indices_,
                                   &storage_->constraints_, storage_->config_.items, true);
      }

      CreateAndLinkDelta(&transaction_, vertex_ptr, delta, edge_type, opposing_vertex, edge_ref);
    }

    return std::make_optional<ReturnType>();
  };

  // delete the in and out edges from the nodes we want to delete
  for (auto *vertex_ptr : vertices) {
    auto maybe_error = clear_edges(vertex_ptr, &vertex_ptr->in_edges, Delta::AddInEdgeTag(), false);
    if (maybe_error.HasError()) {
      return maybe_error;
    }

    maybe_error = clear_edges(vertex_ptr, &vertex_ptr->out_edges, Delta::AddOutEdgeTag(), true);
    if (maybe_error.HasError()) {
      return maybe_error;
    }
  }

  return std::make_optional<ReturnType>(deleted_edges);
}

std::vector<EdgeAccessor> Storage::Accessor::DetachEdgesFromNodes(EdgeInfoForDeletion info,
                                                                  std::set<EdgeRef> &deleted_edges_set) {
  std::vector<EdgeAccessor> deleted_edges{};

  auto detach_non_deletable_nodes = [this, &deleted_edges, &deleted_edges_set](auto *vertex_ptr, auto *edges_collection,
                                                                               auto &set_for_erasure, auto delta,
                                                                               auto reverse_vertex_order) {
    std::unique_lock<utils::RWSpinLock> vertex_lock{vertex_ptr->lock};

    auto mid = std::partition(
        edges_collection->begin(), edges_collection->end(),
        [this, &set_for_erasure, &deleted_edges, &deleted_edges_set, vertex_ptr, delta,
         reverse_vertex_order](auto &edge) {
          auto [edge_type, opposing_vertex, edge_ref] = edge;
          if (set_for_erasure.contains(edge_ref)) {
            std::unique_lock<utils::RWSpinLock> guard;
            if (storage_->config_.items.properties_on_edges) {
              auto edge_ptr = edge_ref.ptr;
              guard = std::unique_lock{edge_ptr->lock};
              // this can happen only if we marked edges for deletion with no nodes,
              // so the method detaching nodes will not do anything
              MarkEdgeAsDeleted(edge_ptr);
            }

            if (!deleted_edges_set.insert(edge_ref).second) {
              auto *from_vertex = reverse_vertex_order ? vertex_ptr : opposing_vertex;
              auto *to_vertex = reverse_vertex_order ? opposing_vertex : vertex_ptr;
              deleted_edges.emplace_back(edge_ref, edge_type, from_vertex, to_vertex, &transaction_,
                                         &storage_->indices_, &storage_->constraints_, storage_->config_.items, true);
            }

            CreateAndLinkDelta(&transaction_, vertex_ptr, delta, edge_type, opposing_vertex, edge_ref);
            return false;
          }
          return true;
        });

    edges_collection->erase(mid, edges_collection->end());
  };

  // remove edges from vertex collections which we aggregated for just detaching
  for (auto *vertex_ptr : info.partial_src_vertices) {
    detach_non_deletable_nodes(vertex_ptr, &vertex_ptr->out_edges, info.partial_src_edges, Delta::AddOutEdgeTag(),
                               false);
  }
  for (auto *vertex_ptr : info.partial_dest_vertices) {
    detach_non_deletable_nodes(vertex_ptr, &vertex_ptr->in_edges, info.partial_dest_edges, Delta::AddInEdgeTag(), true);
  }

  return deleted_edges;
}

void Storage::Accessor::MarkEdgeAsDeleted(Edge *edge) {
  if (!edge->deleted) {
    CreateAndLinkDelta(&transaction_, edge, Delta::RecreateObjectTag());
    edge->deleted = true;
    storage_->edge_count_.fetch_add(-1, std::memory_order_acq_rel);
  }
}

}  // namespace memgraph::storage
