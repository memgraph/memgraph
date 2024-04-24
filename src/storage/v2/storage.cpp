// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <thread>
#include "absl/container/flat_hash_set.h"
#include "spdlog/spdlog.h"

#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "storage/v2/disk/name_id_mapper.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/atomic_memory_block.hpp"
#include "utils/event_counter.hpp"
#include "utils/event_histogram.hpp"
#include "utils/exceptions.hpp"
#include "utils/file.hpp"
#include "utils/logging.hpp"
#include "utils/stat.hpp"
#include "utils/timer.hpp"
#include "utils/typeinfo.hpp"
#include "utils/uuid.hpp"

namespace memgraph::storage {

class InMemoryStorage;

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
      indices_(config, storage_mode),
      constraints_(config, storage_mode) {
  spdlog::info("Created database with {} storage mode.", StorageModeToString(storage_mode));
}

Storage::Accessor::Accessor(SharedAccess /* tag */, Storage *storage, IsolationLevel isolation_level,
                            StorageMode storage_mode,
                            memgraph::replication_coordination_glue::ReplicationRole replication_role)
    : storage_(storage),
      // The lock must be acquired before creating the transaction object to
      // prevent freshly created transactions from dangling in an active state
      // during exclusive operations.
      storage_guard_(storage_->main_lock_),
      unique_guard_(storage_->main_lock_, std::defer_lock),
      transaction_(storage->CreateTransaction(isolation_level, storage_mode, replication_role)),
      is_transaction_active_(true),
      creation_storage_mode_(storage_mode) {}

Storage::Accessor::Accessor(UniqueAccess /* tag */, Storage *storage, IsolationLevel isolation_level,
                            StorageMode storage_mode,
                            memgraph::replication_coordination_glue::ReplicationRole replication_role)
    : storage_(storage),
      // The lock must be acquired before creating the transaction object to
      // prevent freshly created transactions from dangling in an active state
      // during exclusive operations.
      storage_guard_(storage_->main_lock_, std::defer_lock),
      unique_guard_(storage_->main_lock_),
      transaction_(storage->CreateTransaction(isolation_level, storage_mode, replication_role)),
      is_transaction_active_(true),
      creation_storage_mode_(storage_mode) {}

Storage::Accessor::Accessor(Accessor &&other) noexcept
    : storage_(other.storage_),
      storage_guard_(std::move(other.storage_guard_)),
      unique_guard_(std::move(other.unique_guard_)),
      transaction_(std::move(other.transaction_)),
      commit_timestamp_(other.commit_timestamp_),
      is_transaction_active_(other.is_transaction_active_),
      creation_storage_mode_(other.creation_storage_mode_) {
  // Don't allow the other accessor to abort our transaction in destructor.
  other.is_transaction_active_ = false;
  other.commit_timestamp_.reset();
}

StorageMode Storage::GetStorageMode() const noexcept { return storage_mode_; }

IsolationLevel Storage::GetIsolationLevel() const noexcept { return isolation_level_; }

utils::BasicResult<Storage::SetIsolationLevelError> Storage::SetIsolationLevel(IsolationLevel isolation_level) {
  std::unique_lock main_guard{main_lock_};
  isolation_level_ = isolation_level;
  return {};
}

StorageMode Storage::Accessor::GetCreationStorageMode() const noexcept { return creation_storage_mode_; }

std::optional<uint64_t> Storage::Accessor::GetTransactionId() const {
  if (is_transaction_active_) {
    return transaction_.transaction_id;
  }
  return {};
}

std::vector<LabelId> Storage::Accessor::ListAllPossiblyPresentVertexLabels() const {
  std::vector<LabelId> vertex_labels;
  storage_->stored_node_labels_.for_each([&vertex_labels](const auto &label) { vertex_labels.push_back(label); });
  return vertex_labels;
}

std::vector<EdgeTypeId> Storage::Accessor::ListAllPossiblyPresentEdgeTypes() const {
  std::vector<EdgeTypeId> edge_types;
  storage_->stored_edge_types_.for_each([&edge_types](const auto &type) { edge_types.push_back(type); });
  return edge_types;
}

void Storage::Accessor::AdvanceCommand() {
  transaction_.manyDeltasCache.Clear();  // TODO: Just invalidate the View::OLD cache, NEW should still be fine
  ++transaction_.command_id;
}

Result<std::optional<VertexAccessor>> Storage::Accessor::DeleteVertex(VertexAccessor *vertex) {
  /// NOTE: Checking whether the vertex can be deleted must be done by loading edges from disk.
  /// Loading edges is done through VertexAccessor so we do it here.
  if (storage_->storage_mode_ == StorageMode::ON_DISK_TRANSACTIONAL) {
    auto out_edges_res = vertex->OutEdges(View::OLD);
    auto in_edges_res = vertex->InEdges(View::OLD);
    if (out_edges_res.HasError() && out_edges_res.GetError() != Error::NONEXISTENT_OBJECT) {
      return out_edges_res.GetError();
    }
    if (!out_edges_res.HasError() && !out_edges_res->edges.empty()) {
      return Error::VERTEX_HAS_EDGES;
    }
    if (in_edges_res.HasError() && in_edges_res.GetError() != Error::NONEXISTENT_OBJECT) {
      return in_edges_res.GetError();
    }
    if (!in_edges_res.HasError() && !in_edges_res->edges.empty()) {
      return Error::VERTEX_HAS_EDGES;
    }
  }
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
  if (storage_->storage_mode_ == StorageMode::ON_DISK_TRANSACTIONAL) {
    auto out_edges_res = vertex->OutEdges(View::OLD);
    auto in_edges_res = vertex->InEdges(View::OLD);
    if (out_edges_res.HasError() && out_edges_res.GetError() != Error::NONEXISTENT_OBJECT) {
      return out_edges_res.GetError();
    }
    if (in_edges_res.HasError() && in_edges_res.GetError() != Error::NONEXISTENT_OBJECT) {
      return in_edges_res.GetError();
    }
  }

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
  if (storage_->storage_mode_ == StorageMode::ON_DISK_TRANSACTIONAL) {
    for (const auto *vertex : nodes) {
      /// TODO: (andi) Extract into a separate function.
      auto out_edges_res = vertex->OutEdges(View::OLD);
      auto in_edges_res = vertex->InEdges(View::OLD);
      if (out_edges_res.HasError() && out_edges_res.GetError() != Error::NONEXISTENT_OBJECT) {
        return out_edges_res.GetError();
      }
      if (in_edges_res.HasError() && in_edges_res.GetError() != Error::NONEXISTENT_OBJECT) {
        return in_edges_res.GetError();
      }
    }
  }

  // 1. Gather nodes which are not deleted yet in the system
  auto maybe_nodes_to_delete = PrepareDeletableNodes(nodes);
  if (maybe_nodes_to_delete.HasError()) {
    return maybe_nodes_to_delete.GetError();
  }
  const std::unordered_set<Vertex *> nodes_to_delete = *maybe_nodes_to_delete.GetValue();

  // 2. Gather edges and corresponding node on the other end of the edge for the deletable nodes
  EdgeInfoForDeletion edge_deletion_info = PrepareDeletableEdges(nodes_to_delete, edges, detach);

  // Detach nodes which need to be deleted
  std::unordered_set<Gid> deleted_edge_ids;
  std::vector<EdgeAccessor> deleted_edges;
  if (detach) {
    auto maybe_cleared_edges = ClearEdgesOnVertices(nodes_to_delete, deleted_edge_ids);
    if (maybe_cleared_edges.HasError()) {
      return maybe_cleared_edges.GetError();
    }

    deleted_edges = *maybe_cleared_edges.GetValue();
  }

  // Detach nodes on the other end, which don't need deletion, by passing once through their vectors
  auto maybe_remaining_edges = DetachRemainingEdges(std::move(edge_deletion_info), deleted_edge_ids);
  if (maybe_remaining_edges.HasError()) {
    return maybe_remaining_edges.GetError();
  }
  const std::vector<EdgeAccessor> remaining_edges = *maybe_remaining_edges.GetValue();
  deleted_edges.insert(deleted_edges.end(), remaining_edges.begin(), remaining_edges.end());

  auto const maybe_deleted_vertices = TryDeleteVertices(nodes_to_delete);
  if (maybe_deleted_vertices.HasError()) {
    return maybe_deleted_vertices.GetError();
  }

  if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    for (auto *node : nodes_to_delete) {
      storage_->indices_.text_index_.RemoveNode(node);
    }
  }

  auto deleted_vertices = maybe_deleted_vertices.GetValue();

  return std::make_optional<ReturnType>(std::move(deleted_vertices), std::move(deleted_edges));
}

Result<std::optional<std::unordered_set<Vertex *>>> Storage::Accessor::PrepareDeletableNodes(
    const std::vector<VertexAccessor *> &vertices) {
  // Some of the vertices could be already deleted in the system so we need to check
  std::unordered_set<Vertex *> nodes_to_delete{};
  for (const auto &vertex : vertices) {
    MG_ASSERT(vertex->transaction_ == &transaction_,
              "VertexAccessor must be from the same transaction as the storage "
              "accessor when deleting a vertex!");
    auto *vertex_ptr = vertex->vertex_;
    /// TODO: (andi) This is overhead for disk storage, we should not lock the vertex here
    {
      auto vertex_lock = std::unique_lock{vertex_ptr->lock};

      if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;

      if (vertex_ptr->deleted) {
        continue;
      }
    }

    nodes_to_delete.insert(vertex_ptr);
  }

  return std::make_optional<std::unordered_set<Vertex *>>(nodes_to_delete);
}

EdgeInfoForDeletion Storage::Accessor::PrepareDeletableEdges(const std::unordered_set<Vertex *> &vertices,
                                                             const std::vector<EdgeAccessor *> &edges,
                                                             bool detach) noexcept {
  std::unordered_set<Vertex *> partial_src_vertices;
  std::unordered_set<Vertex *> partial_dest_vertices;
  std::unordered_set<Gid> src_edge_ids;
  std::unordered_set<Gid> dest_edge_ids;

  auto try_adding_partial_delete_vertices = [this, &vertices](auto &partial_delete_vertices, auto &edge_ids,
                                                              auto &item) {
    // For the nodes on the other end of the edge, they might not get deleted in the system but only cut out
    // of the edge. Therefore, information is gathered in this step to account for every vertices' in and out
    // edges and what must be deleted
    const auto &[edge_type, opposing_vertex, edge] = item;
    if (!vertices.contains(opposing_vertex)) {
      partial_delete_vertices.insert(opposing_vertex);
      auto const edge_gid = storage_->config_.salient.items.properties_on_edges ? edge.ptr->gid : edge.gid;
      edge_ids.insert(edge_gid);
    }
  };

  // add nodes which need to be detached on the other end of the edge
  if (detach) {
    for (auto *vertex_ptr : vertices) {
      TcoVector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> in_edges;
      TcoVector<std::tuple<EdgeTypeId, Vertex *, EdgeRef>> out_edges;

      {
        auto vertex_lock = std::shared_lock{vertex_ptr->lock};
        in_edges = vertex_ptr->in_edges;
        out_edges = vertex_ptr->out_edges;
      }

      for (auto const &item : in_edges) {
        try_adding_partial_delete_vertices(partial_src_vertices, src_edge_ids, item);
      }
      for (auto const &item : out_edges) {
        try_adding_partial_delete_vertices(partial_dest_vertices, dest_edge_ids, item);
      }
    }
  }

  // also add edges which we want to delete from the query
  for (const auto &edge_accessor : edges) {
    if (edge_accessor->from_vertex_->deleted || edge_accessor->to_vertex_->deleted) {
      continue;
    }
    partial_src_vertices.insert(edge_accessor->from_vertex_);
    partial_dest_vertices.insert(edge_accessor->to_vertex_);

    auto const edge_gid = edge_accessor->Gid();
    src_edge_ids.insert(edge_gid);
    dest_edge_ids.insert(edge_gid);
  }

  return EdgeInfoForDeletion{.partial_src_edge_ids = std::move(src_edge_ids),
                             .partial_dest_edge_ids = std::move(dest_edge_ids),
                             .partial_src_vertices = std::move(partial_src_vertices),
                             .partial_dest_vertices = std::move(partial_dest_vertices)};
}

Result<std::optional<std::vector<EdgeAccessor>>> Storage::Accessor::ClearEdgesOnVertices(
    const std::unordered_set<Vertex *> &vertices, std::unordered_set<Gid> &deleted_edge_ids) {
  // We want to gather all edges that we delete in this step so that we can proceed with
  // further deletion
  using ReturnType = std::vector<EdgeAccessor>;
  std::vector<EdgeAccessor> deleted_edges{};

  auto clear_edges = [this, &deleted_edges, &deleted_edge_ids](
                         auto *vertex_ptr, auto *attached_edges_to_vertex, auto deletion_delta,
                         auto reverse_vertex_order) -> Result<std::optional<ReturnType>> {
    auto vertex_lock = std::unique_lock{vertex_ptr->lock};
    while (!attached_edges_to_vertex->empty()) {
      // get the information about the last edge in the vertex collection
      auto const &[edge_type, opposing_vertex, edge_ref] = *attached_edges_to_vertex->rbegin();

      /// TODO: (andi) Again here, no need to lock the edge if using on disk storage.
      std::unique_lock<utils::RWSpinLock> guard;
      if (storage_->config_.salient.items.properties_on_edges) {
        auto edge_ptr = edge_ref.ptr;
        guard = std::unique_lock{edge_ptr->lock};

        if (!PrepareForWrite(&transaction_, edge_ptr)) return Error::SERIALIZATION_ERROR;
      }

      if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;
      MG_ASSERT(!vertex_ptr->deleted, "Invalid database state!");

      // MarkEdgeAsDeleted allocates additional memory
      // and CreateAndLinkDelta needs memory
      utils::AtomicMemoryBlock([&attached_edges_to_vertex, &deleted_edge_ids, &reverse_vertex_order, &vertex_ptr,
                                &deleted_edges, deletion_delta = deletion_delta, edge_type = edge_type,
                                opposing_vertex = opposing_vertex, edge_ref = edge_ref, this]() {
        attached_edges_to_vertex->pop_back();
        if (this->storage_->config_.salient.items.properties_on_edges) {
          auto *edge_ptr = edge_ref.ptr;
          MarkEdgeAsDeleted(edge_ptr);
        }

        auto const edge_gid = storage_->config_.salient.items.properties_on_edges ? edge_ref.ptr->gid : edge_ref.gid;
        auto const [_, was_inserted] = deleted_edge_ids.insert(edge_gid);
        bool const edge_cleared_from_both_directions = !was_inserted;
        if (edge_cleared_from_both_directions) {
          auto *from_vertex = reverse_vertex_order ? vertex_ptr : opposing_vertex;
          auto *to_vertex = reverse_vertex_order ? opposing_vertex : vertex_ptr;
          deleted_edges.emplace_back(edge_ref, edge_type, from_vertex, to_vertex, storage_, &transaction_, true);
        }
        CreateAndLinkDelta(&transaction_, vertex_ptr, deletion_delta, edge_type, opposing_vertex, edge_ref);
      });
    }

    return std::make_optional<ReturnType>();
  };

  // delete the in and out edges from the nodes we want to delete
  // no need to lock here, we are just passing the pointer of the in and out edges collections
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

Result<std::optional<std::vector<EdgeAccessor>>> Storage::Accessor::DetachRemainingEdges(
    EdgeInfoForDeletion info, std::unordered_set<Gid> &partially_detached_edge_ids) {
  using ReturnType = std::vector<EdgeAccessor>;
  std::vector<EdgeAccessor> deleted_edges{};

  auto clear_edges_on_other_direction = [this, &deleted_edges, &partially_detached_edge_ids](
                                            auto *vertex_ptr, auto *edges_attached_to_vertex, auto &set_for_erasure,
                                            auto deletion_delta,
                                            auto reverse_vertex_order) -> Result<std::optional<ReturnType>> {
    auto vertex_lock = std::unique_lock{vertex_ptr->lock};

    if (!PrepareForWrite(&transaction_, vertex_ptr)) return Error::SERIALIZATION_ERROR;
    MG_ASSERT(!vertex_ptr->deleted, "Invalid database state!");

    auto mid = std::partition(
        edges_attached_to_vertex->begin(), edges_attached_to_vertex->end(), [this, &set_for_erasure](auto &edge) {
          auto const &[edge_type, opposing_vertex, edge_ref] = edge;
          auto const edge_gid = storage_->config_.salient.items.properties_on_edges ? edge_ref.ptr->gid : edge_ref.gid;
          return !set_for_erasure.contains(edge_gid);
        });

    // Creating deltas and erasing edge only at the end -> we might have incomplete state as
    // delta might cause OOM, so we don't remove edges from edges_attached_to_vertex
    utils::AtomicMemoryBlock([&mid, &edges_attached_to_vertex, &deleted_edges, &partially_detached_edge_ids, this,
                              vertex_ptr, deletion_delta, reverse_vertex_order]() {
      for (auto it = mid; it != edges_attached_to_vertex->end(); it++) {
        auto const &[edge_type, opposing_vertex, edge_ref] = *it;
        std::unique_lock<utils::RWSpinLock> guard;
        if (storage_->config_.salient.items.properties_on_edges) {
          auto edge_ptr = edge_ref.ptr;
          guard = std::unique_lock{edge_ptr->lock};
          // this can happen only if we marked edges for deletion with no nodes,
          // so the method detaching nodes will not do anything
          MarkEdgeAsDeleted(edge_ptr);
        }

        CreateAndLinkDelta(&transaction_, vertex_ptr, deletion_delta, edge_type, opposing_vertex, edge_ref);

        auto const edge_gid = storage_->config_.salient.items.properties_on_edges ? edge_ref.ptr->gid : edge_ref.gid;
        auto const [_, was_inserted] = partially_detached_edge_ids.insert(edge_gid);
        bool const edge_cleared_from_both_directions = !was_inserted;
        if (edge_cleared_from_both_directions) {
          auto *from_vertex = reverse_vertex_order ? opposing_vertex : vertex_ptr;
          auto *to_vertex = reverse_vertex_order ? vertex_ptr : opposing_vertex;
          deleted_edges.emplace_back(edge_ref, edge_type, from_vertex, to_vertex, storage_, &transaction_, true);
        }
      }
      edges_attached_to_vertex->erase(mid, edges_attached_to_vertex->end());
    });

    return std::make_optional<ReturnType>();
  };

  // remove edges from vertex collections which we aggregated for just detaching
  for (auto *vertex_ptr : info.partial_src_vertices) {
    auto maybe_error = clear_edges_on_other_direction(vertex_ptr, &vertex_ptr->out_edges, info.partial_src_edge_ids,
                                                      Delta::AddOutEdgeTag(), false);
    if (maybe_error.HasError()) {
      return maybe_error;
    }
  }
  for (auto *vertex_ptr : info.partial_dest_vertices) {
    auto maybe_error = clear_edges_on_other_direction(vertex_ptr, &vertex_ptr->in_edges, info.partial_dest_edge_ids,
                                                      Delta::AddInEdgeTag(), true);
    if (maybe_error.HasError()) {
      return maybe_error;
    }
  }

  return std::make_optional<ReturnType>(deleted_edges);
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

    deleted_vertices.emplace_back(vertex_ptr, storage_, &transaction_, true);
  }

  return deleted_vertices;
}

void Storage::Accessor::MarkEdgeAsDeleted(Edge *edge) {
  if (!edge->deleted) {
    CreateAndLinkDelta(&transaction_, edge, Delta::RecreateObjectTag());
    edge->deleted = true;
    storage_->edge_count_.fetch_sub(1, std::memory_order_acq_rel);
  }
}

void Storage::Accessor::CreateTextIndex(const std::string &index_name, LabelId label, query::DbAccessor *db) {
  MG_ASSERT(unique_guard_.owns_lock(), "Creating a text index requires unique access to storage!");
  storage_->indices_.text_index_.CreateIndex(storage_->config_.durability.storage_directory, index_name, label, db);
  transaction_.md_deltas.emplace_back(MetadataDelta::text_index_create, index_name, label);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveTextIndices);
}

void Storage::Accessor::DropTextIndex(const std::string &index_name) {
  MG_ASSERT(unique_guard_.owns_lock(), "Dropping a text index requires unique access to storage!");
  auto deleted_index_label =
      storage_->indices_.text_index_.DropIndex(storage_->config_.durability.storage_directory, index_name);
  transaction_.md_deltas.emplace_back(MetadataDelta::text_index_drop, index_name, deleted_index_label);
  memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveTextIndices);
}

}  // namespace memgraph::storage
