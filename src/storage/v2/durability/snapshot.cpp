// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/durability/snapshot.hpp"

#include <fmt/core.h>
#include <openssl/x509v3.h>
#include <sys/sendfile.h>
#include <sys/stat.h>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <future>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <usearch/index_plugins.hpp>

#include "flags/experimental.hpp"
#include "flags/run_time_configurable.hpp"
#include "license/license.hpp"
#include "spdlog/spdlog.h"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/fmt.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_index_stats.hpp"
#include "storage/v2/indices/label_property_index_stats.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/file.hpp"
#include "utils/file_locker.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/on_scope_exit.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"
#include "utils/timer.hpp"

using namespace std::chrono_literals;
namespace {
constexpr auto kCheckIfSnapshotAborted = 3s;
}  // namespace

namespace memgraph::storage::durability {

// Snapshot format:
//
// 1) Magic string (non-encoded)
//
// 2) Snapshot version (non-encoded, little-endian)
//
// 3) Section offsets:
//     * offset to the first edge in the snapshot (`0` if properties on edges
//       are disabled)
//     * offset to the first vertex in the snapshot
//     * offset to the indices section
//     * offset to the constraints section
//     * offset to the mapper section
//     * offset to the metadata section
//     * offset to the offset-count pair of the first edge batch (`0` if properties on edges are disabled)
//     * offset to the offset-count pair of the first vertex batch
//
// 4) Encoded edges (if properties on edges are enabled); each edge is written
//    in the following format:
//     * gid
//     * properties
//
// 5) Encoded vertices; each vertex is written in the following format:
//     * gid
//     * labels
//     * properties
//     * in edges
//         * edge gid
//         * from vertex gid
//         * edge type
//     * out edges
//         * edge gid
//         * to vertex gid
//         * edge type
//
// 6) Indices
//     * label indices
//         * label
//     * label+property indices
//         * label
//         * property
//
// 7) Constraints
//     * existence constraints
//         * label
//         * property
//     * unique constraints (from version 13)
//         * label
//         * properties
//
// 8) Name to ID mapper data
//     * id to name mappings
//         * id
//         * name
//
// 9) Metadata
//     * storage UUID
//     * snapshot transaction start timestamp (required when recovering
//       from snapshot combined with WAL to determine what deltas need to be
//       applied)
//     * number of edges
//     * number of vertices
//
// 10) Batch infos
//     * number of edge batch infos
//     * edge batch infos
//        * starting offset of the batch
//        * number of edges in the batch
//     * vertex batch infos
//        * starting offset of the batch
//        * number of vertices in the batch
//
// IMPORTANT: When changing snapshot encoding/decoding bump the snapshot/WAL
// version in `version.hpp`.

struct BatchInfo {
  uint64_t offset;
  uint64_t count;
};

using SnapshotEncoder = Encoder<utils::NonConcurrentOutputFile>;

// Used for an upper limit, ie. something beyond the last item we should be processing
constexpr auto kEnd = std::numeric_limits<int64_t>::max();

// Result of a partial snapshot creation
struct SnapshotPartialRes {
  std::vector<BatchInfo> batch_info{};      // Batch information in the current part
  std::unordered_set<uint64_t> used_ids{};  // Used ids in the current part
  uint64_t count{0};                        // Number of handled elements
  std::filesystem::path snapshot_path{};    // File location
  size_t snapshot_size{0};                  // Size of the file
};

class SafeTaskQueue {
 public:
  using task_t = std::function<void()>;
  SafeTaskQueue() = default;

  // Add a task to the queue.
  void AddTask(task_t task) {
    std::lock_guard<std::mutex> lock(mtx_);
    tasks_.push(std::move(task));
  }

  std::optional<task_t> PopTask() {
    std::lock_guard<std::mutex> lock(mtx_);
    if (tasks_.empty()) {
      return std::nullopt;
    }
    auto task = std::move(tasks_.front());
    tasks_.pop();
    return std::move(task);
  }

  auto size() const {
    std::lock_guard<std::mutex> lock(mtx_);
    return tasks_.size();
  }

 private:
  mutable std::mutex mtx_;
  std::queue<task_t> tasks_;
};

using task_results_t = std::vector<std::pair<SnapshotPartialRes, std::promise<bool>>>;

void WaitAndCombine(task_results_t &partial_results, SnapshotEncoder &snapshot_encoder, uint64_t &element_count,
                    std::vector<BatchInfo> &batch_infos, std::unordered_set<uint64_t> &used_ids,
                    auto &&snapshot_aborted) {
  // NOTE: They have to be combined in order
  for (auto &[res, promise] : partial_results) {
    promise.get_future().wait();  // Wait for incoming result

    spdlog::trace("Handling snapshot part {}, size {}, count {}...", res.snapshot_path, res.snapshot_size, res.count);
    utils::OnScopeExit cleanup{[path = res.snapshot_path] {
      std::error_code ec;
      std::filesystem::remove(path, ec);
      if (ec) spdlog::warn("Couldn't remove temporary snapshot part {}: {}", path, ec.message());
    }};

    if (snapshot_aborted()) {
      continue;  // Run through and clean up
    }

    if (res.snapshot_size > 0) {
      element_count += res.count;
      used_ids.merge(std::move(res.used_ids));
      const auto current_offset = snapshot_encoder.GetPosition();  // Flushes as well
      // Update batch positions
      for (auto &[offset, _] : res.batch_info) {
        offset += current_offset;
      }
      // TODO Regenerate batches (currently could be less then optimal)
      batch_infos.insert(batch_infos.end(), std::make_move_iterator(res.batch_info.begin()),
                         std::make_move_iterator(res.batch_info.end()));
      // Append the edge part to the snapshot
      int part_fd = open(res.snapshot_path.string().c_str(), O_RDONLY);
      if (part_fd == -1) {
        throw RecoveryFailure("Couldn't open snapshot part {}!", res.snapshot_path);
      }
      utils::OnScopeExit cleanup{[part_fd] { close(part_fd); }};
      // Use sendfile for efficient copying (zero-copy)
      off_t offset = 0;
      auto size = res.snapshot_size;
      while (size > 0) {
        ssize_t bytes_sent = sendfile(snapshot_encoder.native_handle(), part_fd, &offset, size);
        if (bytes_sent == -1) {
          throw RecoveryFailure("Couldn't copy {} part to snapshot! Error: {}", res.snapshot_path, strerror(errno));
        }
        if (bytes_sent == 0) {
          spdlog::trace("EOF {}", res.snapshot_path);
          break;
        }
        size -= bytes_sent;
      }
    }
  }
}

// Return at least one batch (at least 2 elements: start and end gid)
auto Batch(auto &&acc, const uint64_t items_per_batch) {
  // Skiplist sizes can change, last thread will pick up any new elements. In the end, these will be skiped (MVCC)
  const auto n_batches = (items_per_batch < 1) ? 1 : (acc.size() + items_per_batch - 1) / items_per_batch;

  if (n_batches < 2) {
    // Single batch, scan whole skiplist
    return std::vector<int64_t>{0, kEnd};
  }

  // TODO Use sampling iterator (after composite index)
  std::vector<int64_t> batches(n_batches + 1, kEnd);  // start and end gids
  batches[0] = 0;                                     // Always start from the lowest possible
  int i = 0;
  int batch_id = 1;
  for (const auto &elem : acc) {
    if (items_per_batch == i++) {
      batches[batch_id++] = elem.gid.AsInt();  // This batch's start ID and previous batch's end ID
      i = 1;                                   // 1 on purpose, as the first element is already in the batch
      // Check if we have enough batches
      if (batch_id == n_batches) break;  // THIS IS IMPORTANT, we do not want to remove the kEnd as our upper limit
    }
  }
  return batches;
}

void MultiThreadedWorkflow(utils::SkipList<Edge> *edges, utils::SkipList<Vertex> *vertices, auto &&partial_edge_handler,
                           auto &&partial_vertex_handler, const uint64_t items_per_batch, uint64_t &offset_edges,
                           uint64_t &offset_vertices, SnapshotEncoder &snapshot_encoder, uint64_t &edges_count,
                           uint64_t &vertices_count, std::vector<BatchInfo> &edge_batch_infos,
                           std::vector<BatchInfo> &vertex_batch_infos, std::unordered_set<uint64_t> &used_ids,
                           uint64_t thread_count, auto &&snapshot_aborted) {
  SafeTaskQueue tasks;

  // Generate edge tasks
  std::vector<int64_t> edge_batch_gid{};
  task_results_t edge_res{};
  if (edges != nullptr) {  // No edges skiplist <=> no properties on edges
    edge_batch_gid = Batch(edges->access(), items_per_batch);
    edge_res = task_results_t{edge_batch_gid.size() - 1};  // last element is an end marker
    for (int id = 0; id < edge_res.size(); ++id) {
      tasks.AddTask([&edge_res, &partial_edge_handler, id, start_gid = edge_batch_gid[id],
                     end_gid = edge_batch_gid[id + 1], path = snapshot_encoder.GetPath()] {
        // Create workers temporary file
        {
          SnapshotEncoder edges_snapshot;
          const auto snapshot_path = fmt::format("{}_edge_part_{}", path, id);
          edges_snapshot.Initialize(snapshot_path);
          // Fill snapshot with edges
          edge_res[id].first = partial_edge_handler(start_gid, end_gid, edges_snapshot);
          edges_snapshot.Finalize();
        }
        // Signal that the snapshot is done
        edge_res[id].second.set_value(true);
      });
    }
  }

  // Generate vertex tasks
  auto vertex_batch_gid = Batch(vertices->access(), items_per_batch);
  task_results_t vertex_res(vertex_batch_gid.size() - 1);  // last element is an end marker
  for (int id = 0; id < vertex_res.size(); ++id) {
    tasks.AddTask([&vertex_res, &partial_vertex_handler, id, start_gid = vertex_batch_gid[id],
                   end_gid = vertex_batch_gid[id + 1], path = snapshot_encoder.GetPath()] {
      // Create workers temporary file
      {
        SnapshotEncoder vertex_snapshot;
        const auto snapshot_path = fmt::format("{}_vertex_part_{}", path, id);
        vertex_snapshot.Initialize(snapshot_path);
        // Fill snapshot with edges
        vertex_res[id].first = partial_vertex_handler(start_gid, end_gid, vertex_snapshot);
        vertex_snapshot.Finalize();
      }
      // Signal that the snapshot is done
      vertex_res[id].second.set_value(true);
    });
  }

  const auto n_workers = std::min(thread_count, tasks.size());
  std::vector<std::jthread> workers;
  workers.reserve(n_workers);
  for (int i = 0; i < n_workers; ++i) {
    workers.emplace_back([&, i] {
      utils::ThreadSetName("snapshot" + std::to_string(i));
      while (true) {
        auto task = tasks.PopTask();
        if (!task) break;  // No more tasks; if aborted, run through all tasks to mark them as done
        (*task)();         // Execute the task
      }
    });
  }

  // Wait for tasks to finish and combine results as they come in
  if (!edge_res.empty()) offset_edges = snapshot_encoder.GetPosition();  // 0 -> edges without properties
  WaitAndCombine(edge_res, snapshot_encoder, edges_count, edge_batch_infos, used_ids, snapshot_aborted);
  offset_vertices = snapshot_encoder.GetPosition();
  WaitAndCombine(vertex_res, snapshot_encoder, vertices_count, vertex_batch_infos, used_ids, snapshot_aborted);
};

// Function used to read information about the snapshot file.
SnapshotInfo ReadSnapshotInfoPreVersion23(const std::filesystem::path &path) {
  // Check magic and version.
  Decoder snapshot;
  auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version) throw RecoveryFailure("Couldn't read snapshot magic and/or version!");
  if (!IsVersionSupported(*version)) throw RecoveryFailure("Invalid snapshot version!");

  // Prepare return value.
  SnapshotInfo info;

  // Read offsets.
  {
    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_OFFSETS)
      throw RecoveryFailure("Couldn't read marker for section offsets!");

    auto snapshot_size = snapshot.GetSize();
    if (!snapshot_size) throw RecoveryFailure("Couldn't read snapshot size!");

    auto read_offset = [&snapshot, snapshot_size] {
      auto maybe_offset = snapshot.ReadUint();
      if (!maybe_offset) throw RecoveryFailure("Invalid snapshot format!");
      auto offset = *maybe_offset;
      if (offset > *snapshot_size) throw RecoveryFailure("Invalid snapshot format!");
      return offset;
    };

    info.offset_edges = read_offset();
    info.offset_vertices = read_offset();
    info.offset_indices = read_offset();
    if (*version >= kEdgeIndicesVersion) {
      info.offset_edge_indices = read_offset();
    } else {
      info.offset_edge_indices = 0U;
    }
    info.offset_constraints = read_offset();
    info.offset_mapper = read_offset();
    if (*version >= kEnumsVersion) {
      info.offset_enums = read_offset();
    } else {
      info.offset_enums = 0U;
    }
    info.offset_epoch_history = read_offset();
    info.offset_metadata = read_offset();
    if (*version >= 15U) {
      info.offset_edge_batches = read_offset();
      info.offset_vertex_batches = read_offset();
    } else {
      info.offset_edge_batches = 0U;
      info.offset_vertex_batches = 0U;
    }
  }

  // Read metadata.
  {
    if (!snapshot.SetPosition(info.offset_metadata)) throw RecoveryFailure("Couldn't read metadata offset!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_METADATA)
      throw RecoveryFailure("Couldn't read marker for section metadata!");

    auto maybe_uuid = snapshot.ReadString();
    if (!maybe_uuid) throw RecoveryFailure("Couldn't read storage_uuid!");
    info.uuid = std::move(*maybe_uuid);

    auto maybe_epoch_id = snapshot.ReadString();
    if (!maybe_epoch_id) throw RecoveryFailure("Couldn't read epoch id!");
    info.epoch_id = std::move(*maybe_epoch_id);

    auto maybe_timestamp = snapshot.ReadUint();
    if (!maybe_timestamp) throw RecoveryFailure("Couldn't read start timestamp!");
    info.start_timestamp = *maybe_timestamp;
    info.durable_timestamp = *maybe_timestamp;  // Falling back to the old logic

    auto maybe_edges = snapshot.ReadUint();
    if (!maybe_edges) throw RecoveryFailure("Couldn't read the number of edges!");
    info.edges_count = *maybe_edges;

    auto maybe_vertices = snapshot.ReadUint();
    if (!maybe_vertices) throw RecoveryFailure("Couldn't read the number of vertices!");
    info.vertices_count = *maybe_vertices;
  }

  return info;
}
// Function used to read information about the snapshot file.
SnapshotInfo ReadSnapshotInfo(const std::filesystem::path &path) {
  // Check magic and version.
  Decoder snapshot;
  auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version) throw RecoveryFailure("Couldn't read snapshot magic and/or version!");
  if (!IsVersionSupported(*version)) throw RecoveryFailure("Invalid snapshot version!");

  if (version < kDurableTS) {
    return ReadSnapshotInfoPreVersion23(path);
  }

  // Prepare return value.
  SnapshotInfo info;

  // Read offsets.
  {
    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_OFFSETS)
      throw RecoveryFailure("Couldn't read marker for section offsets!");

    auto snapshot_size = snapshot.GetSize();
    if (!snapshot_size) throw RecoveryFailure("Couldn't read snapshot size!");

    auto read_offset = [&snapshot, snapshot_size] {
      auto maybe_offset = snapshot.ReadUint();
      if (!maybe_offset) throw RecoveryFailure("Invalid snapshot format!");
      auto offset = *maybe_offset;
      if (offset > *snapshot_size) throw RecoveryFailure("Invalid snapshot format!");
      return offset;
    };

    info.offset_edges = read_offset();
    info.offset_vertices = read_offset();
    info.offset_indices = read_offset();
    if (*version >= kEdgeIndicesVersion) {
      info.offset_edge_indices = read_offset();
    } else {
      info.offset_edge_indices = 0U;
    }
    info.offset_constraints = read_offset();
    info.offset_mapper = read_offset();
    if (*version >= kEnumsVersion) {
      info.offset_enums = read_offset();
    } else {
      info.offset_enums = 0U;
    }
    info.offset_epoch_history = read_offset();
    info.offset_metadata = read_offset();
    if (*version >= 15U) {
      info.offset_edge_batches = read_offset();
      info.offset_vertex_batches = read_offset();
    } else {
      info.offset_edge_batches = 0U;
      info.offset_vertex_batches = 0U;
    }
  }

  // Read metadata.
  {
    if (!snapshot.SetPosition(info.offset_metadata)) throw RecoveryFailure("Couldn't read metadata offset!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_METADATA)
      throw RecoveryFailure("Couldn't read marker for section metadata!");

    auto maybe_uuid = snapshot.ReadString();
    if (!maybe_uuid) throw RecoveryFailure("Couldn't read storage_uuid!");
    info.uuid = std::move(*maybe_uuid);

    auto maybe_epoch_id = snapshot.ReadString();
    if (!maybe_epoch_id) throw RecoveryFailure("Couldn't read epoch id!");
    info.epoch_id = std::move(*maybe_epoch_id);

    auto maybe_timestamp = snapshot.ReadUint();
    if (!maybe_timestamp) throw RecoveryFailure("Couldn't read start timestamp!");
    info.start_timestamp = *maybe_timestamp;

    auto maybe_durable_timestamp = snapshot.ReadUint();
    if (!maybe_durable_timestamp) throw RecoveryFailure("Couldn't read durable timestamp!");
    info.durable_timestamp = *maybe_durable_timestamp;

    auto maybe_edges = snapshot.ReadUint();
    if (!maybe_edges) throw RecoveryFailure("Couldn't read the number of edges!");
    info.edges_count = *maybe_edges;

    auto maybe_vertices = snapshot.ReadUint();
    if (!maybe_vertices) throw RecoveryFailure("Couldn't read the number of vertices!");
    info.vertices_count = *maybe_vertices;
  }

  return info;
}

std::vector<BatchInfo> ReadBatchInfos(Decoder &snapshot) {
  std::vector<BatchInfo> infos;
  const auto infos_size = snapshot.ReadUint();
  if (!infos_size.has_value()) {
    throw RecoveryFailure("Couldn't read number of batch infos!");
  }
  infos.reserve(*infos_size);

  for (auto i{0U}; i < *infos_size; ++i) {
    const auto offset = snapshot.ReadUint();
    if (!offset.has_value()) {
      throw RecoveryFailure("Couldn't read batch info offset!");
    }

    const auto count = snapshot.ReadUint();
    if (!count.has_value()) {
      throw RecoveryFailure("Couldn't read batch info count!");
    }
    infos.push_back(BatchInfo{*offset, *count});
  }
  return infos;
}

template <typename TFunc>
void LoadPartialEdges(const std::filesystem::path &path, utils::SkipList<Edge> &edges, const uint64_t from_offset,
                      const uint64_t edges_count, const SalientConfig::Items items, TFunc get_property_from_id,
                      NameIdMapper *name_id_mapper,
                      std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) {
  Decoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic);

  // Recover edges.
  auto edge_acc = edges.access();
  uint64_t last_edge_gid = 0;
  spdlog::info("Recovering {} edges.", edges_count);
  if (!snapshot.SetPosition(from_offset)) throw RecoveryFailure("Couldn't set offset position for reading edges!");

  std::vector<std::pair<PropertyId, PropertyValue>> read_properties;
  uint64_t five_percent_chunk = edges_count / 20;
  if (five_percent_chunk == 0) {
    spdlog::debug("Started to recover edge set <0 - {}>", edges_count);
  } else {
    spdlog::debug("Started to recover edge set <0 - {}>.", 0 + five_percent_chunk);
  }

  uint64_t percentage_delta = 0;
  for (uint64_t i = 0; i < edges_count; ++i) {
    if (five_percent_chunk != 0) {
      if (i > 0 && i % five_percent_chunk == 0 && percentage_delta != 100) {
        percentage_delta += 5;
        spdlog::info("Recovered {}% of edges.", percentage_delta);
        if (percentage_delta == 95)
          spdlog::debug("Started to recover edge set <{} - {}>", i, edges_count);
        else if (percentage_delta != 100)
          spdlog::debug("Started to recover edge set <{} - {}>", i, i + five_percent_chunk);
      }
    }

    {
      const auto marker = snapshot.ReadMarker();
      if (!marker || *marker != Marker::SECTION_EDGE) throw RecoveryFailure("Couldn't read section edge marker!");
    }
    // Read edge GID.
    auto gid = snapshot.ReadUint();
    if (!gid) throw RecoveryFailure("Failed to read edge gid!");
    if (i > 0 && *gid <= last_edge_gid) throw RecoveryFailure("Invalid edge gid read!");
    last_edge_gid = *gid;

    if (items.properties_on_edges) {
      auto [it, inserted] = edge_acc.insert(Edge{Gid::FromUint(*gid), nullptr});
      if (!inserted) throw RecoveryFailure("The edge must be inserted here!");

      // Recover properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Couldn't read the size of edge properties!");
        auto &props = it->properties;
        read_properties.clear();
        read_properties.reserve(*props_size);
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Couldn't read edge property id!");
          auto value = snapshot.ReadExternalPropertyValue();
          if (!value) throw RecoveryFailure("Couldn't read edge property value!");
          read_properties.emplace_back(get_property_from_id(*key), ToPropertyValue(*value, name_id_mapper));
        }
        props.InitProperties(std::move(read_properties));
      }
    } else {
      spdlog::debug("Ensuring edge {} doesn't have any properties.", *gid);
      // Read properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Couldn't read size of edge properties!");
        if (*props_size != 0)
          throw RecoveryFailure(
              "The snapshot has properties on edges, but the storage is "
              "configured without properties on edges!");
      }
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::EDGES);
    }
  }
  spdlog::info("Process of recovering {} edges is finished.", edges_count);
}

// Returns the gid of the last recovered vertex
template <typename TLabelFromIdFunc, typename TPropertyFromIdFunc>
uint64_t LoadPartialVertices(const std::filesystem::path &path, utils::SkipList<Vertex> &vertices,
                             SharedSchemaTracking *schema_info, const uint64_t from_offset,
                             const uint64_t vertices_count, TLabelFromIdFunc get_label_from_id,
                             TPropertyFromIdFunc get_property_from_id, NameIdMapper *name_id_mapper,
                             std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) {
  Decoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic);
  if (!snapshot.SetPosition(from_offset))
    throw RecoveryFailure("Couldn't set offset for reading vertices from a snapshot!");

  auto vertex_acc = vertices.access();
  uint64_t last_vertex_gid = 0;
  spdlog::info("Recovering {} vertices.", vertices_count);
  std::vector<std::pair<PropertyId, PropertyValue>> read_properties;
  uint64_t five_percent_chunk = vertices_count / 20;
  if (five_percent_chunk == 0) {
    spdlog::debug("Started to recover vertex set <0 - {}>", vertices_count);
  } else {
    spdlog::debug("Started to recover vertex set <0 - {}>", 0 + five_percent_chunk);
  }

  uint64_t percentage_delta = 0;
  for (uint64_t i = 0; i < vertices_count; ++i) {
    if (five_percent_chunk != 0) {
      if (i > 0 && i % five_percent_chunk == 0 && percentage_delta != 100) {
        percentage_delta += 5;
        spdlog::info("Recovered {}% of vertices.", percentage_delta);
        if (percentage_delta == 95)
          spdlog::debug("Started to recover vertex set <{} - {}>", i, vertices_count);
        else if (percentage_delta != 100)
          spdlog::debug("Started to recover vertex set <{} - {}>", i, i + five_percent_chunk);
      }
    }
    {
      auto marker = snapshot.ReadMarker();
      if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Couldn't read section vertex marker!");
    }

    // Insert vertex.
    auto gid = snapshot.ReadUint();
    if (!gid) throw RecoveryFailure("Couldn't read vertex gid!");
    if (i > 0 && *gid <= last_vertex_gid) {
      throw RecoveryFailure("Read vertex gid is invalid!");
    }
    last_vertex_gid = *gid;
    auto [it, inserted] = vertex_acc.insert(Vertex{Gid::FromUint(*gid), nullptr});
    if (!inserted) throw RecoveryFailure("The vertex must be inserted here!");

    // Recover labels.
    {
      auto labels_size = snapshot.ReadUint();
      if (!labels_size) throw RecoveryFailure("Couldn't read the size of vertex labels!");
      auto &labels = it->labels;
      labels.reserve(*labels_size);
      for (uint64_t j = 0; j < *labels_size; ++j) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read vertex label!");
        labels.emplace_back(get_label_from_id(*label));
      }
    }

    // Recover properties.
    {
      auto props_size = snapshot.ReadUint();
      if (!props_size) throw RecoveryFailure("Couldn't read size of vertex properties!");
      if (*props_size != 0) {
        read_properties.clear();
        read_properties.reserve(*props_size);
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Couldn't read vertex property id!");
          auto value = snapshot.ReadExternalPropertyValue();
          if (!value) throw RecoveryFailure("Couldn't read vertex property value!");
          read_properties.emplace_back(get_property_from_id(*key), ToPropertyValue(*value, name_id_mapper));
        }
        it->properties.InitProperties(std::move(read_properties));
      }
    }

    // Update schema info
    if (schema_info) schema_info->RecoverVertex(&*it);

    // Skip in edges.
    {
      auto in_size = snapshot.ReadUint();
      if (!in_size) throw RecoveryFailure("Couldn't read the number of in edges!");
      for (uint64_t j = 0; j < *in_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Couldn't read edge gid!");
        auto from_gid = snapshot.ReadUint();
        if (!from_gid) throw RecoveryFailure("Couldn't read from vertex gid!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read in edge type!");
      }
    }

    // Skip out edges.
    auto out_size = snapshot.ReadUint();
    if (!out_size) throw RecoveryFailure("Couldn't read the number of out edges!");
    for (uint64_t j = 0; j < *out_size; ++j) {
      auto edge_gid = snapshot.ReadUint();
      if (!edge_gid) throw RecoveryFailure("Couldn't read edge gid!");
      auto to_gid = snapshot.ReadUint();
      if (!to_gid) throw RecoveryFailure("Couldn't read to vertex gid!");
      auto edge_type = snapshot.ReadUint();
      if (!edge_type) throw RecoveryFailure("Couldn't read out edge type!");
    }
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::VERTICES);
    }
  }
  spdlog::info("Process of recovering {} vertices is finished.", vertices_count);

  return last_vertex_gid;
}

// Returns the number of edges recovered

struct LoadPartialConnectivityResult {
  uint64_t edge_count;
  uint64_t highest_edge_id;
  Gid first_vertex_gid;
};

template <typename TEdgeTypeFromIdFunc>
LoadPartialConnectivityResult LoadPartialConnectivity(
    const std::filesystem::path &path, utils::SkipList<Vertex> &vertices, utils::SkipList<Edge> &edges,
    utils::SkipList<EdgeMetadata> &edges_metadata, SharedSchemaTracking *schema_info, const uint64_t from_offset,
    const uint64_t vertices_count, const SalientConfig::Items items, const bool snapshot_has_edges,
    TEdgeTypeFromIdFunc get_edge_type_from_id, NameIdMapper *name_id_mapper,
    std::optional<SnapshotObserverInfo> const &snapshot_info = std::nullopt) {
  Decoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic);
  if (!snapshot.SetPosition(from_offset))
    throw RecoveryFailure("Couldn't set snapshot offset position doing loading partial connectivity!");

  auto vertex_acc = vertices.access();
  auto edge_acc = edges.access();
  auto edge_metadata_acc = edges_metadata.access();

  // Read the first gid to find the necessary iterator in vertices
  const auto first_vertex_gid = std::invoke([&]() mutable {
    {
      auto marker = snapshot.ReadMarker();
      if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Couldn't set section vertex marker!");
    }

    auto gid = snapshot.ReadUint();
    if (!gid) throw RecoveryFailure("Couldn't read vertex gid!");
    return Gid::FromUint(*gid);
  });

  uint64_t edge_count{0};
  uint64_t highest_edge_gid{0};
  auto vertex_it = vertex_acc.find(first_vertex_gid);
  if (vertex_it == vertex_acc.end()) {
    throw RecoveryFailure("Couldn't find vertex with first vertex gid!");
  }

  spdlog::info("Recovering connectivity for {} vertices.", vertices_count);

  if (!snapshot.SetPosition(from_offset)) throw RecoveryFailure("Couldn't set from_offset position!");

  uint64_t five_percent_chunk = vertices_count / 20;

  if (five_percent_chunk == 0) {
    spdlog::debug("Started to recover vertices connectivity set <0 - {}>", vertices_count);
  } else {
    spdlog::debug("Started to recover vertices connectivity set <0 - {}>", 0 + five_percent_chunk);
  }

  uint64_t percentage_delta = 0;
  for (uint64_t i = 0; i < vertices_count; ++i) {
    if (five_percent_chunk != 0) {
      if (i > 0 && i % five_percent_chunk == 0 && percentage_delta != 100) {
        percentage_delta += 5;
        spdlog::info("Recovered {}% of vertices connectivity.", percentage_delta);
        if (percentage_delta == 95)
          spdlog::debug("Started to recover vertices connectivity set <{} - {}>", i, vertices_count);
        else if (percentage_delta != 100)
          spdlog::debug("Started to recover vertices connectivity set <{} - {}>", i, i + five_percent_chunk);
      }
    }

    auto &vertex = *vertex_it;
    {
      auto marker = snapshot.ReadMarker();
      if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Couldn't read section vertex marker!");
    }

    auto gid = snapshot.ReadUint();
    if (!gid) throw RecoveryFailure("Couldn't read vertex gid!");
    if (gid != vertex.gid.AsUint()) throw RecoveryFailure("Read vertex gid is different from the existing one!");

    // Skip labels.
    {
      auto labels_size = snapshot.ReadUint();
      if (!labels_size) throw RecoveryFailure("Couldn't read the number of labels!");
      for (uint64_t j = 0; j < *labels_size; ++j) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label!");
      }
    }

    // Skip properties.
    {
      auto props_size = snapshot.ReadUint();
      if (!props_size) throw RecoveryFailure("Couldn't read the number of vertex properties!");
      for (uint64_t j = 0; j < *props_size; ++j) {
        auto key = snapshot.ReadUint();
        if (!key) throw RecoveryFailure("Couldn't read vertex property id!");
        auto value = snapshot.SkipExternalPropertyValue();
        if (!value) throw RecoveryFailure("Couldn't read vertex property value!");
      }
    }

    // Recover in edges.
    {
      auto in_size = snapshot.ReadUint();
      if (!in_size) throw RecoveryFailure("Couldn't read the number of in edges!");
      vertex.in_edges.reserve(*in_size);
      for (uint64_t j = 0; j < *in_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Couldn't read the edge gid!");
        highest_edge_gid = std::max(highest_edge_gid, *edge_gid);

        auto from_gid = snapshot.ReadUint();
        if (!from_gid) throw RecoveryFailure("Couldn't read from vertex gid!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge type!");

        auto from_vertex = vertex_acc.find(Gid::FromUint(*from_gid));
        if (from_vertex == vertex_acc.end()) throw RecoveryFailure("Couldn't find from vertex in loaded vertices!");

        EdgeRef edge_ref(Gid::FromUint(*edge_gid));
        if (items.properties_on_edges) {
          // The snapshot contains the individiual edges only if it was created with a config where properties are
          // allowed on edges. That means the snapshots that were created without edge properties will only contain the
          // edges in the in/out edges list of vertices, therefore the edges has to be created here.
          if (snapshot_has_edges) {
            auto edge = edge_acc.find(Gid::FromUint(*edge_gid));
            if (edge == edge_acc.end()) throw RecoveryFailure("Invalid edge!");
            edge_ref = EdgeRef(&*edge);
          } else {
            auto [edge, inserted] = edge_acc.insert(Edge{Gid::FromUint(*edge_gid), nullptr});
            edge_ref = EdgeRef(&*edge);
          }
        }
        vertex.in_edges.emplace_back(get_edge_type_from_id(*edge_type), &*from_vertex, edge_ref);
        if (snapshot_info) {
          snapshot_info->Update(UpdateType::EDGES);
        }
      }
    }

    // Recover out edges.
    {
      auto out_size = snapshot.ReadUint();
      if (!out_size) throw RecoveryFailure("Couldn't read the number of out edges!");
      vertex.out_edges.reserve(*out_size);
      for (uint64_t j = 0; j < *out_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Couldn't read edge gid!");

        auto to_gid = snapshot.ReadUint();
        if (!to_gid) throw RecoveryFailure("Couldn't read to vertex gid!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge type!");

        auto to_vertex = vertex_acc.find(Gid::FromUint(*to_gid));
        if (to_vertex == vertex_acc.end()) throw RecoveryFailure("Couldn't find to vertex in loaded vertices!");

        EdgeRef edge_ref(Gid::FromUint(*edge_gid));
        if (items.properties_on_edges) {
          // The snapshot contains the individual edges only if it was created with a config where properties are
          // allowed on edges. That means the snapshots that were created without edge properties will only contain the
          // edges in the in/out edges list of vertices, therefore the edges has to be created here.
          if (snapshot_has_edges) {
            auto edge = edge_acc.find(Gid::FromUint(*edge_gid));
            if (edge == edge_acc.end()) throw RecoveryFailure("Couldn't find edge in the loaded edges!");
            edge_ref = EdgeRef(&*edge);
          } else {
            auto [edge, inserted] = edge_acc.insert(Edge{Gid::FromUint(*edge_gid), nullptr});
            edge_ref = EdgeRef(&*edge);
          }
          if (items.enable_edges_metadata) {
            edge_metadata_acc.insert(EdgeMetadata{Gid::FromUint(*edge_gid), &vertex});
          }
        }
        vertex.out_edges.emplace_back(get_edge_type_from_id(*edge_type), &*to_vertex, edge_ref);
        // Increment edge count. We only increment the count here because the
        // information is duplicated in in_edges.
        edge_count++;

        // Update schema info
        if (schema_info) {
          schema_info->RecoverEdge(get_edge_type_from_id(*edge_type), edge_ref, &vertex, &*to_vertex,
                                   items.properties_on_edges);
        }
        if (snapshot_info) {
          snapshot_info->Update(UpdateType::EDGES);
        }
      }
    }
    ++vertex_it;
  }
  spdlog::info("Process of recovering connectivity for {} vertices is finished.", vertices_count);

  return {edge_count, highest_edge_gid, first_vertex_gid};
}

template <typename TFunc>
void RecoverOnMultipleThreads(size_t thread_count, const TFunc &func, const std::vector<BatchInfo> &batches) {
  utils::Synchronized<std::optional<RecoveryFailure>, utils::SpinLock> maybe_error{};
  {
    std::atomic<uint64_t> batch_counter = 0;
    thread_count = std::min(thread_count, batches.size());
    std::vector<std::jthread> threads;
    threads.reserve(thread_count);

    for (auto i{0U}; i < thread_count; ++i) {
      threads.emplace_back([&func, &batches, &maybe_error, &batch_counter]() {
        while (!maybe_error.Lock()->has_value()) {
          const auto batch_index = batch_counter++;
          if (batch_index >= batches.size()) {
            return;
          }
          const auto &batch = batches[batch_index];
          try {
            func(batch_index, batch);
          } catch (RecoveryFailure &failure) {
            *maybe_error.Lock() = std::move(failure);
          }
        }
      });
    }
  }
  if (maybe_error.Lock()->has_value()) {
    throw RecoveryFailure((*maybe_error.Lock())->what());
  }
}

RecoveredSnapshot LoadSnapshotVersion14(Decoder &snapshot, const std::filesystem::path &path,
                                        utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                        utils::SkipList<EdgeMetadata> *edges_metadata,
                                        std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                        NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                        SharedSchemaTracking *schema_info, SalientConfig::Items items) {
  RecoveryInfo ret;
  RecoveredIndicesAndConstraints indices_constraints;

  // Cleanup of loaded data in case of failure.
  bool success = false;
  const utils::OnScopeExit cleanup([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      edges_metadata->clear();
      epoch_history->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  const bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Failed to read section mapper!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Failed to read name-id mapper size!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Failed to read id for name-id mapper!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Failed to read name for name-id mapper!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }
  auto get_label_from_id = [&snapshot_id_map](uint64_t label_id) {
    auto it = snapshot_id_map.find(label_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find label id in snapshot_id_map!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t property_id) {
    auto it = snapshot_id_map.find(property_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find property id in snapshot_id_map!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t edge_type_id) {
    auto it = snapshot_id_map.find(edge_type_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find edge type id in snapshot_id_map!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
    // Recover edges.
    auto edge_acc = edges->access();
    uint64_t last_edge_gid = 0;
    if (snapshot_has_edges) {
      spdlog::info("Recovering {} edges.", info.edges_count);
      if (!snapshot.SetPosition(info.offset_edges)) throw RecoveryFailure("Couldn't read data from snapshot!");
      for (uint64_t i = 0; i < info.edges_count; ++i) {
        {
          const auto marker = snapshot.ReadMarker();
          if (!marker || *marker != Marker::SECTION_EDGE) throw RecoveryFailure("Couldn't read section edge marker");
        }

        if (items.properties_on_edges) {
          // Insert edge.
          auto gid = snapshot.ReadUint();
          if (!gid) throw RecoveryFailure("Couldn't read edge gid!");
          if (i > 0 && *gid <= last_edge_gid) throw RecoveryFailure("Invalid edge gid read!");
          last_edge_gid = *gid;
          spdlog::debug("Recovering edge {} with properties.", *gid);
          auto [it, inserted] = edge_acc.insert(Edge{Gid::FromUint(*gid), nullptr});
          if (!inserted) throw RecoveryFailure("The edge must be inserted here!");

          // Recover properties.
          {
            auto props_size = snapshot.ReadUint();
            if (!props_size) throw RecoveryFailure("Couldn't read the size of properties!");
            auto &props = it->properties;
            for (uint64_t j = 0; j < *props_size; ++j) {
              auto key = snapshot.ReadUint();
              if (!key) throw RecoveryFailure("Couldn't read edge property id!");
              auto value = snapshot.ReadExternalPropertyValue();
              if (!value) throw RecoveryFailure("Couldn't read edge property value!");
              SPDLOG_TRACE("Recovered property \"{}\" with value \"{}\" for edge {}.",
                           name_id_mapper->IdToName(snapshot_id_map.at(*key)), *value, *gid);
              props.SetProperty(get_property_from_id(*key), ToPropertyValue(*value, name_id_mapper));
            }
          }
        } else {
          // Read edge GID.
          auto gid = snapshot.ReadUint();
          if (!gid) throw RecoveryFailure("Couldn't read edge gid!");
          if (i > 0 && *gid <= last_edge_gid) throw RecoveryFailure("Invalid edge gid read!");
          last_edge_gid = *gid;

          spdlog::debug("Ensuring edge {} doesn't have any properties.", *gid);
          // Read properties.
          {
            auto props_size = snapshot.ReadUint();
            if (!props_size) throw RecoveryFailure("Couldn't read the size of properties!");
            if (*props_size != 0)
              throw RecoveryFailure(
                  "The snapshot has properties on edges, but the storage is "
                  "configured without properties on edges!");
          }
        }
      }
      spdlog::info("Edges are recovered.");
    }

    // Recover vertices (labels and properties).
    if (!snapshot.SetPosition(info.offset_vertices)) throw RecoveryFailure("Couldn't read data from snapshot!");
    auto vertex_acc = vertices->access();
    uint64_t last_vertex_gid = 0;
    spdlog::info("Recovering {} vertices.", info.vertices_count);
    for (uint64_t i = 0; i < info.vertices_count; ++i) {
      {
        auto marker = snapshot.ReadMarker();
        if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Couldn't read section vertex marker!");
      }

      // Insert vertex.
      auto gid = snapshot.ReadUint();
      if (!gid) throw RecoveryFailure("Couldn't read vertex gid!");
      if (i > 0 && *gid <= last_vertex_gid) {
        throw RecoveryFailure("Invalid vertex gid read!");
      }
      last_vertex_gid = *gid;
      spdlog::debug("Recovering vertex {}.", *gid);
      auto [it, inserted] = vertex_acc.insert(Vertex{Gid::FromUint(*gid), nullptr});
      if (!inserted) throw RecoveryFailure("The vertex must be inserted here!");

      // Recover labels.
      spdlog::trace("Recovering labels for vertex {}.", *gid);
      {
        auto labels_size = snapshot.ReadUint();
        if (!labels_size) throw RecoveryFailure("Couldn't read the size of labels!");
        auto &labels = it->labels;
        labels.reserve(*labels_size);
        for (uint64_t j = 0; j < *labels_size; ++j) {
          auto label = snapshot.ReadUint();
          if (!label) throw RecoveryFailure("Couldn't read vertex label!");
          SPDLOG_TRACE("Recovered label \"{}\" for vertex {}.", name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                       *gid);
          labels.emplace_back(get_label_from_id(*label));
        }
      }

      // Recover properties.
      spdlog::trace("Recovering properties for vertex {}.", *gid);
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Couldn't read the size of properties!");
        auto &props = it->properties;
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Couldn't read the vertex property id!");
          auto value = snapshot.ReadExternalPropertyValue();
          if (!value) throw RecoveryFailure("Couldn't read the vertex property value!");
          SPDLOG_TRACE("Recovered property \"{}\" with value \"{}\" for vertex {}.",
                       name_id_mapper->IdToName(snapshot_id_map.at(*key)), *value, *gid);
          props.SetProperty(get_property_from_id(*key), ToPropertyValue(*value, name_id_mapper));
        }
      }

      // Update schema info
      if (schema_info) schema_info->RecoverVertex(&*it);

      // Skip in edges.
      {
        auto in_size = snapshot.ReadUint();
        if (!in_size) throw RecoveryFailure("Couldn't the read the size of input edges!");
        for (uint64_t j = 0; j < *in_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Couldn't read the edge gid!");
          auto from_gid = snapshot.ReadUint();
          if (!from_gid) throw RecoveryFailure("Couldn't read from vertex gid!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Couldn't read edge type!");
        }
      }

      // Skip out edges.
      auto out_size = snapshot.ReadUint();
      if (!out_size) throw RecoveryFailure("Couldn't read the number of out edges!");
      for (uint64_t j = 0; j < *out_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Couldn't read the edge gid!");
        auto to_gid = snapshot.ReadUint();
        if (!to_gid) throw RecoveryFailure("Couldn't read to vertex gid!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge type!");
      }
    }
    spdlog::info("Vertices are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recovering connectivity.");
    if (!snapshot.SetPosition(info.offset_vertices)) throw RecoveryFailure("Couldn't read data from snapshot!");
    for (auto &vertex : vertex_acc) {
      {
        auto marker = snapshot.ReadMarker();
        if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Couldn't read section vertex marker!");
      }

      spdlog::trace("Recovering connectivity for vertex {}.", vertex.gid.AsUint());
      // Check vertex.
      auto gid = snapshot.ReadUint();
      if (!gid) throw RecoveryFailure("Couldn't read vertex gid!");
      if (gid != vertex.gid.AsUint()) throw RecoveryFailure("Invalid vertex read!");

      // Skip labels.
      {
        auto labels_size = snapshot.ReadUint();
        if (!labels_size) throw RecoveryFailure("Couldn't read the size of labels!");
        for (uint64_t j = 0; j < *labels_size; ++j) {
          auto label = snapshot.ReadUint();
          if (!label) throw RecoveryFailure("Couldn't read label!");
        }
      }

      // Skip properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Couldn't read the size of properties!");
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Couldn't read property key while skipping properties!");
          auto value = snapshot.SkipExternalPropertyValue();
          if (!value) throw RecoveryFailure("Couldn't read property value while skipping properties!");
        }
      }

      // Recover in edges.
      {
        spdlog::trace("Recovering inbound edges for vertex {}.", vertex.gid.AsUint());
        auto in_size = snapshot.ReadUint();
        if (!in_size) throw RecoveryFailure("Couldn't read the size of in edges!");
        vertex.in_edges.reserve(*in_size);
        for (uint64_t j = 0; j < *in_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Couldn't read egde gid!");
          last_edge_gid = std::max(last_edge_gid, *edge_gid);

          auto from_gid = snapshot.ReadUint();
          if (!from_gid) throw RecoveryFailure("Couldn't read from vertex gid!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Couldn't read edge type");

          auto from_vertex = vertex_acc.find(Gid::FromUint(*from_gid));
          if (from_vertex == vertex_acc.end()) throw RecoveryFailure("Invalid from vertex!");

          EdgeRef edge_ref(Gid::FromUint(*edge_gid));
          if (items.properties_on_edges) {
            if (snapshot_has_edges) {
              auto edge = edge_acc.find(Gid::FromUint(*edge_gid));
              if (edge == edge_acc.end()) throw RecoveryFailure("Invalid edge!");
              edge_ref = EdgeRef(&*edge);
            } else {
              auto [edge, inserted] = edge_acc.insert(Edge{Gid::FromUint(*edge_gid), nullptr});
              edge_ref = EdgeRef(&*edge);
            }
          }
          SPDLOG_TRACE("Recovered inbound edge {} with label \"{}\" from vertex {}.", *edge_gid,
                       name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)), from_vertex->gid.AsUint());
          vertex.in_edges.emplace_back(get_edge_type_from_id(*edge_type), &*from_vertex, edge_ref);
        }
      }

      // Recover out edges.
      {
        spdlog::trace("Recovering outbound edges for vertex {}.", vertex.gid.AsUint());
        auto out_size = snapshot.ReadUint();
        if (!out_size) throw RecoveryFailure("Couldn't read the number of out edges!");
        vertex.out_edges.reserve(*out_size);
        for (uint64_t j = 0; j < *out_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Couldn't read edge gid!");
          last_edge_gid = std::max(last_edge_gid, *edge_gid);

          auto to_gid = snapshot.ReadUint();
          if (!to_gid) throw RecoveryFailure("Couldn't read to vertex gid!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Couldn't read edge type!");

          auto to_vertex = vertex_acc.find(Gid::FromUint(*to_gid));
          if (to_vertex == vertex_acc.end()) throw RecoveryFailure("Invalid to vertex!");

          EdgeRef edge_ref(Gid::FromUint(*edge_gid));
          if (items.properties_on_edges) {
            if (snapshot_has_edges) {
              auto edge = edge_acc.find(Gid::FromUint(*edge_gid));
              if (edge == edge_acc.end()) throw RecoveryFailure("Invalid edge!");
              edge_ref = EdgeRef(&*edge);
            } else {
              auto [edge, inserted] = edge_acc.insert(Edge{Gid::FromUint(*edge_gid), nullptr});
              edge_ref = EdgeRef(&*edge);
            }
          }
          SPDLOG_TRACE("Recovered outbound edge {} with label \"{}\" to vertex {}.", *edge_gid,
                       name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)), to_vertex->gid.AsUint());
          vertex.out_edges.emplace_back(get_edge_type_from_id(*edge_type), &*to_vertex, edge_ref);

          // Update schema info
          if (schema_info)
            schema_info->RecoverEdge(get_edge_type_from_id(*edge_type), edge_ref, &vertex, &*to_vertex,
                                     items.properties_on_edges);
        }
        // Increment edge count. We only increment the count here because the
        // information is duplicated in in_edges.
        edge_count->fetch_add(*out_size, std::memory_order_acq_rel);
      }
    }
    spdlog::info("Connectivity is recovered.");

    // Set initial values for edge/vertex ID generators.
    ret.next_edge_id = last_edge_gid + 1;
    ret.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Couldn't read section indices!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of label indices");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of label index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of label property indices!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label_properties,
                                    {get_label_from_id(*label), {get_property_from_id(*property)}},
                                    "The label+property index already exists!");
        SPDLOG_TRACE("Recovered metadata of label+property index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    spdlog::info("Metadata of indices are recovered.");
  }

  // Recover constraints.
  {
    spdlog::info("Recovering metadata of constraints.");
    if (!snapshot.SetPosition(info.offset_constraints)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Couldn't read section constraints marker!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of existence constraints!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of existence constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of existence constraints!");
        AddRecoveredIndexConstraint(&indices_constraints.constraints.existence,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The existence constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of existence constraint for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of existence constraints are recovered.");
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of unique constraints!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of unique constraint!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Couldn't read the number of properties of unique constraint");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property of unique constraint!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique, {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of unique constraints for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of unique constraints are recovered.");
    }
    spdlog::info("Metadata of constraints are recovered.");
  }

  spdlog::info("Recovering metadata.");
  // Recover epoch history
  {
    if (!snapshot.SetPosition(info.offset_epoch_history)) throw RecoveryFailure("Couldn't read data from snapshot!");

    const auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY)
      throw RecoveryFailure("Couldn't recover section epoch history marker!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Couldn't read history size!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Couldn't read epoch id!");
      }
      const auto maybe_last_durable_timestamp = snapshot.ReadUint();
      if (!maybe_last_durable_timestamp) {
        throw RecoveryFailure("Couldn't read last durable timestamp!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_durable_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  ret.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, ret, std::move(indices_constraints)};
}

RecoveredSnapshot LoadSnapshotVersion15(Decoder &snapshot, const std::filesystem::path &path,
                                        utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                        utils::SkipList<EdgeMetadata> *edges_metadata,
                                        std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                        NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                        SharedSchemaTracking *schema_info, const Config &config) {
  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;

  // Cleanup of loaded data in case of failure.
  bool success = false;
  const utils::OnScopeExit cleanup([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      edges_metadata->clear();
      epoch_history->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  const bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Couldn't read section mapper marker!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Couldn't read snapshot size!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Failed to read id for name-id mapper!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Failed to read name for name-id mapper!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }
  auto get_label_from_id = [&snapshot_id_map](uint64_t label_id) {
    auto it = snapshot_id_map.find(label_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find label id in snapshot_id_map!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t property_id) {
    auto it = snapshot_id_map.find(property_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find property id in snapshot_id_map!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t edge_type_id) {
    auto it = snapshot_id_map.find(edge_type_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find edge type id in snapshot_id_map!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
    // Recover vertices (labels and properties).
    spdlog::info("Recovering vertices.", info.vertices_count);
    uint64_t last_vertex_gid{0};

    if (!snapshot.SetPosition(info.offset_vertex_batches)) {
      throw RecoveryFailure("Couldn't read data from snapshot!");
    }

    const auto vertex_batches = ReadBatchInfos(snapshot);
    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, schema_info, &vertex_batches, &get_label_from_id, &get_property_from_id, &last_vertex_gid,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto last_vertex_gid_in_batch =
              LoadPartialVertices(path, *vertices, schema_info, batch.offset, batch.count, get_label_from_id,
                                  get_property_from_id, name_id_mapper);
          if (batch_index == vertex_batches.size() - 1) {
            last_vertex_gid = last_vertex_gid_in_batch;
          }
        },
        vertex_batches);

    spdlog::info("Vertices are recovered.");

    spdlog::info("Recovering edges.");
    // Recover edges.
    if (snapshot_has_edges) {
      // We don't need to check whether we store properties on edge or not, because `LoadPartialEdges` will always
      // iterate over the edges in the snapshot (if they exist) and the current configuration of properties on edge only
      // affect what it does:
      // 1. If properties are allowed on edges, then it loads the edges.
      // 2. If properties are not allowed on edges, then it checks that none of the edges have any properties.
      if (!snapshot.SetPosition(info.offset_edge_batches)) {
        throw RecoveryFailure("Couldn't read data from snapshot!");
      }
      const auto edge_batches = ReadBatchInfos(snapshot);

      RecoverOnMultipleThreads(
          config.durability.recovery_thread_count,
          [path, edges, items = config.salient.items, &get_property_from_id, name_id_mapper](
              const size_t /*batch_index*/, const BatchInfo &batch) {
            LoadPartialEdges(path, *edges, batch.offset, batch.count, items, get_property_from_id, name_id_mapper);
          },
          edge_batches);
    }
    spdlog::info("Edges are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recover connectivity.");
    recovery_info.vertex_batches.reserve(vertex_batches.size());
    for (const auto batch : vertex_batches) {
      recovery_info.vertex_batches.emplace_back(Gid::FromUint(0), batch.count);
    }
    std::atomic<uint64_t> highest_edge_gid{0};

    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, edges, edges_metadata, schema_info, edge_count, items = config.salient.items,
         snapshot_has_edges, &get_edge_type_from_id, &highest_edge_gid, &recovery_info,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto result =
              LoadPartialConnectivity(path, *vertices, *edges, *edges_metadata, schema_info, batch.offset, batch.count,
                                      items, snapshot_has_edges, get_edge_type_from_id, name_id_mapper);
          edge_count->fetch_add(result.edge_count);
          auto known_highest_edge_gid = highest_edge_gid.load();
          while (known_highest_edge_gid < result.highest_edge_id) {
            highest_edge_gid.compare_exchange_weak(known_highest_edge_gid, result.highest_edge_id);
          }
          recovery_info.vertex_batches[batch_index].first = result.first_vertex_gid;
        },
        vertex_batches);

    spdlog::info("Connectivity is recovered.");

    // Set initial values for edge/vertex ID generators.
    recovery_info.next_edge_id = highest_edge_gid + 1;
    recovery_info.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Couldn't read section indices!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of label indices");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of label index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of label property indices!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index");

        AddRecoveredIndexConstraint(&indices_constraints.indices.label_properties,
                                    {get_label_from_id(*label), {get_property_from_id(*property)}},
                                    "The label+property index already exists!");
        SPDLOG_TRACE("Recovered metadata of label+property index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }
    spdlog::info("Metadata of indices are recovered.");
  }

  // Recover constraints.
  {
    spdlog::info("Recovering metadata of constraints.");
    if (!snapshot.SetPosition(info.offset_constraints))
      throw RecoveryFailure("Couldn't read offset constraints marker!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Couldn't read section constraints marker!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of existence constraints!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of existence constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of existence constraints!");
        AddRecoveredIndexConstraint(&indices_constraints.constraints.existence,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The existence constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of existence constraint for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of existence constraints are recovered.");
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of unique constraints!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of unique constraint!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Couldn't read the number of properties of unique constraint");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property of unique constraint!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique, {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of unique constraints for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of unique constraints are recovered.");
    }
    spdlog::info("Metadata of constraints are recovered.");
  }

  spdlog::info("Recovering metadata.");
  // Recover epoch history
  {
    if (!snapshot.SetPosition(info.offset_epoch_history)) throw RecoveryFailure("Couldn't read data from snapshot!");

    const auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY)
      throw RecoveryFailure("Couldn't recover section epoch history marker!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Couldn't read history size!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Couldn't read epoch id!");
      }
      const auto maybe_last_durable_timestamp = snapshot.ReadUint();
      if (!maybe_last_durable_timestamp) {
        throw RecoveryFailure("Couldn't read last durable timestamp!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_durable_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  recovery_info.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, recovery_info, std::move(indices_constraints)};
}

RecoveredSnapshot LoadSnapshotVersion16(Decoder &snapshot, const std::filesystem::path &path,
                                        utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                        utils::SkipList<EdgeMetadata> *edges_metadata,
                                        std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                        NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                        SharedSchemaTracking *schema_info, const Config &config) {
  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;

  // Cleanup of loaded data in case of failure.
  bool success = false;
  const utils::OnScopeExit cleanup([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      edges_metadata->clear();
      epoch_history->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  const bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Failed to read section mapper!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Failed to read name-id mapper size!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Failed to read id for name-id mapper!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Failed to read name for name-id mapper!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }
  auto get_label_from_id = [&snapshot_id_map](uint64_t label_id) {
    auto it = snapshot_id_map.find(label_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find label id in snapshot_id_map!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t property_id) {
    auto it = snapshot_id_map.find(property_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find property id in snapshot_id_map!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t edge_type_id) {
    auto it = snapshot_id_map.find(edge_type_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find edge type id in snapshot_id_map!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
    // Recover vertices (labels and properties).
    spdlog::info("Recovering vertices.", info.vertices_count);
    uint64_t last_vertex_gid{0};

    if (!snapshot.SetPosition(info.offset_vertex_batches)) {
      throw RecoveryFailure("Couldn't read data from snapshot!");
    }

    const auto vertex_batches = ReadBatchInfos(snapshot);
    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, schema_info, &vertex_batches, &get_label_from_id, &get_property_from_id, &last_vertex_gid,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto last_vertex_gid_in_batch =
              LoadPartialVertices(path, *vertices, schema_info, batch.offset, batch.count, get_label_from_id,
                                  get_property_from_id, name_id_mapper);
          if (batch_index == vertex_batches.size() - 1) {
            last_vertex_gid = last_vertex_gid_in_batch;
          }
        },
        vertex_batches);

    spdlog::info("Vertices are recovered.");

    spdlog::info("Recovering edges.");
    // Recover edges.
    if (snapshot_has_edges) {
      // We don't need to check whether we store properties on edge or not, because `LoadPartialEdges` will always
      // iterate over the edges in the snapshot (if they exist) and the current configuration of properties on edge only
      // affect what it does:
      // 1. If properties are allowed on edges, then it loads the edges.
      // 2. If properties are not allowed on edges, then it checks that none of the edges have any properties.
      if (!snapshot.SetPosition(info.offset_edge_batches)) {
        throw RecoveryFailure("Couldn't read data from snapshot!");
      }
      const auto edge_batches = ReadBatchInfos(snapshot);

      RecoverOnMultipleThreads(
          config.durability.recovery_thread_count,
          [path, edges, items = config.salient.items, &get_property_from_id, name_id_mapper](
              const size_t /*batch_index*/, const BatchInfo &batch) {
            LoadPartialEdges(path, *edges, batch.offset, batch.count, items, get_property_from_id, name_id_mapper);
          },
          edge_batches);
    }
    spdlog::info("Edges are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recover connectivity.");
    recovery_info.vertex_batches.reserve(vertex_batches.size());
    for (const auto batch : vertex_batches) {
      recovery_info.vertex_batches.emplace_back(Gid::FromUint(0), batch.count);
    }
    std::atomic<uint64_t> highest_edge_gid{0};

    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, edges, edges_metadata, schema_info, edge_count, items = config.salient.items,
         snapshot_has_edges, &get_edge_type_from_id, &highest_edge_gid, &recovery_info,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto result =
              LoadPartialConnectivity(path, *vertices, *edges, *edges_metadata, schema_info, batch.offset, batch.count,
                                      items, snapshot_has_edges, get_edge_type_from_id, name_id_mapper);
          edge_count->fetch_add(result.edge_count);
          auto known_highest_edge_gid = highest_edge_gid.load();
          while (known_highest_edge_gid < result.highest_edge_id) {
            highest_edge_gid.compare_exchange_weak(known_highest_edge_gid, result.highest_edge_id);
          }
          recovery_info.vertex_batches[batch_index].first = result.first_vertex_gid;
        },
        vertex_batches);

    spdlog::info("Connectivity is recovered.");

    // Set initial values for edge/vertex ID generators.
    recovery_info.next_edge_id = highest_edge_gid + 1;
    recovery_info.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Couldn't read section indices!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of label indices");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of label index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of entries for label index statistics!");
      spdlog::info("Recovering metadata of {} label indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label while recovering label index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label index statistics");
        const auto label_id = get_label_from_id(*label);
        indices_constraints.indices.label_stats.emplace_back(label_id, LabelIndexStats{*count, *avg_degree});
        SPDLOG_TRACE("Recovered metadata of label index statistics for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of label property indices!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label_properties,
                                    {get_label_from_id(*label), {get_property_from_id(*property)}},
                                    "The label+property index already exists!");
        SPDLOG_TRACE("Recovered metadata of label+property index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    // Recover label+property indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of entries for label property statistics!");
      spdlog::info("Recovering metadata of {} label+property indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index statistics!");
        const auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label property index statistics!!");
        const auto distinct_values_count = snapshot.ReadUint();
        if (!distinct_values_count)
          throw RecoveryFailure("Couldn't read distinct values count for label property index statistics!");
        const auto statistic = snapshot.ReadDouble();
        if (!statistic) throw RecoveryFailure("Couldn't read statistics value for label-property index statistics!");
        const auto avg_group_size = snapshot.ReadDouble();
        if (!avg_group_size)
          throw RecoveryFailure("Couldn't read average group size for label property index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label property index statistics!");
        const auto label_id = get_label_from_id(*label);
        auto property_id = std::vector{get_property_from_id(*property)};
        indices_constraints.indices.label_property_stats.emplace_back(
            label_id,
            std::make_pair(std::move(property_id), LabelPropertyIndexStats{*count, *distinct_values_count, *statistic,
                                                                           *avg_group_size, *avg_degree}));
        SPDLOG_TRACE("Recovered metadata of label+property index statistics for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    spdlog::info("Metadata of indices are recovered.");
  }

  // Recover constraints.
  {
    spdlog::info("Recovering metadata of constraints.");
    if (!snapshot.SetPosition(info.offset_constraints)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Couldn't read section constraints marker!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of existence constraints!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of existence constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of existence constraints!");
        AddRecoveredIndexConstraint(&indices_constraints.constraints.existence,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The existence constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of existence constraint for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of existence constraints are recovered.");
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of unique constraints!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of unique constraints!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Couldn't read the number of properties in unique constraint!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property of unique constraint!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique, {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of unique constraints for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of unique constraints are recovered.");
    }
    spdlog::info("Metadata of constraints are recovered.");
  }

  spdlog::info("Recovering metadata.");
  // Recover epoch history
  {
    if (!snapshot.SetPosition(info.offset_epoch_history)) throw RecoveryFailure("Couldn't read data from snapshot!");

    const auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY)
      throw RecoveryFailure("Couldn't read section epoch history marker!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Couldn't read history size!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Couldn't read maybe epoch id!");
      }
      const auto maybe_last_durable_timestamp = snapshot.ReadUint();
      if (!maybe_last_durable_timestamp) {
        throw RecoveryFailure("Couldn't read maybe last durable timestamp!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_durable_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  recovery_info.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, recovery_info, std::move(indices_constraints)};
}

RecoveredSnapshot LoadSnapshotVersion17(Decoder &snapshot, const std::filesystem::path &path,
                                        utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                        utils::SkipList<EdgeMetadata> *edges_metadata,
                                        std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                        NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                        SharedSchemaTracking *schema_info, const Config &config) {
  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;

  // Cleanup of loaded data in case of failure.
  bool success = false;
  const utils::OnScopeExit cleanup([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      edges_metadata->clear();
      epoch_history->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  const bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Failed to read section mapper!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Failed to read name-id mapper size!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Failed to read id for name-id mapper!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Failed to read name for name-id mapper!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }
  auto get_label_from_id = [&snapshot_id_map](uint64_t label_id) {
    auto it = snapshot_id_map.find(label_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find label id in snapshot_id_map!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t property_id) {
    auto it = snapshot_id_map.find(property_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find property id in snapshot_id_map!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t edge_type_id) {
    auto it = snapshot_id_map.find(edge_type_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find edge type id in snapshot_id_map!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
    // Recover vertices (labels and properties).
    spdlog::info("Recovering vertices.", info.vertices_count);
    uint64_t last_vertex_gid{0};

    if (!snapshot.SetPosition(info.offset_vertex_batches)) {
      throw RecoveryFailure("Couldn't read data from snapshot!");
    }

    const auto vertex_batches = ReadBatchInfos(snapshot);
    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, schema_info, &vertex_batches, &get_label_from_id, &get_property_from_id, &last_vertex_gid,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto last_vertex_gid_in_batch =
              LoadPartialVertices(path, *vertices, schema_info, batch.offset, batch.count, get_label_from_id,
                                  get_property_from_id, name_id_mapper);
          if (batch_index == vertex_batches.size() - 1) {
            last_vertex_gid = last_vertex_gid_in_batch;
          }
        },
        vertex_batches);

    spdlog::info("Vertices are recovered.");

    spdlog::info("Recovering edges.");
    // Recover edges.
    if (snapshot_has_edges) {
      // We don't need to check whether we store properties on edge or not, because `LoadPartialEdges` will always
      // iterate over the edges in the snapshot (if they exist) and the current configuration of properties on edge only
      // affect what it does:
      // 1. If properties are allowed on edges, then it loads the edges.
      // 2. If properties are not allowed on edges, then it checks that none of the edges have any properties.
      if (!snapshot.SetPosition(info.offset_edge_batches)) {
        throw RecoveryFailure("Couldn't read data from snapshot!");
      }
      const auto edge_batches = ReadBatchInfos(snapshot);

      RecoverOnMultipleThreads(
          config.durability.recovery_thread_count,
          [path, edges, items = config.salient.items, &get_property_from_id, name_id_mapper](
              const size_t /*batch_index*/, const BatchInfo &batch) {
            LoadPartialEdges(path, *edges, batch.offset, batch.count, items, get_property_from_id, name_id_mapper);
          },
          edge_batches);
    }
    spdlog::info("Edges are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recover connectivity.");
    recovery_info.vertex_batches.reserve(vertex_batches.size());
    for (const auto batch : vertex_batches) {
      recovery_info.vertex_batches.emplace_back(Gid::FromUint(0), batch.count);
    }
    std::atomic<uint64_t> highest_edge_gid{0};

    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, edges, edges_metadata, schema_info, edge_count, items = config.salient.items,
         snapshot_has_edges, &get_edge_type_from_id, &highest_edge_gid, &recovery_info,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto result =
              LoadPartialConnectivity(path, *vertices, *edges, *edges_metadata, schema_info, batch.offset, batch.count,
                                      items, snapshot_has_edges, get_edge_type_from_id, name_id_mapper);
          edge_count->fetch_add(result.edge_count);
          auto known_highest_edge_gid = highest_edge_gid.load();
          while (known_highest_edge_gid < result.highest_edge_id) {
            highest_edge_gid.compare_exchange_weak(known_highest_edge_gid, result.highest_edge_id);
          }
          recovery_info.vertex_batches[batch_index].first = result.first_vertex_gid;
        },
        vertex_batches);

    spdlog::info("Connectivity is recovered.");

    // Set initial values for edge/vertex ID generators.
    recovery_info.next_edge_id = highest_edge_gid + 1;
    recovery_info.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Couldn't read section indices!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of label indices");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of label index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of entries for label index statistics!");
      spdlog::info("Recovering metadata of {} label indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label while recovering label index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label index statistics");
        const auto label_id = get_label_from_id(*label);
        indices_constraints.indices.label_stats.emplace_back(label_id, LabelIndexStats{*count, *avg_degree});
        SPDLOG_TRACE("Recovered metadata of label index statistics for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of label property indices!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label_properties,
                                    {get_label_from_id(*label), {get_property_from_id(*property)}},
                                    "The label+property index already exists!");
        SPDLOG_TRACE("Recovered metadata of label+property index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    // Recover label+property indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of entries for label property statistics!");
      spdlog::info("Recovering metadata of {} label+property indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index statistics!");
        const auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label property index statistics!!");
        const auto distinct_values_count = snapshot.ReadUint();
        if (!distinct_values_count)
          throw RecoveryFailure("Couldn't read distinct values count for label property index statistics!");
        const auto statistic = snapshot.ReadDouble();
        if (!statistic) throw RecoveryFailure("Couldn't read statistics value for label-property index statistics!");
        const auto avg_group_size = snapshot.ReadDouble();
        if (!avg_group_size)
          throw RecoveryFailure("Couldn't read average group size for label property index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label property index statistics!");
        const auto label_id = get_label_from_id(*label);
        auto property_id = std::vector{get_property_from_id(*property)};
        indices_constraints.indices.label_property_stats.emplace_back(
            label_id,
            std::make_pair(std::move(property_id), LabelPropertyIndexStats{*count, *distinct_values_count, *statistic,
                                                                           *avg_group_size, *avg_degree}));
        SPDLOG_TRACE("Recovered metadata of label+property index statistics for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    // Recover edge-type indices.
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_edge_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EDGE_INDICES)
      throw RecoveryFailure("Couldn't read section edge-indices!");

    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge, get_edge_type_from_id(*edge_type),
                                    "The edge-type index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)));
      }
      spdlog::info("Metadata of edge-type indices are recovered.");
    }

    // Recover text indices.
    if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of text indices!");
      spdlog::info("Recovering metadata of {} text indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto index_name = snapshot.ReadString();
        if (!index_name.has_value()) throw RecoveryFailure("Couldn't read text index name!");
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read text index label!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.text_indices,
                                    {index_name.value(), get_label_from_id(*label)}, "The text index already exists!");
        SPDLOG_TRACE("Recovered metadata of text index {} for :{}", index_name.value(),
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of text indices are recovered.");
    }

    spdlog::info("Metadata of indices are recovered.");
  }

  // Recover constraints.
  {
    spdlog::info("Recovering metadata of constraints.");
    if (!snapshot.SetPosition(info.offset_constraints)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Couldn't read section constraints marker!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of existence constraints!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of existence constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of existence constraints!");
        AddRecoveredIndexConstraint(&indices_constraints.constraints.existence,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The existence constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of existence constraint for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of existence constraints are recovered.");
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of unique constraints!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of unique constraints!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Couldn't read the number of properties in unique constraint!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property of unique constraint!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique, {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of unique constraints for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of unique constraints are recovered.");
    }
    spdlog::info("Metadata of constraints are recovered.");
  }

  spdlog::info("Recovering metadata.");
  // Recover epoch history
  {
    if (!snapshot.SetPosition(info.offset_epoch_history)) throw RecoveryFailure("Couldn't read data from snapshot!");

    const auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY)
      throw RecoveryFailure("Couldn't read section epoch history marker!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Couldn't read history size!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Couldn't read maybe epoch id!");
      }
      const auto maybe_last_durable_timestamp = snapshot.ReadUint();
      if (!maybe_last_durable_timestamp) {
        throw RecoveryFailure("Couldn't read maybe last durable timestamp!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_durable_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  recovery_info.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, recovery_info, std::move(indices_constraints)};
}

/// We messed up and accidentally introduced a version bump in a release it was not needed for
/// hence same load for 18 will work for 19
RecoveredSnapshot LoadSnapshotVersion18or19(Decoder &snapshot, const std::filesystem::path &path,
                                            utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                            utils::SkipList<EdgeMetadata> *edges_metadata,
                                            std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                            NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                            SharedSchemaTracking *schema_info, const Config &config,
                                            memgraph::storage::EnumStore *enum_store) {
  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;

  // Cleanup of loaded data in case of failure.
  bool success = false;
  auto const cleanup = utils::OnScopeExit([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      edges_metadata->clear();
      epoch_history->clear();
      enum_store->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  bool const snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Failed to read section mapper!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Failed to read name-id mapper size!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Failed to read id for name-id mapper!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Failed to read name for name-id mapper!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }

  // Recover enums.
  // TODO: when we have enum deletion/edits we will need to handle remapping
  {
    spdlog::info("Recovering metadata of enums.");
    if (!snapshot.SetPosition(info.offset_enums)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_ENUMS) {
      throw RecoveryFailure("Couldn't read section enums marker!");
    }

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Couldn't read the number of enums!");
    spdlog::info("Recovering metadata of {} enums.", *size);
    for (uint64_t i = 0; i < *size; ++i) {
      auto etype = snapshot.ReadString();
      if (!etype) throw RecoveryFailure("Couldn't read enum type of enums!");

      auto value_count = snapshot.ReadUint();
      if (!value_count) throw RecoveryFailure("Couldn't read enum values length of enums!");

      auto evalues = std::vector<std::string>{};
      evalues.reserve(*value_count);
      for (uint64_t j = 0; j < *value_count; ++j) {
        auto evalue = snapshot.ReadString();
        if (!evalue) throw RecoveryFailure("Couldn't read enum value of enums!");
        evalues.emplace_back(*std::move(evalue));
      }

      auto ret = enum_store->RegisterEnum(*std::move(etype), std::move(evalues));
      if (ret.HasError()) {
        throw storage::durability::RecoveryFailure("The enum could not be created!");
      }
    }
    spdlog::info("Metadata of enums are recovered.");
  }

  auto get_label_from_id = [&snapshot_id_map](uint64_t label_id) {
    auto it = snapshot_id_map.find(label_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find label id in snapshot_id_map!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t property_id) {
    auto it = snapshot_id_map.find(property_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find property id in snapshot_id_map!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t edge_type_id) {
    auto it = snapshot_id_map.find(edge_type_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find edge type id in snapshot_id_map!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
    // Recover vertices (labels and properties).
    spdlog::info("Recovering vertices.", info.vertices_count);
    uint64_t last_vertex_gid{0};

    if (!snapshot.SetPosition(info.offset_vertex_batches)) {
      throw RecoveryFailure("Couldn't read data from snapshot!");
    }

    const auto vertex_batches = ReadBatchInfos(snapshot);
    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, &vertex_batches, &get_label_from_id, &get_property_from_id, &last_vertex_gid, schema_info,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto last_vertex_gid_in_batch =
              LoadPartialVertices(path, *vertices, schema_info, batch.offset, batch.count, get_label_from_id,
                                  get_property_from_id, name_id_mapper);
          if (batch_index == vertex_batches.size() - 1) {
            last_vertex_gid = last_vertex_gid_in_batch;
          }
        },
        vertex_batches);

    spdlog::info("Vertices are recovered.");

    spdlog::info("Recovering edges.");
    // Recover edges.
    if (snapshot_has_edges) {
      // We don't need to check whether we store properties on edge or not, because `LoadPartialEdges` will always
      // iterate over the edges in the snapshot (if they exist) and the current configuration of properties on edge only
      // affect what it does:
      // 1. If properties are allowed on edges, then it loads the edges.
      // 2. If properties are not allowed on edges, then it checks that none of the edges have any properties.
      if (!snapshot.SetPosition(info.offset_edge_batches)) {
        throw RecoveryFailure("Couldn't read data from snapshot!");
      }
      const auto edge_batches = ReadBatchInfos(snapshot);

      RecoverOnMultipleThreads(
          config.durability.recovery_thread_count,
          [path, edges, items = config.salient.items, &get_property_from_id, name_id_mapper](
              const size_t /*batch_index*/, const BatchInfo &batch) {
            LoadPartialEdges(path, *edges, batch.offset, batch.count, items, get_property_from_id, name_id_mapper);
          },
          edge_batches);
    }
    spdlog::info("Edges are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recover connectivity.");
    recovery_info.vertex_batches.reserve(vertex_batches.size());
    for (const auto batch : vertex_batches) {
      recovery_info.vertex_batches.emplace_back(Gid::FromUint(0), batch.count);
    }
    std::atomic<uint64_t> highest_edge_gid{0};

    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, edges, edges_metadata, edge_count, items = config.salient.items, snapshot_has_edges,
         &get_edge_type_from_id, &highest_edge_gid, &recovery_info, schema_info,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto result =
              LoadPartialConnectivity(path, *vertices, *edges, *edges_metadata, schema_info, batch.offset, batch.count,
                                      items, snapshot_has_edges, get_edge_type_from_id, name_id_mapper);
          edge_count->fetch_add(result.edge_count);
          auto known_highest_edge_gid = highest_edge_gid.load();
          while (known_highest_edge_gid < result.highest_edge_id) {
            highest_edge_gid.compare_exchange_weak(known_highest_edge_gid, result.highest_edge_id);
          }
          recovery_info.vertex_batches[batch_index].first = result.first_vertex_gid;
        },
        vertex_batches);

    spdlog::info("Connectivity is recovered.");

    // Set initial values for edge/vertex ID generators.
    recovery_info.next_edge_id = highest_edge_gid + 1;
    recovery_info.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Couldn't read section indices!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of label indices");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of label index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of entries for label index statistics!");
      spdlog::info("Recovering metadata of {} label indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label while recovering label index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label index statistics");
        const auto label_id = get_label_from_id(*label);
        indices_constraints.indices.label_stats.emplace_back(label_id, LabelIndexStats{*count, *avg_degree});
        SPDLOG_TRACE("Recovered metadata of label index statistics for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of label property indices!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label_properties,
                                    {get_label_from_id(*label), {get_property_from_id(*property)}},
                                    "The label+property index already exists!");
        SPDLOG_TRACE("Recovered metadata of label+property index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    // Recover label+property indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of entries for label property statistics!");
      spdlog::info("Recovering metadata of {} label+property indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index statistics!");
        const auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label property index statistics!!");
        const auto distinct_values_count = snapshot.ReadUint();
        if (!distinct_values_count)
          throw RecoveryFailure("Couldn't read distinct values count for label property index statistics!");
        const auto statistic = snapshot.ReadDouble();
        if (!statistic) throw RecoveryFailure("Couldn't read statistics value for label-property index statistics!");
        const auto avg_group_size = snapshot.ReadDouble();
        if (!avg_group_size)
          throw RecoveryFailure("Couldn't read average group size for label property index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label property index statistics!");
        const auto label_id = get_label_from_id(*label);
        auto property_id = std::vector{get_property_from_id(*property)};
        indices_constraints.indices.label_property_stats.emplace_back(
            label_id,
            std::make_pair(std::move(property_id), LabelPropertyIndexStats{*count, *distinct_values_count, *statistic,
                                                                           *avg_group_size, *avg_degree}));
        SPDLOG_TRACE("Recovered metadata of label+property index statistics for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_edge_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EDGE_INDICES)
      throw RecoveryFailure("Couldn't read section edge-indices!");

    {
      // Recover edge-type indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge, get_edge_type_from_id(*edge_type),
                                    "The edge-type index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)));
      }
      spdlog::info("Metadata of edge-type indices are recovered.");
    }
    {
      // Recover edge-type + property indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type + property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of edge-type + property index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge_type_property,
                                    {get_edge_type_from_id(*edge_type), get_property_from_id(*property)},
                                    "The edge-type + property index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of edge-type + property indices are recovered.");
    }

    // Recover text indices.
    if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of text indices!");
      spdlog::info("Recovering metadata of {} text indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto index_name = snapshot.ReadString();
        if (!index_name.has_value()) throw RecoveryFailure("Couldn't read text index name!");
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read text index label!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.text_indices,
                                    {index_name.value(), get_label_from_id(*label)}, "The text index already exists!");
        SPDLOG_TRACE("Recovered metadata of text index {} for :{}", index_name.value(),
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of text indices are recovered.");
    }

    spdlog::info("Metadata of indices are recovered.");
  }

  // Recover constraints.
  {
    spdlog::info("Recovering metadata of constraints.");
    if (!snapshot.SetPosition(info.offset_constraints)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Couldn't read section constraints marker!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of existence constraints!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of existence constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of existence constraints!");
        AddRecoveredIndexConstraint(&indices_constraints.constraints.existence,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The existence constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of existence constraint for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of existence constraints are recovered.");
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of unique constraints!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of unique constraints!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Couldn't read the number of properties in unique constraint!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property of unique constraint!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique, {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of unique constraints for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of unique constraints are recovered.");
    }
    spdlog::info("Metadata of constraints are recovered.");
  }

  spdlog::info("Recovering metadata.");
  // Recover epoch history
  {
    if (!snapshot.SetPosition(info.offset_epoch_history)) throw RecoveryFailure("Couldn't read data from snapshot!");

    const auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY)
      throw RecoveryFailure("Couldn't read section epoch history marker!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Couldn't read history size!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Couldn't read maybe epoch id!");
      }
      const auto maybe_last_commit_timestamp = snapshot.ReadUint();
      if (!maybe_last_commit_timestamp) {
        throw RecoveryFailure("Couldn't read maybe last commit timestamp!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_commit_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  recovery_info.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, recovery_info, std::move(indices_constraints)};
}

RecoveredSnapshot LoadSnapshotVersion20or21(Decoder &snapshot, const std::filesystem::path &path,
                                            utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                            utils::SkipList<EdgeMetadata> *edges_metadata,
                                            std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                            NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                            SharedSchemaTracking *schema_info, const Config &config,
                                            memgraph::storage::EnumStore *enum_store) {
  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;

  // Cleanup of loaded data in case of failure.
  bool success = false;
  auto const cleanup = utils::OnScopeExit([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      edges_metadata->clear();
      epoch_history->clear();
      enum_store->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  const auto snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Failed to read section mapper!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Failed to read name-id mapper size!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Failed to read id for name-id mapper!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Failed to read name for name-id mapper!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }

  // Recover enums.
  // TODO: when we have enum deletion/edits we will need to handle remapping
  {
    spdlog::info("Recovering metadata of enums.");
    if (!snapshot.SetPosition(info.offset_enums)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_ENUMS) {
      throw RecoveryFailure("Couldn't read section enums marker!");
    }

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Couldn't read the number of enums!");
    spdlog::info("Recovering metadata of {} enums.", *size);
    for (uint64_t i = 0; i < *size; ++i) {
      auto etype = snapshot.ReadString();
      if (!etype) throw RecoveryFailure("Couldn't read enum type of enums!");

      auto value_count = snapshot.ReadUint();
      if (!value_count) throw RecoveryFailure("Couldn't read enum values length of enums!");

      auto evalues = std::vector<std::string>{};
      evalues.reserve(*value_count);
      for (uint64_t j = 0; j < *value_count; ++j) {
        auto evalue = snapshot.ReadString();
        if (!evalue) throw RecoveryFailure("Couldn't read enum value of enums!");
        evalues.emplace_back(*std::move(evalue));
      }

      auto ret = enum_store->RegisterEnum(*std::move(etype), std::move(evalues));
      if (ret.HasError()) {
        throw storage::durability::RecoveryFailure("The enum could not be created!");
      }
    }
    spdlog::info("Metadata of enums are recovered.");
  }

  auto get_label_from_id = [&snapshot_id_map](uint64_t label_id) {
    auto it = snapshot_id_map.find(label_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find label id in snapshot_id_map!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t property_id) {
    auto it = snapshot_id_map.find(property_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find property id in snapshot_id_map!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t edge_type_id) {
    auto it = snapshot_id_map.find(edge_type_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find edge type id in snapshot_id_map!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
    // Recover vertices (labels and properties).
    spdlog::info("Recovering vertices.", info.vertices_count);
    uint64_t last_vertex_gid{0};

    if (!snapshot.SetPosition(info.offset_vertex_batches)) {
      throw RecoveryFailure("Couldn't read data from snapshot!");
    }

    const auto vertex_batches = ReadBatchInfos(snapshot);
    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, schema_info, &vertex_batches, &get_label_from_id, &get_property_from_id, &last_vertex_gid,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto last_vertex_gid_in_batch =
              LoadPartialVertices(path, *vertices, schema_info, batch.offset, batch.count, get_label_from_id,
                                  get_property_from_id, name_id_mapper);
          if (batch_index == vertex_batches.size() - 1) {
            last_vertex_gid = last_vertex_gid_in_batch;
          }
        },
        vertex_batches);

    spdlog::info("Vertices are recovered.");

    spdlog::info("Recovering edges.");
    // Recover edges.
    if (snapshot_has_edges) {
      // We don't need to check whether we store properties on edge or not, because `LoadPartialEdges` will always
      // iterate over the edges in the snapshot (if they exist) and the current configuration of properties on edge only
      // affect what it does:
      // 1. If properties are allowed on edges, then it loads the edges.
      // 2. If properties are not allowed on edges, then it checks that none of the edges have any properties.
      if (!snapshot.SetPosition(info.offset_edge_batches)) {
        throw RecoveryFailure("Couldn't read data from snapshot!");
      }
      const auto edge_batches = ReadBatchInfos(snapshot);

      RecoverOnMultipleThreads(
          config.durability.recovery_thread_count,
          [path, edges, items = config.salient.items, &get_property_from_id, name_id_mapper](
              const size_t /*batch_index*/, const BatchInfo &batch) {
            LoadPartialEdges(path, *edges, batch.offset, batch.count, items, get_property_from_id, name_id_mapper);
          },
          edge_batches);
    }
    spdlog::info("Edges are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recover connectivity.");
    recovery_info.vertex_batches.reserve(vertex_batches.size());
    for (const auto batch : vertex_batches) {
      recovery_info.vertex_batches.emplace_back(Gid::FromUint(0), batch.count);
    }
    std::atomic<uint64_t> highest_edge_gid{0};

    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, edges, edges_metadata, schema_info, edge_count, items = config.salient.items,
         snapshot_has_edges, &get_edge_type_from_id, &highest_edge_gid, &recovery_info,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto result =
              LoadPartialConnectivity(path, *vertices, *edges, *edges_metadata, schema_info, batch.offset, batch.count,
                                      items, snapshot_has_edges, get_edge_type_from_id, name_id_mapper);
          edge_count->fetch_add(result.edge_count);
          auto known_highest_edge_gid = highest_edge_gid.load();
          while (known_highest_edge_gid < result.highest_edge_id) {
            highest_edge_gid.compare_exchange_weak(known_highest_edge_gid, result.highest_edge_id);
          }
          recovery_info.vertex_batches[batch_index].first = result.first_vertex_gid;
        },
        vertex_batches);

    spdlog::info("Connectivity is recovered.");

    // Set initial values for edge/vertex ID generators.
    recovery_info.next_edge_id = highest_edge_gid + 1;
    recovery_info.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Couldn't read section indices!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of label indices");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of label index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of entries for label index statistics!");
      spdlog::info("Recovering metadata of {} label indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label while recovering label index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label index statistics");
        const auto label_id = get_label_from_id(*label);
        indices_constraints.indices.label_stats.emplace_back(label_id, LabelIndexStats{*count, *avg_degree});
        SPDLOG_TRACE("Recovered metadata of label index statistics for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of label property indices!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label_properties,
                                    {get_label_from_id(*label), {get_property_from_id(*property)}},
                                    "The label+property index already exists!");
        SPDLOG_TRACE("Recovered metadata of label+property index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    // Recover label+property indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of entries for label property statistics!");
      spdlog::info("Recovering metadata of {} label+property indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index statistics!");
        const auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label property index statistics!!");
        const auto distinct_values_count = snapshot.ReadUint();
        if (!distinct_values_count)
          throw RecoveryFailure("Couldn't read distinct values count for label property index statistics!");
        const auto statistic = snapshot.ReadDouble();
        if (!statistic) throw RecoveryFailure("Couldn't read statistics value for label-property index statistics!");
        const auto avg_group_size = snapshot.ReadDouble();
        if (!avg_group_size)
          throw RecoveryFailure("Couldn't read average group size for label property index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label property index statistics!");
        const auto label_id = get_label_from_id(*label);
        auto property_id = std::vector{get_property_from_id(*property)};
        indices_constraints.indices.label_property_stats.emplace_back(
            label_id,
            std::make_pair(std::move(property_id), LabelPropertyIndexStats{*count, *distinct_values_count, *statistic,
                                                                           *avg_group_size, *avg_degree}));
        SPDLOG_TRACE("Recovered metadata of label+property index statistics for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_edge_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EDGE_INDICES)
      throw RecoveryFailure("Couldn't read section edge-indices!");

    {
      // Recover edge-type indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge, get_edge_type_from_id(*edge_type),
                                    "The edge-type index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)));
      }
      spdlog::info("Metadata of edge-type indices are recovered.");
    }
    {
      // Recover edge-type + property indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type + property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of edge-type + property index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge_type_property,
                                    {get_edge_type_from_id(*edge_type), get_property_from_id(*property)},
                                    "The edge-type + property index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of edge-type + property indices are recovered.");
    }

    // Recover point indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of point indices!");
      spdlog::info("Recovering metadata of {} point indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for point index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for point index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.point_label_property,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The point index already exists!");
        SPDLOG_TRACE("Recovered metadata of point index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of point indices are recovered.");
    }

    // Recover text indices.
    // NOTE: while this is experimental and hence optional
    //       it must be last in the SECTION_INDICES
    if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of text indices!");
      spdlog::info("Recovering metadata of {} text indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto index_name = snapshot.ReadString();
        if (!index_name.has_value()) throw RecoveryFailure("Couldn't read text index name!");
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read text index label!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.text_indices,
                                    {index_name.value(), get_label_from_id(*label)}, "The text index already exists!");
        SPDLOG_TRACE("Recovered metadata of text index {} for :{}", index_name.value(),
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of text indices are recovered.");
    }

    spdlog::info("Metadata of indices are recovered.");
  }

  // Recover constraints.
  {
    spdlog::info("Recovering metadata of constraints.");
    if (!snapshot.SetPosition(info.offset_constraints)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Couldn't read section constraints marker!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of existence constraints!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of existence constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of existence constraints!");
        AddRecoveredIndexConstraint(&indices_constraints.constraints.existence,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The existence constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of existence constraint for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of existence constraints are recovered.");
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of unique constraints!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of unique constraints!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Couldn't read the number of properties in unique constraint!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property of unique constraint!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique, {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of unique constraints for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of unique constraints are recovered.");
    }

    // Recover type constraints.
    // Snapshot version should be checked since type constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of type constraints!");

      spdlog::info("Recovering metadata of {} type constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of type constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of type constraint!");
        auto type = snapshot.ReadUint();
        if (!type) throw RecoveryFailure("Couldn't read type of type constraint!");

        AddRecoveredIndexConstraint(
            &indices_constraints.constraints.type,
            {get_label_from_id(*label), get_property_from_id(*property), static_cast<TypeConstraintKind>(*type)},
            "The type constraint already exists!");
        SPDLOG_TRACE("Recovered metadata for IS TYPED {} constraint for :{}({})",
                     TypeConstraintKindToString(static_cast<TypeConstraintKind>(*type)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of type constraints are recovered.");
    }

    spdlog::info("Metadata of constraints are recovered.");
  }

  spdlog::info("Recovering metadata.");
  // Recover epoch history
  {
    if (!snapshot.SetPosition(info.offset_epoch_history)) throw RecoveryFailure("Couldn't read data from snapshot!");

    const auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY)
      throw RecoveryFailure("Couldn't read section epoch history marker!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Couldn't read history size!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Couldn't read maybe epoch id!");
      }
      const auto maybe_last_durable_timestamp = snapshot.ReadUint();
      if (!maybe_last_durable_timestamp) {
        throw RecoveryFailure("Couldn't read maybe last durable timestamp!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_durable_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  recovery_info.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, recovery_info, std::move(indices_constraints)};
}

RecoveredSnapshot LoadSnapshotVersion22or23(Decoder &snapshot, const std::filesystem::path &path,
                                            utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                            utils::SkipList<EdgeMetadata> *edges_metadata,
                                            std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                            NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                            const Config &config, memgraph::storage::EnumStore *enum_store,
                                            SharedSchemaTracking *schema_info,
                                            std::optional<SnapshotObserverInfo> const &snapshot_info) {
  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;

  // Cleanup of loaded data in case of failure.
  bool success = false;
  const utils::OnScopeExit cleanup([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      edges_metadata->clear();
      epoch_history->clear();
      enum_store->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  const bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Failed to read section mapper!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Failed to read name-id mapper size!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Failed to read id for name-id mapper!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Failed to read name for name-id mapper!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }

  // Recover enums.
  // TODO: when we have enum deletion/edits we will need to handle remapping
  {
    spdlog::info("Recovering metadata of enums.");
    if (!snapshot.SetPosition(info.offset_enums)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_ENUMS) {
      throw RecoveryFailure("Couldn't read section enums marker!");
    }

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Couldn't read the number of enums!");
    spdlog::info("Recovering metadata of {} enums.", *size);
    for (uint64_t i = 0; i < *size; ++i) {
      auto etype = snapshot.ReadString();
      if (!etype) throw RecoveryFailure("Couldn't read enum type of enums!");

      auto value_count = snapshot.ReadUint();
      if (!value_count) throw RecoveryFailure("Couldn't read enum values length of enums!");

      auto evalues = std::vector<std::string>{};
      evalues.reserve(*value_count);
      for (uint64_t j = 0; j < *value_count; ++j) {
        auto evalue = snapshot.ReadString();
        if (!evalue) throw RecoveryFailure("Couldn't read enum value of enums!");
        evalues.emplace_back(*std::move(evalue));
      }

      auto ret = enum_store->RegisterEnum(*std::move(etype), std::move(evalues));
      if (ret.HasError()) {
        throw storage::durability::RecoveryFailure("The enum could not be created!");
      }
    }
    spdlog::info("Metadata of enums are recovered.");
  }

  auto get_label_from_id = [&snapshot_id_map](uint64_t label_id) {
    auto it = snapshot_id_map.find(label_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find label id in snapshot_id_map!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t property_id) {
    auto it = snapshot_id_map.find(property_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find property id in snapshot_id_map!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t edge_type_id) {
    auto it = snapshot_id_map.find(edge_type_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find edge type id in snapshot_id_map!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
    // Recover vertices (labels and properties).
    spdlog::info("Recovering vertices.", info.vertices_count);
    uint64_t last_vertex_gid{0};

    if (!snapshot.SetPosition(info.offset_vertex_batches)) {
      throw RecoveryFailure("Couldn't read data from snapshot!");
    }

    const auto vertex_batches = ReadBatchInfos(snapshot);
    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, schema_info, &vertex_batches, &get_label_from_id, &get_property_from_id, &last_vertex_gid,
         &snapshot_info, name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto last_vertex_gid_in_batch =
              LoadPartialVertices(path, *vertices, schema_info, batch.offset, batch.count, get_label_from_id,
                                  get_property_from_id, name_id_mapper, snapshot_info);
          if (batch_index == vertex_batches.size() - 1) {
            last_vertex_gid = last_vertex_gid_in_batch;
          }
        },
        vertex_batches);

    spdlog::info("Vertices are recovered.");

    spdlog::info("Recovering edges.");
    // Recover edges.
    if (snapshot_has_edges) {
      // We don't need to check whether we store properties on edge or not, because `LoadPartialEdges` will always
      // iterate over the edges in the snapshot (if they exist) and the current configuration of properties on edge only
      // affect what it does:
      // 1. If properties are allowed on edges, then it loads the edges.
      // 2. If properties are not allowed on edges, then it checks that none of the edges have any properties.
      if (!snapshot.SetPosition(info.offset_edge_batches)) {
        throw RecoveryFailure("Couldn't read data from snapshot!");
      }
      const auto edge_batches = ReadBatchInfos(snapshot);

      RecoverOnMultipleThreads(
          config.durability.recovery_thread_count,
          [path, edges, items = config.salient.items, &get_property_from_id, &snapshot_info, name_id_mapper](
              const size_t /*batch_index*/, const BatchInfo &batch) {
            LoadPartialEdges(path, *edges, batch.offset, batch.count, items, get_property_from_id, name_id_mapper,
                             snapshot_info);
          },
          edge_batches);
    }
    spdlog::info("Edges are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recover connectivity.");
    recovery_info.vertex_batches.reserve(vertex_batches.size());
    for (const auto batch : vertex_batches) {
      recovery_info.vertex_batches.emplace_back(Gid::FromUint(0), batch.count);
    }
    std::atomic<uint64_t> highest_edge_gid{0};

    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, edges, edges_metadata, schema_info, edge_count, items = config.salient.items,
         snapshot_has_edges, &get_edge_type_from_id, &highest_edge_gid, &recovery_info, &snapshot_info,
         name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
          const auto result =
              LoadPartialConnectivity(path, *vertices, *edges, *edges_metadata, schema_info, batch.offset, batch.count,
                                      items, snapshot_has_edges, get_edge_type_from_id, name_id_mapper, snapshot_info);
          edge_count->fetch_add(result.edge_count);
          auto known_highest_edge_gid = highest_edge_gid.load();
          while (known_highest_edge_gid < result.highest_edge_id) {
            highest_edge_gid.compare_exchange_weak(known_highest_edge_gid, result.highest_edge_id);
          }
          recovery_info.vertex_batches[batch_index].first = result.first_vertex_gid;
        },
        vertex_batches);

    spdlog::info("Connectivity is recovered.");

    // Set initial values for edge/vertex ID generators.
    recovery_info.next_edge_id = highest_edge_gid + 1;
    recovery_info.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Couldn't read section indices!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of label indices");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of label index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of entries for label index statistics!");
      spdlog::info("Recovering metadata of {} label indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label while recovering label index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label index statistics");
        const auto label_id = get_label_from_id(*label);
        indices_constraints.indices.label_stats.emplace_back(label_id, LabelIndexStats{*count, *avg_degree});
        SPDLOG_TRACE("Recovered metadata of label index statistics for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of label property indices!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label_properties,
                                    {get_label_from_id(*label), {get_property_from_id(*property)}},
                                    "The label+property index already exists!");
        SPDLOG_TRACE("Recovered metadata of label+property index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    // Recover label+property indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of entries for label property statistics!");
      spdlog::info("Recovering metadata of {} label+property indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index statistics!");
        const auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for label property index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label property index statistics!!");
        const auto distinct_values_count = snapshot.ReadUint();
        if (!distinct_values_count)
          throw RecoveryFailure("Couldn't read distinct values count for label property index statistics!");
        const auto statistic = snapshot.ReadDouble();
        if (!statistic) throw RecoveryFailure("Couldn't read statistics value for label-property index statistics!");
        const auto avg_group_size = snapshot.ReadDouble();
        if (!avg_group_size)
          throw RecoveryFailure("Couldn't read average group size for label property index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label property index statistics!");
        const auto label_id = get_label_from_id(*label);
        auto property_id = std::vector{get_property_from_id(*property)};
        indices_constraints.indices.label_property_stats.emplace_back(
            label_id,
            std::make_pair(std::move(property_id), LabelPropertyIndexStats{*count, *distinct_values_count, *statistic,
                                                                           *avg_group_size, *avg_degree}));
        SPDLOG_TRACE("Recovered metadata of label+property index statistics for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_edge_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EDGE_INDICES)
      throw RecoveryFailure("Couldn't read section edge-indices!");

    {
      // Recover edge-type indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge, get_edge_type_from_id(*edge_type),
                                    "The edge-type index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)));
      }
      spdlog::info("Metadata of edge-type indices are recovered.");
    }
    {
      // Recover edge-type + property indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type+property indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type + property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of edge-type + property index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge_type_property,
                                    {get_edge_type_from_id(*edge_type), get_property_from_id(*property)},
                                    "The edge-type + property index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of edge-type + property indices are recovered.");
    }

    // Recover point indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of point indices!");
      spdlog::info("Recovering metadata of {} point indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for point index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for point index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.point_label_property,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The point index already exists!");
        SPDLOG_TRACE("Recovered metadata of point index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of point indices are recovered.");
    }

    // Recover vector indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of vector indices!");
      spdlog::info("Recovering metadata of {} vector indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto index_name = snapshot.ReadString();
        if (!index_name.has_value()) throw RecoveryFailure("Couldn't read vector index name!");

        // We only need to check for the existence of the vector index name -> we can't have two vector indices with the
        // same name
        if (std::ranges::any_of(indices_constraints.indices.vector_indices, [&index_name](const auto &vector_index) {
              return vector_index.index_name == index_name;
            })) {
          throw RecoveryFailure("The vector index already exists!");
        }

        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read vector index label!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read vector index property!");
        auto metric = snapshot.ReadString();
        if (!metric) throw RecoveryFailure("Couldn't read vector index metric!");
        auto metric_kind = VectorIndex::MetricFromName(metric.value());
        auto dimension = snapshot.ReadUint();
        if (!dimension) throw RecoveryFailure("Couldn't read vector index dimension!");
        auto resize_coefficient = snapshot.ReadUint();
        if (!resize_coefficient) throw RecoveryFailure("Couldn't read vector index resize coefficient!");
        auto capacity = snapshot.ReadUint();
        if (!capacity) throw RecoveryFailure("Couldn't read vector index capacity!");
        SPDLOG_TRACE("Recovered metadata of vector index {} for :{}({})", *index_name,
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));

        indices_constraints.indices.vector_indices.emplace_back(
            std::move(index_name.value()), get_label_from_id(*label), get_property_from_id(*property), metric_kind,
            static_cast<std::uint16_t>(*dimension), static_cast<std::uint16_t>(*resize_coefficient), *capacity);
      }
      spdlog::info("Metadata of vector indices are recovered.");
    }

    // Recover text indices.
    // NOTE: while this is experimental and hence optional
    //       it must be last in the SECTION_INDICES
    if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of text indices!");
      spdlog::info("Recovering metadata of {} text indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto index_name = snapshot.ReadString();
        if (!index_name.has_value()) throw RecoveryFailure("Couldn't read text index name!");
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read text index label!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.text_indices,
                                    {index_name.value(), get_label_from_id(*label)}, "The text index already exists!");
        SPDLOG_TRACE("Recovered metadata of text index {} for :{}", index_name.value(),
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of text indices are recovered.");
    }

    spdlog::info("Metadata of indices are recovered.");
  }

  // Recover constraints.
  {
    spdlog::info("Recovering metadata of constraints.");
    if (!snapshot.SetPosition(info.offset_constraints)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Couldn't read section constraints marker!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of existence constraints!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of existence constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of existence constraints!");
        AddRecoveredIndexConstraint(&indices_constraints.constraints.existence,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The existence constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of existence constraint for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of existence constraints are recovered.");
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of unique constraints!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of unique constraints!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Couldn't read the number of properties in unique constraint!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property of unique constraint!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique, {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of unique constraints for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of unique constraints are recovered.");
    }

    // Recover type constraints.
    // Snapshot version should be checked since type constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of type constraints!");

      spdlog::info("Recovering metadata of {} type constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of type constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of type constraint!");
        auto type = snapshot.ReadUint();
        if (!type) throw RecoveryFailure("Couldn't read type of type constraint!");

        AddRecoveredIndexConstraint(
            &indices_constraints.constraints.type,
            {get_label_from_id(*label), get_property_from_id(*property), static_cast<TypeConstraintKind>(*type)},
            "The type constraint already exists!");
        SPDLOG_TRACE("Recovered metadata for IS TYPED {} constraint for :{}({})",
                     TypeConstraintKindToString(static_cast<TypeConstraintKind>(*type)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of type constraints are recovered.");
    }

    spdlog::info("Metadata of constraints are recovered.");
  }

  spdlog::info("Recovering metadata.");
  // Recover epoch history
  {
    if (!snapshot.SetPosition(info.offset_epoch_history)) throw RecoveryFailure("Couldn't read data from snapshot!");

    const auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY)
      throw RecoveryFailure("Couldn't read section epoch history marker!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Couldn't read history size!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Couldn't read maybe epoch id!");
      }
      const auto maybe_last_durable_timestamp = snapshot.ReadUint();
      if (!maybe_last_durable_timestamp) {
        throw RecoveryFailure("Couldn't read maybe last durable timestamp!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_durable_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  recovery_info.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, recovery_info, std::move(indices_constraints)};
}

RecoveredSnapshot LoadCurrentVersionSnapshot(Decoder &snapshot, std::filesystem::path const &path,
                                             utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges,
                                             utils::SkipList<EdgeMetadata> *edges_metadata,
                                             std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                             NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                             Config const &config, EnumStore *enum_store,
                                             SharedSchemaTracking *schema_info,
                                             std::optional<SnapshotObserverInfo> const &snapshot_info) {
  // Cleanup of loaded data in case of failure.

  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;

  bool success = false;
  auto const cleanup = utils::OnScopeExit([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      edges_metadata->clear();
      epoch_history->clear();
      enum_store->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  bool const snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Failed to read section mapper!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Failed to read name-id mapper size!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Failed to read id for name-id mapper!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Failed to read name for name-id mapper!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }

  // Recover enums.
  // TODO: when we have enum deletion/edits we will need to handle remapping
  {
    spdlog::info("Recovering metadata of enums.");
    if (!snapshot.SetPosition(info.offset_enums)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_ENUMS) {
      throw RecoveryFailure("Couldn't read section enums marker!");
    }

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Couldn't read the number of enums!");
    spdlog::info("Recovering metadata of {} enums.", *size);
    for (uint64_t i = 0; i < *size; ++i) {
      auto etype = snapshot.ReadString();
      if (!etype) throw RecoveryFailure("Couldn't read enum type of enums!");

      auto value_count = snapshot.ReadUint();
      if (!value_count) throw RecoveryFailure("Couldn't read enum values length of enums!");

      auto evalues = std::vector<std::string>{};
      evalues.reserve(*value_count);
      for (uint64_t j = 0; j < *value_count; ++j) {
        auto evalue = snapshot.ReadString();
        if (!evalue) throw RecoveryFailure("Couldn't read enum value of enums!");
        evalues.emplace_back(*std::move(evalue));
      }

      auto ret = enum_store->RegisterEnum(*std::move(etype), std::move(evalues));
      if (ret.HasError()) {
        throw RecoveryFailure("The enum could not be created!");
      }
    }
    spdlog::info("Metadata of enums are recovered.");
  }

  auto get_label_from_id = [&snapshot_id_map](uint64_t label_id) {
    auto it = snapshot_id_map.find(label_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find label id in snapshot_id_map!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t property_id) {
    auto it = snapshot_id_map.find(property_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find property id in snapshot_id_map!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t edge_type_id) {
    auto it = snapshot_id_map.find(edge_type_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Couldn't find edge type id in snapshot_id_map!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
    // Recover vertices (labels and properties).
    spdlog::info("Recovering vertices.");
    uint64_t last_vertex_gid{0};

    if (!snapshot.SetPosition(info.offset_vertex_batches)) {
      throw RecoveryFailure("Couldn't read data from snapshot!");
    }

    const auto vertex_batches = ReadBatchInfos(snapshot);
    {
      RecoverOnMultipleThreads(
          config.durability.recovery_thread_count,
          [path, vertices, schema_info, &vertex_batches, &get_label_from_id, &get_property_from_id, &last_vertex_gid,
           &snapshot_info, name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
            const auto last_vertex_gid_in_batch =
                LoadPartialVertices(path, *vertices, schema_info, batch.offset, batch.count, get_label_from_id,
                                    get_property_from_id, name_id_mapper, snapshot_info);
            if (batch_index == vertex_batches.size() - 1) {
              last_vertex_gid = last_vertex_gid_in_batch;
            }
          },
          vertex_batches);
    }

    spdlog::info("Vertices are recovered.");

    spdlog::info("Recovering edges.");
    // Recover edges.
    if (snapshot_has_edges) {
      // We don't need to check whether we store properties on edge or not, because `LoadPartialEdges` will always
      // iterate over the edges in the snapshot (if they exist) and the current configuration of properties on edge only
      // affect what it does:
      // 1. If properties are allowed on edges, then it loads the edges.
      // 2. If properties are not allowed on edges, then it checks that none of the edges have any properties.
      if (!snapshot.SetPosition(info.offset_edge_batches)) {
        throw RecoveryFailure("Couldn't read data from snapshot!");
      }
      const auto edge_batches = ReadBatchInfos(snapshot);

      {
        RecoverOnMultipleThreads(
            config.durability.recovery_thread_count,
            [path, edges, items = config.salient.items, &get_property_from_id, &snapshot_info, name_id_mapper](
                const size_t /*batch_index*/, const BatchInfo &batch) {
              LoadPartialEdges(path, *edges, batch.offset, batch.count, items, get_property_from_id, name_id_mapper,
                               snapshot_info);
            },
            edge_batches);
      }
    }
    spdlog::info("Edges are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recover connectivity.");
    recovery_info.vertex_batches.reserve(vertex_batches.size());
    for (const auto batch : vertex_batches) {
      recovery_info.vertex_batches.emplace_back(Gid::FromUint(0), batch.count);
    }
    std::atomic<uint64_t> highest_edge_gid{0};

    {
      RecoverOnMultipleThreads(
          config.durability.recovery_thread_count,
          [path, vertices, edges, edges_metadata, schema_info, edge_count, items = config.salient.items,
           snapshot_has_edges, &get_edge_type_from_id, &highest_edge_gid, &recovery_info, &snapshot_info,
           name_id_mapper](const size_t batch_index, const BatchInfo &batch) {
            const auto result = LoadPartialConnectivity(path, *vertices, *edges, *edges_metadata, schema_info,
                                                        batch.offset, batch.count, items, snapshot_has_edges,
                                                        get_edge_type_from_id, name_id_mapper, snapshot_info);
            edge_count->fetch_add(result.edge_count);
            auto known_highest_edge_gid = highest_edge_gid.load();
            while (known_highest_edge_gid < result.highest_edge_id) {
              highest_edge_gid.compare_exchange_weak(known_highest_edge_gid, result.highest_edge_id);
            }
            recovery_info.vertex_batches[batch_index].first = result.first_vertex_gid;
          },
          vertex_batches);
    }
    spdlog::info("Connectivity is recovered.");

    // Set initial values for edge/vertex ID generators.
    recovery_info.next_edge_id = highest_edge_gid + 1;
    recovery_info.next_vertex_id = last_vertex_gid + 1;
  }

  // Recover indices.
  {
    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Couldn't read section indices!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of label indices");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of label index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of entries for label index statistics!");
      spdlog::info("Recovering metadata of {} label indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label while recovering label index statistics!");
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label index statistics");
        const auto label_id = get_label_from_id(*label);
        indices_constraints.indices.label_stats.emplace_back(label_id, LabelIndexStats{*count, *avg_degree});
        SPDLOG_TRACE("Recovered metadata of label index statistics for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of label properties indices.");
      spdlog::info("Recovering metadata of {} label+properties indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label properties index.");
        auto properties = std::invoke([&]() {
          auto n_props = snapshot.ReadUint();
          if (!n_props) throw RecoveryFailure("Couldn't read number of properties for label properties index.");
          std::vector<PropertyId> props;
          props.reserve(*n_props);
          for (uint64_t i = 0; i < *n_props; ++i) {
            auto property = snapshot.ReadUint();
            if (!property) throw RecoveryFailure("Couldn't read property for label properties index.");
            props.emplace_back(get_property_from_id(*property));
          }
          return props;
        });

        auto properties_string =
            properties |
            ranges::views::transform([&](PropertyId prop_id) { return name_id_mapper->IdToName(prop_id.AsUint()); }) |
            ranges::views::join(", ") | ranges::_to_::to<std::string>;

        AddRecoveredIndexConstraint(&indices_constraints.indices.label_properties,
                                    {get_label_from_id(*label), std::move(properties)},
                                    "The label+property index already exists!");
        SPDLOG_TRACE("Recovered metadata of label+property index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)), properties_string);
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    // Recover label+property indices statistics.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of entries for label property statistics!");
      spdlog::info("Recovering metadata of {} label+property indices statistics.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        const auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for label property index statistics!");
        auto n_props = snapshot.ReadUint();
        if (!n_props)
          throw RecoveryFailure("Couldn't read the number of properties for label property index statistics!");
        std::vector<PropertyId> properties;
        properties.reserve(*n_props);
        for (auto i = 0; i != *n_props; ++i) {
          const auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property for label property index statistics!");
          const auto property_id = get_property_from_id(*property);
          properties.emplace_back(property_id);
        }
        const auto count = snapshot.ReadUint();
        if (!count) throw RecoveryFailure("Couldn't read count for label property index statistics!!");
        const auto distinct_values_count = snapshot.ReadUint();
        if (!distinct_values_count)
          throw RecoveryFailure("Couldn't read distinct values count for label property index statistics!");
        const auto statistic = snapshot.ReadDouble();
        if (!statistic) throw RecoveryFailure("Couldn't read statistics value for label-property index statistics!");
        const auto avg_group_size = snapshot.ReadDouble();
        if (!avg_group_size)
          throw RecoveryFailure("Couldn't read average group size for label property index statistics!");
        const auto avg_degree = snapshot.ReadDouble();
        if (!avg_degree) throw RecoveryFailure("Couldn't read average degree for label property index statistics!");
        const auto label_id = get_label_from_id(*label);
        indices_constraints.indices.label_property_stats.emplace_back(
            label_id,
            std::make_pair(std::move(properties), LabelPropertyIndexStats{*count, *distinct_values_count, *statistic,
                                                                          *avg_group_size, *avg_degree}));
        SPDLOG_TRACE("Recovered metadata of label+property index statistics for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of label+property indices are recovered.");
    }

    spdlog::info("Recovering metadata of indices.");
    if (!snapshot.SetPosition(info.offset_edge_indices)) throw RecoveryFailure("Couldn't read data from snapshot!");

    marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EDGE_INDICES)
      throw RecoveryFailure("Couldn't read section edge-indices!");

    {
      // Recover edge-type indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge, get_edge_type_from_id(*edge_type),
                                    "The edge-type index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)));
      }
      spdlog::info("Metadata of edge-type indices are recovered.");
    }
    {
      // Recover edge-type + property indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of edge-type indices");
      spdlog::info("Recovering metadata of {} edge-type indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Couldn't read edge-type of edge-type + property index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of edge-type + property index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge_type_property,
                                    {get_edge_type_from_id(*edge_type), get_property_from_id(*property)},
                                    "The edge-type + property index already exists!");
        SPDLOG_TRACE("Recovered metadata of edge-type index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*edge_type)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of edge-type + property indices are recovered.");
    }

    {
      // Recover global edge property indices.
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of global edge property indices");
      spdlog::info("Recovering metadata of {} global edge property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of global edge property index!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.edge_property, get_property_from_id(*property),
                                    "The global edge property index already exists!");
        SPDLOG_TRACE("Recovered metadata of global edge property index for ({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of global edge property indices are recovered.");
    }

    // Recover point indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of point indices!");
      spdlog::info("Recovering metadata of {} point indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label for point index!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property for point index");
        AddRecoveredIndexConstraint(&indices_constraints.indices.point_label_property,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The point index already exists!");
        SPDLOG_TRACE("Recovered metadata of point index for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of point indices are recovered.");
    }

    // Recover vector indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of vector indices!");
      spdlog::info("Recovering metadata of {} vector indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto index_name = snapshot.ReadString();
        if (!index_name.has_value()) throw RecoveryFailure("Couldn't read vector index name!");

        // We only need to check for the existence of the vector index name -> we can't have two vector indices with the
        // same name
        if (std::ranges::any_of(indices_constraints.indices.vector_indices, [&index_name](const auto &vector_index) {
              return vector_index.index_name == index_name;
            })) {
          throw RecoveryFailure("The vector index already exists!");
        }

        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read vector index label!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read vector index property!");
        auto metric = snapshot.ReadString();
        if (!metric) throw RecoveryFailure("Couldn't read vector index metric!");
        auto metric_kind = VectorIndex::MetricFromName(metric.value());
        auto dimension = snapshot.ReadUint();
        if (!dimension) throw RecoveryFailure("Couldn't read vector index dimension!");
        auto resize_coefficient = snapshot.ReadUint();
        if (!resize_coefficient) throw RecoveryFailure("Couldn't read vector index resize coefficient!");
        auto capacity = snapshot.ReadUint();
        if (!capacity) throw RecoveryFailure("Couldn't read vector index capacity!");
        SPDLOG_TRACE("Recovered metadata of vector index {} for :{}({})", *index_name,
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));

        indices_constraints.indices.vector_indices.emplace_back(
            std::move(index_name.value()), get_label_from_id(*label), get_property_from_id(*property), metric_kind,
            static_cast<uint16_t>(*dimension), static_cast<uint16_t>(*resize_coefficient), *capacity);
      }
      spdlog::info("Metadata of vector indices are recovered.");
    }

    // Recover text indices.
    // NOTE: while this is experimental and hence optional
    //       it must be last in the SECTION_INDICES
    if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't recover the number of text indices!");
      spdlog::info("Recovering metadata of {} text indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto index_name = snapshot.ReadString();
        if (!index_name.has_value()) throw RecoveryFailure("Couldn't read text index name!");
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read text index label!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.text_indices,
                                    {index_name.value(), get_label_from_id(*label)}, "The text index already exists!");
        SPDLOG_TRACE("Recovered metadata of text index {} for :{}", index_name.value(),
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of text indices are recovered.");
    }

    spdlog::info("Metadata of indices are recovered.");
  }

  // Recover constraints.
  {
    spdlog::info("Recovering metadata of constraints.");
    if (!snapshot.SetPosition(info.offset_constraints)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS)
      throw RecoveryFailure("Couldn't read section constraints marker!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of existence constraints!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of existence constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of existence constraints!");
        AddRecoveredIndexConstraint(&indices_constraints.constraints.existence,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
                                    "The existence constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of existence constraint for :{}({})",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of existence constraints are recovered.");
    }

    // Recover unique constraints.
    // Snapshot version should be checked since unique constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of unique constraints!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of unique constraints!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Couldn't read the number of properties in unique constraint!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Couldn't read property of unique constraint!");
          properties.insert(get_property_from_id(*property));
        }
        AddRecoveredIndexConstraint(&indices_constraints.constraints.unique, {get_label_from_id(*label), properties},
                                    "The unique constraint already exists!");
        SPDLOG_TRACE("Recovered metadata of unique constraints for :{}",
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of unique constraints are recovered.");
    }

    // Recover type constraints.
    // Snapshot version should be checked since type constraints were
    // implemented in later versions of snapshot.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Couldn't read the number of type constraints!");

      spdlog::info("Recovering metadata of {} type constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Couldn't read label of type constraints!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Couldn't read property of type constraint!");
        auto type = snapshot.ReadUint();
        if (!type) throw RecoveryFailure("Couldn't read type of type constraint!");

        AddRecoveredIndexConstraint(
            &indices_constraints.constraints.type,
            {get_label_from_id(*label), get_property_from_id(*property), static_cast<TypeConstraintKind>(*type)},
            "The type constraint already exists!");
        SPDLOG_TRACE("Recovered metadata for IS TYPED {} constraint for :{}({})",
                     TypeConstraintKindToString(static_cast<TypeConstraintKind>(*type)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                     name_id_mapper->IdToName(snapshot_id_map.at(*property)));
      }
      spdlog::info("Metadata of type constraints are recovered.");
    }

    spdlog::info("Metadata of constraints are recovered.");
  }

  spdlog::info("Recovering metadata.");
  // Recover epoch history
  {
    if (!snapshot.SetPosition(info.offset_epoch_history)) throw RecoveryFailure("Couldn't read data from snapshot!");

    const auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY)
      throw RecoveryFailure("Couldn't read section epoch history marker!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Couldn't read history size!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Couldn't read maybe epoch id!");
      }
      const auto maybe_last_durable_timestamp = snapshot.ReadUint();
      if (!maybe_last_durable_timestamp) {
        throw RecoveryFailure("Couldn't read maybe last durable timestamp!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_durable_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  recovery_info.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, recovery_info, std::move(indices_constraints)};
}

RecoveredSnapshot LoadSnapshot(const std::filesystem::path &path, utils::SkipList<Vertex> *vertices,
                               utils::SkipList<Edge> *edges, utils::SkipList<EdgeMetadata> *edges_metadata,
                               std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                               NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count, const Config &config,
                               memgraph::storage::EnumStore *enum_store, SharedSchemaTracking *schema_info,
                               std::optional<SnapshotObserverInfo> const &snapshot_info) {
  Decoder snapshot;
  const auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version) throw RecoveryFailure("Couldn't read snapshot magic and/or version!");
  if (!IsVersionSupported(*version)) throw RecoveryFailure(fmt::format("Invalid snapshot version {}", *version));

  switch (*version) {
    case 14U: {
      return LoadSnapshotVersion14(snapshot, path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                   edge_count, schema_info, config.salient.items);
    }
    case 15U: {
      return LoadSnapshotVersion15(snapshot, path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                   edge_count, schema_info, config);
    }
    case 16U: {
      return LoadSnapshotVersion16(snapshot, path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                   edge_count, schema_info, config);
    }
    case 17U: {
      return LoadSnapshotVersion17(snapshot, path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                   edge_count, schema_info, config);
    }
    case 18U:
    case 19U: {
      return LoadSnapshotVersion18or19(snapshot, path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                       edge_count, schema_info, config, enum_store);
    }
    case 20U:
    case 21U: {
      return LoadSnapshotVersion20or21(snapshot, path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                       edge_count, schema_info, config, enum_store);
    }
    case 22U:
    case 23U: {
      // Version 23 updated the snapshot info (handled via the ReadSnapshotInfo function)
      return LoadSnapshotVersion22or23(snapshot, path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                       edge_count, config, enum_store, schema_info, snapshot_info);
    }
    case 24U: {
      return LoadCurrentVersionSnapshot(snapshot, path, vertices, edges, edges_metadata, epoch_history, name_id_mapper,
                                        edge_count, config, enum_store, schema_info, snapshot_info);
    }

    default: {
      // `IsVersionSupported` checks that the version is within the supported
      // range. This catches the case of the version having been updated but no
      // matching implementation yet having being written.
      MG_ASSERT(false, "Trying to load snapshot for unimplemented version");
    }
  }
}

using OldSnapshotFiles = std::vector<std::pair<uint64_t, std::filesystem::path>>;
void EnsureNecessaryWalFilesExist(const std::filesystem::path &wal_directory, const std::string &uuid,
                                  OldSnapshotFiles old_snapshot_files, Transaction *transaction,
                                  utils::FileRetainer *file_retainer, NameIdMapper *name_id_mapper) {
  std::vector<std::tuple<uint64_t, uint64_t, uint64_t, std::filesystem::path>> wal_files;
  std::error_code error_code;
  for (const auto &item : std::filesystem::directory_iterator(wal_directory, error_code)) {
    if (!item.is_regular_file()) continue;
    try {
      auto info = ReadWalInfo(item.path());
      if (info.uuid != uuid) continue;
      wal_files.emplace_back(info.seq_num, info.from_timestamp, info.to_timestamp, item.path());
    } catch (const RecoveryFailure &e) {
      spdlog::warn("Found a corrupt WAL file {} because of: {}. WAL file will NOT be deleted.", item.path(), e.what());
      // TODO If we want to do this we need a way to protect current wal file
      // We can't get the engine lock here
      // Maybe the file locker can help us. Careful, in any case we don't really want to delete it by accident.
      // spdlog::warn("Found a corrupt WAL file {} because of: {}. WAL file will be deleted.", item.path(), e.what());
      // file_retainer->DeleteFile(item.path());
    }
  }

  if (error_code) {
    spdlog::error(
        utils::MessageWithLink("Couldn't ensure that only the absolutely necessary WAL files exist "
                               "because an error occurred: {}.",
                               error_code.message(), "https://memgr.ph/snapshots"));
  }
  std::sort(wal_files.begin(), wal_files.end());
  uint64_t snapshot_start_timestamp = transaction->start_timestamp;
  if (!old_snapshot_files.empty()) {
    snapshot_start_timestamp = old_snapshot_files.front().first;
  }
  std::optional<uint64_t> pos = 0;
  for (uint64_t i = 0; i < wal_files.size(); ++i) {
    const auto &[seq_num, from_timestamp, to_timestamp, wal_path] = wal_files[i];
    if (from_timestamp <= snapshot_start_timestamp) {
      pos = i;
    } else {
      break;
    }
  }
  if (pos && *pos > 0) {
    // We need to leave at least one WAL file that contains deltas that were
    // created before the oldest snapshot. Because we always leave at least
    // one WAL file that contains deltas before the snapshot, this correctly
    // handles the edge case when that one file is the current WAL file that
    // is being appended to.
    for (uint64_t i = 0; i < *pos; ++i) {
      const auto &[seq_num, from_timestamp, to_timestamp, wal_path] = wal_files[i];
      file_retainer->DeleteFile(wal_path);
    }
  }
}

auto EnsureRetentionCountSnapshotsExist(const std::filesystem::path &snapshot_directory, const std::string &path,
                                        const std::string &uuid, utils::FileRetainer *file_retainer,
                                        Storage *storage) -> OldSnapshotFiles {
  OldSnapshotFiles old_snapshot_files;
  std::error_code error_code;
  for (const auto &item : std::filesystem::directory_iterator(snapshot_directory, error_code)) {
    if (!item.is_regular_file()) continue;
    if (item.path() == path) continue;
    try {
      auto info = ReadSnapshotInfo(item.path());
      if (info.uuid != uuid) continue;
      old_snapshot_files.emplace_back(info.start_timestamp, item.path());
    } catch (const RecoveryFailure &e) {
      spdlog::warn("Found a corrupt snapshot file {} becuase of: {}. Corrupted snapshot file will be deleted.",
                   item.path(), e.what());
      file_retainer->DeleteFile(item.path());
    }
  }

  if (error_code) {
    spdlog::error(utils::MessageWithLink(
        "Couldn't ensure that exactly {} snapshots exist because an error occurred: {}.",
        storage->config_.durability.snapshot_retention_count, error_code.message(), "https://memgr.ph/snapshots"));
  }

  std::sort(old_snapshot_files.begin(), old_snapshot_files.end());
  if (old_snapshot_files.size() <= storage->config_.durability.snapshot_retention_count - 1) return old_snapshot_files;

  const uint32_t num_to_erase = old_snapshot_files.size() - (storage->config_.durability.snapshot_retention_count - 1);
  for (size_t i = 0; i < num_to_erase; ++i) {
    const auto &[_, snapshot_path] = old_snapshot_files[i];
    file_retainer->DeleteFile(snapshot_path);
  }
  old_snapshot_files.erase(old_snapshot_files.begin(), old_snapshot_files.begin() + num_to_erase);
  return old_snapshot_files;
}

bool CreateSnapshot(Storage *storage, Transaction *transaction, const std::filesystem::path &snapshot_directory,
                    const std::filesystem::path &wal_directory, utils::SkipList<Vertex> *vertices,
                    utils::SkipList<Edge> *edges, utils::UUID const &uuid,
                    const memgraph::replication::ReplicationEpoch &epoch,
                    const std::deque<std::pair<std::string, uint64_t>> &epoch_history,
                    utils::FileRetainer *file_retainer, std::atomic_bool *abort_snapshot) {
  utils::Timer timer;

  auto const snapshot_aborted = [abort_snapshot, &timer]() -> bool {
    if (abort_snapshot == nullptr) return false;
    if (timer.Elapsed() >= kCheckIfSnapshotAborted) {
      const bool abort = abort_snapshot->load(std::memory_order_acquire);
      if (!abort) timer.ResetStartTime();  // Leave timer as elapsed, so future checks also retrun true
      return abort;
    }
    return false;
  };

  // Ensure that the storage directory exists.
  utils::EnsureDirOrDie(snapshot_directory);

  // Create snapshot file.
  auto path = snapshot_directory / MakeSnapshotName(transaction->last_durable_ts_ ? *transaction->last_durable_ts_
                                                                                  : transaction->start_timestamp);
  spdlog::info("Starting snapshot creation to {}", path);
  SnapshotEncoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic, kVersion);

  // Write placeholder offsets.
  uint64_t offset_offsets = 0;
  uint64_t offset_edges = 0;
  uint64_t offset_vertices = 0;
  uint64_t offset_indices = 0;
  uint64_t offset_edge_indices = 0;
  uint64_t offset_constraints = 0;
  uint64_t offset_mapper = 0;
  uint64_t offset_enums = 0;
  uint64_t offset_metadata = 0;
  uint64_t offset_epoch_history = 0;
  uint64_t offset_edge_batches = 0;
  uint64_t offset_vertex_batches = 0;

  auto write_offsets = [&] {
    snapshot.WriteUint(offset_edges);
    snapshot.WriteUint(offset_vertices);
    snapshot.WriteUint(offset_indices);
    snapshot.WriteUint(offset_edge_indices);
    snapshot.WriteUint(offset_constraints);
    snapshot.WriteUint(offset_mapper);
    snapshot.WriteUint(offset_enums);
    snapshot.WriteUint(offset_epoch_history);
    snapshot.WriteUint(offset_metadata);
    snapshot.WriteUint(offset_edge_batches);
    snapshot.WriteUint(offset_vertex_batches);
  };

  {
    snapshot.WriteMarker(Marker::SECTION_OFFSETS);
    offset_offsets = snapshot.GetPosition();
    // write placeholders...will correct them later
    write_offsets();
  }

  // Object counters.
  uint64_t edges_count = 0;
  uint64_t vertices_count = 0;

  // Mapper data.
  std::unordered_set<uint64_t> used_ids;
  auto write_mapping = [&snapshot, &used_ids](auto mapping) {
    used_ids.insert(mapping.AsUint());
    snapshot.WriteUint(mapping.AsUint());
  };

  // Specify destination for the mapper data (used in multi-threaded)
  auto write_mapping_to = [](auto &snapshot, auto &used_ids, auto mapping) {
    used_ids.insert(mapping.AsUint());
    snapshot.WriteUint(mapping.AsUint());
  };

  // Store edges.
  auto partial_edge_handler = [&edges, storage, transaction, &snapshot_aborted, &write_mapping_to](
                                  int64_t start_gid, int64_t end_gid, auto &edges_snapshot) -> SnapshotPartialRes {
    if (start_gid >= end_gid) return {};

    auto counter = utils::ResettableCounter{50};  // Counter used to reduce the frequency of checking abort
    auto res = SnapshotPartialRes{.snapshot_path = edges_snapshot.GetPath()};
    auto items_in_current_batch{0UL};
    auto batch_start_offset = edges_snapshot.GetPosition();

    auto acc = edges->access();
    // edge_id_ is monotonically increasing, holds a value which is currently unused
    auto const unused_edge_gid = storage->edge_id_.load(std::memory_order_acquire);

    // Comparison start <= elem.gid < end with GID is important here because we need to
    // ensure that we are not reading elemets that are not in the batch.
    auto it = acc.find_equal_or_greater(Gid::FromInt(start_gid));
    for (; it != acc.end() && it->gid.AsInt() < end_gid; ++it) {
      // This is a hot loop, use counter to reduce the frequency that we check for abort
      if (counter() && snapshot_aborted()) [[unlikely]] {
        break;
      }

      auto &edge = *it;

      // If we have reached a newly inserted edge, we can stop processing
      if (unused_edge_gid <= edge.gid.AsUint()) [[unlikely]] {
        break;
      }

      // The edge visibility check must be done here manually because we don't
      // allow direct access to the edges through the public API.
      bool is_visible = true;
      Delta *delta = nullptr;
      {
        auto guard = std::shared_lock{edge.lock};
        is_visible = !edge.deleted;
        delta = edge.delta;
      }
      ApplyDeltasForRead(transaction, delta, View::OLD, [&is_visible](const Delta &delta) {
        switch (delta.action) {
          case Delta::Action::ADD_LABEL:
          case Delta::Action::REMOVE_LABEL:
          case Delta::Action::SET_PROPERTY:
          case Delta::Action::ADD_IN_EDGE:
          case Delta::Action::ADD_OUT_EDGE:
          case Delta::Action::REMOVE_IN_EDGE:
          case Delta::Action::REMOVE_OUT_EDGE:
            break;
          case Delta::Action::RECREATE_OBJECT: {
            is_visible = true;
            break;
          }
          case Delta::Action::DELETE_DESERIALIZED_OBJECT:
          case Delta::Action::DELETE_OBJECT: {
            is_visible = false;
            break;
          }
        }
      });
      if (!is_visible) continue;
      EdgeRef edge_ref(&edge);
      // Here we create an edge accessor that we will use to get the
      // properties of the edge. The accessor is created with an invalid
      // type and invalid from/to pointers because we don't know them here,
      // but that isn't an issue because we won't use that part of the API
      // here.
      auto ea = EdgeAccessor{edge_ref, EdgeTypeId::FromUint(0UL), nullptr, nullptr, storage, transaction};

      // Get edge data.
      auto maybe_props = ea.Properties(View::OLD);
      MG_ASSERT(maybe_props.HasValue(), "Invalid database state!");

      // Store the edge.
      {
        edges_snapshot.WriteMarker(Marker::SECTION_EDGE);
        edges_snapshot.WriteUint(edge.gid.AsUint());
        const auto &props = maybe_props.GetValue();
        edges_snapshot.WriteUint(props.size());
        for (const auto &item : props) {
          write_mapping_to(edges_snapshot, res.used_ids, item.first);
          edges_snapshot.WriteExternalPropertyValue(
              ToExternalPropertyValue(item.second, storage->name_id_mapper_.get()));
        }
      }

      ++res.count;
      ++items_in_current_batch;
      if (items_in_current_batch == storage->config_.durability.items_per_batch) {
        res.batch_info.push_back(BatchInfo{batch_start_offset, items_in_current_batch});
        batch_start_offset = edges_snapshot.GetPosition();
        items_in_current_batch = 0;
      }
    }
    res.snapshot_size = edges_snapshot.GetSize();

    if (items_in_current_batch > 0) {
      // Needs updating before appending to the snapshot
      res.batch_info.push_back(BatchInfo{batch_start_offset, items_in_current_batch});
    }

    return res;
  };

  // Store vertices.
  auto partial_vertex_handler = [&vertices, storage, transaction, &snapshot_aborted, &write_mapping_to](
                                    int64_t start_gid, int64_t end_gid, auto &vertex_snapshot) -> SnapshotPartialRes {
    if (start_gid >= end_gid) return {};

    auto counter = utils::ResettableCounter{50};  // Counter used to reduce the frequency of checking abort
    auto res = SnapshotPartialRes{.snapshot_path = vertex_snapshot.GetPath()};
    auto items_in_current_batch = 0UL;
    auto batch_start_offset = vertex_snapshot.GetPosition();

    auto acc = vertices->access();

    // vertex_id_ is monotonically increasing, holds a value which is currently unused
    auto const unused_vertex_gid = storage->vertex_id_.load(std::memory_order_acquire);

    // Comparison start <= elem.gid < end with GID is important here because we need to
    // ensure that we are not reading elemets that are not in the batch.
    auto it = acc.find_equal_or_greater(Gid::FromInt(start_gid));
    for (; it != acc.end() && it->gid.AsInt() < end_gid; ++it) {
      // This is a hot loop, use counter to reduce the frequency that we check for abort
      if (counter() && snapshot_aborted()) [[unlikely]] {
        break;
      }

      auto &vertex = *it;

      // If we have reached a newly inserted vertex, we can stop processing
      if (unused_vertex_gid <= vertex.gid.AsUint()) [[unlikely]] {
        break;
      }

      // The visibility check is implemented for vertices so we use it here.
      auto va = VertexAccessor::Create(&vertex, storage, transaction, View::OLD);
      if (!va) continue;

      // Get vertex data.
      // TODO (mferencevic): All of these functions could be written into a
      // single function so that we traverse the undo deltas only once.
      auto maybe_labels = va->Labels(View::OLD);
      MG_ASSERT(maybe_labels.HasValue(), "Invalid database state!");
      auto maybe_props = va->Properties(View::OLD);
      MG_ASSERT(maybe_props.HasValue(), "Invalid database state!");
      auto maybe_in_edges = va->InEdges(View::OLD);
      MG_ASSERT(maybe_in_edges.HasValue(), "Invalid database state!");
      auto maybe_out_edges = va->OutEdges(View::OLD);
      MG_ASSERT(maybe_out_edges.HasValue(), "Invalid database state!");

      // Store the vertex.
      {
        vertex_snapshot.WriteMarker(Marker::SECTION_VERTEX);
        vertex_snapshot.WriteUint(vertex.gid.AsUint());
        const auto &labels = maybe_labels.GetValue();
        vertex_snapshot.WriteUint(labels.size());
        for (const auto &item : labels) {
          write_mapping_to(vertex_snapshot, res.used_ids, item);
        }
        const auto &props = maybe_props.GetValue();
        vertex_snapshot.WriteUint(props.size());
        for (const auto &item : props) {
          write_mapping_to(vertex_snapshot, res.used_ids, item.first);
          vertex_snapshot.WriteExternalPropertyValue(
              ToExternalPropertyValue(item.second, storage->name_id_mapper_.get()));
        }
        const auto &in_edges = maybe_in_edges.GetValue().edges;
        const auto &out_edges = maybe_out_edges.GetValue().edges;

        if (storage->config_.salient.items.properties_on_edges) {
          vertex_snapshot.WriteUint(in_edges.size());
          for (const auto &item : in_edges) {
            vertex_snapshot.WriteUint(item.GidPropertiesOnEdges().AsUint());
            vertex_snapshot.WriteUint(item.FromVertex().Gid().AsUint());
            write_mapping_to(vertex_snapshot, res.used_ids, item.EdgeType());
          }
          vertex_snapshot.WriteUint(out_edges.size());
          for (const auto &item : out_edges) {
            vertex_snapshot.WriteUint(item.GidPropertiesOnEdges().AsUint());
            vertex_snapshot.WriteUint(item.ToVertex().Gid().AsUint());
            write_mapping_to(vertex_snapshot, res.used_ids, item.EdgeType());
          }
        } else {
          vertex_snapshot.WriteUint(in_edges.size());
          for (const auto &item : in_edges) {
            vertex_snapshot.WriteUint(item.GidNoPropertiesOnEdges().AsUint());
            vertex_snapshot.WriteUint(item.FromVertex().Gid().AsUint());
            write_mapping_to(vertex_snapshot, res.used_ids, item.EdgeType());
          }
          vertex_snapshot.WriteUint(out_edges.size());
          for (const auto &item : out_edges) {
            vertex_snapshot.WriteUint(item.GidNoPropertiesOnEdges().AsUint());
            vertex_snapshot.WriteUint(item.ToVertex().Gid().AsUint());
            write_mapping_to(vertex_snapshot, res.used_ids, item.EdgeType());
          }
        }
      }

      ++res.count;
      ++items_in_current_batch;
      if (items_in_current_batch == storage->config_.durability.items_per_batch) {
        res.batch_info.push_back(BatchInfo{batch_start_offset, items_in_current_batch});
        batch_start_offset = vertex_snapshot.GetPosition();
        items_in_current_batch = 0;
      }
    }

    if (items_in_current_batch > 0) {
      // This needs to be updated
      res.batch_info.push_back(BatchInfo{batch_start_offset, items_in_current_batch});
    }

    res.snapshot_size = vertex_snapshot.GetSize();
    return res;
  };

  std::vector<BatchInfo> edge_batch_infos;
  std::vector<BatchInfo> vertex_batch_infos;

  if (storage->config_.durability.allow_parallel_snapshot_creation) {
    auto *edge_ptr = storage->config_.salient.items.properties_on_edges ? edges : nullptr;
    MultiThreadedWorkflow(edge_ptr, vertices, partial_edge_handler, partial_vertex_handler,
                          storage->config_.durability.items_per_batch, offset_edges, offset_vertices, snapshot,
                          edges_count, vertices_count, edge_batch_infos, vertex_batch_infos, used_ids,
                          storage->config_.durability.snapshot_thread_count, snapshot_aborted);
  } else {
    if (storage->config_.salient.items.properties_on_edges) {
      offset_edges = snapshot.GetPosition();  // Global edge offset
      // Handle edges
      const auto res = partial_edge_handler(0, kEnd, snapshot);
      edges_count = res.count;
      edge_batch_infos = res.batch_info;
      used_ids.insert(res.used_ids.begin(), res.used_ids.end());
    }
    if (snapshot_aborted()) {
      return false;
    }
    {
      offset_vertices = snapshot.GetPosition();  // Global vertex offset
      // Handle vertices
      const auto res = partial_vertex_handler(0, kEnd, snapshot);
      vertices_count = res.count;
      vertex_batch_infos = res.batch_info;
      used_ids.insert(res.used_ids.begin(), res.used_ids.end());
    }
    if (snapshot_aborted()) {
      return false;
    }
  }

  // Write indices.
  {
    offset_indices = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_INDICES);

    // Write label indices.
    {
      auto label = storage->indices_.label_index_->ListIndices();
      snapshot.WriteUint(label.size());
      for (const auto &item : label) {
        write_mapping(item);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write label indices statistics.
    {
      // NOTE: On-disk does not support snapshots
      auto *inmem_index = static_cast<InMemoryLabelIndex *>(storage->indices_.label_index_.get());
      auto label = inmem_index->ListIndices();
      const auto size_pos = snapshot.GetPosition();
      snapshot.WriteUint(0);  // Just a place holder
      unsigned i = 0;
      for (const auto &item : label) {
        auto stats = inmem_index->GetIndexStats(item);
        if (stats) {
          snapshot.WriteUint(item.AsUint());
          snapshot.WriteUint(stats->count);
          snapshot.WriteDouble(stats->avg_degree);
          ++i;
        }
      }
      if (i != 0) {
        const auto last_pos = snapshot.GetPosition();
        snapshot.SetPosition(size_pos);
        snapshot.WriteUint(i);  // Write real size
        snapshot.SetPosition(last_pos);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write label+properties indices.
    {
      auto label_property = transaction->active_indices_->ListIndices();
      snapshot.WriteUint(label_property.size());
      for (const auto &[label, properties] : label_property) {
        write_mapping(label);
        snapshot.WriteUint(properties.size());
        for (auto prop : properties) {
          write_mapping(prop);
        }
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write label+property indices statistics.
    {
      // NOTE: On-disk does not support snapshots
      auto *inmem_index = static_cast<InMemoryLabelPropertyIndex *>(storage->indices_.label_property_index_.get());
      auto label = transaction->active_indices_->ListIndices();
      const auto size_pos = snapshot.GetPosition();
      snapshot.WriteUint(0);  // Just a place holder
      unsigned i = 0;
      for (const auto &item : label) {
        auto stats = inmem_index->GetIndexStats(item);
        if (stats) {
          // TODO: should we use write_mapping for the label + properties here?
          snapshot.WriteUint(item.first.AsUint());
          snapshot.WriteUint(item.second.size());
          for (auto const &prop : item.second) {
            snapshot.WriteUint(prop.AsUint());
          }
          snapshot.WriteUint(stats->count);
          snapshot.WriteUint(stats->distinct_values_count);
          snapshot.WriteDouble(stats->statistic);
          snapshot.WriteDouble(stats->avg_group_size);
          snapshot.WriteDouble(stats->avg_degree);
          ++i;
        }
      }
      if (i != 0) {
        const auto last_pos = snapshot.GetPosition();
        snapshot.SetPosition(size_pos);
        snapshot.WriteUint(i);  // Write real size
        snapshot.SetPosition(last_pos);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write edge-type indices.
    offset_edge_indices = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_EDGE_INDICES);
    {
      auto edge_type = storage->indices_.edge_type_index_->ListIndices();
      snapshot.WriteUint(edge_type.size());
      for (const auto &item : edge_type) {
        write_mapping(item);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write edge-type + property indices.
    {
      auto edge_type = storage->indices_.edge_type_property_index_->ListIndices();
      snapshot.WriteUint(edge_type.size());
      for (const auto &item : edge_type) {
        write_mapping(item.first);
        write_mapping(item.second);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write global edge property indices.
    {
      auto indices = storage->indices_.edge_property_index_->ListIndices();
      snapshot.WriteUint(indices.size());
      for (const auto &property : indices) {
        write_mapping(property);
      }
    }

    // Write point indices.
    {
      auto point_keys = storage->indices_.point_index_.ListIndices();
      snapshot.WriteUint(point_keys.size());
      for (const auto &[label, property] : point_keys) {
        write_mapping(label);
        write_mapping(property);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write vector indices.
    {
      auto vector_indices = storage->indices_.vector_index_.ListIndices();
      snapshot.WriteUint(vector_indices.size());
      for (const auto &[index_name, label, property, metric, dimension, resize_coefficient, capacity] :
           vector_indices) {
        snapshot.WriteString(index_name);
        write_mapping(label);
        write_mapping(property);
        snapshot.WriteString(VectorIndex::NameFromMetric(metric));
        snapshot.WriteUint(dimension);
        snapshot.WriteUint(resize_coefficient);
        snapshot.WriteUint(capacity);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write text indices.
    if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
      auto text_indices = storage->indices_.text_index_.ListIndices();
      snapshot.WriteUint(text_indices.size());
      for (const auto &[index_name, label] : text_indices) {
        snapshot.WriteString(index_name);
        write_mapping(label);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }
  }

  // Write constraints.
  {
    offset_constraints = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_CONSTRAINTS);

    // Write existence constraints.
    {
      auto existence = storage->constraints_.existence_constraints_->ListConstraints();
      snapshot.WriteUint(existence.size());
      for (const auto &item : existence) {
        write_mapping(item.first);
        write_mapping(item.second);
      }
      if (snapshot_aborted()) {
        return false;
      }
    }

    // Write unique constraints.
    {
      auto unique = storage->constraints_.unique_constraints_->ListConstraints();
      snapshot.WriteUint(unique.size());
      for (const auto &item : unique) {
        write_mapping(item.first);
        snapshot.WriteUint(item.second.size());
        for (const auto &property : item.second) {
          write_mapping(property);
        }
      }
      if (snapshot_aborted()) {
        return false;
      }
    }
    // Write type constraints
    {
      auto type_constraints = storage->constraints_.type_constraints_->ListConstraints();
      snapshot.WriteUint(type_constraints.size());
      for (const auto &[label, property, type] : type_constraints) {
        write_mapping(label);
        write_mapping(property);
        snapshot.WriteUint(static_cast<uint64_t>(type));
      }
      if (snapshot_aborted()) {
        return false;
      }
    }
  }

  // Write mapper data.
  {
    offset_mapper = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_MAPPER);
    snapshot.WriteUint(used_ids.size());
    // Sort ids so they match expectations
    std::vector<uint64_t> sorted_ids(used_ids.begin(), used_ids.end());
    std::sort(sorted_ids.begin(), sorted_ids.end());
    for (auto item : sorted_ids) {
      snapshot.WriteUint(item);
      snapshot.WriteString(storage->name_id_mapper_->IdToName(item));
    }
    if (snapshot_aborted()) {
      return false;
    }
  }

  // Write enums
  {
    offset_enums = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_ENUMS);
    // TODO: once delete/modify of enums added we need to be more careful in the
    //       mapping of current inmemory enum properties
    auto all_enums = storage->enum_store_.AllRegistered();
    snapshot.WriteUint(all_enums.size());
    for (auto const &[etype, evalues] : all_enums) {
      snapshot.WriteString(etype);
      snapshot.WriteUint(evalues.size());
      for (auto const &evalue : evalues) {
        snapshot.WriteString(evalue);
      }
    }
    if (snapshot_aborted()) {
      return false;
    }
  }

  // Write epoch history
  {
    offset_epoch_history = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_EPOCH_HISTORY);
    snapshot.WriteUint(epoch_history.size());
    for (const auto &[epoch_id, last_durable_timestamp] : epoch_history) {
      snapshot.WriteString(epoch_id);
      snapshot.WriteUint(last_durable_timestamp);
    }
    if (snapshot_aborted()) {
      return false;
    }
  }

  // Write metadata.
  auto const uuid_str = std::string{uuid};
  {
    offset_metadata = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_METADATA);
    snapshot.WriteString(uuid_str);
    snapshot.WriteString(epoch.id());
    snapshot.WriteUint(transaction->start_timestamp);
    snapshot.WriteUint(transaction->last_durable_ts_ ? *transaction->last_durable_ts_
                                                     : transaction->start_timestamp);  // Fallback to start ts
    snapshot.WriteUint(edges_count);
    snapshot.WriteUint(vertices_count);
    if (snapshot_aborted()) {
      return false;
    }
  }

  auto write_batch_infos = [&snapshot, &snapshot_aborted](const std::vector<BatchInfo> &batch_infos) -> bool {
    snapshot.WriteUint(batch_infos.size());
    for (const auto &batch_info : batch_infos) {
      snapshot.WriteUint(batch_info.offset);
      snapshot.WriteUint(batch_info.count);
      if (snapshot_aborted()) {
        return false;
      }
    }
    return true;
  };

  // Write edge batches
  {
    offset_edge_batches = snapshot.GetPosition();
    if (!write_batch_infos(edge_batch_infos)) {
      return false;
    }
  }

  // Write vertex batches
  {
    offset_vertex_batches = snapshot.GetPosition();
    if (!write_batch_infos(vertex_batch_infos)) {
      return false;
    }
  }

  // Write true offsets.
  {
    snapshot.SetPosition(offset_offsets);
    write_offsets();
  }

  if (snapshot_aborted()) {
    return false;
  }

  // Finalize snapshot file.
  spdlog::trace("Finalizing snapshot file!");
  snapshot.Finalize();
  spdlog::info("Snapshot creation successful!");

  OldSnapshotFiles old_snapshot_files =
      EnsureRetentionCountSnapshotsExist(snapshot_directory, path, uuid_str, file_retainer, storage);

  if (old_snapshot_files.size() == storage->config_.durability.snapshot_retention_count - 1 &&
      utils::DirExists(wal_directory)) {
    EnsureNecessaryWalFilesExist(wal_directory, uuid_str, std::move(old_snapshot_files), transaction, file_retainer,
                                 storage->name_id_mapper_.get());
  }

  // We are not updating ldt here; we are only updating it when recovering from snapshot (because there is no other
  // timestamp to use) and we are relaxing the ts checks on replica
  return true;
}

}  // namespace memgraph::storage::durability
