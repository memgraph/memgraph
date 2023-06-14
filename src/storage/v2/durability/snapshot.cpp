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

#include "storage/v2/durability/snapshot.hpp"

#include <thread>

#include "storage/v2/durability/exceptions.hpp"
#include "storage/v2/durability/paths.hpp"
#include "storage/v2/durability/serialization.hpp"
#include "storage/v2/durability/version.hpp"
#include "storage/v2/durability/wal.hpp"
#include "storage/v2/edge.hpp"
#include "storage/v2/edge_accessor.hpp"
#include "storage/v2/edge_ref.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/mvcc.hpp"
#include "storage/v2/vertex.hpp"
#include "storage/v2/vertex_accessor.hpp"
#include "utils/concepts.hpp"
#include "utils/file_locker.hpp"
#include "utils/logging.hpp"
#include "utils/message.hpp"
#include "utils/spin_lock.hpp"
#include "utils/synchronized.hpp"

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

// Function used to read information about the snapshot file.
SnapshotInfo ReadSnapshotInfo(const std::filesystem::path &path) {
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
    if (!marker || *marker != Marker::SECTION_OFFSETS) throw RecoveryFailure("Invalid snapshot data!");

    auto snapshot_size = snapshot.GetSize();
    if (!snapshot_size) throw RecoveryFailure("Couldn't read data from snapshot!");

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
    info.offset_constraints = read_offset();
    info.offset_mapper = read_offset();
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
    if (!snapshot.SetPosition(info.offset_metadata)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_METADATA) throw RecoveryFailure("Invalid snapshot data!");

    auto maybe_uuid = snapshot.ReadString();
    if (!maybe_uuid) throw RecoveryFailure("Invalid snapshot data!");
    info.uuid = std::move(*maybe_uuid);

    auto maybe_epoch_id = snapshot.ReadString();
    if (!maybe_epoch_id) throw RecoveryFailure("Invalid snapshot data!");
    info.epoch_id = std::move(*maybe_epoch_id);

    auto maybe_timestamp = snapshot.ReadUint();
    if (!maybe_timestamp) throw RecoveryFailure("Invalid snapshot data!");
    info.start_timestamp = *maybe_timestamp;

    auto maybe_edges = snapshot.ReadUint();
    if (!maybe_edges) throw RecoveryFailure("Invalid snapshot data!");
    info.edges_count = *maybe_edges;

    auto maybe_vertices = snapshot.ReadUint();
    if (!maybe_vertices) throw RecoveryFailure("Invalid snapshot data!");
    info.vertices_count = *maybe_vertices;
  }

  return info;
}

std::vector<BatchInfo> ReadBatchInfos(Decoder &snapshot) {
  std::vector<BatchInfo> infos;
  const auto infos_size = snapshot.ReadUint();
  if (!infos_size.has_value()) {
    throw RecoveryFailure("Invalid snapshot data!");
  }
  infos.reserve(*infos_size);

  for (auto i{0U}; i < *infos_size; ++i) {
    const auto offset = snapshot.ReadUint();
    if (!offset.has_value()) {
      throw RecoveryFailure("Invalid snapshot data!");
    }

    const auto count = snapshot.ReadUint();
    if (!count.has_value()) {
      throw RecoveryFailure("Invalid snapshot data!");
    }
    infos.push_back(BatchInfo{*offset, *count});
  }
  return infos;
}

template <typename TFunc>
void LoadPartialEdges(const std::filesystem::path &path, utils::SkipList<Edge> &edges, const uint64_t from_offset,
                      const uint64_t edges_count, const Config::Items items, TFunc get_property_from_id) {
  Decoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic);

  // Recover edges.
  auto edge_acc = edges.access();
  uint64_t last_edge_gid = 0;
  spdlog::info("Recovering {} edges.", edges_count);
  if (!snapshot.SetPosition(from_offset)) throw RecoveryFailure("Couldn't read data from snapshot!");

  std::vector<std::pair<PropertyId, PropertyValue>> read_properties;
  for (uint64_t i = 0; i < edges_count; ++i) {
    {
      const auto marker = snapshot.ReadMarker();
      if (!marker || *marker != Marker::SECTION_EDGE) throw RecoveryFailure("Invalid snapshot data!");
    }

    // Read edge GID.
    auto gid = snapshot.ReadUint();
    if (!gid) throw RecoveryFailure("Invalid snapshot data!");
    if (i > 0 && *gid <= last_edge_gid) throw RecoveryFailure("Invalid snapshot data!");
    last_edge_gid = *gid;

    if (items.properties_on_edges) {
      spdlog::debug("Recovering edge {} with properties.", *gid);
      auto [it, inserted] = edge_acc.insert(Edge{Gid::FromUint(*gid), nullptr});
      if (!inserted) throw RecoveryFailure("The edge must be inserted here!");

      // Recover properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
        auto &props = it->properties;
        read_properties.clear();
        read_properties.reserve(*props_size);
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Invalid snapshot data!");
          auto value = snapshot.ReadPropertyValue();
          if (!value) throw RecoveryFailure("Invalid snapshot data!");
          read_properties.emplace_back(get_property_from_id(*key), std::move(*value));
        }
        props.InitProperties(std::move(read_properties));
      }
    } else {
      spdlog::debug("Ensuring edge {} doesn't have any properties.", *gid);
      // Read properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
        if (*props_size != 0)
          throw RecoveryFailure(
              "The snapshot has properties on edges, but the storage is "
              "configured without properties on edges!");
      }
    }
  }
  spdlog::info("Partial edges are recovered.");
}

// Returns the gid of the last recovered vertex
template <typename TLabelFromIdFunc, typename TPropertyFromIdFunc>
uint64_t LoadPartialVertices(const std::filesystem::path &path, utils::SkipList<Vertex> &vertices,
                             const uint64_t from_offset, const uint64_t vertices_count,
                             TLabelFromIdFunc get_label_from_id, TPropertyFromIdFunc get_property_from_id) {
  Decoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic);
  if (!snapshot.SetPosition(from_offset)) throw RecoveryFailure("Couldn't read data from snapshot!");

  auto vertex_acc = vertices.access();
  uint64_t last_vertex_gid = 0;
  spdlog::info("Recovering {} vertices.", vertices_count);
  std::vector<std::pair<PropertyId, PropertyValue>> read_properties;
  for (uint64_t i = 0; i < vertices_count; ++i) {
    {
      auto marker = snapshot.ReadMarker();
      if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Invalid snapshot data!");
    }

    // Insert vertex.
    auto gid = snapshot.ReadUint();
    if (!gid) throw RecoveryFailure("Invalid snapshot data!");
    if (i > 0 && *gid <= last_vertex_gid) {
      throw RecoveryFailure("Invalid snapshot data!");
    }
    last_vertex_gid = *gid;
    spdlog::debug("Recovering vertex {}.", *gid);
    auto [it, inserted] = vertex_acc.insert(Vertex{Gid::FromUint(*gid), nullptr});
    if (!inserted) throw RecoveryFailure("The vertex must be inserted here!");

    // Recover labels.
    spdlog::trace("Recovering labels for vertex {}.", *gid);
    {
      auto labels_size = snapshot.ReadUint();
      if (!labels_size) throw RecoveryFailure("Invalid snapshot data!");
      auto &labels = it->labels;
      labels.reserve(*labels_size);
      for (uint64_t j = 0; j < *labels_size; ++j) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        labels.emplace_back(get_label_from_id(*label));
      }
    }

    // Recover properties.
    spdlog::trace("Recovering properties for vertex {}.", *gid);
    {
      auto props_size = snapshot.ReadUint();
      if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
      auto &props = it->properties;
      read_properties.clear();
      read_properties.reserve(*props_size);
      for (uint64_t j = 0; j < *props_size; ++j) {
        auto key = snapshot.ReadUint();
        if (!key) throw RecoveryFailure("Invalid snapshot data!");
        auto value = snapshot.ReadPropertyValue();
        if (!value) throw RecoveryFailure("Invalid snapshot data!");
        read_properties.emplace_back(get_property_from_id(*key), std::move(*value));
      }
      props.InitProperties(std::move(read_properties));
    }

    // Skip in edges.
    {
      auto in_size = snapshot.ReadUint();
      if (!in_size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t j = 0; j < *in_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto from_gid = snapshot.ReadUint();
        if (!from_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");
      }
    }

    // Skip out edges.
    auto out_size = snapshot.ReadUint();
    if (!out_size) throw RecoveryFailure("Invalid snapshot data!");
    for (uint64_t j = 0; j < *out_size; ++j) {
      auto edge_gid = snapshot.ReadUint();
      if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
      auto to_gid = snapshot.ReadUint();
      if (!to_gid) throw RecoveryFailure("Invalid snapshot data!");
      auto edge_type = snapshot.ReadUint();
      if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");
    }
  }
  spdlog::info("Partial vertices are recovered.");

  return last_vertex_gid;
}

// Returns the number of edges recovered

struct LoadPartialConnectivityResult {
  uint64_t edge_count;
  uint64_t highest_edge_id;
  Gid first_vertex_gid;
};

template <typename TEdgeTypeFromIdFunc>
LoadPartialConnectivityResult LoadPartialConnectivity(const std::filesystem::path &path,
                                                      utils::SkipList<Vertex> &vertices, utils::SkipList<Edge> &edges,
                                                      const uint64_t from_offset, const uint64_t vertices_count,
                                                      const Config::Items items, const bool snapshot_has_edges,
                                                      TEdgeTypeFromIdFunc get_edge_type_from_id) {
  Decoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic);
  if (!snapshot.SetPosition(from_offset)) throw RecoveryFailure("Couldn't read data from snapshot!");

  auto vertex_acc = vertices.access();
  auto edge_acc = edges.access();

  // Read the first gid to find the necessary iterator in vertices
  const auto first_vertex_gid = std::invoke([&]() mutable {
    {
      auto marker = snapshot.ReadMarker();
      if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Invalid snapshot data!");
    }

    auto gid = snapshot.ReadUint();
    if (!gid) throw RecoveryFailure("Invalid snapshot data!");
    return Gid::FromUint(*gid);
  });

  uint64_t edge_count{0};
  uint64_t highest_edge_gid{0};
  auto vertex_it = vertex_acc.find(first_vertex_gid);
  if (vertex_it == vertex_acc.end()) {
    throw RecoveryFailure("Invalid snapshot data!");
  }

  spdlog::info("Recovering connectivity for {} vertices.", vertices_count);

  if (!snapshot.SetPosition(from_offset)) throw RecoveryFailure("Couldn't read data from snapshot!");

  for (uint64_t i = 0; i < vertices_count; ++i) {
    auto &vertex = *vertex_it;
    {
      auto marker = snapshot.ReadMarker();
      if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Invalid snapshot data!");
    }

    auto gid = snapshot.ReadUint();
    if (!gid) throw RecoveryFailure("Invalid snapshot data!");
    if (gid != vertex.gid.AsUint()) throw RecoveryFailure("Invalid snapshot data!");

    // Skip labels.
    {
      auto labels_size = snapshot.ReadUint();
      if (!labels_size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t j = 0; j < *labels_size; ++j) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
      }
    }

    // Skip properties.
    {
      auto props_size = snapshot.ReadUint();
      if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t j = 0; j < *props_size; ++j) {
        auto key = snapshot.ReadUint();
        if (!key) throw RecoveryFailure("Invalid snapshot data!");
        auto value = snapshot.SkipPropertyValue();
        if (!value) throw RecoveryFailure("Invalid snapshot data!");
      }
    }

    // Recover in edges.
    {
      spdlog::trace("Recovering inbound edges for vertex {}.", vertex.gid.AsUint());
      auto in_size = snapshot.ReadUint();
      if (!in_size) throw RecoveryFailure("Invalid snapshot data!");
      vertex.in_edges.reserve(*in_size);
      for (uint64_t j = 0; j < *in_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
        highest_edge_gid = std::max(highest_edge_gid, *edge_gid);

        auto from_gid = snapshot.ReadUint();
        if (!from_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");

        auto from_vertex = vertex_acc.find(Gid::FromUint(*from_gid));
        if (from_vertex == vertex_acc.end()) throw RecoveryFailure("Invalid from vertex!");

        EdgeRef edge_ref(Gid::FromUint(*edge_gid));
        if (items.properties_on_edges) {
          // The snapshot contains the individual edges only if it was created with a config where properties are
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
      }
    }

    // Recover out edges.
    {
      spdlog::trace("Recovering outbound edges for vertex {}.", vertex.gid.AsUint());
      auto out_size = snapshot.ReadUint();
      if (!out_size) throw RecoveryFailure("Invalid snapshot data!");
      vertex.out_edges.reserve(*out_size);
      for (uint64_t j = 0; j < *out_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");

        auto to_gid = snapshot.ReadUint();
        if (!to_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");

        auto to_vertex = vertex_acc.find(Gid::FromUint(*to_gid));
        if (to_vertex == vertex_acc.end()) throw RecoveryFailure("Invalid to vertex!");

        EdgeRef edge_ref(Gid::FromUint(*edge_gid));
        if (items.properties_on_edges) {
          // The snapshot contains the individual edges only if it was created with a config where properties are
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
        vertex.out_edges.emplace_back(get_edge_type_from_id(*edge_type), &*to_vertex, edge_ref);
        // Increment edge count. We only increment the count here because the
        // information is duplicated in in_edges.
        edge_count++;
      }
    }
    ++vertex_it;
  }
  spdlog::info("Partial connectivities are recovered.");
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

RecoveredSnapshot LoadSnapshotVersion14(const std::filesystem::path &path, utils::SkipList<Vertex> *vertices,
                                        utils::SkipList<Edge> *edges,
                                        std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                                        NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count,
                                        Config::Items items) {
  RecoveryInfo ret;
  RecoveredIndicesAndConstraints indices_constraints;

  Decoder snapshot;
  auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version) throw RecoveryFailure("Couldn't read snapshot magic and/or version!");
  if (*version != 14U) throw RecoveryFailure(fmt::format("Expected snapshot version is 14, but got {}", *version));

  // Cleanup of loaded data in case of failure.
  bool success = false;
  utils::OnScopeExit cleanup([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      epoch_history->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Invalid snapshot data!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Invalid snapshot data!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Invalid snapshot data!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Invalid snapshot data!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }
  auto get_label_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Invalid snapshot data!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Invalid snapshot data!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Invalid snapshot data!");
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
          if (!marker || *marker != Marker::SECTION_EDGE) throw RecoveryFailure("Invalid snapshot data!");
        }

        if (items.properties_on_edges) {
          // Insert edge.
          auto gid = snapshot.ReadUint();
          if (!gid) throw RecoveryFailure("Invalid snapshot data!");
          if (i > 0 && *gid <= last_edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = *gid;
          spdlog::debug("Recovering edge {} with properties.", *gid);
          auto [it, inserted] = edge_acc.insert(Edge{Gid::FromUint(*gid), nullptr});
          if (!inserted) throw RecoveryFailure("The edge must be inserted here!");

          // Recover properties.
          {
            auto props_size = snapshot.ReadUint();
            if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
            auto &props = it->properties;
            for (uint64_t j = 0; j < *props_size; ++j) {
              auto key = snapshot.ReadUint();
              if (!key) throw RecoveryFailure("Invalid snapshot data!");
              auto value = snapshot.ReadPropertyValue();
              if (!value) throw RecoveryFailure("Invalid snapshot data!");
              SPDLOG_TRACE("Recovered property \"{}\" with value \"{}\" for edge {}.",
                           name_id_mapper->IdToName(snapshot_id_map.at(*key)), *value, *gid);
              props.SetProperty(get_property_from_id(*key), *value);
            }
          }
        } else {
          // Read edge GID.
          auto gid = snapshot.ReadUint();
          if (!gid) throw RecoveryFailure("Invalid snapshot data!");
          if (i > 0 && *gid <= last_edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = *gid;

          spdlog::debug("Ensuring edge {} doesn't have any properties.", *gid);
          // Read properties.
          {
            auto props_size = snapshot.ReadUint();
            if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
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
        if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Invalid snapshot data!");
      }

      // Insert vertex.
      auto gid = snapshot.ReadUint();
      if (!gid) throw RecoveryFailure("Invalid snapshot data!");
      if (i > 0 && *gid <= last_vertex_gid) {
        throw RecoveryFailure("Invalid snapshot data!");
      }
      last_vertex_gid = *gid;
      spdlog::debug("Recovering vertex {}.", *gid);
      auto [it, inserted] = vertex_acc.insert(Vertex{Gid::FromUint(*gid), nullptr});
      if (!inserted) throw RecoveryFailure("The vertex must be inserted here!");

      // Recover labels.
      spdlog::trace("Recovering labels for vertex {}.", *gid);
      {
        auto labels_size = snapshot.ReadUint();
        if (!labels_size) throw RecoveryFailure("Invalid snapshot data!");
        auto &labels = it->labels;
        labels.reserve(*labels_size);
        for (uint64_t j = 0; j < *labels_size; ++j) {
          auto label = snapshot.ReadUint();
          if (!label) throw RecoveryFailure("Invalid snapshot data!");
          SPDLOG_TRACE("Recovered label \"{}\" for vertex {}.", name_id_mapper->IdToName(snapshot_id_map.at(*label)),
                       *gid);
          labels.emplace_back(get_label_from_id(*label));
        }
      }

      // Recover properties.
      spdlog::trace("Recovering properties for vertex {}.", *gid);
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
        auto &props = it->properties;
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Invalid snapshot data!");
          auto value = snapshot.ReadPropertyValue();
          if (!value) throw RecoveryFailure("Invalid snapshot data!");
          SPDLOG_TRACE("Recovered property \"{}\" with value \"{}\" for vertex {}.",
                       name_id_mapper->IdToName(snapshot_id_map.at(*key)), *value, *gid);
          props.SetProperty(get_property_from_id(*key), *value);
        }
      }

      // Skip in edges.
      {
        auto in_size = snapshot.ReadUint();
        if (!in_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *in_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto from_gid = snapshot.ReadUint();
          if (!from_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Skip out edges.
      auto out_size = snapshot.ReadUint();
      if (!out_size) throw RecoveryFailure("Invalid snapshot data!");
      for (uint64_t j = 0; j < *out_size; ++j) {
        auto edge_gid = snapshot.ReadUint();
        if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto to_gid = snapshot.ReadUint();
        if (!to_gid) throw RecoveryFailure("Invalid snapshot data!");
        auto edge_type = snapshot.ReadUint();
        if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");
      }
    }
    spdlog::info("Vertices are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recovering connectivity.");
    if (!snapshot.SetPosition(info.offset_vertices)) throw RecoveryFailure("Couldn't read data from snapshot!");
    for (auto &vertex : vertex_acc) {
      {
        auto marker = snapshot.ReadMarker();
        if (!marker || *marker != Marker::SECTION_VERTEX) throw RecoveryFailure("Invalid snapshot data!");
      }

      spdlog::trace("Recovering connectivity for vertex {}.", vertex.gid.AsUint());
      // Check vertex.
      auto gid = snapshot.ReadUint();
      if (!gid) throw RecoveryFailure("Invalid snapshot data!");
      if (gid != vertex.gid.AsUint()) throw RecoveryFailure("Invalid snapshot data!");

      // Skip labels.
      {
        auto labels_size = snapshot.ReadUint();
        if (!labels_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *labels_size; ++j) {
          auto label = snapshot.ReadUint();
          if (!label) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Skip properties.
      {
        auto props_size = snapshot.ReadUint();
        if (!props_size) throw RecoveryFailure("Invalid snapshot data!");
        for (uint64_t j = 0; j < *props_size; ++j) {
          auto key = snapshot.ReadUint();
          if (!key) throw RecoveryFailure("Invalid snapshot data!");
          auto value = snapshot.SkipPropertyValue();
          if (!value) throw RecoveryFailure("Invalid snapshot data!");
        }
      }

      // Recover in edges.
      {
        spdlog::trace("Recovering inbound edges for vertex {}.", vertex.gid.AsUint());
        auto in_size = snapshot.ReadUint();
        if (!in_size) throw RecoveryFailure("Invalid snapshot data!");
        vertex.in_edges.reserve(*in_size);
        for (uint64_t j = 0; j < *in_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = std::max(last_edge_gid, *edge_gid);

          auto from_gid = snapshot.ReadUint();
          if (!from_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");

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
        if (!out_size) throw RecoveryFailure("Invalid snapshot data!");
        vertex.out_edges.reserve(*out_size);
        for (uint64_t j = 0; j < *out_size; ++j) {
          auto edge_gid = snapshot.ReadUint();
          if (!edge_gid) throw RecoveryFailure("Invalid snapshot data!");
          last_edge_gid = std::max(last_edge_gid, *edge_gid);

          auto to_gid = snapshot.ReadUint();
          if (!to_gid) throw RecoveryFailure("Invalid snapshot data!");
          auto edge_type = snapshot.ReadUint();
          if (!edge_type) throw RecoveryFailure("Invalid snapshot data!");

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
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Invalid snapshot data!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label_property,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
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
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS) throw RecoveryFailure("Invalid snapshot data!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Invalid snapshot data!");
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
    if (*version >= kUniqueConstraintVersion) {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Invalid snapshot data!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Invalid snapshot data!");
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
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY) throw RecoveryFailure("Invalid snapshot data!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Invalid snapshot data!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Invalid snapshot data!");
      }
      const auto maybe_last_commit_timestamp = snapshot.ReadUint();
      if (!maybe_last_commit_timestamp) {
        throw RecoveryFailure("Invalid snapshot data!");
      }
      epoch_history->emplace_back(std::move(*maybe_epoch_id), *maybe_last_commit_timestamp);
    }
  }

  spdlog::info("Metadata recovered.");
  // Recover timestamp.
  ret.next_timestamp = info.start_timestamp + 1;

  // Set success flag (to disable cleanup).
  success = true;

  return {info, ret, std::move(indices_constraints)};
}

RecoveredSnapshot LoadSnapshot(const std::filesystem::path &path, utils::SkipList<Vertex> *vertices,
                               utils::SkipList<Edge> *edges,
                               std::deque<std::pair<std::string, uint64_t>> *epoch_history,
                               NameIdMapper *name_id_mapper, std::atomic<uint64_t> *edge_count, const Config &config) {
  RecoveryInfo recovery_info;
  RecoveredIndicesAndConstraints indices_constraints;

  Decoder snapshot;
  const auto version = snapshot.Initialize(path, kSnapshotMagic);
  if (!version) throw RecoveryFailure("Couldn't read snapshot magic and/or version!");

  if (!IsVersionSupported(*version)) throw RecoveryFailure(fmt::format("Invalid snapshot version {}", *version));
  if (*version == 14U) {
    return LoadSnapshotVersion14(path, vertices, edges, epoch_history, name_id_mapper, edge_count, config.items);
  }

  // Cleanup of loaded data in case of failure.
  bool success = false;
  utils::OnScopeExit cleanup([&] {
    if (!success) {
      edges->clear();
      vertices->clear();
      epoch_history->clear();
    }
  });

  // Read snapshot info.
  const auto info = ReadSnapshotInfo(path);
  spdlog::info("Recovering {} vertices and {} edges.", info.vertices_count, info.edges_count);
  // Check for edges.
  bool snapshot_has_edges = info.offset_edges != 0;

  // Recover mapper.
  std::unordered_map<uint64_t, uint64_t> snapshot_id_map;
  {
    spdlog::info("Recovering mapper metadata.");
    if (!snapshot.SetPosition(info.offset_mapper)) throw RecoveryFailure("Couldn't read data from snapshot!");

    auto marker = snapshot.ReadMarker();
    if (!marker || *marker != Marker::SECTION_MAPPER) throw RecoveryFailure("Invalid snapshot data!");

    auto size = snapshot.ReadUint();
    if (!size) throw RecoveryFailure("Invalid snapshot data!");

    for (uint64_t i = 0; i < *size; ++i) {
      auto id = snapshot.ReadUint();
      if (!id) throw RecoveryFailure("Invalid snapshot data!");
      auto name = snapshot.ReadString();
      if (!name) throw RecoveryFailure("Invalid snapshot data!");
      auto my_id = name_id_mapper->NameToId(*name);
      snapshot_id_map.emplace(*id, my_id);
      SPDLOG_TRACE("Mapping \"{}\"from snapshot id {} to actual id {}.", *name, *id, my_id);
    }
  }
  auto get_label_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Invalid snapshot data!");
    return LabelId::FromUint(it->second);
  };
  auto get_property_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Invalid snapshot data!");
    return PropertyId::FromUint(it->second);
  };
  auto get_edge_type_from_id = [&snapshot_id_map](uint64_t snapshot_id) {
    auto it = snapshot_id_map.find(snapshot_id);
    if (it == snapshot_id_map.end()) throw RecoveryFailure("Invalid snapshot data!");
    return EdgeTypeId::FromUint(it->second);
  };

  // Reset current edge count.
  edge_count->store(0, std::memory_order_release);

  {
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
          [path, edges, items = config.items, &get_property_from_id](const size_t /*batch_index*/,
                                                                     const BatchInfo &batch) {
            LoadPartialEdges(path, *edges, batch.offset, batch.count, items, get_property_from_id);
          },
          edge_batches);
    }
    spdlog::info("Edges are recovered.");

    // Recover vertices (labels and properties).
    spdlog::info("Recovering vertices.", info.vertices_count);
    uint64_t last_vertex_gid{0};

    if (!snapshot.SetPosition(info.offset_vertex_batches)) {
      throw RecoveryFailure("Couldn't read data from snapshot!");
    }

    const auto vertex_batches = ReadBatchInfos(snapshot);
    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, &vertex_batches, &get_label_from_id, &get_property_from_id, &last_vertex_gid](
            const size_t batch_index, const BatchInfo &batch) {
          const auto last_vertex_gid_in_batch =
              LoadPartialVertices(path, *vertices, batch.offset, batch.count, get_label_from_id, get_property_from_id);
          if (batch_index == vertex_batches.size() - 1) {
            last_vertex_gid = last_vertex_gid_in_batch;
          }
        },
        vertex_batches);

    spdlog::info("Vertices are recovered.");

    // Recover vertices (in/out edges).
    spdlog::info("Recover connectivity.");
    recovery_info.vertex_batches.reserve(vertex_batches.size());
    for (const auto batch : vertex_batches) {
      recovery_info.vertex_batches.emplace_back(std::make_pair(Gid::FromUint(0), batch.count));
    }
    std::atomic<uint64_t> highest_edge_gid{0};

    RecoverOnMultipleThreads(
        config.durability.recovery_thread_count,
        [path, vertices, edges, edge_count, items = config.items, snapshot_has_edges, &get_edge_type_from_id,
         &highest_edge_gid, &recovery_info](const size_t batch_index, const BatchInfo &batch) {
          const auto result = LoadPartialConnectivity(path, *vertices, *edges, batch.offset, batch.count, items,
                                                      snapshot_has_edges, get_edge_type_from_id);
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
    if (!marker || *marker != Marker::SECTION_INDICES) throw RecoveryFailure("Invalid snapshot data!");

    // Recover label indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      spdlog::info("Recovering metadata of {} label indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label, get_label_from_id(*label),
                                    "The label index already exists!");
        SPDLOG_TRACE("Recovered metadata of label index for :{}", name_id_mapper->IdToName(snapshot_id_map.at(*label)));
      }
      spdlog::info("Metadata of label indices are recovered.");
    }

    // Recover label+property indices.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      spdlog::info("Recovering metadata of {} label+property indices.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Invalid snapshot data!");
        AddRecoveredIndexConstraint(&indices_constraints.indices.label_property,
                                    {get_label_from_id(*label), get_property_from_id(*property)},
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
    if (!marker || *marker != Marker::SECTION_CONSTRAINTS) throw RecoveryFailure("Invalid snapshot data!");

    // Recover existence constraints.
    {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      spdlog::info("Recovering metadata of {} existence constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto property = snapshot.ReadUint();
        if (!property) throw RecoveryFailure("Invalid snapshot data!");
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
    if (*version >= kUniqueConstraintVersion) {
      auto size = snapshot.ReadUint();
      if (!size) throw RecoveryFailure("Invalid snapshot data!");
      spdlog::info("Recovering metadata of {} unique constraints.", *size);
      for (uint64_t i = 0; i < *size; ++i) {
        auto label = snapshot.ReadUint();
        if (!label) throw RecoveryFailure("Invalid snapshot data!");
        auto properties_count = snapshot.ReadUint();
        if (!properties_count) throw RecoveryFailure("Invalid snapshot data!");
        std::set<PropertyId> properties;
        for (uint64_t j = 0; j < *properties_count; ++j) {
          auto property = snapshot.ReadUint();
          if (!property) throw RecoveryFailure("Invalid snapshot data!");
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
    if (!marker || *marker != Marker::SECTION_EPOCH_HISTORY) throw RecoveryFailure("Invalid snapshot data!");

    const auto history_size = snapshot.ReadUint();
    if (!history_size) {
      throw RecoveryFailure("Invalid snapshot data!");
    }

    for (int i = 0; i < *history_size; ++i) {
      auto maybe_epoch_id = snapshot.ReadString();
      if (!maybe_epoch_id) {
        throw RecoveryFailure("Invalid snapshot data!");
      }
      const auto maybe_last_commit_timestamp = snapshot.ReadUint();
      if (!maybe_last_commit_timestamp) {
        throw RecoveryFailure("Invalid snapshot data!");
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

void CreateSnapshot(Transaction *transaction, const std::filesystem::path &snapshot_directory,
                    const std::filesystem::path &wal_directory, uint64_t snapshot_retention_count,
                    utils::SkipList<Vertex> *vertices, utils::SkipList<Edge> *edges, NameIdMapper *name_id_mapper,
                    Indices *indices, Constraints *constraints, const Config &config, const std::string &uuid,
                    const std::string_view epoch_id, const std::deque<std::pair<std::string, uint64_t>> &epoch_history,
                    utils::FileRetainer *file_retainer) {
  // Ensure that the storage directory exists.
  utils::EnsureDirOrDie(snapshot_directory);

  // Create snapshot file.
  auto path = snapshot_directory / MakeSnapshotName(transaction->start_timestamp);
  spdlog::info("Starting snapshot creation to {}", path);
  Encoder snapshot;
  snapshot.Initialize(path, kSnapshotMagic, kVersion);

  // Write placeholder offsets.
  uint64_t offset_offsets = 0;
  uint64_t offset_edges = 0;
  uint64_t offset_vertices = 0;
  uint64_t offset_indices = 0;
  uint64_t offset_constraints = 0;
  uint64_t offset_mapper = 0;
  uint64_t offset_metadata = 0;
  uint64_t offset_epoch_history = 0;
  uint64_t offset_edge_batches = 0;
  uint64_t offset_vertex_batches = 0;
  {
    snapshot.WriteMarker(Marker::SECTION_OFFSETS);
    offset_offsets = snapshot.GetPosition();
    snapshot.WriteUint(offset_edges);
    snapshot.WriteUint(offset_vertices);
    snapshot.WriteUint(offset_indices);
    snapshot.WriteUint(offset_constraints);
    snapshot.WriteUint(offset_mapper);
    snapshot.WriteUint(offset_epoch_history);
    snapshot.WriteUint(offset_metadata);
    snapshot.WriteUint(offset_edge_batches);
    snapshot.WriteUint(offset_vertex_batches);
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

  std::vector<BatchInfo> edge_batch_infos;
  auto items_in_current_batch{0UL};
  auto batch_start_offset{0UL};
  // Store all edges.
  if (config.items.properties_on_edges) {
    offset_edges = snapshot.GetPosition();
    batch_start_offset = offset_edges;
    auto acc = edges->access();
    for (auto &edge : acc) {
      // The edge visibility check must be done here manually because we don't
      // allow direct access to the edges through the public API.
      bool is_visible = true;
      Delta *delta = nullptr;
      {
        std::lock_guard<utils::SpinLock> guard(edge.lock);
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
      auto ea = EdgeAccessor{
          edge_ref, EdgeTypeId::FromUint(0UL), nullptr, nullptr, transaction, indices, constraints, config.items};

      // Get edge data.
      auto maybe_props = ea.Properties(View::OLD);
      MG_ASSERT(maybe_props.HasValue(), "Invalid database state!");

      // Store the edge.
      {
        snapshot.WriteMarker(Marker::SECTION_EDGE);
        snapshot.WriteUint(edge.gid.AsUint());
        const auto &props = maybe_props.GetValue();
        snapshot.WriteUint(props.size());
        for (const auto &item : props) {
          write_mapping(item.first);
          snapshot.WritePropertyValue(item.second);
        }
      }

      ++edges_count;
      ++items_in_current_batch;
      if (items_in_current_batch == config.durability.items_per_batch) {
        edge_batch_infos.push_back(BatchInfo{batch_start_offset, items_in_current_batch});
        batch_start_offset = snapshot.GetPosition();
        items_in_current_batch = 0;
      }
    }
  }

  if (items_in_current_batch > 0) {
    edge_batch_infos.push_back(BatchInfo{batch_start_offset, items_in_current_batch});
  }

  std::vector<BatchInfo> vertex_batch_infos;
  // Store all vertices.
  {
    items_in_current_batch = 0;
    offset_vertices = snapshot.GetPosition();
    batch_start_offset = offset_vertices;
    auto acc = vertices->access();
    for (auto &vertex : acc) {
      // The visibility check is implemented for vertices so we use it here.
      auto va = VertexAccessor::Create(&vertex, transaction, indices, constraints, config.items, View::OLD);
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
        snapshot.WriteMarker(Marker::SECTION_VERTEX);
        snapshot.WriteUint(vertex.gid.AsUint());
        const auto &labels = maybe_labels.GetValue();
        snapshot.WriteUint(labels.size());
        for (const auto &item : labels) {
          write_mapping(item);
        }
        const auto &props = maybe_props.GetValue();
        snapshot.WriteUint(props.size());
        for (const auto &item : props) {
          write_mapping(item.first);
          snapshot.WritePropertyValue(item.second);
        }
        const auto &in_edges = maybe_in_edges.GetValue();
        snapshot.WriteUint(in_edges.size());
        for (const auto &item : in_edges) {
          snapshot.WriteUint(item.Gid().AsUint());
          snapshot.WriteUint(item.FromVertex().Gid().AsUint());
          write_mapping(item.EdgeType());
        }
        const auto &out_edges = maybe_out_edges.GetValue();
        snapshot.WriteUint(out_edges.size());
        for (const auto &item : out_edges) {
          snapshot.WriteUint(item.Gid().AsUint());
          snapshot.WriteUint(item.ToVertex().Gid().AsUint());
          write_mapping(item.EdgeType());
        }
      }

      ++vertices_count;
      ++items_in_current_batch;
      if (items_in_current_batch == config.durability.items_per_batch) {
        vertex_batch_infos.push_back(BatchInfo{batch_start_offset, items_in_current_batch});
        batch_start_offset = snapshot.GetPosition();
        items_in_current_batch = 0;
      }
    }

    if (items_in_current_batch > 0) {
      vertex_batch_infos.push_back(BatchInfo{batch_start_offset, items_in_current_batch});
    }
  }

  // Write indices.
  {
    offset_indices = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_INDICES);

    // Write label indices.
    {
      auto label = indices->label_index.ListIndices();
      snapshot.WriteUint(label.size());
      for (const auto &item : label) {
        write_mapping(item);
      }
    }

    // Write label+property indices.
    {
      auto label_property = indices->label_property_index.ListIndices();
      snapshot.WriteUint(label_property.size());
      for (const auto &item : label_property) {
        write_mapping(item.first);
        write_mapping(item.second);
      }
    }
  }

  // Write constraints.
  {
    offset_constraints = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_CONSTRAINTS);

    // Write existence constraints.
    {
      auto existence = ListExistenceConstraints(*constraints);
      snapshot.WriteUint(existence.size());
      for (const auto &item : existence) {
        write_mapping(item.first);
        write_mapping(item.second);
      }
    }

    // Write unique constraints.
    {
      auto unique = constraints->unique_constraints.ListConstraints();
      snapshot.WriteUint(unique.size());
      for (const auto &item : unique) {
        write_mapping(item.first);
        snapshot.WriteUint(item.second.size());
        for (const auto &property : item.second) {
          write_mapping(property);
        }
      }
    }
  }

  // Write mapper data.
  {
    offset_mapper = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_MAPPER);
    snapshot.WriteUint(used_ids.size());
    for (auto item : used_ids) {
      snapshot.WriteUint(item);
      snapshot.WriteString(name_id_mapper->IdToName(item));
    }
  }

  // Write epoch history
  {
    offset_epoch_history = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_EPOCH_HISTORY);
    snapshot.WriteUint(epoch_history.size());
    for (const auto &[epoch_id, last_commit_timestamp] : epoch_history) {
      snapshot.WriteString(epoch_id);
      snapshot.WriteUint(last_commit_timestamp);
    }
  }

  // Write metadata.
  {
    offset_metadata = snapshot.GetPosition();
    snapshot.WriteMarker(Marker::SECTION_METADATA);
    snapshot.WriteString(uuid);
    snapshot.WriteString(epoch_id);
    snapshot.WriteUint(transaction->start_timestamp);
    snapshot.WriteUint(edges_count);
    snapshot.WriteUint(vertices_count);
  }

  auto write_batch_infos = [&snapshot](const std::vector<BatchInfo> &batch_infos) {
    snapshot.WriteUint(batch_infos.size());
    for (const auto &batch_info : batch_infos) {
      snapshot.WriteUint(batch_info.offset);
      snapshot.WriteUint(batch_info.count);
    }
  };

  // Write edge batches
  {
    offset_edge_batches = snapshot.GetPosition();
    write_batch_infos(edge_batch_infos);
  }

  // Write vertex batches
  {
    offset_vertex_batches = snapshot.GetPosition();
    write_batch_infos(vertex_batch_infos);
  }

  // Write true offsets.
  {
    snapshot.SetPosition(offset_offsets);
    snapshot.WriteUint(offset_edges);
    snapshot.WriteUint(offset_vertices);
    snapshot.WriteUint(offset_indices);
    snapshot.WriteUint(offset_constraints);
    snapshot.WriteUint(offset_mapper);
    snapshot.WriteUint(offset_epoch_history);
    snapshot.WriteUint(offset_metadata);
    snapshot.WriteUint(offset_edge_batches);
    snapshot.WriteUint(offset_vertex_batches);
  }

  // Finalize snapshot file.
  snapshot.Finalize();
  spdlog::info("Snapshot creation successful!");

  // Ensure exactly `snapshot_retention_count` snapshots exist.
  std::vector<std::pair<uint64_t, std::filesystem::path>> old_snapshot_files;
  {
    std::error_code error_code;
    for (const auto &item : std::filesystem::directory_iterator(snapshot_directory, error_code)) {
      if (!item.is_regular_file()) continue;
      if (item.path() == path) continue;
      try {
        auto info = ReadSnapshotInfo(item.path());
        if (info.uuid != uuid) continue;
        old_snapshot_files.emplace_back(info.start_timestamp, item.path());
      } catch (const RecoveryFailure &e) {
        spdlog::warn("Found a corrupt snapshot file {} because of: {}", item.path(), e.what());
        continue;
      }
    }

    if (error_code) {
      spdlog::error(
          utils::MessageWithLink("Couldn't ensure that exactly {} snapshots exist because an error occurred: {}.",
                                 snapshot_retention_count, error_code.message(), "https://memgr.ph/snapshots"));
    }
    std::sort(old_snapshot_files.begin(), old_snapshot_files.end());
    if (old_snapshot_files.size() > snapshot_retention_count - 1) {
      auto num_to_erase = old_snapshot_files.size() - (snapshot_retention_count - 1);
      for (size_t i = 0; i < num_to_erase; ++i) {
        const auto &[start_timestamp, snapshot_path] = old_snapshot_files[i];
        file_retainer->DeleteFile(snapshot_path);
      }
      old_snapshot_files.erase(old_snapshot_files.begin(), old_snapshot_files.begin() + num_to_erase);
    }
  }

  // Ensure that only the absolutely necessary WAL files exist.
  if (old_snapshot_files.size() == snapshot_retention_count - 1 && utils::DirExists(wal_directory)) {
    std::vector<std::tuple<uint64_t, uint64_t, uint64_t, std::filesystem::path>> wal_files;
    std::error_code error_code;
    for (const auto &item : std::filesystem::directory_iterator(wal_directory, error_code)) {
      if (!item.is_regular_file()) continue;
      try {
        auto info = ReadWalInfo(item.path());
        if (info.uuid != uuid) continue;
        wal_files.emplace_back(info.seq_num, info.from_timestamp, info.to_timestamp, item.path());
      } catch (const RecoveryFailure &e) {
        continue;
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
}

}  // namespace memgraph::storage::durability
