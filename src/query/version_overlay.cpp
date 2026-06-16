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

#include "query/version_overlay.hpp"

#include <map>

#include "query/db_accessor.hpp"
#include "query/edge_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/trigger_context.hpp"
#include "query/vertex_accessor.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_store.hpp"
#include "storage/v2/view.hpp"

namespace memgraph::query {

namespace {

storage::PropertyStore BufferToStore(const std::string &buffer) {
  return storage::PropertyStore::CreateFromBuffer(buffer);
}

// Serializes a property map into an opaque PropertyStore buffer for an OverlayDelta.
std::string PropertiesToBuffer(const std::map<storage::PropertyId, storage::PropertyValue> &properties) {
  storage::PropertyStore store;
  for (const auto &[key, value] : properties) {
    store.SetProperty(key, value);
  }
  return store.StringBuffer();
}

storage::InMemoryStorage::InMemoryAccessor *AsInMemory(DbAccessor *dba) {
  auto *mem = dynamic_cast<storage::InMemoryStorage::InMemoryAccessor *>(dba->GetStorageAccessor());
  if (mem == nullptr) {
    throw QueryRuntimeException("Versioning is supported only on in-memory storage.");
  }
  return mem;
}

[[noreturn]] void ReplayError(std::string_view what) {
  throw QueryRuntimeException("Versioning failed to replay overlay: {}.", what);
}

}  // namespace

void ApplyVersionOverlay(DbAccessor *dba, const std::vector<storage::OverlayDelta> &deltas, bool strict) {
  using storage::EdgeTypeId;
  using storage::Gid;
  using storage::LabelId;
  using storage::View;

  auto *mem = AsInMemory(dba);

  // In strict mode a missing target means the overlay is internally inconsistent (a referenced object
  // was never created in the replayed set) — the caller (REVERT) wants this surfaced as a conflict.
  const auto missing = [strict](std::string_view what) {
    if (strict) ReplayError(what);
  };

  for (const auto &delta : deltas) {
    switch (delta.op) {
      case storage::OverlayOp::kCreateVertex: {
        auto vertex = mem->CreateVertexWithGid(Gid::FromUint(delta.gid));
        if (!vertex) ReplayError("could not create vertex");
        for (auto label : delta.labels) {
          if (!vertex->AddLabel(LabelId::FromUint(label)).has_value()) ReplayError("add label");
        }
        for (const auto &[key, value] : BufferToStore(delta.properties).Properties()) {
          if (!vertex->SetProperty(key, value).has_value()) ReplayError("set vertex property");
        }
        break;
      }
      case storage::OverlayOp::kDeleteVertex: {
        auto vertex = mem->FindVertex(Gid::FromUint(delta.gid), View::NEW);
        if (!vertex) {
          missing("delete vertex: target missing");
          break;
        }
        if (!mem->DetachDeleteVertex(&*vertex).has_value()) ReplayError("delete vertex");
        break;
      }
      case storage::OverlayOp::kAddLabel: {
        auto vertex = mem->FindVertex(Gid::FromUint(delta.gid), View::NEW);
        if (!vertex) {
          missing("add label: target vertex missing");
          break;
        }
        if (!vertex->AddLabel(LabelId::FromUint(delta.label_id)).has_value()) ReplayError("add label");
        break;
      }
      case storage::OverlayOp::kRemoveLabel: {
        auto vertex = mem->FindVertex(Gid::FromUint(delta.gid), View::NEW);
        if (!vertex) {
          missing("remove label: target vertex missing");
          break;
        }
        if (!vertex->RemoveLabel(LabelId::FromUint(delta.label_id)).has_value()) ReplayError("remove label");
        break;
      }
      case storage::OverlayOp::kSetVertexProperty: {
        auto vertex = mem->FindVertex(Gid::FromUint(delta.gid), View::NEW);
        if (!vertex) {
          missing("set vertex property: target vertex missing");
          break;
        }
        for (const auto &[key, value] : BufferToStore(delta.properties).Properties()) {
          if (!vertex->SetProperty(key, value).has_value()) ReplayError("set vertex property");
        }
        break;
      }
      case storage::OverlayOp::kCreateEdge: {
        auto from = mem->FindVertex(Gid::FromUint(delta.from_gid), View::NEW);
        auto to = mem->FindVertex(Gid::FromUint(delta.to_gid), View::NEW);
        if (!from || !to) ReplayError("edge endpoint missing");
        auto edge =
            mem->CreateEdgeWithGid(&*from, &*to, EdgeTypeId::FromUint(delta.edge_type_id), Gid::FromUint(delta.gid));
        if (!edge.has_value()) ReplayError("create edge");
        for (const auto &[key, value] : BufferToStore(delta.properties).Properties()) {
          if (!edge->SetProperty(key, value).has_value()) ReplayError("set edge property");
        }
        break;
      }
      case storage::OverlayOp::kDeleteEdge: {
        auto edge = mem->FindEdge(Gid::FromUint(delta.gid), View::NEW);
        if (!edge) {
          missing("delete edge: target missing");
          break;
        }
        if (!mem->DeleteEdge(&*edge).has_value()) ReplayError("delete edge");
        break;
      }
      case storage::OverlayOp::kSetEdgeProperty: {
        auto edge = mem->FindEdge(Gid::FromUint(delta.gid), View::NEW);
        if (!edge) {
          missing("set edge property: target edge missing");
          break;
        }
        for (const auto &[key, value] : BufferToStore(delta.properties).Properties()) {
          if (!edge->SetProperty(key, value).has_value()) ReplayError("set edge property");
        }
        break;
      }
    }
  }
}

std::vector<storage::OverlayDelta> CaptureVersionOverlay(DbAccessor * /*dba*/,
                                                         const TriggerContextCollector &collector) {
  using storage::View;
  std::vector<storage::OverlayDelta> out;

  const auto &vreg = collector.VersioningVertexRegistry();
  const auto &ereg = collector.VersioningEdgeRegistry();

  // Created vertices (with their final labels + properties).
  for (const auto &[gid, created] : vreg.created_objects) {
    const auto &vertex = created.object;
    storage::OverlayDelta delta;
    delta.op = storage::OverlayOp::kCreateVertex;
    delta.gid = vertex.Gid().AsUint();
    if (auto labels = vertex.Labels(View::NEW); labels.has_value()) {
      for (auto label : *labels) delta.labels.push_back(label.AsUint());
    }
    if (auto props = vertex.Properties(View::NEW); props.has_value()) {
      delta.properties = PropertiesToBuffer(*props);
    }
    out.push_back(std::move(delta));
  }

  // Deleted vertices.
  for (const auto &deleted : vreg.deleted_objects) {
    storage::OverlayDelta delta;
    delta.op = storage::OverlayOp::kDeleteVertex;
    delta.gid = deleted.object.Gid().AsUint();
    out.push_back(std::move(delta));
  }

  // Property changes on pre-existing vertices (created vertices are captured whole above).
  for (const auto &[object_key, change] : vreg.property_changes) {
    const auto &vertex = object_key.first;
    const auto property = object_key.second;
    auto value = vertex.GetProperty(View::NEW, property);
    if (!value.has_value() || value->IsNull()) continue;  // removals not representable yet
    storage::OverlayDelta delta;
    delta.op = storage::OverlayOp::kSetVertexProperty;
    delta.gid = vertex.Gid().AsUint();
    delta.property_id = property.AsUint();
    delta.properties = PropertiesToBuffer({{property, *value}});
    out.push_back(std::move(delta));
  }

  // Label changes on pre-existing vertices.
  for (const auto &[vertex, label, added] : collector.VersioningLabelChanges()) {
    storage::OverlayDelta delta;
    delta.op = added ? storage::OverlayOp::kAddLabel : storage::OverlayOp::kRemoveLabel;
    delta.gid = vertex.Gid().AsUint();
    delta.label_id = label.AsUint();
    out.push_back(std::move(delta));
  }

  // Created edges.
  for (const auto &[gid, created] : ereg.created_objects) {
    auto edge = created.object;
    storage::OverlayDelta delta;
    delta.op = storage::OverlayOp::kCreateEdge;
    delta.gid = edge.Gid().AsUint();
    delta.edge_type_id = edge.EdgeType().AsUint();
    delta.from_gid = edge.From().Gid().AsUint();
    delta.to_gid = edge.To().Gid().AsUint();
    if (auto props = edge.Properties(View::NEW); props.has_value()) {
      delta.properties = PropertiesToBuffer(*props);
    }
    out.push_back(std::move(delta));
  }

  // Deleted edges.
  for (const auto &deleted : ereg.deleted_objects) {
    storage::OverlayDelta delta;
    delta.op = storage::OverlayOp::kDeleteEdge;
    delta.gid = deleted.object.Gid().AsUint();
    out.push_back(std::move(delta));
  }

  // Property changes on pre-existing edges.
  for (const auto &[object_key, change] : ereg.property_changes) {
    auto edge = object_key.first;
    const auto property = object_key.second;
    auto props = edge.Properties(View::NEW);
    if (!props.has_value()) continue;
    auto it = props->find(property);
    if (it == props->end() || it->second.IsNull()) continue;
    storage::OverlayDelta delta;
    delta.op = storage::OverlayOp::kSetEdgeProperty;
    delta.gid = edge.Gid().AsUint();
    delta.property_id = property.AsUint();
    delta.properties = PropertiesToBuffer({{property, it->second}});
    out.push_back(std::move(delta));
  }

  return out;
}

}  // namespace memgraph::query
