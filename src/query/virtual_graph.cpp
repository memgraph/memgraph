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

#include "query/virtual_graph.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <string_view>

#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/typed_value.hpp"
#include "utils/logging.hpp"
#include "utils/pmr/unordered_map.hpp"

namespace memgraph::query {

VirtualGraph::VirtualGraph(const VirtualGraph &other, allocator_type alloc)
    : nodes_(other.nodes_, alloc), edges_(other.edges_, alloc), out_index_(alloc), in_index_(alloc) {
  RebuildEdgeIndexes();
}

VirtualGraph &VirtualGraph::operator=(const VirtualGraph &other) {
  if (this == &other) return *this;
  nodes_ = other.nodes_;
  edges_ = other.edges_;
  out_index_.clear();
  in_index_.clear();
  RebuildEdgeIndexes();
  return *this;
}

VirtualGraph &VirtualGraph::operator=(VirtualGraph &&other) noexcept {
  if (this == &other) return *this;
  DMG_ASSERT(get_allocator() == other.get_allocator(), "VirtualGraph move-assign across allocators is not supported");
  nodes_ = std::move(other.nodes_);
  edges_ = std::move(other.edges_);
  out_index_ = std::move(other.out_index_);
  in_index_ = std::move(other.in_index_);
  return *this;
}

VirtualGraph::VirtualGraph(VirtualGraph &&other, allocator_type alloc)
    : nodes_(alloc), edges_(alloc), out_index_(alloc), in_index_(alloc) {
  if (alloc == other.nodes_.get_allocator()) {
    nodes_ = std::move(other.nodes_);
    edges_ = std::move(other.edges_);
    out_index_ = std::move(other.out_index_);
    in_index_ = std::move(other.in_index_);
  } else {
    nodes_ = node_map(other.nodes_, alloc);
    edges_ = edge_set(other.edges_, alloc);
    RebuildEdgeIndexes();
  }
}

void VirtualGraph::IndexEdge(const VirtualEdge *edge) {
  out_index_[edge->FromGid()].push_back(edge);
  in_index_[edge->ToGid()].push_back(edge);
}

void VirtualGraph::RebuildEdgeIndexes() {
  for (const auto &edge : edges_) IndexEdge(&edge);
}

const VirtualNode &VirtualGraph::InsertNode(VirtualNode node) {
  const auto synthetic_gid = node.Gid();
  auto shared = std::allocate_shared<VirtualNode>(
      std::pmr::polymorphic_allocator<VirtualNode>(nodes_.get_allocator().resource()), std::move(node));
  const auto [it, inserted] = nodes_.try_emplace(synthetic_gid, std::move(shared));
  DMG_ASSERT(inserted, "NextSyntheticGid counter collision: synthetic gid not unique");
  return *it->second;
}

std::shared_ptr<const VirtualNode> VirtualGraph::FindNode(storage::Gid synthetic_gid) const {
  if (const auto it = nodes_.find(synthetic_gid); it != nodes_.end()) return it->second;
  return {};
}

bool VirtualGraph::InsertEdgeIfNew(VirtualEdge edge) {
  const auto [it, inserted] = edges_.insert(std::move(edge));
  if (!inserted) return false;
  IndexEdge(&*it);
  return true;
}

std::span<const VirtualEdge *const> VirtualGraph::OutEdges(storage::Gid vertex_gid) const {
  if (const auto it = out_index_.find(vertex_gid); it != out_index_.end()) return it->second;
  return {};
}

std::span<const VirtualEdge *const> VirtualGraph::InEdges(storage::Gid vertex_gid) const {
  if (const auto it = in_index_.find(vertex_gid); it != in_index_.end()) return it->second;
  return {};
}

void VirtualGraph::Merge(const VirtualGraph &other, const VirtualGraphAliasMap &aliases) {
  for (const auto &[synth_gid, node] : other.nodes_) {
    if (aliases.contains(synth_gid)) continue;
    nodes_.try_emplace(synth_gid, node);
  }
  const auto resolve = [&aliases](storage::Gid g) {
    if (const auto it = aliases.find(g); it != aliases.end()) return it->second;
    return g;
  };
  const VirtualEdge::allocator_type edge_alloc(get_allocator().resource());
  for (const auto &edge : other.edges_) {
    auto from_shared = FindNode(resolve(edge.FromGid()));
    auto to_shared = FindNode(resolve(edge.ToGid()));
    DMG_ASSERT(from_shared && to_shared, "merge alias resolution failed");
    InsertEdgeIfNew(VirtualEdge(edge, std::move(from_shared), std::move(to_shared), edge_alloc));
  }
}

VirtualGraph AssembleVirtualGraph(std::span<const VirtualNode> nodes, std::span<const VirtualEdge> edges,
                                  DanglingEdgePolicy policy, VirtualGraph::allocator_type alloc) {
  VirtualGraph graph(alloc);

  // The synthetic gid of the node carrying each import handle, so a handle endpoint binds to a node.
  utils::pmr::unordered_map<int64_t, storage::Gid> handle_to_gid(alloc.resource());
  for (const auto &node : nodes) {
    const auto &inserted = graph.InsertNode(node);
    if (const auto handle = inserted.Handle()) {
      const auto [it, added] = handle_to_gid.try_emplace(*handle, inserted.Gid());
      if (!added && it->second != inserted.Gid()) {
        throw QueryRuntimeException("Duplicate import handle {} in the virtualGraph() node list.", *handle);
      }
    }
  }

  // Bind an endpoint to a listed node: a handle endpoint by matching handle, a resolved endpoint by
  // its synthetic gid. Returns nullptr when no listed node matches (the endpoint is dangling).
  const auto bind = [&](std::optional<int64_t> handle, storage::Gid gid) -> std::shared_ptr<const VirtualNode> {
    if (handle) {
      const auto it = handle_to_gid.find(*handle);
      if (it == handle_to_gid.end()) return nullptr;
      return graph.FindNode(it->second);
    }
    return graph.FindNode(gid);
  };

  const VirtualEdge::allocator_type edge_alloc(alloc.resource());
  for (const auto &edge : edges) {
    auto from = bind(edge.FromHandle(), edge.FromGid());
    auto to = bind(edge.ToHandle(), edge.ToGid());
    if (!from || !to) {
      if (policy == DanglingEdgePolicy::kDrop) continue;
      throw QueryRuntimeException("virtualGraph() edge references a node absent from the node list (dangling edge).");
    }
    graph.InsertEdgeIfNew(VirtualEdge(edge, std::move(from), std::move(to), edge_alloc));
  }
  return graph;
}

namespace {

// Applies a derive() option that is a {propertyName: value} map, resolving each name to an id and
// calling setter(id, value). Absent option or null db_accessor is a no-op; a non-map option errors.
template <class Setter>
void ApplyPropertyMap(const TypedValue::TMap &options, std::string_view key, DbAccessor *db_accessor, Setter &&setter) {
  const auto it = options.find(key);
  if (it == options.end() || !db_accessor) return;
  if (it->second.type() != TypedValue::Type::Map) {
    throw QueryRuntimeException("derive() option '{}' must be a map of property names to values.", key);
  }
  auto *name_id_mapper = db_accessor->GetStorageAccessor()->GetNameIdMapper();
  for (const auto &[name, val] : it->second.ValueMap()) {
    setter(db_accessor->NameToProperty(name), val.ToPropertyValue(name_id_mapper));
  }
}

constexpr auto kPropertyPolicy = "propertyPolicy";

// Builds a derive()'s shared, static schema from its propertyPolicy: the hidden keys and the
// 'overlay'-bound keys, plus the provenance ref. Role-independent - a projection's source and target
// nodes share one instance - so per-role labels and property overrides are applied per node in
// BuildDerivedNode. A binding other than hidden/overlay/origin is a construction error.
std::shared_ptr<const ProjectionSchema> MakeProjectionSchema(const TypedValue::TMap &options, int64_t projection_ref,
                                                             DbAccessor *db_accessor,
                                                             VirtualNode::allocator_type alloc) {
  ProjectionSchema::key_set hidden{alloc};
  ProjectionSchema::key_set overlay_bound{alloc};
  if (const auto policy_it = options.find(kPropertyPolicy); policy_it != options.end()) {
    if (policy_it->second.type() != TypedValue::Type::Map) {
      throw QueryRuntimeException("derive() option '{}' must be a map of property names to bindings.", kPropertyPolicy);
    }
    for (const auto &[name, binding] : policy_it->second.ValueMap()) {
      if (binding.type() != TypedValue::Type::String) {
        throw QueryRuntimeException(
            "derive() '{}' binding for '{}' must be 'origin', 'overlay', or 'hidden'.", kPropertyPolicy, name);
      }
      const auto id = db_accessor->NameToProperty(name);
      const auto &value = binding.ValueString();
      if (value == "hidden") {
        hidden.push_back(id);
      } else if (value == "overlay") {
        overlay_bound.push_back(id);
      } else if (value != "origin") {
        throw QueryRuntimeException(
            "derive() '{}' binding for '{}' must be 'origin', 'overlay', or 'hidden'.", kPropertyPolicy, name);
      }
    }
  }
  return std::make_shared<const ProjectionSchema>(std::move(hidden), std::move(overlay_bound), projection_ref);
}

// Builds one overlay node over a real vertex: its label override (or the origin's labels) and its
// per-node overlay property values. The static binding + ref come from the shared `schema`. A key
// the policy bound 'origin' may not also carry an overlay value here - a construction conflict.
VirtualNode BuildDerivedNode(const VertexAccessor &real_vertex, std::string_view labels_key, std::string_view props_key,
                             const TypedValue::TMap &options, const std::shared_ptr<const ProjectionSchema> &schema,
                             DbAccessor *db_accessor, VirtualNode::allocator_type alloc) {
  VirtualNode::label_list labels{alloc};
  if (const auto labels_it = options.find(labels_key); labels_it != options.end()) {
    if (labels_it->second.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("derive() option '{}' must be a list of label strings.", labels_key);
    }
    for (const auto &label : labels_it->second.ValueList()) {
      if (label.type() != TypedValue::Type::String) {
        throw QueryRuntimeException("derive() option '{}' must contain only strings.", labels_key);
      }
      labels.emplace_back(label.ValueString());
    }
  } else {
    // no labels option -> inherit every label from the original vertex
    auto maybe_labels = real_vertex.Labels(storage::View::NEW);
    if (!maybe_labels) throw QueryRuntimeException("derive() could not read labels of a path vertex.");
    for (const auto label_id : *maybe_labels) {
      const auto &name = db_accessor->LabelToName(label_id);
      labels.emplace_back(name.data(), name.size());
    }
  }

  // The overlay holds only the explicitly-overridden properties; every other property is read
  // through the origin lazily, so a projection never copies the origin's properties (e.g. vector
  // embeddings) unless they are actually read. These override keys carry a value, so they are
  // overlay-bound through the overlay store itself - IsOverlayBound checks it before the schema.
  VirtualNode::property_map overlay{alloc};
  if (options.find(props_key) != options.end()) {
    ApplyPropertyMap(options, props_key, db_accessor, [&](storage::PropertyId id, storage::PropertyValue pv) {
      overlay.insert_or_assign(id, pv);
    });
  }

  // Read source and write target are coupled, so a key the policy bound 'origin' may not also be
  // overlaid here - rejected at construction rather than resolved silently.
  if (const auto policy_it = options.find(kPropertyPolicy);
      policy_it != options.end() && policy_it->second.type() == TypedValue::Type::Map) {
    for (const auto &[name, binding] : policy_it->second.ValueMap()) {
      if (binding.type() == TypedValue::Type::String && binding.ValueString() == "origin" &&
          overlay.contains(db_accessor->NameToProperty(name))) {
        throw QueryRuntimeException("derive() property '{}' is both overlaid and bound to origin.", name);
      }
    }
  }

  return {std::move(labels), std::move(overlay), alloc, std::optional<VertexAccessor>{real_vertex}, schema};
}

}  // namespace

void AddPathToProjection(const TypedValue &path_value, const TypedValue &options_value, VirtualGraph &projected_graph,
                         DerivedNodeDedup &dedup, int64_t projection_ref, DbAccessor *db_accessor) {
  static constexpr auto kVirtualEdgeType = "virtualEdgeType";
  static constexpr auto kSourceLabels = "sourceNodeLabels";
  static constexpr auto kSourceProperties = "sourceNodeProperties";
  static constexpr auto kTargetLabels = "targetNodeLabels";
  static constexpr auto kTargetProperties = "targetNodeProperties";
  static constexpr auto kRelationshipProperties = "relationshipProperties";
  static constexpr auto kUndirectedEdgeTypes = "undirectedEdgeTypes";

  if (path_value.type() != TypedValue::Type::Path) {
    throw QueryRuntimeException("derive() requires a path as argument 1.");
  }
  if (options_value.type() != TypedValue::Type::Map) {
    throw QueryRuntimeException("derive() argument 2 must be a map of options (e.g. {virtualEdgeType: 'TYPE'}).");
  }
  const auto &options = options_value.ValueMap();

  const auto &path_vertices = path_value.ValuePath().vertices();
  if (path_vertices.empty()) return;

  const auto alloc = projected_graph.get_allocator();
  // The static binding + ref are role-independent, so build the schema once and share it across
  // both endpoints' overlay nodes (and, via the dedup, across rows).
  const auto schema = MakeProjectionSchema(options, projection_ref, db_accessor, alloc);
  auto canonical = [&](const VertexAccessor &real_vertex,
                       std::string_view labels_key,
                       std::string_view props_key) -> std::shared_ptr<const VirtualNode> {
    const auto real_gid = real_vertex.Gid();
    if (const auto it = dedup.find(real_gid); it != dedup.end()) {
      return projected_graph.FindNode(it->second);
    }
    auto new_node = BuildDerivedNode(real_vertex, labels_key, props_key, options, schema, db_accessor, alloc);
    const auto synth_gid = new_node.Gid();
    dedup[real_gid] = synth_gid;
    projected_graph.InsertNode(std::move(new_node));
    return projected_graph.FindNode(synth_gid);
  };

  auto stored_from = canonical(path_vertices.front(), kSourceLabels, kSourceProperties);

  if (path_vertices.size() < 2) return;

  // The path has edges, so a virtual edge type is now required. It is not needed to project a
  // single-vertex path's overlay node, so a bare derive(p, {}) over one node is allowed.
  const auto type_it = options.find(kVirtualEdgeType);
  if (type_it == options.end() || type_it->second.type() != TypedValue::Type::String) {
    throw QueryRuntimeException("derive() requires a 'virtualEdgeType' string option when the path has edges.");
  }
  const auto &type_name = type_it->second.ValueString();
  const bool type_is_undirected = [&] {
    const auto it = options.find(kUndirectedEdgeTypes);
    if (it == options.end()) return false;
    if (it->second.type() != TypedValue::Type::List) {
      throw QueryRuntimeException("derive() option '{}' must be a list of edge-type strings.", kUndirectedEdgeTypes);
    }
    const auto &list = it->second.ValueList();
    if (std::ranges::any_of(list, [](const auto &e) { return e.type() != TypedValue::Type::String; })) {
      throw QueryRuntimeException("derive() option '{}' entries must be strings.", kUndirectedEdgeTypes);
    }
    return std::ranges::any_of(list,
                               [&](const auto &e) { return e.ValueString() == "*" || e.ValueString() == type_name; });
  }();

  auto stored_to = canonical(path_vertices.back(), kTargetLabels, kTargetProperties);

  auto build_edge = [&](std::shared_ptr<const VirtualNode> from, std::shared_ptr<const VirtualNode> to) {
    VirtualEdge e(std::move(from), std::move(to), utils::pmr::string{type_name, alloc}, alloc);
    ApplyPropertyMap(
        options, kRelationshipProperties, db_accessor, [&](storage::PropertyId id, storage::PropertyValue pv) {
          e.SetProperty(id, std::move(pv));
        });
    return e;
  };

  projected_graph.InsertEdgeIfNew(build_edge(stored_from, stored_to));
  if (type_is_undirected && stored_from.get() != stored_to.get()) {
    projected_graph.InsertEdgeIfNew(build_edge(std::move(stored_to), std::move(stored_from)));
  }
}

}  // namespace memgraph::query
