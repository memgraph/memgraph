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

#include "algo.hpp"

#include <algorithm>
#include <ranges>
#include <utility>

#include "mgp.hpp"

Algo::PathFinder::PathFinder(mgp::Node start_node, const mgp::Node &end_node, int64_t max_length,
                             const mgp::List &rel_types, const mgp::RecordFactory &record_factory)
    : start_node_(std::move(start_node)),
      end_node_id_(end_node.Id()),
      max_length_(max_length),
      any_incoming_(false),
      any_outgoing_(false),
      record_factory_(record_factory) {
  UpdateRelationshipDirection(rel_types);
}

void Algo::PathFinder::UpdateRelationshipDirection(const mgp::List &relationship_types) {
  all_incoming_ = false;
  all_outgoing_ = false;

  if (relationship_types.Size() == 0) {  // if no relationships were passed as arguments, all relationships are allowed
    any_outgoing_ = true;
    any_incoming_ = true;
    return;
  }

  bool in_rel = false;
  bool out_rel = false;

  for (const auto &rel : relationship_types) {
    const std::string rel_type{std::string(rel.ValueString())};
    const bool starts_with = rel_type.starts_with('<');
    const bool ends_with = rel_type.ends_with('>');

    // '<' -> all incoming relationship are accepted
    // '>' -> all outgoing relationships are good
    if (rel_type.size() == 1) {
      if (starts_with) {
        any_incoming_ = true;
        in_rel = true;
      } else if (ends_with) {
        any_outgoing_ = true;
        out_rel = true;
      } else {
        rel_direction_[rel_type] = RelDirection::kAny;
        in_rel = out_rel = true;
      }
      continue;
    }

    if (starts_with && ends_with) {  // <type>
      const std::string rel_key = rel_type.substr(1, rel_type.size() - 2);
      rel_direction_[rel_key] = RelDirection::kBoth;
      in_rel = out_rel = true;
    } else if (starts_with) {  // <type
      const std::string rel_key = rel_type.substr(1);
      rel_direction_[rel_key] = RelDirection::kIncoming;
      in_rel = true;
    } else if (ends_with) {  // type>
      const std::string rel_key = rel_type.substr(0, rel_type.size() - 1);
      rel_direction_[rel_key] = RelDirection::kOutgoing;
      out_rel = true;
    } else {  // type
      rel_direction_[rel_type] = RelDirection::kAny;
      in_rel = out_rel = true;
    }
  }

  if (!in_rel) {
    all_outgoing_ = true;
  } else if (!out_rel) {
    all_incoming_ = true;
  }
}

Algo::RelDirection Algo::PathFinder::GetDirection(const std::string &rel_type) const {
  auto it = rel_direction_.find(rel_type);
  if (it == rel_direction_.end()) {
    return RelDirection::kNone;
  }
  return it->second;
}

void Algo::PathFinder::DFS(const mgp::Node &curr_node, mgp::Path &curr_path, std::unordered_set<int64_t> &visited) {
  if (curr_node.Id() == end_node_id_) {
    auto record = record_factory_.NewRecord();
    record.Insert(std::string(kResultAllSimplePaths).c_str(), curr_path);
    return;
  }

  if (std::cmp_equal(curr_path.Length(), max_length_)) {
    return;
  }

  visited.insert(curr_node.Id().AsInt());
  std::unordered_set<int64_t> seen;

  auto iterate = [&visited, &seen, &curr_path, this](
                     mgp::Relationships relationships, RelDirection direction, bool always_expand) {
    for (const auto relationship : relationships) {
      auto next_node_id =
          direction == RelDirection::kOutgoing ? relationship.To().Id().AsInt() : relationship.From().Id().AsInt();

      if (visited.contains(next_node_id)) {
        continue;
      }

      auto type = std::string(relationship.Type());
      auto wanted_direction = GetDirection(type);

      if (always_expand || wanted_direction == RelDirection::kAny || wanted_direction == direction) {
        curr_path.Expand(relationship);
        DFS(direction == RelDirection::kOutgoing ? relationship.To() : relationship.From(), curr_path, visited);
        curr_path.Pop();
      } else if (wanted_direction == RelDirection::kBoth) {
        if (direction == RelDirection::kOutgoing && seen.contains(relationship.To().Id().AsInt())) {
          curr_path.Expand(relationship);
          DFS(relationship.To(), curr_path, visited);
          curr_path.Pop();
        } else if (direction == RelDirection::kIncoming) {
          seen.insert(relationship.From().Id().AsInt());
        }
      }
    }
  };

  if (!all_outgoing_) {
    iterate(curr_node.InRelationships(), RelDirection::kIncoming, any_incoming_);
  }
  if (!all_incoming_) {
    iterate(curr_node.OutRelationships(), RelDirection::kOutgoing, any_outgoing_);
  }
  visited.erase(curr_node.Id().AsInt());
}

void Algo::PathFinder::FindAllPaths() {
  mgp::Path path{start_node_};
  std::unordered_set<int64_t> visited;
  DFS(start_node_, path, visited);
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Algo::AllSimplePaths(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto start_node{arguments[0].ValueNode()};
    const auto end_node{arguments[1].ValueNode()};
    const auto rel_types{arguments[2].ValueList()};
    const auto max_nodes{arguments[3].ValueInt()};

    PathFinder pathfinder{start_node, end_node, max_nodes, rel_types, record_factory};
    pathfinder.FindAllPaths();

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Algo::Cover(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    auto list_nodes = arguments[0].ValueList();
    std::unordered_set<mgp::Node> nodes;
    for (const auto &elem : list_nodes) {
      auto node = elem.ValueNode();
      nodes.insert(node);
    }

    for (const auto &node : nodes) {
      for (const auto rel : node.OutRelationships()) {
        if (nodes.contains(rel.To())) {
          auto record = record_factory.NewRecord();
          record.Insert(std::string(kCoverRet1).c_str(), rel);
        }
      }
    }

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

void Algo::CheckConfigTypes(const mgp::Map &map) {
  if (!map.At("unweighted").IsNull() && !map.At("unweighted").IsBool()) {
    throw mgp::ValueException("unweighted config option should be bool!");
  }
  if (!map.At("epsilon").IsNull() && !map.At("epsilon").IsNumeric()) {
    throw mgp::ValueException("epsilon config option should be numeric!");
  }
  if (!map.At("distance_prop").IsNull() && !map.At("distance_prop").IsString()) {
    throw mgp::ValueException("distance_prop config option should be string!");
  }
  if (!map.At("heuristic_name").IsNull() && !map.At("heuristic_name").IsString()) {
    throw mgp::ValueException("heuristic_name config option should be string!");
  }
  if (!map.At("latitude_name").IsNull() && !map.At("latitude_name").IsString()) {
    throw mgp::ValueException("latitude_name config option should be string!");
  }
  if (!map.At("longitude_name").IsNull() && !map.At("longitude_name").IsString()) {
    throw mgp::ValueException("longitude_name config option should be string!");
  }
  if (!map.At("whitelisted_labels").IsNull() && !map.At("whitelisted_labels").IsList()) {
    throw mgp::ValueException("whitelisted_labels config option should be list!");
  }
  if (!map.At("whitelisted_labels").IsNull()) {
    auto list = map.At("whitelisted_labels").ValueList();
    for (const auto value : list) {
      if (!value.IsString()) {
        throw mgp::ValueException("Labels in the whitelisted_labels config list must be strings!");
      }
    }
  }
  if (!map.At("blacklisted_labels").IsNull() && !map.At("blacklisted_labels").IsList()) {
    throw mgp::ValueException("blacklisted_labels config option should be list!");
  }
  if (!map.At("blacklisted_labels").IsNull()) {
    auto list = map.At("blacklisted_labels").ValueList();
    for (const auto value : list) {
      if (!value.IsString()) {
        throw mgp::ValueException("Labels in the blacklisted_labels config list must be strings!");
      }
    }
  }
  if (!map.At("relationships_filter").IsNull() && !map.At("relationships_filter").IsList()) {
    throw mgp::ValueException("relationships_filter config option should be list!");
  }
  if (!map.At("relationships_filter").IsNull() && map.At("relationships_filter").IsList()) {
    auto list = map.At("relationships_filter").ValueList();
    for (const auto value : list) {
      if (!value.IsString()) {
        throw mgp::ValueException("Elements in relationships_filter config list must be strings!");
      }
      auto rel_type = std::string(value.ValueString());
      const size_t size = rel_type.size();
      const char first_elem = rel_type[0];
      const char last_elem = rel_type[size - 1];

      if (first_elem == '<' && last_elem == '>') {
        throw mgp::ValueException("Wrong relationship format => <relationship> is not allowed!");
      }
    }
  }

  if (!map.At("duration").IsNull() && !map.At("duration").IsBool()) {
    throw mgp::ValueException("duration config option should be bool!");
  }
}

double Algo::GetRadians(double degrees) { return degrees * M_PI / 180.0; }

double Algo::GetHaversineDistance(double lat1, double lon1, double lat2, double lon2) {
  const double earthRadius = 6371.0;
  lat1 = GetRadians(lat1);
  lon1 = GetRadians(lon1);
  lat2 = GetRadians(lat2);
  lon2 = GetRadians(lon2);

  const double dLat = lat2 - lat1;
  const double dLon = lon2 - lon1;
  const double a = (sin(dLat / 2) * sin(dLat / 2)) + (cos(lat1) * cos(lat2) * sin(dLon / 2) * sin(dLon / 2));
  const double c = 2 * atan2(sqrt(a), sqrt(1 - a));
  return earthRadius * c;
}

double Algo::CalculateHeuristic(const Config &config, int64_t node_id, const GoalNodes &nodes,
                                const GraphCache &cache) {
  if (config.heuristic_name != kDefaultHeuristic) {
    auto it = cache.heuristic_val.find(node_id);
    if (it != cache.heuristic_val.end()) return it->second;
    throw mgp::ValueException("Custom heuristic property must be of a numeric, or duration data type!");
  }
  auto it = cache.coords.find(node_id);
  if (it == cache.coords.end()) {
    throw mgp::ValueException(
        "Latitude and longitude properties, or a custom heuristic value, must be specified in every node!");
  }
  return GetHaversineDistance(
      it->second.first, it->second.second, nodes.target_lat_lon.first, nodes.target_lat_lon.second);
}

// Helper: clean up raw C API resources and throw an exception.
[[noreturn]] static void ThrowWithCleanup(mgp_edges_iterator *eit, mgp_vertex *v, const char *msg) {
  if (eit) mgp::edges_iterator_destroy(eit);
  if (v) mgp::vertex_destroy(v);
  throw mgp::ValueException(msg);
}

// Lazily expand a single node's edges using the raw C API.
// Only reads neighbors of the node being expanded (not the entire graph).
void Algo::ExpandRelationships(int64_t prev_id, double prev_g_score, const RelationshipType rel_type,
                               const GoalNodes &nodes, TrackingLists &lists, const Config &config, mgp_graph *raw_graph,
                               mgp_memory *mem, GraphCache &cache) {
  mgp_vertex_id vid{prev_id};
  auto *v = mgp::graph_get_vertex_by_id(raw_graph, vid, mem);
  if (!v) return;

  auto *eit =
      (rel_type == RelationshipType::OUT) ? mgp::vertex_iter_out_edges(v, mem) : mgp::vertex_iter_in_edges(v, mem);

  for (auto *e = mgp::edges_iterator_get(eit); e != nullptr; e = mgp::edges_iterator_next(eit)) {
    // Relationship type filtering (equivalent to master's RelOk)
    if (!config.in_rels.empty() || !config.out_rels.empty()) {
      std::string type_name(mgp::edge_get_type(e).name);
      bool rel_ok = (rel_type == RelationshipType::IN && config.in_rels.contains(type_name)) ||
                    (rel_type == RelationshipType::OUT && config.out_rels.contains(type_name));
      if (!rel_ok) continue;
    }

    auto *neighbor_v = (rel_type == RelationshipType::OUT) ? mgp::edge_get_to(e) : mgp::edge_get_from(e);
    int64_t neighbor_id = mgp::vertex_get_id(neighbor_v).as_int;

    // Label filtering (equivalent to master's IsLabelOk)
    if (!config.whitelist.empty() || !config.blacklist.empty()) {
      size_t label_count = mgp::vertex_labels_count(neighbor_v);
      bool label_ok = true;
      for (size_t i = 0; i < label_count; ++i) {
        std::string label_name(mgp::vertex_label_at(neighbor_v, i).name);
        if (config.blacklist.contains(label_name)) {
          label_ok = false;
          break;
        }
        if (!config.whitelist.empty() && !config.whitelist.contains(label_name)) {
          label_ok = false;
          break;
        }
      }
      if (!label_ok) continue;
    }

    // Read edge weight (equivalent to master's CalculateDistance)
    double weight = 10.0;
    if (!config.unweighted) {
      auto *dist_val = mgp::edge_get_property(e, config.distance_prop.c_str(), mem);
      if (!dist_val || mgp::value_is_null(dist_val)) {
        if (dist_val) mgp::value_destroy(dist_val);
        ThrowWithCleanup(eit, v, "If the graph is weighted, distance property of the relationship must be specified!");
      }
      if (mgp::value_is_numeric(dist_val)) {
        weight = mgp::value_get_numeric(dist_val);
      } else if (config.duration && mgp::value_is_duration(dist_val)) {
        weight = static_cast<double>(mgp::duration_get_microseconds(mgp::value_get_duration(dist_val)));
      } else {
        mgp::value_destroy(dist_val);
        ThrowWithCleanup(eit, v, "Distance property must be a numeric or duration datatype!");
      }
      mgp::value_destroy(dist_val);
    }

    double new_g_score = weight + prev_g_score;
    if (lists.closed.Contains(neighbor_id)) {
      continue;
    }

    // Cache neighbor heuristic data if not already cached
    if (config.heuristic_name != kDefaultHeuristic) {
      if (cache.heuristic_val.find(neighbor_id) == cache.heuristic_val.end()) {
        auto *hval = mgp::vertex_get_property(neighbor_v, config.heuristic_name.c_str(), mem);
        if (!hval || mgp::value_is_null(hval)) {
          if (hval) mgp::value_destroy(hval);
          ThrowWithCleanup(eit, v, "Custom heuristic property must be of a numeric, or duration data type!");
        }
        if (mgp::value_is_numeric(hval)) {
          cache.heuristic_val[neighbor_id] = mgp::value_get_numeric(hval);
        } else if (config.duration && mgp::value_is_duration(hval)) {
          cache.heuristic_val[neighbor_id] =
              static_cast<double>(mgp::duration_get_microseconds(mgp::value_get_duration(hval)));
        } else {
          mgp::value_destroy(hval);
          ThrowWithCleanup(eit, v, "Custom heuristic property must be of a numeric, or duration data type!");
        }
        mgp::value_destroy(hval);
      }
    } else {
      if (cache.coords.find(neighbor_id) == cache.coords.end()) {
        auto *lat_val = mgp::vertex_get_property(neighbor_v, config.latitude_name.c_str(), mem);
        auto *lng_val = mgp::vertex_get_property(neighbor_v, config.longitude_name.c_str(), mem);
        if (!lat_val || !lng_val || mgp::value_is_null(lat_val) || mgp::value_is_null(lng_val)) {
          if (lat_val) mgp::value_destroy(lat_val);
          if (lng_val) mgp::value_destroy(lng_val);
          ThrowWithCleanup(eit,
                           v,
                           "Latitude and longitude properties, or a custom heuristic value, "
                           "must be specified in every node!");
        }
        if (mgp::value_is_numeric(lat_val) && mgp::value_is_numeric(lng_val)) {
          cache.coords[neighbor_id] = {mgp::value_get_numeric(lat_val), mgp::value_get_numeric(lng_val)};
        } else {
          mgp::value_destroy(lat_val);
          mgp::value_destroy(lng_val);
          ThrowWithCleanup(eit, v, "Latitude and longitude must be numeric data types!");
        }
        mgp::value_destroy(lat_val);
        mgp::value_destroy(lng_val);
      }
    }

    int64_t edge_id = mgp::edge_get_id(e).as_int;
    auto heuristic = CalculateHeuristic(config, neighbor_id, nodes, cache) * config.epsilon;
    if (lists.open.InsertOrUpdate(neighbor_id, new_g_score, heuristic)) {
      lists.parent_edge.insert_or_assign(neighbor_id, edge_id);
    }
  }
  mgp::edges_iterator_destroy(eit);
  mgp::vertex_destroy(v);
}

std::pair<mgp::Path, double> Algo::HelperAstar(const GoalNodes &nodes, const Config &config, mgp_graph *raw_graph,
                                               mgp_memory *mem, const mgp::Graph &graph, const mgp::Node &start_node) {
  TrackingLists lists;
  GraphCache cache;

  // Pre-cache target coordinates so the heuristic works from the first expansion
  if (config.heuristic_name == kDefaultHeuristic) {
    cache.coords[nodes.target_id] = nodes.target_lat_lon;
  }

  lists.open.InsertOrUpdate(nodes.start_id, 0.0, 0.0);

  while (!lists.open.Empty()) {
    auto entry = lists.open.Top();
    lists.open.Pop();
    if (entry.node_id == nodes.target_id) {
      return BuildResult(nodes.target_id, entry.g_score, graph, start_node, lists.parent_edge);
    }
    lists.closed.Insert(entry.node_id);
    ExpandRelationships(
        entry.node_id, entry.g_score, RelationshipType::OUT, nodes, lists, config, raw_graph, mem, cache);
    ExpandRelationships(
        entry.node_id, entry.g_score, RelationshipType::IN, nodes, lists, config, raw_graph, mem, cache);
  }
  return {mgp::Path(start_node), 0};
}

std::pair<mgp::Path, double> Algo::BuildResult(int64_t target_id, double weight, const mgp::Graph &graph,
                                               const mgp::Node &start,
                                               const std::unordered_map<int64_t, int64_t> &parent_edge) {
  mgp::Path path = mgp::Path(start);
  const int64_t start_id = start.Id().AsInt();

  // Collect (node_id, edge_id) pairs from target back to start
  std::vector<std::pair<int64_t, int64_t>> chain;  // (child_id, edge_id)
  int64_t current_id = target_id;
  while (current_id != start_id) {
    auto it = parent_edge.find(current_id);
    if (it == parent_edge.end()) break;
    chain.push_back({current_id, it->second});
    // Find the other end of the edge by looking up the node and scanning edges
    auto node = graph.GetNodeById(mgp::Id::FromInt(current_id));
    int64_t eid = it->second;
    bool found = false;
    for (const auto rel : node.InRelationships()) {
      if (rel.Id().AsInt() == eid) {
        current_id = rel.From().Id().AsInt();
        found = true;
        break;
      }
    }
    if (!found) {
      for (const auto rel : node.OutRelationships()) {
        if (rel.Id().AsInt() == eid) {
          current_id = rel.To().Id().AsInt();
          found = true;
          break;
        }
      }
    }
    if (!found) break;
  }

  // Build path from start to target by expanding relationships in order
  for (auto it = chain.rbegin(); it != chain.rend(); ++it) {
    auto node = graph.GetNodeById(mgp::Id::FromInt(it == chain.rbegin() ? start_id : std::prev(it)->first));
    for (const auto rel : node.OutRelationships()) {
      if (rel.Id().AsInt() == it->second) {
        path.Expand(rel);
        goto next;
      }
    }
    for (const auto rel : node.InRelationships()) {
      if (rel.Id().AsInt() == it->second) {
        path.Expand(rel);
        goto next;
      }
    }
  next:;
  }

  return {std::move(path), weight};
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Algo::AStar(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto graph = mgp::Graph(memgraph_graph);
    auto start = arguments[0].ValueNode();
    auto target = arguments[1].ValueNode();
    auto config_map = arguments[2].ValueMap();
    CheckConfigTypes(config_map);
    auto config = Config(config_map);

    auto start_id = start.Id().AsInt();
    auto target_id = target.Id().AsInt();
    auto *mem = mgp::MemoryDispatcher::GetMemoryResource();

    // Read target coordinates for haversine heuristic (equivalent to master's GetLatLon)
    std::pair<double, double> target_coords = {0.0, 0.0};
    if (config.heuristic_name == kDefaultHeuristic) {
      mgp_vertex_id tvid{target_id};
      auto *tv = mgp::graph_get_vertex_by_id(memgraph_graph, tvid, mem);
      if (tv) {
        auto *lat_val = mgp::vertex_get_property(tv, config.latitude_name.c_str(), mem);
        auto *lng_val = mgp::vertex_get_property(tv, config.longitude_name.c_str(), mem);
        if (!lat_val || !lng_val || mgp::value_is_null(lat_val) || mgp::value_is_null(lng_val)) {
          if (lat_val) mgp::value_destroy(lat_val);
          if (lng_val) mgp::value_destroy(lng_val);
          mgp::vertex_destroy(tv);
          throw mgp::ValueException(
              "Latitude and longitude properties, or a custom heuristic value, "
              "must be specified in every node!");
        }
        if (mgp::value_is_numeric(lat_val) && mgp::value_is_numeric(lng_val)) {
          target_coords = {mgp::value_get_numeric(lat_val), mgp::value_get_numeric(lng_val)};
        } else {
          mgp::value_destroy(lat_val);
          mgp::value_destroy(lng_val);
          mgp::vertex_destroy(tv);
          throw mgp::ValueException("Latitude and longitude must be numeric data types!");
        }
        if (lat_val) mgp::value_destroy(lat_val);
        if (lng_val) mgp::value_destroy(lng_val);
        mgp::vertex_destroy(tv);
      }
    }

    GoalNodes nodes = (config.heuristic_name == kDefaultHeuristic) ? GoalNodes(start_id, target_id, target_coords)
                                                                   : GoalNodes(start_id, target_id);

    const auto pair = HelperAstar(nodes, config, memgraph_graph, mem, graph, start);
    const mgp::Path &path = pair.first;
    const double weight = pair.second;

    auto record = record_factory.NewRecord();
    record.Insert(std::string(kAStarPath).c_str(), path);
    record.Insert(std::string(kAStarWeight).c_str(), weight);

  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}
