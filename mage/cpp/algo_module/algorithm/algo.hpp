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

#include <cmath>
#include <mgp.hpp>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace Algo {

enum class RelDirection { kNone = -1, kAny = 0, kIncoming = 1, kOutgoing = 2, kBoth = 3 };

class PathFinder {
 public:
  PathFinder(mgp::Node start_node, const mgp::Node &end_node, int64_t max_length, const mgp::List &rel_types,
             const mgp::RecordFactory &record_factory);

  RelDirection GetDirection(const std::string &rel_type) const;

  void UpdateRelationshipDirection(const mgp::List &relationship_types);
  void DFS(const mgp::Node &curr_node, mgp::Path &curr_path, std::unordered_set<int64_t> &visited);
  void FindAllPaths();

 private:
  const mgp::Node start_node_;
  const mgp::Id end_node_id_;
  const int64_t max_length_;
  bool any_incoming_;
  bool any_outgoing_;
  bool all_incoming_;
  bool all_outgoing_;

  std::unordered_map<std::string, RelDirection> rel_direction_;
  const mgp::RecordFactory &record_factory_;
};

/* all_simple_paths constants */
constexpr const std::string_view kProcedureAllSimplePaths = "all_simple_paths";
constexpr const std::string_view kAllSimplePathsArg1 = "start_node";
constexpr const std::string_view kAllSimplePathsArg2 = "end_node";
constexpr const std::string_view kAllSimplePathsArg3 = "relationship_types";
constexpr const std::string_view kAllSimplePathsArg4 = "max_length";
constexpr const std::string_view kResultAllSimplePaths = "path";

/* cover constants */
constexpr std::string_view kProcedureCover = "cover";
constexpr std::string_view kCoverArg1 = "nodes";
constexpr std::string_view kCoverRet1 = "rel";

void AllSimplePaths(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Cover(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

/* from_nodes constants */
constexpr const std::string_view kProcedureAStar = "astar";
constexpr const std::string_view kAStarStart = "start";
constexpr const std::string_view kAStarTarget = "target";
constexpr const std::string_view kAStarConfig = "config";
constexpr const std::string_view kAStarPath = "path";
constexpr const std::string_view kAStarWeight = "weight";
const std::string kDefaultHeuristic = "";
const std::string kDefaultDistance = "distance";
const std::string kDefaultLatitude = "lat";
const std::string kDefaultLongitude = "lon";

enum class RelationshipType { IN, OUT };

// Lazily-populated cache for heuristic data — only stores data for visited nodes
struct GraphCache {
  std::unordered_map<int64_t, double> heuristic_val;              // custom heuristic values
  std::unordered_map<int64_t, std::pair<double, double>> coords;  // (lat, lng)
};

// PQ entry — 24 bytes, no heap allocations
struct PQEntry {
  double f_score;
  double g_score;
  int64_t node_id;

  bool operator>(const PQEntry &other) const { return f_score > other.f_score; }
};

class Open {
 public:
  std::priority_queue<PQEntry, std::vector<PQEntry>, std::greater<PQEntry>> pq;
  std::unordered_map<int64_t, double> g_scores;

  bool Empty() const { return g_scores.empty(); }

  PQEntry Top() {
    while (!pq.empty()) {
      auto it = g_scores.find(pq.top().node_id);
      if (it != g_scores.end() && pq.top().g_score <= it->second) {
        return pq.top();
      }
      pq.pop();
    }
    return {0, 0, -1};
  }

  void Pop() {
    g_scores.erase(pq.top().node_id);
    pq.pop();
  }

  bool InsertOrUpdate(int64_t node_id, double g_score, double heuristic) {
    auto it = g_scores.find(node_id);
    if (it != g_scores.end()) {
      if (g_score < it->second) {
        it->second = g_score;
        pq.push({g_score + heuristic, g_score, node_id});
        return true;
      }
      return false;
    }
    g_scores[node_id] = g_score;
    pq.push({g_score + heuristic, g_score, node_id});
    return true;
  }
};

class Closed {
 public:
  std::unordered_set<int64_t> closed;

  bool Contains(int64_t node_id) const { return closed.count(node_id) > 0; }

  void Insert(int64_t node_id) { closed.insert(node_id); }
};

class Config {
 public:
  bool unweighted = false;
  double epsilon = 1.0;
  std::string distance_prop = kDefaultDistance;
  std::string heuristic_name = kDefaultHeuristic;
  std::string latitude_name = kDefaultLatitude;
  std::string longitude_name = kDefaultLongitude;
  std::unordered_set<std::string> whitelist;
  std::unordered_set<std::string> blacklist;
  std::unordered_set<std::string> in_rels;
  std::unordered_set<std::string> out_rels;
  bool duration = false;

  Config(const mgp::Map &map) {
    if (!map.At("unweighted").IsNull()) {
      unweighted = map.At("unweighted").ValueBool();
    }
    if (!map.At("epsilon").IsNull()) {
      epsilon = map.At("epsilon").ValueNumeric();
    }
    if (!map.At("distance_prop").IsNull()) {
      distance_prop = map.At("distance_prop").ValueString();
    }
    if (!map.At("heuristic_name").IsNull()) {
      heuristic_name = map.At("heuristic_name").ValueString();
    }
    if (!map.At("latitude_name").IsNull()) {
      latitude_name = map.At("latitude_name").ValueString();
    }
    if (!map.At("longitude_name").IsNull()) {
      longitude_name = map.At("longitude_name").ValueString();
    }
    if (!map.At("whitelisted_labels").IsNull()) {
      auto list = map.At("whitelisted_labels").ValueList();
      for (const auto value : list) {
        whitelist.insert(std::string(value.ValueString()));
      }
    }
    if (!map.At("blacklisted_labels").IsNull()) {
      auto list = map.At("blacklisted_labels").ValueList();
      for (const auto value : list) {
        blacklist.insert(std::string(value.ValueString()));
      }
    }
    if (!map.At("relationships_filter").IsNull()) {
      auto list = map.At("relationships_filter").ValueList();
      for (const auto value : list) {
        auto rel_type = std::string(value.ValueString());
        const size_t size = rel_type.size();
        const char first_elem = rel_type[0];
        const char last_elem = rel_type[size - 1];

        if (first_elem == '<' && size != 1) {
          in_rels.insert(rel_type.erase(0, 1));  // only specified incoming relationships are allowed
        } else if (last_elem == '>' && size != 1) {
          rel_type.pop_back();
          out_rels.insert(rel_type);  // only specifed outgoing relationships are allowed
        } else {                      // if not specified, a relationship goes both ways
          in_rels.insert(rel_type);
          out_rels.insert(rel_type);
        }
      }
    }

    if (!map.At("duration").IsNull()) {
      duration = map.At("duration").ValueBool();
    }
  }
};

struct GoalNodes {
  int64_t start_id;
  int64_t target_id;
  std::pair<double, double> target_lat_lon;  // target coords for haversine

  GoalNodes(int64_t start, int64_t target, std::pair<double, double> lat_lon)
      : start_id(start), target_id(target), target_lat_lon(lat_lon) {}

  GoalNodes(int64_t start, int64_t target) : start_id(start), target_id(target) {}
};

struct TrackingLists {
  Open open;
  Closed closed;
  std::unordered_map<int64_t, int64_t> parent_edge;  // child node id -> edge_id leading to best parent
};

double GetHaversineDistance(double lat1, double lon1, double lat2, double lon2);
double GetRadians(double degrees);
void AStar(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
double CalculateHeuristic(const Config &config, int64_t node_id, const GoalNodes &nodes, const GraphCache &cache);
std::pair<mgp::Path, double> BuildResult(int64_t target_id, double weight, const mgp::Graph &graph,
                                         const mgp::Node &start,
                                         const std::unordered_map<int64_t, int64_t> &parent_edge);
std::pair<mgp::Path, double> HelperAstar(const GoalNodes &nodes, const Config &config, mgp_graph *raw_graph,
                                         mgp_memory *mem, const mgp::Graph &graph, const mgp::Node &start_node);
void ExpandRelationships(int64_t prev_id, double prev_g_score, const RelationshipType rel_type, const GoalNodes &nodes,
                         TrackingLists &lists, const Config &config, mgp_graph *raw_graph, mgp_memory *mem,
                         GraphCache &cache);
void CheckConfigTypes(const mgp::Map &map);

}  // namespace Algo
