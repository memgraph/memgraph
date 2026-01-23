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
#include <memory>
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
struct NodeObject {
 public:
  // heuristic distance of the node
  double heuristic_distance;
  // total distance of the path to the node
  double total_distance;
  // the node the object represents
  mgp::Node node;
  // path relationship that leads into the node
  mgp::Relationship rel;
  // previous node object
  std::shared_ptr<NodeObject> prev;

  NodeObject(const double heuristic_distance, const double total_distance, const mgp::Node &node,
             const mgp::Relationship &rel, const std::shared_ptr<NodeObject> &prev)
      : heuristic_distance(heuristic_distance), total_distance(total_distance), node(node), rel(rel), prev(prev) {}

  struct Hash {
    size_t operator()(const std::shared_ptr<NodeObject> &nodeObj) const {
      return std::hash<int64_t>()(nodeObj->node.Id().AsInt());
    }
  };

  struct Comp {
    bool operator()(const std::shared_ptr<NodeObject> &nodeObj, const std::shared_ptr<NodeObject> &nodeObj2) {
      return nodeObj->total_distance + nodeObj->heuristic_distance >
             nodeObj2->total_distance + nodeObj2->heuristic_distance;
    }
  };

  struct Equal {
    bool operator()(const std::shared_ptr<NodeObject> &nodeObj, const std::shared_ptr<NodeObject> &nodeObj2) const {
      return nodeObj->node.Id() == nodeObj2->node.Id();
    }
  };
};

class Open {
 public:
  /*since C++ pq doesnt enable to do std::find or loop and find element in pq,
  and for A* we need to see if elements already exist in pq, I created a class open, which
  uses a pq normally for A*, but also has a set which checks if the value is
  already in pq, and if it has lesser path distance to it*/
  std::priority_queue<std::shared_ptr<NodeObject>, std::vector<std::shared_ptr<NodeObject>>, NodeObject::Comp> pq;
  std::unordered_map<mgp::Id, double> set;

  bool Empty() { return set.empty(); }

  const std::shared_ptr<NodeObject> Top() {
    while (set.find(pq.top()->node.Id()) == set.end()) {  // this is to make sure duplicates are ignored
      pq.pop();
    }
    return pq.top();
  }

  void Pop() {
    set.erase(pq.top()->node.Id());
    pq.pop();
  }

  void InsertOrUpdate(const std::shared_ptr<NodeObject> &elem) {
    auto it = set.find(elem->node.Id());
    if (it != set.end()) {
      if (elem->total_distance < it->second) {
        it->second = elem->total_distance;
        pq.push(elem);
      }
      return;
    }
    pq.push(elem);
    set.insert({elem->node.Id(), elem->total_distance});
  }

  Open() = default;
};

class Closed {
 public:
  std::unordered_set<std::shared_ptr<NodeObject>, NodeObject::Hash, NodeObject::Equal> closed;

  bool Empty() { return closed.empty(); }
  void Erase(const std::shared_ptr<NodeObject> &obj) { closed.erase(obj); }

  bool FindAndCompare(const std::shared_ptr<NodeObject> &obj) {
    auto it = closed.find(obj);
    if (it != closed.end()) {
      bool erase = (*it)->total_distance > obj->total_distance;
      if (erase) {
        closed.erase(it);
      }
      return erase;
    }
    return true;
  }

  void Insert(const std::shared_ptr<NodeObject> &obj) { closed.insert(obj); }

  Closed() = default;
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
  const mgp::Node start;
  const mgp::Node target;
  std::pair<double, double> lat_lon;

  GoalNodes(const mgp::Node &start, const mgp::Node &target, const std::pair<double, double> lat_lon)
      : start(start), target(target), lat_lon(lat_lon) {}
  GoalNodes(const mgp::Node &start, const mgp::Node &target) : start(start), target(target) {}
};

struct TrackingLists {
  Open open;
  Closed closed;
};

double GetHaversineDistance(double lat1, double lon1, double lat2, double lon2);
double GetRadians(double degrees);
void AStar(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
bool RelOk(const mgp::Relationship &rel, const Config &config, const RelationshipType rel_type);
bool IsLabelOk(const mgp::Node &node, const Config &config);
std::pair<mgp::Path, double> BuildResult(std::shared_ptr<NodeObject> final_node, const mgp::Node &start);
std::shared_ptr<NodeObject> InitializeStart(const mgp::Node &start);
std::pair<mgp::Path, double> HelperAstar(const GoalNodes &nodes, const Config &config);
void ExpandRelationships(const std::shared_ptr<NodeObject> &prev, const RelationshipType rel_type,
                         const GoalNodes &nodes, TrackingLists &lists, const Config &config);
double CalculateHeuristic(const Config &config, const mgp::Node &node, const GoalNodes &nodes);
std::pair<double, double> GetLatLon(const mgp::Node &target, const Config &config);
double CalculateDistance(const Config &config, const mgp::Relationship &rel);
void CheckConfigTypes(const mgp::Map &map);

}  // namespace Algo
