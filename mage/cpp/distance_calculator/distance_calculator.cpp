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

#include <math.h>
#include <mgp.hpp>
#define KM_MUL 0.001

const char *kProcedureSingle = "single";
const char *kProcedureMultiple = "multiple";

const char *kReturnDistance = "distance";
const char *kReturnDistances = "distances";

const char *kArgumentStartPoint = "start_point";
const char *kArgumentEndPoint = "end_point";
const char *kArgumentStart = "start";
const char *kArgumentEnd = "end";

const char *kArgumentDecimals = "decimals";
const char *kArgumentMetrics = "metrics";

const char *kDefaultArgumentMetrics = "m";
const int64_t kDefaultArgumentDecimals = 2;

const double pi_rad = M_PI / 180.0;
const double R = 6371000.0;

double distance_calc(const mgp::Node &node1, const mgp::Node &node2, bool use_km, int decimals) {
  double lat1 = node1.GetProperty("lat").ValueDouble();
  double lng1 = node1.GetProperty("lng").ValueDouble();

  double lat2 = node2.GetProperty("lat").ValueDouble();
  double lng2 = node2.GetProperty("lng").ValueDouble();

  double phi_1 = lat1 * pi_rad;
  double phi_2 = lat2 * pi_rad;

  double delta_phi = (lat2 - lat1) * pi_rad;
  double delta_lambda = (lng2 - lng1) * pi_rad;

  double sin_delta_phi = sin(delta_phi / 2);
  double sin_delta_lambda = sin(delta_lambda / 2);

  double a = sin_delta_phi * sin_delta_phi + cos(phi_1) * cos(phi_2) * (sin_delta_lambda * sin_delta_lambda);
  double c = 2 * atan2(sqrt(a), sqrt(1 - a));
  double distance = R * c;
  if (use_km) {
    distance *= KM_MUL;
  }
  int rounding = pow(10, decimals);
  return round(distance * rounding) / rounding;
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Single(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const mgp::List arguments = mgp::List(args);
    const mgp::Node &node1 = arguments[0].ValueNode();
    const mgp::Node &node2 = arguments[1].ValueNode();
    const std::string_view metrics = arguments[2].ValueString();
    int64_t decimals = arguments[3].ValueInt();

    mgp::Record record = record_factory.NewRecord();

    record.Insert(kReturnDistance, distance_calc(node1, node2, metrics == "km", decimals));
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

// NOLINTNEXTLINE(misc-unused-parameters)
void Multiple(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto record_factory = mgp::RecordFactory(result);

  try {
    const auto arguments = mgp::List(args);
    mgp::List distances = mgp::List();

    const mgp::List start_points = arguments[0].ValueList();
    const mgp::List end_points = arguments[1].ValueList();
    const std::string_view metrics = arguments[2].ValueString();
    int64_t decimals = arguments[3].ValueInt();

    if (start_points.Size() != end_points.Size()) {
      throw mgp::ValueException("Both arrays must be of equal length.");
    }

    bool use_km = metrics == "km";
    for (decltype(start_points.Size()) i = 0; i < start_points.Size(); i++) {
      const mgp::Node &node1 = start_points[i].ValueNode();
      const mgp::Node &node2 = end_points[i].ValueNode();
      distances.AppendExtend(mgp::Value(distance_calc(node1, node2, use_km, decimals)));
    }

    mgp::Record record = record_factory.NewRecord();
    record.Insert(kReturnDistances, distances);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(
        Single, kProcedureSingle, mgp::ProcedureType::Read,
        {mgp::Parameter(kArgumentStartPoint, mgp::Type::Node), mgp::Parameter(kArgumentEndPoint, mgp::Type::Node),
         mgp::Parameter(kArgumentMetrics, mgp::Type::String, kDefaultArgumentMetrics),
         mgp::Parameter(kArgumentDecimals, mgp::Type::Int, kDefaultArgumentDecimals)},
        {mgp::Return(kReturnDistance, mgp::Type::Double)}, module, memory);

    const auto multiple_input = std::make_pair(mgp::Type::List, mgp::Type::Node);
    const auto multiple_return = std::make_pair(mgp::Type::List, mgp::Type::Double);
    AddProcedure(Multiple, kProcedureMultiple, mgp::ProcedureType::Read,
                 {mgp::Parameter(kArgumentStart, multiple_input), mgp::Parameter(kArgumentEnd, multiple_input),
                  mgp::Parameter(kArgumentMetrics, mgp::Type::String, kDefaultArgumentMetrics),
                  mgp::Parameter(kArgumentDecimals, mgp::Type::Int, kDefaultArgumentDecimals)},
                 {mgp::Return(kReturnDistances, multiple_return)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
