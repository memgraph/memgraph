#pragma once

#include "database/graph_db_accessor.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/parameters.hpp"
#include "query/plan/profile.hpp"

namespace query {

struct EvaluationContext {
  int64_t timestamp{-1};
  Parameters parameters;
  /// All properties indexable via PropertyIx
  std::vector<storage::Property> properties;
  /// All labels indexable via LabelIx
  std::vector<storage::Label> labels;
};

inline std::vector<storage::Property> NamesToProperties(
    const std::vector<std::string> &property_names,
    database::GraphDbAccessor *dba) {
  std::vector<storage::Property> properties;
  properties.reserve(property_names.size());
  for (const auto &name : property_names) {
    properties.push_back(dba->Property(name));
  }
  return properties;
}

inline std::vector<storage::Label> NamesToLabels(
    const std::vector<std::string> &label_names,
    database::GraphDbAccessor *dba) {
  std::vector<storage::Label> labels;
  labels.reserve(label_names.size());
  for (const auto &name : label_names) {
    labels.push_back(dba->Label(name));
  }
  return labels;
}

struct ExecutionContext {
  database::GraphDbAccessor *db_accessor{nullptr};
  SymbolTable symbol_table;
  EvaluationContext evaluation_context;
  bool is_profile_query{false};
  std::chrono::duration<double> profile_execution_time;
  plan::ProfilingStats stats;
  plan::ProfilingStats *stats_root{nullptr};
};

}  // namespace query
