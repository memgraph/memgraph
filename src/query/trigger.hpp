#pragma once

#include "query/cypher_query_interpreter.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"

namespace query {

// move to cpp
constexpr const char *kCreatedVertices = "createdVertices";

struct Trigger {
  explicit Trigger(std::string name, std::string query, utils::SkipList<QueryCacheEntry> *cache,
                   utils::SpinLock *antlr_lock);

  void Execute(utils::SkipList<PlanCacheEntry> *plan_cache, DbAccessor *dba,
               utils::MonotonicBufferResource *execution_memory, double tsc_frequency, double max_execution_time_sec,
               std::atomic<bool> *is_shutting_down, std::unordered_map<std::string, TypedValue> context) const;

  bool operator==(const Trigger &other) const { return name_ == other.name_; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const Trigger &other) const { return name_ < other.name_; }
  bool operator==(const std::string &other) const { return name_ == other; }
  // NOLINTNEXTLINE (modernize-use-nullptr)
  bool operator<(const std::string &other) const { return name_ < other; }

  const auto &name() const { return name_; }

 private:
  std::string name_;
  ParsedQuery parsed_statements_;

  // predefined identifiers
  mutable std::vector<Identifier> identifiers_{Identifier{kCreatedVertices, false}};
};

struct TriggerContext {
  void RegisterCreatedVertex(VertexAccessor created_vertex) { created_vertices_.push_back(created_vertex); }

  // move to cpp
  std::unordered_map<std::string, TypedValue> GetTypedValues() {
    std::unordered_map<std::string, TypedValue> typed_values;

    std::vector<TypedValue> typed_created_vertices;
    typed_created_vertices.reserve(created_vertices_.size());
    std::transform(std::begin(created_vertices_), std::end(created_vertices_),
                   std::back_inserter(typed_created_vertices),
                   [](const auto &accessor) { return TypedValue(accessor); });
    typed_values.emplace(kCreatedVertices, std::move(typed_created_vertices));

    return typed_values;
  }

  TriggerContext ForAccessor(DbAccessor *accessor) {
    TriggerContext new_context;

    for (const auto &created_vertex : created_vertices_) {
      if (auto maybe_vertex = accessor->FindVertex(created_vertex.Gid(), storage::View::OLD); maybe_vertex) {
        new_context.created_vertices_.push_back(*maybe_vertex);
      }
    }

    return new_context;
  }

 private:
  std::vector<VertexAccessor> created_vertices_;
};

}  // namespace query
