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

#include "query/dump.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include <fmt/format.h>

#include "dbms/database.hpp"
#include "query/exceptions.hpp"
#include "query/stream.hpp"
#include "query/string_helpers.hpp"
#include "query/trigger_context.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/constraints/type_constraints_kind.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/algorithm.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/temporal.hpp"

#include "range/v3/all.hpp"

namespace r = ranges;
namespace rv = ranges::views;

namespace memgraph::query {

namespace {

// Property that is used to make a difference among vertices. It is added to
// property set of vertices to match edges and removed after the entire graph
// is built.
const char *kInternalPropertyId = "__mg_id__";

// Label that is attached to each vertex and is used for easier creation of
// index on internal property id.
const char *kInternalVertexLabel = "__mg_vertex__";

/// A helper function that escapes label, edge type and property names.
std::string EscapeName(const std::string_view value) {
  std::string out;
  out.reserve(value.size() + 2);
  out.append(1, '`');
  for (auto c : value) {
    if (c == '`') {
      out.append("``");
    } else {
      out.append(1, c);
    }
  }
  out.append(1, '`');
  return out;
}

void DumpPreciseDouble(std::ostream *os, double value) {
  // A temporary stream is used to keep precision of the original output
  // stream unchanged.
  std::ostringstream temp_oss;
  temp_oss << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
  *os << temp_oss.str();
}

namespace {
void DumpDate(std::ostream &os, const storage::TemporalData &value) {
  utils::Date date(value.microseconds);
  os << "DATE(\"" << date << "\")";
}

void DumpLocalTime(std::ostream &os, const storage::TemporalData &value) {
  utils::LocalTime lt(value.microseconds);
  os << "LOCALTIME(\"" << lt << "\")";
}

void DumpLocalDateTime(std::ostream &os, const storage::TemporalData &value) {
  utils::LocalDateTime ldt(value.microseconds);
  os << "LOCALDATETIME(\"" << ldt << "\")";
}

void DumpDuration(std::ostream &os, const storage::TemporalData &value) {
  utils::Duration dur(value.microseconds);
  os << "DURATION(\"" << dur << "\")";
}

void DumpEnum(std::ostream &os, const storage::Enum &value, query::DbAccessor *dba) {
  auto const opt_str = dba->EnumToName(value);
  if (opt_str.HasError()) throw query::QueryRuntimeException("Unexpected error when getting enum.");
  os << *opt_str;
}

void DumpTemporalData(std::ostream &os, const storage::TemporalData &value) {
  switch (value.type) {
    case storage::TemporalType::Date: {
      DumpDate(os, value);
      return;
    }
    case storage::TemporalType::LocalTime: {
      DumpLocalTime(os, value);
      return;
    }
    case storage::TemporalType::LocalDateTime: {
      DumpLocalDateTime(os, value);
      return;
    }
    case storage::TemporalType::Duration: {
      DumpDuration(os, value);
      return;
    }
  }
}

void DumpZonedDateTime(std::ostream &os, const storage::ZonedTemporalData &value) {
  const utils::ZonedDateTime zdt(value.microseconds, value.timezone);
  os << "DATETIME(\"" << zdt << "\")";
}

void DumpZonedTemporalData(std::ostream &os, const storage::ZonedTemporalData &value) {
  switch (value.type) {
    case storage::ZonedTemporalType::ZonedDateTime: {
      DumpZonedDateTime(os, value);
      return;
    }
  }
}

void DumpPoint2d(std::ostream &os, const storage::Point2d &value) { os << query::CypherConstructionFor(value); }

void DumpPoint3d(std::ostream &os, const storage::Point3d &value) { os << query::CypherConstructionFor(value); }

}  // namespace

void DumpPropertyValue(std::ostream *os, const storage::ExternalPropertyValue &value, query::DbAccessor *dba) {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      *os << "Null";
      return;
    case storage::PropertyValue::Type::Bool:
      *os << (value.ValueBool() ? "true" : "false");
      return;
    case storage::PropertyValue::Type::String:
      *os << utils::Escape(value.ValueString());
      return;
    case storage::PropertyValue::Type::Int:
      *os << value.ValueInt();
      return;
    case storage::PropertyValue::Type::Double:
      DumpPreciseDouble(os, value.ValueDouble());
      return;
    case storage::PropertyValue::Type::List: {
      *os << "[";
      const auto &list = value.ValueList();
      utils::PrintIterable(*os, list, ", ", [&](auto &os, const auto &item) { DumpPropertyValue(&os, item, dba); });
      *os << "]";
      return;
    }
    case storage::PropertyValue::Type::Map: {
      *os << "{";
      const auto &map = value.ValueMap();
      utils::PrintIterable(*os, map, ", ", [&](auto &os, const auto &kv) {
        os << EscapeName(kv.first) << ": ";
        DumpPropertyValue(&os, kv.second, dba);
      });
      *os << "}";
      return;
    }
    case storage::PropertyValue::Type::TemporalData: {
      DumpTemporalData(*os, value.ValueTemporalData());
      return;
    }
    case storage::PropertyValue::Type::ZonedTemporalData: {
      DumpZonedTemporalData(*os, value.ValueZonedTemporalData());
      return;
    }
    case storage::PropertyValue::Type::Enum: {
      DumpEnum(*os, value.ValueEnum(), dba);
      return;
    }
    case storage::PropertyValue::Type::Point2d: {
      DumpPoint2d(*os, value.ValuePoint2d());
      return;
    }
    case storage::PropertyValue::Type::Point3d: {
      DumpPoint3d(*os, value.ValuePoint3d());
      return;
    }
  }
}

void DumpProperties(std::ostream *os, query::DbAccessor *dba,
                    const std::map<storage::PropertyId, storage::PropertyValue> &store,
                    std::optional<int64_t> property_id = std::nullopt) {
  *os << "{";
  if (property_id) {
    *os << kInternalPropertyId << ": " << *property_id;
    if (!store.empty()) *os << ", ";
  }
  utils::PrintIterable(*os, store, ", ", [&dba](auto &os, const auto &kv) {
    os << EscapeName(dba->PropertyToName(kv.first)) << ": ";

    // Convert PropertyValue to ExternalPropertyValue to map keys from PropertyId to strings, preserving property order
    // compatibility with previous database dumps.
    DumpPropertyValue(&os, storage::ToExternalPropertyValue(kv.second, dba->GetStorageAccessor()->GetNameIdMapper()),
                      dba);
  });
  *os << "}";
}

void DumpVertex(std::ostream *os, query::DbAccessor *dba, const query::VertexAccessor &vertex) {
  *os << "CREATE (";
  *os << ":" << kInternalVertexLabel;
  auto maybe_labels = vertex.Labels(storage::View::OLD);
  if (maybe_labels.HasError()) {
    switch (maybe_labels.GetError()) {
      case storage::Error::DELETED_OBJECT:
        throw query::QueryRuntimeException("Trying to get labels from a deleted node.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException("Trying to get labels from a node that doesn't exist.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
        throw query::QueryRuntimeException("Unexpected error when getting labels.");
    }
  }
  for (const auto &label : *maybe_labels) {
    *os << ":" << EscapeName(dba->LabelToName(label));
  }
  *os << " ";
  auto maybe_props = vertex.Properties(storage::View::OLD);
  if (maybe_props.HasError()) {
    switch (maybe_props.GetError()) {
      case storage::Error::DELETED_OBJECT:
        throw query::QueryRuntimeException("Trying to get properties from a deleted object.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException("Trying to get properties from a node that doesn't exist.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
        throw query::QueryRuntimeException("Unexpected error when getting properties.");
    }
  }
  DumpProperties(os, dba, *maybe_props, vertex.CypherId());
  *os << ");";
}

void DumpEdge(std::ostream *os, query::DbAccessor *dba, const query::EdgeAccessor &edge) {
  *os << "MATCH ";
  *os << "(u:" << kInternalVertexLabel << "), ";
  *os << "(v:" << kInternalVertexLabel << ")";
  *os << " WHERE ";
  *os << "u." << kInternalPropertyId << " = " << edge.From().CypherId();
  *os << " AND ";
  *os << "v." << kInternalPropertyId << " = " << edge.To().CypherId() << " ";
  *os << "CREATE (u)-[";
  *os << ":" << EscapeName(dba->EdgeTypeToName(edge.EdgeType()));
  auto maybe_props = edge.Properties(storage::View::OLD);
  if (maybe_props.HasError()) {
    switch (maybe_props.GetError()) {
      case storage::Error::DELETED_OBJECT:
        throw query::QueryRuntimeException("Trying to get properties from a deleted object.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException("Trying to get properties from an edge that doesn't exist.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
        throw query::QueryRuntimeException("Unexpected error when getting properties.");
    }
  }
  if (!maybe_props->empty()) {
    *os << " ";
    DumpProperties(os, dba, *maybe_props);
  }
  *os << "]->(v);";
}

void DumpLabelIndex(std::ostream *os, query::DbAccessor *dba, const storage::LabelId label) {
  *os << "CREATE INDEX ON :" << EscapeName(dba->LabelToName(label)) << ";";
}

void DumpEdgeTypeIndex(std::ostream *os, query::DbAccessor *dba, const storage::EdgeTypeId edge_type) {
  *os << "CREATE EDGE INDEX ON :" << EscapeName(dba->EdgeTypeToName(edge_type)) << ";";
}

void DumpEdgeTypePropertyIndex(std::ostream *os, query::DbAccessor *dba, const storage::EdgeTypeId edge_type,
                               storage::PropertyId property) {
  *os << "CREATE EDGE INDEX ON :" << EscapeName(dba->EdgeTypeToName(edge_type)) << "("
      << EscapeName(dba->PropertyToName(property)) << ");";
}

void DumpEdgePropertyIndex(std::ostream *os, query::DbAccessor *dba, storage::PropertyId property) {
  *os << "CREATE GLOBAL EDGE INDEX ON :(" << EscapeName(dba->PropertyToName(property)) << ");";
}

void DumpLabelPropertiesIndex(std::ostream *os, query::DbAccessor *dba, storage::LabelId label,
                              std::span<storage::PropertyPath const> properties) {
  using namespace std::literals::string_view_literals;
  auto const concat_nested_props = [&](auto &&path) {
    return path | rv::transform([&](auto &&property_id) { return EscapeName(dba->PropertyToName(property_id)); }) |
           rv::join("."sv) | r::to<std::string>();
  };

  auto prop_names = properties | rv::transform(concat_nested_props) | rv::join(", "sv) | r::to<std::string>();

  *os << "CREATE INDEX ON :" << EscapeName(dba->LabelToName(label)) << "(" << prop_names << ");";
}

void DumpTextIndex(std::ostream *os, query::DbAccessor *dba, const std::string &index_name, storage::LabelId label) {
  *os << "CREATE TEXT INDEX " << EscapeName(index_name) << " ON :" << EscapeName(dba->LabelToName(label)) << ";";
}

void DumpPointIndex(std::ostream *os, query::DbAccessor *dba, storage::LabelId label, storage::PropertyId property) {
  *os << "CREATE POINT INDEX ON :" << EscapeName(dba->LabelToName(label)) << "("
      << EscapeName(dba->PropertyToName(property)) << ");";
}

void DumpVectorIndex(std::ostream *os, query::DbAccessor *dba, const storage::VectorIndexSpec &spec) {
  // *os << "CREATE VECTOR INDEX " << EscapeName(spec.index_name) << " ON :" << EscapeName(dba->LabelToName(spec.label))
  //     << "(" << EscapeName(dba->PropertyToName(spec.property)) << ") WITH CONFIG { "
  //     << "\"dimension\": " << spec.dimension << ", "
  //     << R"("metric": ")" << storage::VectorIndex::NameFromMetric(spec.metric_kind) << "\", "
  //     << "\"capacity\": " << spec.capacity << ", "
  //     << "\"resize_coefficient\": " << spec.resize_coefficient << ", "
  //     << R"("scalar_kind": ")" << storage::VectorIndex::NameFromScalar(spec.scalar_kind) << "\" };";
}

void DumpExistenceConstraint(std::ostream *os, query::DbAccessor *dba, storage::LabelId label,
                             storage::PropertyId property) {
  *os << "CREATE CONSTRAINT ON (u:" << EscapeName(dba->LabelToName(label)) << ") ASSERT EXISTS (u."
      << EscapeName(dba->PropertyToName(property)) << ");";
}

void DumpUniqueConstraint(std::ostream *os, query::DbAccessor *dba, storage::LabelId label,
                          const std::set<storage::PropertyId> &properties) {
  *os << "CREATE CONSTRAINT ON (u:" << EscapeName(dba->LabelToName(label)) << ") ASSERT ";
  utils::PrintIterable(*os, properties, ", ", [&dba](auto &stream, const auto &property) {
    stream << "u." << EscapeName(dba->PropertyToName(property));
  });
  *os << " IS UNIQUE;";
}

void DumpTypeConstraint(std::ostream *os, query::DbAccessor *dba, storage::LabelId label, storage::PropertyId property,
                        storage::TypeConstraintKind type) {
  *os << "CREATE CONSTRAINT ON (u:" << EscapeName(dba->LabelToName(label)) << ") ASSERT u."
      << EscapeName(dba->PropertyToName(property)) << " IS TYPED " << storage::TypeConstraintKindToString(type) << ";";
}

const char *triggerPhaseToString(TriggerPhase phase) {
  switch (phase) {
    case TriggerPhase::BEFORE_COMMIT:
      return "BEFORE COMMIT EXECUTE";
    case TriggerPhase::AFTER_COMMIT:
      return "AFTER COMMIT EXECUTE";
  }
}

}  // namespace

PullPlanDump::PullPlanDump(DbAccessor *dba, dbms::DatabaseAccess db_acc)
    : dba_(dba),
      db_acc_(db_acc),
      vertices_iterable_(dba->Vertices(storage::View::OLD)),
      pull_chunks_{/*
                    * IMPORTANT: the order here must reflex the order in `src/storage/v2/durability/snapshot.cpp`
                    * this is so that we have a stable order
                    */

                   /// User defined Datatype Info
                   // Dump all enums
                   CreateEnumsPullChunk(),

                   /// Vertices
                   // Dump all vertices
                   CreateVertexPullChunk(),

                   /// Edges
                   // Create internal index for faster edge creation
                   CreateInternalIndexPullChunk(),
                   // Dump all edges
                   CreateEdgePullChunk(),
                   // Drop the internal index
                   CreateDropInternalIndexPullChunk(),
                   // Internal index cleanup
                   CreateInternalIndexCleanupPullChunk(),

                   /// Indices and constraints (Vertex)
                   // Dump all label indices
                   CreateLabelIndicesPullChunk(),
                   // Dump all label property indices
                   CreateLabelPropertiesIndicesPullChunk(),
                   // Dump all text indices
                   CreateTextIndicesPullChunk(),
                   // Dump all point indices
                   CreatePointIndicesPullChunk(),
                   // Dump all vector indices
                   CreateVectorIndicesPullChunk(),
                   // Dump all existence constraints
                   CreateExistenceConstraintsPullChunk(),
                   // Dump all unique constraints
                   CreateUniqueConstraintsPullChunk(),
                   // Dump all type constraints
                   CreateTypeConstraintsPullChunk(),

                   /// Indices and constraints (Edge)
                   // Dump all edge-type indices
                   CreateEdgeTypeIndicesPullChunk(),
                   // Dump all edge-type property indices
                   CreateEdgeTypePropertyIndicesPullChunk(),
                   // Dump all global edge property indices
                   CreateEdgePropertyIndicesPullChunk(),

                   // Dump all triggers
                   CreateTriggersPullChunk()} {}

bool PullPlanDump::Pull(AnyStream *stream, std::optional<int> n) {
  // Iterate all functions that stream some results.
  // Each function should return number of results it streamed after it
  // finishes. If the function did not finish streaming all the results,
  // std::nullopt should be returned because n results have already been sent.
  while (current_chunk_index_ < pull_chunks_.size() && (!n || *n > 0)) {
    const auto maybe_streamed_count = pull_chunks_[current_chunk_index_](stream, n);

    if (!maybe_streamed_count) {
      // n wasn't large enough to stream all the results from the current chunk
      break;
    }

    if (n) {
      // chunk finished streaming its results
      // subtract number of results streamed in current pull
      // so we know how many results we need to stream from future
      // chunks.
      *n -= *maybe_streamed_count;
    }

    ++current_chunk_index_;
  }
  return current_chunk_index_ == pull_chunks_.size();
}

PullPlanDump::PullChunk PullPlanDump::CreateEnumsPullChunk() {
  auto enums = dba_->ShowEnums();
  auto to_create = [](auto &&p) {
    // rv::c_str is required! https://github.com/ericniebler/range-v3/issues/1699
    return fmt::format("CREATE ENUM {} VALUES {{ {} }};", p.first,
                       p.second | rv::join(rv::c_str(", ")) | r::to<std::string>);
  };
  auto results = enums | rv::transform(to_create) | r::to_vector;

  // Dump all enums
  return [global_index = 0U, results = std::move(results)](AnyStream *stream,
                                                           std::optional<int> n) mutable -> std::optional<size_t> {
    size_t local_counter = 0;
    while (global_index < results.size() && (!n || local_counter < *n)) {
      stream->Result({TypedValue(results[global_index])});

      ++global_index;
      ++local_counter;
    }

    if (global_index == results.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateLabelIndicesPullChunk() {
  // Dump all label indices
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of indices vectors
    if (!indices_info_) {
      indices_info_.emplace(dba_->ListAllIndices());
    }
    const auto &label = indices_info_->label;

    size_t local_counter = 0;
    while (global_index < label.size() && (!n || local_counter < *n)) {
      std::ostringstream os;
      DumpLabelIndex(&os, dba_, label[global_index]);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == label.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateEdgeTypeIndicesPullChunk() {
  // Dump all edge type indices
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of indices vectors
    if (!indices_info_) {
      indices_info_.emplace(dba_->ListAllIndices());
    }
    const auto &edge_type = indices_info_->edge_type;

    size_t local_counter = 0;
    while (global_index < edge_type.size() && (!n || local_counter < *n)) {
      std::ostringstream os;
      DumpEdgeTypeIndex(&os, dba_, edge_type[global_index]);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == edge_type.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateEdgeTypePropertyIndicesPullChunk() {
  // Dump all edge type property indices
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of indices vectors
    if (!indices_info_) {
      indices_info_.emplace(dba_->ListAllIndices());
    }
    const auto &edge_type_property = indices_info_->edge_type_property;

    size_t local_counter = 0;
    while (global_index < edge_type_property.size() && (!n || local_counter < *n)) {
      std::ostringstream os;
      const auto edge_type_property_index = edge_type_property[global_index];
      DumpEdgeTypePropertyIndex(&os, dba_, edge_type_property_index.first, edge_type_property_index.second);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == edge_type_property.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateEdgePropertyIndicesPullChunk() {
  // Dump all global edge property indices
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of indices vectors
    if (!indices_info_) {
      indices_info_.emplace(dba_->ListAllIndices());
    }
    const auto &edge_property = indices_info_->edge_property;

    size_t local_counter = 0;
    while (global_index < edge_property.size() && (!n || local_counter < *n)) {
      std::ostringstream os;
      const auto edge_property_index = edge_property[global_index];
      DumpEdgePropertyIndex(&os, dba_, edge_property_index);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == edge_property.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateLabelPropertiesIndicesPullChunk() {
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of indices vectors
    if (!indices_info_) {
      indices_info_.emplace(dba_->ListAllIndices());
    }
    const auto &label_property = indices_info_->label_properties;

    size_t local_counter = 0;
    while (global_index < label_property.size() && (!n || local_counter < *n)) {
      std::ostringstream os;
      const auto &[label, properties] = label_property[global_index];
      DumpLabelPropertiesIndex(&os, dba_, label, properties);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == label_property.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateTextIndicesPullChunk() {
  // Dump all text indices
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of indices vectors
    if (!indices_info_) {
      indices_info_.emplace(dba_->ListAllIndices());
    }
    const auto &text = indices_info_->text_indices;

    size_t local_counter = 0;
    while (global_index < text.size() && (!n || local_counter < *n)) {
      std::ostringstream os;
      const auto &text_index = text[global_index];
      DumpTextIndex(&os, dba_, text_index.first, text_index.second);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == text.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreatePointIndicesPullChunk() {
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of indices vectors
    if (!indices_info_) {
      indices_info_.emplace(dba_->ListAllIndices());
    }
    const auto &point_label_properties = indices_info_->point_label_property;

    size_t local_counter = 0;
    while (global_index < point_label_properties.size() && (!n || local_counter < *n)) {
      std::ostringstream os;
      const auto &point_index = point_label_properties[global_index];
      DumpPointIndex(&os, dba_, point_index.first, point_index.second);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == point_label_properties.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateVectorIndicesPullChunk() {
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of indices vectors
    if (!indices_info_) {
      indices_info_.emplace(dba_->ListAllIndices());
    }
    const auto &vector = indices_info_->vector_indices_spec;

    size_t local_counter = 0;
    while (global_index < vector.size() && (!n || local_counter < *n)) {
      const auto &index = vector[global_index];
      std::ostringstream os;
      DumpVectorIndex(&os, dba_, index);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == vector.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateExistenceConstraintsPullChunk() {
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of constraint vectors
    if (!constraints_info_) {
      constraints_info_.emplace(dba_->ListAllConstraints());
    }

    const auto &existence = constraints_info_->existence;
    size_t local_counter = 0;
    while (global_index < existence.size() && (!n || local_counter < *n)) {
      const auto &constraint = existence[global_index];
      std::ostringstream os;
      DumpExistenceConstraint(&os, dba_, constraint.first, constraint.second);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == existence.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateUniqueConstraintsPullChunk() {
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of constraint vectors
    if (!constraints_info_) {
      constraints_info_.emplace(dba_->ListAllConstraints());
    }

    const auto &unique = constraints_info_->unique;
    size_t local_counter = 0;
    while (global_index < unique.size() && (!n || local_counter < *n)) {
      const auto &constraint = unique[global_index];
      std::ostringstream os;
      DumpUniqueConstraint(&os, dba_, constraint.first, constraint.second);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == unique.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateTypeConstraintsPullChunk() {
  return [this, global_index = 0U](AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the construction of constraint vectors
    if (!constraints_info_) {
      constraints_info_.emplace(dba_->ListAllConstraints());
    }

    const auto &type = constraints_info_->type;
    size_t local_counter = 0;
    while (global_index < type.size() && (!n || local_counter < *n)) {
      const auto &[label, property, data_type] = type[global_index];
      std::ostringstream os;
      DumpTypeConstraint(&os, dba_, label, property, data_type);
      stream->Result({TypedValue(os.str())});

      ++global_index;
      ++local_counter;
    }

    if (global_index == type.size()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateInternalIndexPullChunk() {
  return [this](AnyStream *stream, std::optional<int>) mutable -> std::optional<size_t> {
    if (vertices_iterable_.begin() != vertices_iterable_.end()) {
      std::ostringstream os;
      os << "CREATE INDEX ON :" << kInternalVertexLabel << "(" << kInternalPropertyId << ");";
      stream->Result({TypedValue(os.str())});
      internal_index_created_ = true;
      return 1;
    }
    return 0;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateVertexPullChunk() {
  return [this, maybe_current_iter = std::optional<VertexAccessorIterableIterator>{}](
             AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the call of begin() function
    // If multiple begins are called before an iteration,
    // one iteration will make the rest of iterators be in undefined
    // states.
    if (!maybe_current_iter) {
      maybe_current_iter.emplace(vertices_iterable_.begin());
    }

    auto &current_iter{*maybe_current_iter};

    size_t local_counter = 0;
    while (current_iter != vertices_iterable_.end() && (!n || local_counter < *n)) {
      std::ostringstream os;
      DumpVertex(&os, dba_, *current_iter);
      stream->Result({TypedValue(os.str())});
      ++local_counter;
      ++current_iter;
    }
    if (current_iter == vertices_iterable_.end()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateEdgePullChunk() {
  return [this, maybe_current_vertex_iter = std::optional<VertexAccessorIterableIterator>{},
          // we need to save the iterable which contains list of accessor so
          // our saved iterator is valid in the next run
          maybe_edge_iterable = std::shared_ptr<EdgeAccessorIterable>{nullptr},
          maybe_current_edge_iter = std::optional<EdgeAccessorIterableIterator>{}](
             AnyStream *stream, std::optional<int> n) mutable -> std::optional<size_t> {
    // Delay the call of begin() function
    // If multiple begins are called before an iteration,
    // one iteration will make the rest of iterators be in undefined
    // states.
    if (!maybe_current_vertex_iter) {
      maybe_current_vertex_iter.emplace(vertices_iterable_.begin());
    }

    auto &current_vertex_iter{*maybe_current_vertex_iter};
    size_t local_counter = 0U;
    for (; current_vertex_iter != vertices_iterable_.end() && (!n || local_counter < *n); ++current_vertex_iter) {
      const auto &vertex = *current_vertex_iter;
      // If we have a saved iterable from a previous pull
      // we need to use the same iterable
      if (!maybe_edge_iterable) {
        maybe_edge_iterable = std::make_shared<EdgeAccessorIterable>(vertex.OutEdges(storage::View::OLD));
      }
      auto &maybe_edges = *maybe_edge_iterable;
      MG_ASSERT(maybe_edges.HasValue(), "Invalid database state!");
      auto current_edge_iter = maybe_current_edge_iter ? *maybe_current_edge_iter : maybe_edges->edges.begin();
      for (; current_edge_iter != maybe_edges->edges.end() && (!n || local_counter < *n); ++current_edge_iter) {
        std::ostringstream os;
        DumpEdge(&os, dba_, *current_edge_iter);
        stream->Result({TypedValue(os.str())});

        ++local_counter;
      }

      if (current_edge_iter != maybe_edges->edges.end()) {
        maybe_current_edge_iter.emplace(current_edge_iter);
        return std::nullopt;
      }

      maybe_current_edge_iter = std::nullopt;
      maybe_edge_iterable = nullptr;
    }

    if (current_vertex_iter == vertices_iterable_.end()) {
      return local_counter;
    }

    return std::nullopt;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateDropInternalIndexPullChunk() {
  return [this](AnyStream *stream, std::optional<int>) {
    if (internal_index_created_) {
      std::ostringstream os;
      os << "DROP INDEX ON :" << kInternalVertexLabel << "(" << kInternalPropertyId << ");";
      stream->Result({TypedValue(os.str())});
      return 1;
    }
    return 0;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateInternalIndexCleanupPullChunk() {
  return [this](AnyStream *stream, std::optional<int>) {
    if (internal_index_created_) {
      std::ostringstream os;
      os << "MATCH (u) REMOVE u:" << kInternalVertexLabel << ", u." << kInternalPropertyId << ";";
      stream->Result({TypedValue(os.str())});
      return 1;
    }
    return 0;
  };
}

PullPlanDump::PullChunk PullPlanDump::CreateTriggersPullChunk() {
  return [this](AnyStream *stream, std::optional<int>) {
    auto triggers = db_acc_->trigger_store()->GetTriggerInfo();
    for (const auto &trigger : triggers) {
      std::ostringstream os;
      auto trigger_statement_copy = trigger.statement;
      std::replace(trigger_statement_copy.begin(), trigger_statement_copy.end(), '\n', ' ');
      os << "CREATE TRIGGER " << trigger.name;
      if (trigger.event_type != TriggerEventType::ANY) {
        os << " ON " << memgraph::query::TriggerEventTypeToString(trigger.event_type);
      }
      os << " " << triggerPhaseToString(trigger.phase) << " " << trigger_statement_copy << ";";
      stream->Result({TypedValue(os.str())});
    }
    return 0;
  };
}

void DumpDatabaseToCypherQueries(query::DbAccessor *dba, AnyStream *stream, dbms::DatabaseAccess db_acc) {
  PullPlanDump(dba, db_acc).Pull(stream, {});
}

}  // namespace memgraph::query
