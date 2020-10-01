#include "query/dump.hpp"

#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "query/db_accessor.hpp"
#include "query/exceptions.hpp"
#include "query/stream.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/storage.hpp"
#include "utils/algorithm.hpp"
#include "utils/string.hpp"

namespace query {

namespace {

// Property that is used to make a difference among vertices. It is added to
// property set of vertices to match edges and removed after the entire graph
// is built.
const char *kInternalPropertyId = "__mg_id__";

// Label that is attached to each vertex and is used for easier creation of
// index on internal property id.
const char *kInternalVertexLabel = "__mg_vertex__";

/// A helper function that escapes label, edge type and property names.
std::string EscapeName(const std::string_view &value) {
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
  temp_oss << std::setprecision(std::numeric_limits<double>::max_digits10)
           << value;
  *os << temp_oss.str();
}

void DumpPropertyValue(std::ostream *os, const storage::PropertyValue &value) {
  switch (value.type()) {
    case storage::PropertyValue::Type::Null:
      *os << "Null";
      return;
    case storage::PropertyValue::Type::Bool:
      *os << (value.ValueBool() ? "true" : "false");
      return;
    case storage::PropertyValue::Type::String:
      *os << ::utils::Escape(value.ValueString());
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
      utils::PrintIterable(*os, list, ", ", [](auto &os, const auto &item) {
        DumpPropertyValue(&os, item);
      });
      *os << "]";
      return;
    }
    case storage::PropertyValue::Type::Map: {
      *os << "{";
      const auto &map = value.ValueMap();
      utils::PrintIterable(*os, map, ", ", [](auto &os, const auto &kv) {
        os << EscapeName(kv.first) << ": ";
        DumpPropertyValue(&os, kv.second);
      });
      *os << "}";
      return;
    }
  }
}

void DumpProperties(
    std::ostream *os, query::DbAccessor *dba,
    const std::map<storage::PropertyId, storage::PropertyValue> &store,
    std::optional<int64_t> property_id = std::nullopt) {
  *os << "{";
  if (property_id) {
    *os << kInternalPropertyId << ": " << *property_id;
    if (store.size() > 0) *os << ", ";
  }
  utils::PrintIterable(*os, store, ", ", [&dba](auto &os, const auto &kv) {
    os << EscapeName(dba->PropertyToName(kv.first)) << ": ";
    DumpPropertyValue(&os, kv.second);
  });
  *os << "}";
}

void DumpVertex(std::ostream *os, query::DbAccessor *dba,
                const query::VertexAccessor &vertex) {
  *os << "CREATE (";
  *os << ":" << kInternalVertexLabel;
  auto maybe_labels = vertex.Labels(storage::View::OLD);
  if (maybe_labels.HasError()) {
    switch (maybe_labels.GetError()) {
      case storage::Error::DELETED_OBJECT:
        throw query::QueryRuntimeException(
            "Trying to get labels from a deleted node.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException(
            "Trying to get labels from a node that doesn't exist.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
        throw query::QueryRuntimeException(
            "Unexpected error when getting labels.");
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
        throw query::QueryRuntimeException(
            "Trying to get properties from a deleted object.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException(
            "Trying to get properties from a node that doesn't exist.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
        throw query::QueryRuntimeException(
            "Unexpected error when getting properties.");
    }
  }
  DumpProperties(os, dba, *maybe_props, vertex.CypherId());
  *os << ");";
}

void DumpEdge(std::ostream *os, query::DbAccessor *dba,
              const query::EdgeAccessor &edge) {
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
        throw query::QueryRuntimeException(
            "Trying to get properties from a deleted object.");
      case storage::Error::NONEXISTENT_OBJECT:
        throw query::QueryRuntimeException(
            "Trying to get properties from an edge that doesn't exist.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
      case storage::Error::PROPERTIES_DISABLED:
        throw query::QueryRuntimeException(
            "Unexpected error when getting properties.");
    }
  }
  if (maybe_props->size() > 0) {
    *os << " ";
    DumpProperties(os, dba, *maybe_props);
  }
  *os << "]->(v);";
}

void DumpLabelIndex(std::ostream *os, query::DbAccessor *dba,
                    const storage::LabelId label) {
  *os << "CREATE INDEX ON :" << EscapeName(dba->LabelToName(label)) << ";";
}

void DumpLabelPropertyIndex(std::ostream *os, query::DbAccessor *dba,
                            storage::LabelId label,
                            storage::PropertyId property) {
  *os << "CREATE INDEX ON :" << EscapeName(dba->LabelToName(label)) << "("
      << EscapeName(dba->PropertyToName(property)) << ");";
}

void DumpExistenceConstraint(std::ostream *os, query::DbAccessor *dba,
                             storage::LabelId label,
                             storage::PropertyId property) {
  *os << "CREATE CONSTRAINT ON (u:" << EscapeName(dba->LabelToName(label))
      << ") ASSERT EXISTS (u." << EscapeName(dba->PropertyToName(property))
      << ");";
}

void DumpUniqueConstraint(std::ostream *os, query::DbAccessor *dba,
                          storage::LabelId label,
                          const std::set<storage::PropertyId> &properties) {
  *os << "CREATE CONSTRAINT ON (u:" << EscapeName(dba->LabelToName(label))
      << ") ASSERT ";
  utils::PrintIterable(
      *os, properties, ", ", [&dba](auto &stream, const auto &property) {
        stream << "u." << EscapeName(dba->PropertyToName(property));
      });
  *os << " IS UNIQUE;";
}

}  // namespace

PullPlanDump::PullPlanDump(DbAccessor *dba)
    : dba_(dba),
      vertices_iterable_(dba->Vertices(storage::View::OLD)),
      pull_functions_{
          [this, global_index = 0U](
              AnyStream *stream,
              std::optional<int> n) mutable -> std::optional<size_t> {
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
          },
          [this, global_index = 0U](
              AnyStream *stream,
              std::optional<int> n) mutable -> std::optional<size_t> {
            if (!indices_info_) {
              indices_info_.emplace(dba_->ListAllIndices());
            }
            const auto &label_property = indices_info_->label_property;

            size_t local_counter = 0;
            while (global_index < label_property.size() &&
                   (!n || local_counter < *n)) {
              std::ostringstream os;
              const auto &label_property_index = label_property[global_index];
              DumpLabelPropertyIndex(&os, dba_, label_property_index.first,
                                     label_property_index.second);
              stream->Result({TypedValue(os.str())});

              ++global_index;
              ++local_counter;
            }

            if (global_index == label_property.size()) {
              return local_counter;
            }

            return std::nullopt;
          },
          [this, global_index = 0U](
              AnyStream *stream,
              std::optional<int> n) mutable -> std::optional<size_t> {
            if (!constraints_info_) {
              constraints_info_.emplace(dba_->ListAllConstraints());
            }

            const auto &existence = constraints_info_->existence;
            size_t local_counter = 0;
            while (global_index < existence.size() &&
                   (!n || local_counter < *n)) {
              const auto &constraint = existence[global_index];
              std::ostringstream os;
              DumpExistenceConstraint(&os, dba_, constraint.first,
                                      constraint.second);
              stream->Result({TypedValue(os.str())});

              ++global_index;
              ++local_counter;
            }

            if (global_index == existence.size()) {
              return local_counter;
            }

            return std::nullopt;
          },
          [this, global_index = 0U](
              AnyStream *stream,
              std::optional<int> n) mutable -> std::optional<size_t> {
            if (!constraints_info_) {
              constraints_info_.emplace(dba_->ListAllConstraints());
            }

            const auto &unique = constraints_info_->unique;
            size_t local_counter = 0;
            while (global_index < unique.size() && (!n || local_counter < *n)) {
              const auto &constraint = unique[global_index];
              std::ostringstream os;
              DumpUniqueConstraint(&os, dba_, constraint.first,
                                   constraint.second);
              stream->Result({TypedValue(os.str())});

              ++global_index;
              ++local_counter;
            }

            if (global_index == unique.size()) {
              return local_counter;
            }

            return std::nullopt;
          },
          [this](AnyStream *stream,
                 std::optional<int>) mutable -> std::optional<size_t> {
            if (vertices_iterable_.begin() != vertices_iterable_.end()) {
              std::ostringstream os;
              os << "CREATE INDEX ON :" << kInternalVertexLabel << "("
                 << kInternalPropertyId << ");";
              stream->Result({TypedValue(os.str())});
              internal_index_created_ = true;
              return 1;
            }
            return 0;
          },
          [this, maybe_current_iter =
                     std::optional<VertexAccessorIterableIterator>{}](
              AnyStream *stream,
              std::optional<int> n) mutable -> std::optional<size_t> {
            if (!maybe_current_iter) {
              maybe_current_iter.emplace(vertices_iterable_.begin());
            }

            auto &current_iter{*maybe_current_iter};

            size_t local_counter = 0;
            while (current_iter != vertices_iterable_.end() &&
                   (!n || local_counter < *n)) {
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
          },
          [this,
           maybe_current_vertex_iter =
               std::optional<VertexAccessorIterableIterator>{},
           maybe_current_edge_iter =
               std::optional<EdgeAcessorIterableIterator>{}](
              AnyStream *stream,
              std::optional<int> n) mutable -> std::optional<size_t> {
            if (!maybe_current_vertex_iter) {
              maybe_current_vertex_iter.emplace(vertices_iterable_.begin());
            }

            auto &current_vertex_iter{*maybe_current_vertex_iter};
            size_t local_counter = 0U;
            for (; current_vertex_iter != vertices_iterable_.end() &&
                   (!n || local_counter < *n);
                 ++current_vertex_iter) {
              const auto &vertex = *current_vertex_iter;
              auto maybe_edges = vertex.OutEdges(storage::View::OLD);
              CHECK(maybe_edges.HasValue()) << "Invalid database state!";
              auto current_edge_iter = maybe_current_edge_iter
                                           ? *maybe_current_edge_iter
                                           : maybe_edges->begin();
              for (; current_edge_iter != maybe_edges->end() &&
                     (!n || local_counter < *n);
                   ++current_edge_iter) {
                std::ostringstream os;
                DumpEdge(&os, dba_, *current_edge_iter);
                stream->Result({TypedValue(os.str())});

                ++local_counter;
              }

              if (current_edge_iter != maybe_edges->end()) {
                maybe_current_edge_iter.emplace(current_edge_iter);
                return std::nullopt;
              } else {
                maybe_current_edge_iter = std::nullopt;
              }
            }

            if (current_vertex_iter == vertices_iterable_.end()) {
              return local_counter;
            }

            return std::nullopt;
          },
          [this](AnyStream *stream, std::optional<int>) {
            if (internal_index_created_) {
              std::ostringstream os;
              os << "DROP INDEX ON :" << kInternalVertexLabel << "("
                 << kInternalPropertyId << ");";
              stream->Result({TypedValue(os.str())});
              return 1;
            }
            return 0;
          },
          [this](AnyStream *stream, std::optional<int>) {
            if (internal_index_created_) {
              std::ostringstream os;
              os << "MATCH (u) REMOVE u:" << kInternalVertexLabel << ", u."
                 << kInternalPropertyId << ";";
              stream->Result({TypedValue(os.str())});
              return 1;
            }
            return 0;
          }} {}

bool PullPlanDump::pull(AnyStream *stream, std::optional<int> n) {
  // Iterate all functions that stream some results.
  // Each function should return number of results it streamed after it
  // finishes. If the function did not finish streaming all the results,
  // std::nullopt should be returned.
  while (current_function_index_ < pull_functions_.size() && (!n || *n > 0)) {
    const auto maybe_streamed_count =
        pull_functions_[current_function_index_](stream, n);

    if (!maybe_streamed_count) {
      return false;
    }

    if (n) {
      *n -= *maybe_streamed_count;
    }

    ++current_function_index_;
  }
  return current_function_index_ == pull_functions_.size();
}

void DumpDatabaseToCypherQueries(query::DbAccessor *dba, AnyStream *stream) {
  {
    auto info = dba->ListAllIndices();
    for (const auto &item : info.label) {
      std::ostringstream os;
      DumpLabelIndex(&os, dba, item);
      stream->Result({TypedValue(os.str())});
    }
    for (const auto &item : info.label_property) {
      std::ostringstream os;
      DumpLabelPropertyIndex(&os, dba, item.first, item.second);
      stream->Result({TypedValue(os.str())});
    }
  }
  {
    auto info = dba->ListAllConstraints();
    for (const auto &item : info.existence) {
      std::ostringstream os;
      DumpExistenceConstraint(&os, dba, item.first, item.second);
      stream->Result({TypedValue(os.str())});
    }
    for (const auto &item : info.unique) {
      std::ostringstream os;
      DumpUniqueConstraint(&os, dba, item.first, item.second);
      stream->Result({TypedValue(os.str())});
    }
  }

  auto vertices = dba->Vertices(storage::View::OLD);
  bool internal_index_created = false;
  if (vertices.begin() != vertices.end()) {
    std::ostringstream os;
    os << "CREATE INDEX ON :" << kInternalVertexLabel << "("
       << kInternalPropertyId << ");";
    stream->Result({TypedValue(os.str())});
    internal_index_created = true;
  }
  for (const auto &vertex : vertices) {
    std::ostringstream os;
    DumpVertex(&os, dba, vertex);
    stream->Result({TypedValue(os.str())});
  }
  for (const auto &vertex : vertices) {
    auto maybe_edges = vertex.OutEdges(storage::View::OLD);
    CHECK(maybe_edges.HasValue()) << "Invalid database state!";
    for (const auto &edge : *maybe_edges) {
      std::ostringstream os;
      DumpEdge(&os, dba, edge);
      stream->Result({TypedValue(os.str())});
    }
  }
  if (internal_index_created) {
    {
      std::ostringstream os;
      os << "DROP INDEX ON :" << kInternalVertexLabel << "("
         << kInternalPropertyId << ");";
      stream->Result({TypedValue(os.str())});
    }
    {
      std::ostringstream os;
      os << "MATCH (u) REMOVE u:" << kInternalVertexLabel << ", u."
         << kInternalPropertyId << ";";
      stream->Result({TypedValue(os.str())});
    }
  }
}

}  // namespace query
