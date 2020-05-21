#include "query/dump.hpp"

#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "query/exceptions.hpp"
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
