#include "database/single_node/dump.hpp"

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

namespace database {

namespace {

// Property that is used to make a difference among vertices. It is added to
// property set of vertices to match edges and removed after the entire graph
// is built.
const char *kInternalPropertyId = "__mg_id__";

// Label that is attached to each vertex and is used for easier creation of
// index on internal property id.
const char *kInternalVertexLabel = "__mg_vertex__";

void DumpPreciseDouble(std::ostream *os, double value) {
  // A temporary stream is used to keep precision of the original output
  // stream unchanged.
  std::ostringstream temp_oss;
  temp_oss << std::setprecision(std::numeric_limits<double>::max_digits10)
           << value;
  *os << temp_oss.str();
}

void DumpPropertyValue(std::ostream *os, const PropertyValue &value) {
  switch (value.type()) {
    case PropertyValue::Type::Null:
      *os << "Null";
      return;
    case PropertyValue::Type::Bool:
      *os << (value.ValueBool() ? "true" : "false");
      return;
    case PropertyValue::Type::String:
      *os << ::utils::Escape(value.ValueString());
      return;
    case PropertyValue::Type::Int:
      *os << value.ValueInt();
      return;
    case PropertyValue::Type::Double:
      DumpPreciseDouble(os, value.ValueDouble());
      return;
    case PropertyValue::Type::List: {
      *os << "[";
      const auto &list = value.ValueList();
      utils::PrintIterable(*os, list, ", ", [](auto &os, const auto &item) {
        DumpPropertyValue(&os, item);
      });
      *os << "]";
      return;
    }
    case PropertyValue::Type::Map: {
      *os << "{";
      const auto &map = value.ValueMap();
      utils::PrintIterable(*os, map, ", ", [](auto &os, const auto &kv) {
        os << kv.first << ": ";
        DumpPropertyValue(&os, kv.second);
      });
      *os << "}";
      return;
    }
  }
}

void DumpProperties(std::ostream *os, query::DbAccessor *dba,
#ifdef MG_SINGLE_NODE_V2
                    const std::map<storage::Property, PropertyValue> &store,
#else
                    const PropertyValueStore &store,
#endif
                    std::optional<uint64_t> property_id = std::nullopt) {
  *os << "{";
  if (property_id) {
    *os << kInternalPropertyId << ": " << *property_id;
    if (store.size() > 0) *os << ", ";
  }
  utils::PrintIterable(*os, store, ", ", [&dba](auto &os, const auto &kv) {
    os << dba->PropertyToName(kv.first) << ": ";
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
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
        throw query::QueryRuntimeException(
            "Unexpected error when getting labels.");
    }
  }
  for (const auto &label : *maybe_labels) {
    *os << ":" << dba->LabelToName(label);
  }
  *os << " ";
  auto maybe_props = vertex.Properties(storage::View::OLD);
  if (maybe_props.HasError()) {
    switch (maybe_props.GetError()) {
      case storage::Error::DELETED_OBJECT:
        throw query::QueryRuntimeException(
            "Trying to get properties from a deleted object.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
        throw query::QueryRuntimeException(
            "Unexpected error when getting properties.");
    }
  }
  DumpProperties(os, dba, *maybe_props,
                 std::optional<uint64_t>(vertex.CypherId()));
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
  *os << ":" << dba->EdgeTypeToName(edge.EdgeType());
  auto maybe_props = edge.Properties(storage::View::OLD);
  if (maybe_props.HasError()) {
    switch (maybe_props.GetError()) {
      case storage::Error::DELETED_OBJECT:
        throw query::QueryRuntimeException(
            "Trying to get properties from a deleted object.");
      case storage::Error::SERIALIZATION_ERROR:
      case storage::Error::VERTEX_HAS_EDGES:
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

#ifndef MG_SINGLE_NODE_V2
void DumpIndexKey(std::ostream *os, query::DbAccessor *dba,
                  const LabelPropertyIndex::Key &key) {
  *os << "CREATE INDEX ON :" << dba->LabelToName(key.label_) << "("
      << dba->PropertyToName(key.property_) << ");";
}

void DumpUniqueConstraint(
    std::ostream *os, query::DbAccessor *dba,
    const storage::constraints::ConstraintEntry &constraint) {
  *os << "CREATE CONSTRAINT ON (u:" << dba->LabelToName(constraint.label)
      << ") ASSERT ";
  utils::PrintIterable(*os, constraint.properties, ", ",
                       [&dba](auto &os, const auto &property) {
                         os << "u." << dba->PropertyToName(property);
                       });
  *os << " IS UNIQUE;";
}
#endif

}  // namespace

CypherDumpGenerator::CypherDumpGenerator(query::DbAccessor *dba)
    : dba_(dba),
      created_internal_index_(false),
      cleaned_internal_index_(false),
      cleaned_internal_label_property_(false) {
  CHECK(dba);
#ifdef MG_SINGLE_NODE_V2
  throw utils::NotYetImplemented("Dumping indices and constraints");
#else
  indices_state_.emplace(dba->GetIndicesKeys());
  unique_constraints_state_.emplace(dba->ListUniqueConstraints());
#endif
  vertices_state_.emplace(dba->Vertices(storage::View::OLD));
  edges_state_.emplace(dba->Edges(storage::View::OLD));
}

bool CypherDumpGenerator::NextQuery(std::ostream *os) {
#ifdef MG_SINGLE_NODE_V2
  if (!vertices_state_->Empty() && !created_internal_index_) {
#else
  if (!indices_state_->ReachedEnd()) {
    DumpIndexKey(os, dba_, *indices_state_->GetCurrentAndAdvance());
    return true;
  } else if (!vertices_state_->Empty() && !created_internal_index_) {
#endif
    *os << "CREATE INDEX ON :" << kInternalVertexLabel << "("
        << kInternalPropertyId << ");";
    created_internal_index_ = true;
    return true;
#ifndef MG_SINGLE_NODE_V2
  } else if (!unique_constraints_state_->ReachedEnd()) {
    DumpUniqueConstraint(os, dba_,
                         *unique_constraints_state_->GetCurrentAndAdvance());
    return true;
#endif
  } else if (!vertices_state_->ReachedEnd()) {
    DumpVertex(os, dba_, *vertices_state_->GetCurrentAndAdvance());
    return true;
  } else if (!edges_state_->ReachedEnd()) {
    DumpEdge(os, dba_, *edges_state_->GetCurrentAndAdvance());
    return true;
  } else if (!vertices_state_->Empty() && !cleaned_internal_index_) {
    *os << "DROP INDEX ON :" << kInternalVertexLabel << "("
        << kInternalPropertyId << ");";
    cleaned_internal_index_ = true;
    return true;
  } else if (!vertices_state_->Empty() && !cleaned_internal_label_property_) {
    *os << "MATCH (u) REMOVE u:" << kInternalVertexLabel << ", u."
        << kInternalPropertyId << ";";
    cleaned_internal_label_property_ = true;
    return true;
  }

  return false;
}

}  // namespace database
