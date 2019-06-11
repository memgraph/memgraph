#include "database/single_node/dump.hpp"

#include <iomanip>
#include <limits>
#include <map>
#include <optional>
#include <ostream>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "database/graph_db_accessor.hpp"
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
      *os << (value.Value<bool>() ? "true" : "false");
      return;
    case PropertyValue::Type::String:
      *os << ::utils::Escape(value.Value<std::string>());
      return;
    case PropertyValue::Type::Int:
      *os << value.Value<int64_t>();
      return;
    case PropertyValue::Type::Double:
      DumpPreciseDouble(os, value.Value<double>());
      return;
    case PropertyValue::Type::List: {
      *os << "[";
      const auto &list = value.Value<std::vector<PropertyValue>>();
      utils::PrintIterable(*os, list, ", ", [](auto &os, const auto &item) {
        DumpPropertyValue(&os, item);
      });
      *os << "]";
      return;
    }
    case PropertyValue::Type::Map: {
      *os << "{";
      const auto &map = value.Value<std::map<std::string, PropertyValue>>();
      utils::PrintIterable(*os, map, ", ", [](auto &os, const auto &kv) {
        os << kv.first << ": ";
        DumpPropertyValue(&os, kv.second);
      });
      *os << "}";
      return;
    }
  }
}

void DumpProperties(std::ostream *os, GraphDbAccessor *dba,
                    const PropertyValueStore &store,
                    std::optional<uint64_t> property_id = std::nullopt) {
  *os << "{";
  if (property_id) {
    *os << kInternalPropertyId << ": " << *property_id;
    if (store.size() > 0) *os << ", ";
  }
  utils::PrintIterable(*os, store, ", ", [&dba](auto &os, const auto &kv) {
    os << dba->PropertyName(kv.first) << ": ";
    DumpPropertyValue(&os, kv.second);
  });
  *os << "}";
}

void DumpVertex(std::ostream *os, GraphDbAccessor *dba,
                const VertexAccessor &vertex) {
  *os << "CREATE (";
  *os << ":" << kInternalVertexLabel;
  for (const auto &label : vertex.labels()) {
    *os << ":" << dba->LabelName(label);
  }
  *os << " ";
  DumpProperties(os, dba, vertex.Properties(),
                 std::optional<uint64_t>(vertex.CypherId()));
  *os << ");";
}

void DumpEdge(std::ostream *os, GraphDbAccessor *dba,
              const EdgeAccessor &edge) {
  *os << "MATCH (u), (v)";
  *os << " WHERE ";
  *os << "u." << kInternalPropertyId << " = " << edge.from().CypherId();
  *os << " AND ";
  *os << "v." << kInternalPropertyId << " = " << edge.to().CypherId() << " ";
  *os << "CREATE (u)-[";
  *os << ":" << dba->EdgeTypeName(edge.EdgeType());
  const auto &props = edge.Properties();
  if (props.size() > 0) {
    *os << " ";
    DumpProperties(os, dba, edge.Properties());
  }
  *os << "]->(v);";
}

void DumpIndexKey(std::ostream *os, GraphDbAccessor *dba,
                  const LabelPropertyIndex::Key &key) {
  *os << "CREATE INDEX ON :" << dba->LabelName(key.label_) << "("
      << dba->PropertyName(key.property_) << ");";
}

void DumpUniqueConstraint(
    std::ostream *os, GraphDbAccessor *dba,
    const storage::constraints::ConstraintEntry &constraint) {
  *os << "CREATE CONSTRAINT ON (u:" << dba->LabelName(constraint.label)
      << ") ASSERT ";
  utils::PrintIterable(*os, constraint.properties, ", ",
                       [&dba](auto &os, const auto &property) {
                         os << "u." << dba->PropertyName(property);
                       });
  *os << " IS UNIQUE;";
}

}  // namespace

CypherDumpGenerator::CypherDumpGenerator(GraphDbAccessor *dba)
    : dba_(dba),
      created_internal_index_(false),
      cleaned_internal_index_(false),
      cleaned_internal_label_property_(false) {
  CHECK(dba);
  indices_state_.emplace(dba->GetIndicesKeys());
  unique_constraints_state_.emplace(dba->ListUniqueConstraints());
  vertices_state_.emplace(dba->Vertices(false));
  edges_state_.emplace(dba->Edges(false));
}

bool CypherDumpGenerator::NextQuery(std::ostream *os) {
  if (!indices_state_->ReachedEnd()) {
    DumpIndexKey(os, dba_, *indices_state_->GetCurrentAndAdvance());
    return true;
  } else if (!vertices_state_->Empty() && !created_internal_index_) {
    *os << "CREATE INDEX ON :" << kInternalVertexLabel << "("
        << kInternalPropertyId << ");";
    created_internal_index_ = true;
    return true;
  } else if (!unique_constraints_state_->ReachedEnd()) {
    DumpUniqueConstraint(os, dba_,
                         *unique_constraints_state_->GetCurrentAndAdvance());
    return true;
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
