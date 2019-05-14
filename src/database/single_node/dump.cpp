#include "database/single_node/dump.hpp"

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
// TODO(tsabolcec): We should create index for that property for faster
// matching.
const char *kInternalPropertyId = "__mg_id__";

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
      // TODO(tsabolcec): By default, this will output only 6 significant digits
      // of the number. We should increase that number to avoid precision loss.
      *os << value.Value<double>();
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
  for (const auto &label : vertex.labels()) {
    *os << ":" << dba->LabelName(label);
  }
  if (!vertex.labels().empty()) *os << " ";
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

void DumpInternalIndexCleanup(std::ostream *os) {
  // TODO(tsabolcec): Don't forget to drop the index by internal id.
  *os << "MATCH (u) REMOVE u." << kInternalPropertyId << ";";
}

}  // namespace

CypherDumpGenerator::CypherDumpGenerator(GraphDbAccessor *dba)
    : dba_(dba), cleaned_internals_(false) {
  CHECK(dba);
  vertices_state_.emplace(dba->Vertices(false));
  edges_state_.emplace(dba->Edges(false));
}

bool CypherDumpGenerator::NextQuery(std::ostream *os) {
  if (!vertices_state_->ReachedEnd()) {
    DumpVertex(os, dba_, *vertices_state_->GetCurrentAndAdvance());
    return true;
  } else if (!edges_state_->ReachedEnd()) {
    DumpEdge(os, dba_, *edges_state_->GetCurrentAndAdvance());
    return true;
  } else if (!vertices_state_->Empty() && !cleaned_internals_) {
    DumpInternalIndexCleanup(os);
    cleaned_internals_ = true;
    return true;
  }

  return false;
}

void DumpToCypher(std::ostream *os, GraphDbAccessor *dba) {
  CHECK(os && dba);

  CypherDumpGenerator dump(dba);
  while (dump.NextQuery(os)) continue;
}

}  // namespace database
