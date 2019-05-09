#include "database/single_node/dump.hpp"

#include <map>
#include <ostream>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "database/graph_db_accessor.hpp"
#include "utils/algorithm.hpp"
#include "utils/string.hpp"

namespace database {

namespace {

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
                    const PropertyValueStore &store) {
  *os << "{";
  utils::PrintIterable(*os, store, ", ", [&dba](auto &os, const auto &kv) {
    os << dba->PropertyName(kv.first) << ": ";
    DumpPropertyValue(&os, kv.second);
  });
  *os << "}";
}

void DumpVertex(std::ostream *os, GraphDbAccessor *dba,
                const VertexAccessor &vertex) {
  *os << "(n" << vertex.gid();
  for (const auto &label : vertex.labels()) {
    *os << ":" << dba->LabelName(label);
  }
  const auto &props = vertex.Properties();
  if (props.size() > 0) {
    *os << " ";
    DumpProperties(os, dba, props);
  }
  *os << ")";
}

void DumpEdge(std::ostream *os, GraphDbAccessor *dba,
              const EdgeAccessor &edge) {
  *os << "(n" << edge.from().gid() << ")-[";
  *os << ":" << dba->EdgeTypeName(edge.EdgeType());
  const auto &props = edge.Properties();
  if (props.size() > 0) {
    *os << " ";
    DumpProperties(os, dba, props);
  }
  *os << "]->(n" << edge.to().gid() << ")";
}

}  // namespace

DumpGenerator::DumpGenerator(GraphDbAccessor *dba) : dba_(dba), first_(true) {
  CHECK(dba);
  vertices_state_.emplace(dba->Vertices(false));
  edges_state_.emplace(dba->Edges(false));
}

bool DumpGenerator::NextQuery(std::ostream *os) {
  if (vertices_state_->ReachedEnd() && edges_state_->ReachedEnd()) return false;

  if (first_) {
    first_ = false;
    *os << "CREATE ";
  } else {
    *os << ", ";
  }

  if (!vertices_state_->ReachedEnd()) {
    DumpVertex(os, dba_, *vertices_state_->GetCurrentAndAdvance());
  } else if (!edges_state_->ReachedEnd()) {
    DumpEdge(os, dba_, *edges_state_->GetCurrentAndAdvance());
  }

  if (vertices_state_->ReachedEnd() && edges_state_->ReachedEnd()) *os << ";";
  return true;
}

void DumpToCypher(std::ostream *os, GraphDbAccessor *dba) {
  CHECK(os && dba);

  DumpGenerator dump(dba);
  while (dump.NextQuery(os)) continue;
}

}  // namespace database
