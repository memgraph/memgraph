#include <iostream>
#include <string>

#include "query/backend/cpp/typed_value.hpp"
#include "query/parameters.hpp"
#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// General query type: MATCH (g:garment {garment_id: 1234}) SET g:'GENERAL'
// RETURN g

bool run_general_query(GraphDbAccessor &db_accessor, const Parameters &args,
                       Stream &stream, const std::string &general_label) {
  std::vector<std::string> headers{std::string("g")};
  stream.Header(headers);
  for (auto vertex : db_accessor.vertices()) {
    if (vertex.has_label(db_accessor.label("garment"))) {
      TypedValue prop = vertex.PropsAt(db_accessor.property("garment_id"));
      if (prop.type() == TypedValue::Type::Null) continue;
      TypedValue cmp = prop == args.At(0);
      if (cmp.type() != TypedValue::Type::Bool) continue;
      if (cmp.Value<bool>() != true) continue;
      vertex.add_label(db_accessor.label(general_label));
      std::vector<TypedValue> result{TypedValue(vertex)};
      stream.Result(result);
    }
  }
  std::map<std::string, TypedValue> meta{
      std::make_pair(std::string("type"), TypedValue(std::string("rw")))};
  stream.Summary(meta);
  return true;
}
