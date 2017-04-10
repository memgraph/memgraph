#include <iostream>
#include <string>

#include "query/parameters.hpp"
#include "query/plan_interface.hpp"
#include "query/typed_value.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;
using query::TypedValue;

// Query: MATCH (g:garment {garment_id: 1234}) RETURN g

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    std::vector<std::string> headers{std::string("g")};
    stream.Header(headers);
    for (auto vertex : db_accessor.vertices()) {
      if (vertex.has_label(db_accessor.label("garment"))) {
        TypedValue prop = vertex.PropsAt(db_accessor.property("garment_id"));
        if (prop.type() == TypedValue::Type::Null) continue;
        auto cmp = prop == args.At(0);
        if (cmp.type() != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;
        std::vector<TypedValue> result{TypedValue(vertex)};
        stream.Result(result);
      }
    }
    std::map<std::string, TypedValue> meta{
        std::make_pair(std::string("type"), TypedValue(std::string("r")))};
    stream.Summary(meta);
    return true;
  }
  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
