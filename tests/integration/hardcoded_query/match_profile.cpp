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

// Query: MATCH (p:profile {profile_id: 112, partner_id: 77}) RETURN p

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    std::vector<std::string> headers{std::string("p")};
    stream.Header(headers);
    for (auto vertex : db_accessor.vertices()) {
      if (vertex.has_label(db_accessor.label("profile"))) {
        TypedValue prop = vertex.PropsAt(db_accessor.property("profile_id"));
        if (prop.type() == TypedValue::Type::Null) continue;
        auto cmp = prop == args.At(0);
        if (cmp.type() != TypedValue::Type::Bool) continue;
        if (cmp.Value<bool>() != true) continue;

        TypedValue prop2 = vertex.PropsAt(db_accessor.property("partner_id"));
        if (prop2.type() == TypedValue::Type::Null) continue;
        auto cmp2 = prop2 == args.At(1);
        if (cmp2.type() != TypedValue::Type::Bool) continue;
        if (cmp2.Value<bool>() != true) continue;
        std::vector<TypedValue> result{TypedValue(vertex)};
        stream.Result(result);
      }
    }
    return true;
  }
  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
