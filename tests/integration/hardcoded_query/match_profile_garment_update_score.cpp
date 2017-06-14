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

// Query: MATCH (p:profile {profile_id: 111, partner_id:
//  55})-[s:score]-(g:garment
//  {garment_id: 1234}) SET s.score = 3137 RETURN s

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    std::vector<std::string> headers{std::string("s")};
    stream.Header(headers);
    auto profile = [&db_accessor, &args](const VertexAccessor &v) -> bool {
      TypedValue prop = v.PropsAt(db_accessor.property("profile_id"));
      if (prop.type() == TypedValue::Type::Null) return false;
      auto cmp = prop == args.At(0).second;
      if (cmp.type() != TypedValue::Type::Bool) return false;
      if (cmp.Value<bool>() != true) return false;

      TypedValue prop2 = v.PropsAt(db_accessor.property("partner_id"));
      if (prop2.type() == TypedValue::Type::Null) return false;
      auto cmp2 = prop2 == args.At(1).second;
      if (cmp2.type() != TypedValue::Type::Bool) return false;
      return cmp2.Value<bool>();
    };
    auto garment = [&db_accessor, &args](const VertexAccessor &v) -> bool {
      TypedValue prop = v.PropsAt(db_accessor.property("garment_id"));
      if (prop.type() == TypedValue::Type::Null) return false;
      auto cmp = prop == args.At(2).second;
      if (cmp.type() != TypedValue::Type::Bool) return false;
      return cmp.Value<bool>();
    };
    for (auto edge : db_accessor.edges(false)) {
      auto from = edge.from();
      auto to = edge.to();
      if (edge.edge_type() != db_accessor.edge_type("score")) continue;
      if ((profile(from) && garment(to)) || (profile(to) && garment(from))) {
        edge.PropsSet(db_accessor.property("score"), args.At(3).second);
        edge.SwitchNew();
        std::vector<TypedValue> result{TypedValue(edge)};
        stream.Result(result);
      }
    }
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
