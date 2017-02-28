#include <iostream>
#include <string>

#include "query/backend/cpp/typed_value.hpp"
#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: MATCH (p:profile {profile_id: 111, partner_id:
//  55})-[s:score]-(g:garment
//  {garment_id: 1234}) RETURN s

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const PropertyValueStore<> &args,
           Stream &stream) {
    stream.write_field("s");
    auto profile = [&db_accessor, &args](const VertexAccessor &v) -> bool {
      TypedValue prop = v.PropsAt(db_accessor.property("profile_id"));
      if (prop.type() == TypedValue::Type::Null) return false;
      auto cmp = prop == args.at(0);
      if (cmp.type() != TypedValue::Type::Bool) return false;
      if (cmp.Value<bool>() != true) return false;

      TypedValue prop2 = v.PropsAt(db_accessor.property("partner_id"));
      if (prop2.type() == TypedValue::Type::Null) return false;
      auto cmp2 = prop2 == args.at(1);
      if (cmp2.type() != TypedValue::Type::Bool) return false;
      return cmp2.Value<bool>();
    };
    auto garment = [&db_accessor, &args](const VertexAccessor &v) -> bool {
      TypedValue prop = v.PropsAt(db_accessor.property("garment_id"));
      if (prop.type() == TypedValue::Type::Null) return false;
      auto cmp = prop == args.at(2);
      if (cmp.type() != TypedValue::Type::Bool) return false;
      return cmp.Value<bool>();
    };
    for (auto edge : db_accessor.edges()) {
      auto from = edge.from();
      auto to = edge.to();
      if (edge.edge_type() != db_accessor.edge_type("score")) continue;
      if ((profile(from) && garment(to)) || (profile(to) && garment(from))) {
        stream.write_edge_record(edge);
      }
    }
    stream.write_meta("r");
    db_accessor.transaction_.commit();
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
