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

// Query: CREATE (p:profile {profile_id: 112, partner_id: 55, conceals: 10})
// RETURN p

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const Parameters &args,
           Stream &stream) {
    auto v = db_accessor.insert_vertex();
    v.PropsSet(db_accessor.property("profile_id"), args.At(0).second);
    v.PropsSet(db_accessor.property("partner_id"), args.At(1).second);
    v.PropsSet(db_accessor.property("conceals"), args.At(2).second);
    v.add_label(db_accessor.label("profile"));
    std::vector<std::string> headers{std::string("p")};
    stream.Header(headers);
    std::vector<TypedValue> result{TypedValue(v)};
    stream.Result(result);
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
