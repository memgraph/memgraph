#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: CREATE (p:profile {profile_id: 112, partner_id: 55}) RETURN p

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const TypedValueStore<> &args,
           Stream &stream) {
    auto v = db_accessor.insert_vertex();
    v.PropsSet(db_accessor.property("profile_id"), args.at(0));
    v.PropsSet(db_accessor.property("partner_id"), args.at(1));
    v.add_label(db_accessor.label("profile"));
    stream.write_field("p");
    stream.write_vertex_record(v);
    stream.write_meta("rw");
    db_accessor.transaction_.commit();
    return true;
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
