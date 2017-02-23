#include <iostream>
#include <string>

#include "query/plan_interface.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"
#include "using.hpp"

using std::cout;
using std::endl;

// Query: CREATE (g:garment {garment_id: 1234, garment_category_id:
// 1,reveals:30}) RETURN g

class CPUPlan : public PlanInterface<Stream> {
 public:
  bool run(GraphDbAccessor &db_accessor, const TypedValueStore<> &args,
           Stream &stream) {
    auto v = db_accessor.insert_vertex();
    v.add_label(db_accessor.label("garment"));
    v.PropsSet(db_accessor.property("garment_id"), args.at(0));
    v.PropsSet(db_accessor.property("garment_category_id"), args.at(1));
    v.PropsSet(db_accessor.property("reveals"), args.at(2));
    stream.write_field("g");
    stream.write_vertex_record(v);
    stream.write_meta("rw");
    return db_accessor.transaction_.commit();
  }

  ~CPUPlan() {}
};

extern "C" PlanInterface<Stream> *produce() { return new CPUPlan(); }

extern "C" void destruct(PlanInterface<Stream> *p) { delete p; }
